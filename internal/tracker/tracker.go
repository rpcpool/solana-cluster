// Copyright 2022 Blockdaemon Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package tracker contains the Solana cluster tracker logic.
package tracker

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gin-gonic/gin"
	"go.blockdaemon.com/solana/cluster-manager/internal/index"
	"go.blockdaemon.com/solana/cluster-manager/internal/ledger"
	"go.blockdaemon.com/solana/cluster-manager/types"
)

// Handler implements the tracker API methods.
type Handler struct {
	DB                     *index.DB
	RPC                    *rpc.Client
	MaxSnapshotAge         uint64
	ProxySnapshotDownloads bool
	ProxySnapshotCacheDir  string
	HTTPClient             *http.Client
	cacheMu                sync.Mutex
}

// NewHandler creates a new tracker API using the provided database.
func NewHandler(db *index.DB, rpcURL string, maxSnapshotAge uint64) *Handler {
	return &Handler{
		DB:             db,
		RPC:            rpc.New(rpcURL),
		MaxSnapshotAge: maxSnapshotAge,
		HTTPClient:     http.DefaultClient,
	}
}

// RegisterHandlers registers this API with Gin web framework.
func (h *Handler) RegisterHandlers(group gin.IRoutes) {
	group.GET("/snapshots", h.GetSnapshots)
	group.GET("/best_snapshots", h.GetBestSnapshots)
	group.HEAD("/snapshot.tar.bz2", h.RedirectBestSnapshot)
	group.GET("/snapshot.tar.bz2", h.RedirectBestSnapshot)
	group.HEAD("/snapshot.tar.zst", h.RedirectBestSnapshot)
	group.GET("/snapshot.tar.zst", h.RedirectBestSnapshot)
	group.HEAD("/incremental-snapshot.tar.bz2", h.RedirectBestIncrementalSnapshot)
	group.GET("/incremental-snapshot.tar.bz2", h.RedirectBestIncrementalSnapshot)
	group.HEAD("/incremental-snapshot.tar.zst", h.RedirectBestIncrementalSnapshot)
	group.GET("/incremental-snapshot.tar.zst", h.RedirectBestIncrementalSnapshot)
	group.GET("/health", h.Health)
	group.HEAD("/:name", h.DownloadSnapshot)
	group.GET("/:name", h.DownloadSnapshot)
}

func (h *Handler) createJson(c *gin.Context, entries []*index.SnapshotEntry) {
	sources := make([]types.SnapshotSource, len(entries))
	for i, entry := range entries {
		sources[i] = h.snapshotSource(c, entry)
	}
	c.JSON(http.StatusOK, sources)
}

func (h *Handler) snapshotSource(c *gin.Context, entry *index.SnapshotEntry) types.SnapshotSource {
	info := *entry.Info
	info.Files = make([]*types.SnapshotFile, len(entry.Info.Files))
	for i, file := range entry.Info.Files {
		fileCopy := *file
		if h.ProxySnapshotDownloads {
			fileCopy.FileName = concreteSnapshotURL(c.Request, entry.Group, file.FileName)
		}
		info.Files[i] = &fileCopy
	}

	target := entry.Target
	if h.ProxySnapshotDownloads {
		target = trackerBaseURL(c.Request)
	}

	return types.SnapshotSource{
		SnapshotInfo: info,
		Target:       target,
		Group:        entry.Group,
		UpdatedAt:    entry.UpdatedAt,
	}
}

func (h *Handler) GetSnapshots(c *gin.Context) {
	var query struct {
		Slot  uint64 `form:"slot"`
		Group string `form:"group"`
	}
	if err := c.BindQuery(&query); err != nil {
		return
	}

	var entries []*index.SnapshotEntry
	if query.Group == "" {
		if query.Slot == 0 {
			entries = h.DB.GetAllSnapshots()
		} else {
			entries = h.DB.GetSnapshotsAtSlot(query.Slot)
		}
	} else {
		if query.Slot == 0 {
			entries = h.DB.GetAllSnapshotsByGroup(query.Group)
		} else {
			entries = h.DB.GetSnapshotsAtSlotByGroup(query.Group, query.Slot)
		}
	}

	h.createJson(c, entries)
}

// GetBestSnapshots returns the currently available best snapshots.
func (h *Handler) GetBestSnapshots(c *gin.Context) {
	var query struct {
		Max   int    `form:"max"`
		Group string `form:"group"`
	}
	if err := c.BindQuery(&query); err != nil {
		return
	}
	const maxItems = 25
	if query.Max < 0 || query.Max > maxItems {
		query.Max = maxItems
	}
	entries := h.DB.GetBestSnapshotsByGroup(query.Group, query.Max)
	h.createJson(c, entries)
}

// RedirectBestSnapshot redirects to the best available full snapshot file.
func (h *Handler) RedirectBestSnapshot(c *gin.Context) {
	h.serveBestSnapshot(c, true)
}

// RedirectBestIncrementalSnapshot redirects to the best available incremental snapshot file.
func (h *Handler) RedirectBestIncrementalSnapshot(c *gin.Context) {
	h.serveBestSnapshot(c, false)
}

func (h *Handler) serveBestSnapshot(c *gin.Context, full bool) {
	var query struct {
		Group string `form:"group"`
	}
	if err := c.BindQuery(&query); err != nil {
		return
	}

	var bestEntry *index.SnapshotEntry
	var bestFile *types.SnapshotFile
	for _, entry := range h.DB.GetBestSnapshotsByGroup(query.Group, -1) {
		for _, file := range entry.Info.Files {
			if file.IsFull() != full {
				continue
			}
			if bestFile == nil || file.Compare(bestFile) > 0 {
				bestEntry = entry
				bestFile = file
			}
		}
	}
	if bestFile == nil {
		c.String(http.StatusAccepted, "no snapshot available")
		return
	}

	location := snapshotFileLocation(bestEntry.Target, bestFile.FileName)
	if h.ProxySnapshotDownloads {
		location = concreteSnapshotLocation(c.Request.URL.Path, bestFile.FileName, query.Group)
	}
	c.Redirect(http.StatusSeeOther, location)
}

// DownloadSnapshot proxies or redirects a concrete snapshot filename.
func (h *Handler) DownloadSnapshot(c *gin.Context) {
	name := c.Param("name")
	if ledger.ParseSnapshotFileName(name) == nil {
		c.String(http.StatusNotFound, "snapshot not found")
		return
	}

	var query struct {
		Group string `form:"group"`
	}
	if err := c.BindQuery(&query); err != nil {
		return
	}

	entry, file := h.findSnapshotFile(query.Group, name)
	if file == nil {
		c.String(http.StatusNotFound, "snapshot not found")
		return
	}

	location := snapshotFileLocation(entry.Target, file.FileName)
	if !h.ProxySnapshotDownloads {
		c.Redirect(http.StatusSeeOther, location)
		return
	}
	if h.canUseSnapshotCache(query.Group, file) {
		if err := h.serveCachedOrProxySnapshot(c, query.Group, file, location); err != nil {
			c.String(http.StatusBadGateway, "proxy snapshot download: %s", err)
		}
		return
	}
	if err := h.proxySnapshot(c, location); err != nil {
		c.String(http.StatusBadGateway, "proxy snapshot download: %s", err)
	}
}

func (h *Handler) findSnapshotFile(group string, name string) (*index.SnapshotEntry, *types.SnapshotFile) {
	for _, entry := range h.DB.GetBestSnapshotsByGroup(group, -1) {
		for _, file := range entry.Info.Files {
			if snapshotFileBase(file.FileName) == name {
				return entry, file
			}
		}
	}
	return nil, nil
}

func snapshotFileLocation(target string, fileName string) string {
	fileURL, err := url.Parse(fileName)
	if err == nil && fileURL.IsAbs() {
		return fileURL.String()
	}

	targetURL, err := url.Parse(target)
	if err != nil || !targetURL.IsAbs() {
		return "/" + fileName
	}
	targetURL.Path = path.Join(targetURL.Path, "v1", fileName)
	targetURL.RawPath = ""
	targetURL.RawQuery = ""
	targetURL.Fragment = ""
	return targetURL.String()
}

func concreteSnapshotLocation(requestPath string, fileName string, group string) string {
	location := path.Join(path.Dir(requestPath), snapshotFileBase(fileName))
	if group != "" {
		values := url.Values{}
		values.Set("group", group)
		location += "?" + values.Encode()
	}
	return location
}

func concreteSnapshotURL(req *http.Request, group string, fileName string) string {
	return trackerBaseURL(req) + concreteSnapshotLocation(req.URL.Path, fileName, group)
}

func trackerBaseURL(req *http.Request) string {
	scheme := req.Header.Get("X-Forwarded-Proto")
	if scheme == "" {
		if req.TLS != nil {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	host := req.Header.Get("X-Forwarded-Host")
	if host == "" {
		host = req.Host
	}
	return scheme + "://" + host
}

func snapshotFileBase(fileName string) string {
	fileURL, err := url.Parse(fileName)
	if err == nil && fileURL.Path != "" {
		return path.Base(fileURL.Path)
	}
	return filepath.Base(fileName)
}

func (h *Handler) canUseSnapshotCache(group string, file *types.SnapshotFile) bool {
	if h.ProxySnapshotCacheDir == "" {
		return false
	}
	best := h.bestSnapshotFile(group, file.IsFull())
	if best == nil {
		return false
	}
	return snapshotFileBase(best.FileName) == snapshotFileBase(file.FileName)
}

func (h *Handler) bestSnapshotFile(group string, full bool) *types.SnapshotFile {
	var best *types.SnapshotFile
	for _, entry := range h.DB.GetBestSnapshotsByGroup(group, -1) {
		for _, file := range entry.Info.Files {
			if file.IsFull() != full {
				continue
			}
			if best == nil || file.Compare(best) > 0 {
				best = file
			}
		}
	}
	return best
}

func (h *Handler) serveCachedOrProxySnapshot(c *gin.Context, group string, file *types.SnapshotFile, location string) error {
	name := snapshotFileBase(file.FileName)
	if h.cachedSnapshotMatches(name) {
		return h.serveCachedSnapshot(c, name)
	}

	if c.Request.Method == http.MethodHead || c.Request.Header.Get("Range") != "" || !h.cacheMu.TryLock() {
		return h.proxySnapshot(c, location)
	}
	defer h.cacheMu.Unlock()

	if !h.canUseSnapshotCache(group, file) {
		return h.proxySnapshot(c, location)
	}
	if h.cachedSnapshotMatches(name) {
		return h.serveCachedSnapshot(c, name)
	}
	if err := h.prepareSnapshotCache(name); err != nil {
		return err
	}
	return h.proxyAndCacheSnapshot(c, location, name)
}

func (h *Handler) cachedSnapshotMatches(name string) bool {
	_, err := os.Stat(filepath.Join(h.ProxySnapshotCacheDir, name))
	return err == nil
}

func (h *Handler) serveCachedSnapshot(c *gin.Context, name string) error {
	filePath := filepath.Join(h.ProxySnapshotCacheDir, name)
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	http.ServeContent(c.Writer, c.Request, name, stat.ModTime(), file)
	return nil
}

func (h *Handler) prepareSnapshotCache(name string) error {
	if err := os.MkdirAll(h.ProxySnapshotCacheDir, 0755); err != nil {
		return err
	}
	entries, err := os.ReadDir(h.ProxySnapshotCacheDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() == name {
			continue
		}
		if ledger.ParseSnapshotFileName(entry.Name()) == nil && !strings.HasPrefix(entry.Name(), ".tmp-") {
			continue
		}
		if err := os.RemoveAll(filepath.Join(h.ProxySnapshotCacheDir, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) proxyAndCacheSnapshot(c *gin.Context, location string, name string) error {
	req, err := http.NewRequestWithContext(c.Request.Context(), c.Request.Method, location, nil)
	if err != nil {
		return err
	}
	copyProxyHeaders(req.Header, c.Request.Header)

	client := h.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	tmpFile, err := os.CreateTemp(h.ProxySnapshotCacheDir, ".tmp-"+name+".")
	if err != nil {
		copyProxyHeaders(c.Writer.Header(), res.Header)
		c.Writer.WriteHeader(res.StatusCode)
		_, _ = io.Copy(c.Writer, res.Body)
		return nil
	}
	tmpName := tmpFile.Name()
	defer func() {
		_ = os.Remove(tmpName)
	}()

	copyProxyHeaders(c.Writer.Header(), res.Header)
	c.Writer.WriteHeader(res.StatusCode)

	_, copyErr := io.Copy(c.Writer, io.TeeReader(res.Body, tmpFile))
	closeErr := tmpFile.Close()
	if copyErr != nil {
		return fmt.Errorf("copy response body: %w", copyErr)
	}
	if closeErr != nil {
		return closeErr
	}
	if res.StatusCode != http.StatusOK {
		return nil
	}
	return os.Rename(tmpName, filepath.Join(h.ProxySnapshotCacheDir, name))
}

func (h *Handler) proxySnapshot(c *gin.Context, location string) error {
	req, err := http.NewRequestWithContext(c.Request.Context(), c.Request.Method, location, nil)
	if err != nil {
		return err
	}
	copyProxyHeaders(req.Header, c.Request.Header)

	client := h.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	copyProxyHeaders(c.Writer.Header(), res.Header)
	c.Writer.WriteHeader(res.StatusCode)
	if c.Request.Method == http.MethodHead {
		return nil
	}
	if _, err := io.Copy(c.Writer, res.Body); err != nil {
		return fmt.Errorf("copy response body: %w", err)
	}
	return nil
}

func copyProxyHeaders(dst http.Header, src http.Header) {
	for key, values := range src {
		if isHopByHopHeader(key) {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func isHopByHopHeader(key string) bool {
	switch http.CanonicalHeaderKey(key) {
	case "Connection",
		"Keep-Alive",
		"Proxy-Authenticate",
		"Proxy-Authorization",
		"Te",
		"Trailer",
		"Transfer-Encoding",
		"Upgrade":
		return true
	default:
		return false
	}
}

func (h *Handler) Health(c *gin.Context) {
	var query struct {
		Max   int    `form:"max"`
		Group string `form:"group"`
	}
	if err := c.BindQuery(&query); err != nil {
		return
	}
	query.Max = 1
	entries := h.DB.GetBestSnapshotsByGroup(query.Group, query.Max)

	var health struct {
		MaxSnapshot uint64
		CurrentSlot uint64
		Health      string
	}

	if len(entries) <= 0 {
		health.Health = "no snapshots found"
		c.JSON(http.StatusInternalServerError, health)
	} else {
		health.MaxSnapshot = entries[0].Info.Slot

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()
		out, err := h.RPC.GetSlot(
			ctx,
			rpc.CommitmentFinalized,
		)
		if err != nil {
			health.Health = "rpc unhealthy"
			c.JSON(http.StatusBadGateway, health)
			return
		}
		health.CurrentSlot = out

		group := query.Group
		if group == "" {
			group = entries[0].Group
		}
		if health.CurrentSlot >= health.MaxSnapshot {
			snapshotAge.WithLabelValues(group).Set(float64(health.CurrentSlot - health.MaxSnapshot))
		}

		if (health.CurrentSlot - health.MaxSnapshot) > h.MaxSnapshotAge {
			health.Health = "snapshot too old"
			c.JSON(http.StatusServiceUnavailable, health)
			return
		} else {
			health.Health = "healthy"
			c.JSON(http.StatusOK, health)
			return
		}
	}
}
