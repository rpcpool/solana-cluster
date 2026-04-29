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

package tracker

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.blockdaemon.com/solana/cluster-manager/internal/index"
	"go.blockdaemon.com/solana/cluster-manager/types"
)

func TestHandler_RedirectsBestSnapshots(t *testing.T) {
	const hash = "AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr"

	db := index.NewDB()
	db.UpsertSnapshots(
		snapshotEntry("mainnet", "http://sidecar-1:13080", 200, 200, []*types.SnapshotFile{
			snapshotFile("snapshot-200-"+hash+".tar.zst", 200, 0),
		}),
		snapshotEntry("mainnet", "http://sidecar-2:13080", 300, 200, []*types.SnapshotFile{
			snapshotFile("incremental-snapshot-200-300-"+hash+".tar.zst", 300, 200),
			snapshotFile("snapshot-200-"+hash+".tar.zst", 200, 0),
		}),
		snapshotEntry("devnet", "http://sidecar-3:13080", 400, 400, []*types.SnapshotFile{
			snapshotFile("snapshot-400-"+hash+".tar.zst", 400, 0),
		}),
	)

	router := newTrackerRouter(db)

	req, err := http.NewRequest(http.MethodHead, "/v1/snapshot.tar.bz2?group=mainnet", nil)
	require.NoError(t, err)
	res := httptest.NewRecorder()
	router.ServeHTTP(res, req)
	assert.Equal(t, http.StatusSeeOther, res.Code)
	assert.Equal(t, "http://sidecar-2:13080/v1/snapshot-200-"+hash+".tar.zst", res.Header().Get("Location"))

	req, err = http.NewRequest(http.MethodHead, "/v1/incremental-snapshot.tar.zst?group=mainnet", nil)
	require.NoError(t, err)
	res = httptest.NewRecorder()
	router.ServeHTTP(res, req)
	assert.Equal(t, http.StatusSeeOther, res.Code)
	assert.Equal(t, "http://sidecar-2:13080/v1/incremental-snapshot-200-300-"+hash+".tar.zst", res.Header().Get("Location"))
}

func TestHandler_RedirectPreservesHTTPFileLocations(t *testing.T) {
	const hash = "AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr"
	const fileURL = "https://snapshots.example.com/snapshot-500-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.zst"

	db := index.NewDB()
	db.UpsertSnapshots(snapshotEntry("mainnet", "https://snapshots.example.com", 500, 500, []*types.SnapshotFile{
		snapshotFile(fileURL, 500, 0),
	}))

	req, err := http.NewRequest(http.MethodHead, "/v1/snapshot.tar.zst?group=mainnet", nil)
	require.NoError(t, err)
	res := httptest.NewRecorder()
	newTrackerRouter(db).ServeHTTP(res, req)

	assert.Equal(t, http.StatusSeeOther, res.Code)
	assert.Equal(t, fileURL, res.Header().Get("Location"))
}

func TestHandler_ProxiesSnapshotDownloads(t *testing.T) {
	const hash = "AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr"
	const fileName = "/snapshot-500-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.zst"
	const body = "snapshot-data"

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, fileName, r.URL.Path)
		w.Header().Set("Content-Length", "13")
		w.Header().Set("Content-Type", "application/zstd")
		w.Header().Set("X-Upstream", "yes")
		if r.Method == http.MethodHead {
			return
		}
		_, err := w.Write([]byte(body))
		require.NoError(t, err)
	}))
	defer upstream.Close()

	db := index.NewDB()
	db.UpsertSnapshots(snapshotEntry("mainnet", upstream.URL, 500, 500, []*types.SnapshotFile{
		snapshotFile(upstream.URL+fileName, 500, 0),
	}))

	handler := NewHandler(db, "http://localhost:8899", 1000)
	handler.ProxySnapshotDownloads = true
	handler.HTTPClient = upstream.Client()
	router := newTrackerRouterWithHandler(handler)

	req, err := http.NewRequest(http.MethodGet, "/v1/snapshot.tar.zst?group=mainnet", nil)
	require.NoError(t, err)
	res := httptest.NewRecorder()
	router.ServeHTTP(res, req)

	assert.Equal(t, http.StatusSeeOther, res.Code)
	assert.Equal(t, "/v1"+fileName+"?group=mainnet", res.Header().Get("Location"))

	req, err = http.NewRequest(http.MethodGet, "/v1"+fileName+"?group=mainnet", nil)
	require.NoError(t, err)
	res = httptest.NewRecorder()
	router.ServeHTTP(res, req)

	assert.Equal(t, http.StatusOK, res.Code)
	assert.Empty(t, res.Header().Get("Location"))
	assert.Equal(t, "application/zstd", res.Header().Get("Content-Type"))
	assert.Equal(t, "yes", res.Header().Get("X-Upstream"))
	assert.Equal(t, body, res.Body.String())

	req, err = http.NewRequest(http.MethodHead, "/v1"+fileName+"?group=mainnet", nil)
	require.NoError(t, err)
	res = httptest.NewRecorder()
	router.ServeHTTP(res, req)

	assert.Equal(t, http.StatusOK, res.Code)
	assert.Equal(t, "13", res.Header().Get("Content-Length"))
	assert.Equal(t, "application/zstd", res.Header().Get("Content-Type"))
	assert.Equal(t, "yes", res.Header().Get("X-Upstream"))
	assert.Empty(t, res.Body.String())
}

func TestHandler_ProxySnapshotDownloadsRewritesSnapshotAPIs(t *testing.T) {
	const hash = "AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr"

	db := index.NewDB()
	db.UpsertSnapshots(snapshotEntry("mainnet", "https://snapshots.example.com", 500, 500, []*types.SnapshotFile{
		snapshotFile("https://snapshots.example.com/snapshot-500-"+hash+".tar.zst", 500, 0),
	}))

	handler := NewHandler(db, "http://localhost:8899", 1000)
	handler.ProxySnapshotDownloads = true
	router := newTrackerRouterWithHandler(handler)

	for _, path := range []string{
		"/v1/best_snapshots?group=mainnet&max=1",
		"/v1/snapshots?group=mainnet",
	} {
		req, err := http.NewRequest(http.MethodGet, path, nil)
		require.NoError(t, err)
		req.Host = "tracker.example.com"
		req.Header.Set("X-Forwarded-Proto", "https")

		res := httptest.NewRecorder()
		router.ServeHTTP(res, req)

		require.Equal(t, http.StatusOK, res.Code)
		var sources []types.SnapshotSource
		require.NoError(t, json.Unmarshal(res.Body.Bytes(), &sources))
		require.Len(t, sources, 1)
		assert.Equal(t, "https://tracker.example.com", sources[0].Target)
		require.Len(t, sources[0].Files, 1)
		assert.Equal(t, "https://tracker.example.com/v1/snapshot-500-"+hash+".tar.zst?group=mainnet", sources[0].Files[0].FileName)
	}
}

func TestHandler_RedirectsConcreteSnapshotWhenProxyDisabled(t *testing.T) {
	const fileName = "/snapshot-500-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.zst"

	db := index.NewDB()
	db.UpsertSnapshots(snapshotEntry("mainnet", "http://sidecar:13080", 500, 500, []*types.SnapshotFile{
		snapshotFile("snapshot-500-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.zst", 500, 0),
	}))

	req, err := http.NewRequest(http.MethodHead, "/v1"+fileName+"?group=mainnet", nil)
	require.NoError(t, err)
	res := httptest.NewRecorder()
	newTrackerRouter(db).ServeHTTP(res, req)

	assert.Equal(t, http.StatusSeeOther, res.Code)
	assert.Equal(t, "http://sidecar:13080/v1"+fileName, res.Header().Get("Location"))
}

func TestHandler_CachesLatestProxiedSnapshotDownload(t *testing.T) {
	const fileName = "/snapshot-500-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.zst"
	const body = "snapshot-data"

	var upstreamRequests int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamRequests, 1)
		assert.Equal(t, fileName, r.URL.Path)
		w.Header().Set("Content-Length", "13")
		w.Header().Set("Content-Type", "application/zstd")
		if r.Method == http.MethodHead {
			return
		}
		_, err := w.Write([]byte(body))
		require.NoError(t, err)
	}))
	defer upstream.Close()

	cacheDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, "snapshot-1-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.zst"), []byte("old"), 0o600))

	db := index.NewDB()
	db.UpsertSnapshots(snapshotEntry("mainnet", upstream.URL, 500, 500, []*types.SnapshotFile{
		snapshotFile(upstream.URL+fileName, 500, 0),
	}))

	handler := NewHandler(db, "http://localhost:8899", 1000)
	handler.ProxySnapshotDownloads = true
	handler.ProxySnapshotCacheDir = cacheDir
	handler.HTTPClient = upstream.Client()
	router := newTrackerRouterWithHandler(handler)

	req, err := http.NewRequest(http.MethodGet, "/v1"+fileName+"?group=mainnet", nil)
	require.NoError(t, err)
	res := httptest.NewRecorder()
	router.ServeHTTP(res, req)

	assert.Equal(t, http.StatusOK, res.Code)
	assert.Equal(t, body, res.Body.String())
	assert.Equal(t, int32(1), atomic.LoadInt32(&upstreamRequests))
	assert.FileExists(t, filepath.Join(cacheDir, "snapshot-500-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.zst"))
	assert.NoFileExists(t, filepath.Join(cacheDir, "snapshot-1-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.zst"))

	req, err = http.NewRequest(http.MethodGet, "/v1"+fileName+"?group=mainnet", nil)
	require.NoError(t, err)
	res = httptest.NewRecorder()
	router.ServeHTTP(res, req)

	assert.Equal(t, http.StatusOK, res.Code)
	assert.Equal(t, body, res.Body.String())
	assert.Equal(t, int32(1), atomic.LoadInt32(&upstreamRequests))

	req, err = http.NewRequest(http.MethodHead, "/v1"+fileName+"?group=mainnet", nil)
	require.NoError(t, err)
	res = httptest.NewRecorder()
	router.ServeHTTP(res, req)

	assert.Equal(t, http.StatusOK, res.Code)
	assert.Equal(t, "13", res.Header().Get("Content-Length"))
	assert.Empty(t, res.Body.String())
	assert.Equal(t, int32(1), atomic.LoadInt32(&upstreamRequests))
}

func TestHandler_ProxiesWithoutCachingWhenCacheRefreshInProgress(t *testing.T) {
	const fileName = "/snapshot-500-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.zst"
	const body = "snapshot-data"

	var upstreamRequests int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamRequests, 1)
		_, err := w.Write([]byte(body))
		require.NoError(t, err)
	}))
	defer upstream.Close()

	cacheDir := t.TempDir()
	db := index.NewDB()
	db.UpsertSnapshots(snapshotEntry("mainnet", upstream.URL, 500, 500, []*types.SnapshotFile{
		snapshotFile(upstream.URL+fileName, 500, 0),
	}))

	handler := NewHandler(db, "http://localhost:8899", 1000)
	handler.ProxySnapshotDownloads = true
	handler.ProxySnapshotCacheDir = cacheDir
	handler.HTTPClient = upstream.Client()
	handler.cacheMu.Lock()
	router := newTrackerRouterWithHandler(handler)

	req, err := http.NewRequest(http.MethodGet, "/v1"+fileName+"?group=mainnet", nil)
	require.NoError(t, err)
	res := httptest.NewRecorder()
	router.ServeHTTP(res, req)

	assert.Equal(t, http.StatusOK, res.Code)
	assert.Equal(t, body, res.Body.String())
	assert.Equal(t, int32(1), atomic.LoadInt32(&upstreamRequests))
	assert.NoFileExists(t, filepath.Join(cacheDir, "snapshot-500-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.zst"))
	handler.cacheMu.Unlock()
}

func newTrackerRouter(db *index.DB) http.Handler {
	handler := NewHandler(db, "http://localhost:8899", 1000)
	return newTrackerRouterWithHandler(handler)
}

func newTrackerRouterWithHandler(handler *Handler) http.Handler {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	handler.RegisterHandlers(engine.Group("/v1"))
	return engine
}

func snapshotEntry(group string, target string, slot uint64, baseSlot uint64, files []*types.SnapshotFile) *index.SnapshotEntry {
	return &index.SnapshotEntry{
		SnapshotKey: index.NewSnapshotKey(group, target, slot, baseSlot),
		Info: &types.SnapshotInfo{
			Slot:      slot,
			BaseSlot:  baseSlot,
			Hash:      files[0].Hash,
			Files:     files,
			TotalSize: uint64(len(files)),
		},
		UpdatedAt: time.Now(),
	}
}

func snapshotFile(name string, slot uint64, baseSlot uint64) *types.SnapshotFile {
	return &types.SnapshotFile{
		FileName: name,
		Slot:     slot,
		BaseSlot: baseSlot,
		Hash:     solana.MustHashFromBase58("AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr"),
		Ext:      ".tar.zst",
		Size:     1,
	}
}
