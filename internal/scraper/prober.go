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

package scraper

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"time"

	"go.blockdaemon.com/solana/cluster-manager/internal/fetch"
	"go.blockdaemon.com/solana/cluster-manager/internal/ledger"
	"go.blockdaemon.com/solana/cluster-manager/types"
)

// Prober checks snapshot info from Solana nodes.
type Prober struct {
	group   string
	client  *http.Client
	scheme  string
	apiPath string
	header  http.Header
	urlMode bool
}

func NewProber(group *types.TargetGroup) (*Prober, error) {
	var tlsConfig *tls.Config
	if group.TLSConfig != nil {
		var err error
		tlsConfig, err = group.TLSConfig.Build()
		if err != nil {
			return nil, err
		}
	}

	header := make(http.Header)
	if group.BasicAuth != nil {
		group.BasicAuth.Apply(header)
	}
	if group.BearerAuth != nil {
		group.BearerAuth.Apply(header)
	}

	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 5 * time.Second,
			}).DialContext,
			TLSClientConfig:       tlsConfig,
			MaxIdleConnsPerHost:   1,
			MaxConnsPerHost:       3,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   5 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ForceAttemptHTTP2:     true,
		},
		Timeout: 10 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	return &Prober{
		group:   group.Group,
		client:  client,
		scheme:  group.Scheme,
		apiPath: group.APIPath,
		header:  header,
		urlMode: group.HttpTargets != nil,
	}, nil
}

// Probe fetches the snapshots of a single target.
func (p *Prober) Probe(ctx context.Context, target string) ([]*types.SnapshotInfo, error) {
	if p.urlMode {
		return p.probeURL(ctx, target)
	}

	u := url.URL{
		Scheme: p.scheme,
		Host:   target,
		Path:   p.apiPath,
	}
	return fetch.NewSidecarClient(u.String()).ListSnapshots(ctx)
}

func (p *Prober) ProbeTarget(target string) string {
	if p.urlMode {
		return target
	}
	return p.scheme + "://" + target
}

func (p *Prober) probeURL(ctx context.Context, target string) ([]*types.SnapshotInfo, error) {
	full, err := p.headSnapshot(ctx, target, "snapshot.tar.bz2")
	if err != nil {
		return nil, err
	}
	incremental, err := p.headSnapshot(ctx, target, "incremental-snapshot.tar.bz2")
	if err != nil {
		return nil, err
	}

	infos := make([]*types.SnapshotInfo, 0, 2)
	if full != nil {
		infos = append(infos, &types.SnapshotInfo{
			Slot:      full.Slot,
			BaseSlot:  full.Slot,
			Hash:      full.Hash,
			Files:     []*types.SnapshotFile{full},
			TotalSize: full.Size,
		})
	}
	if incremental != nil {
		files := []*types.SnapshotFile{incremental}
		totalSize := incremental.Size
		if full != nil && incremental.BaseSlot == full.Slot {
			files = append(files, full)
			totalSize += full.Size
		}
		infos = append(infos, &types.SnapshotInfo{
			Slot:      incremental.Slot,
			BaseSlot:  incremental.BaseSlot,
			Hash:      incremental.Hash,
			Files:     files,
			TotalSize: totalSize,
		})
	}
	return infos, nil
}

func (p *Prober) headSnapshot(ctx context.Context, target string, name string) (*types.SnapshotFile, error) {
	u, err := url.Parse(target)
	if err != nil {
		return nil, fmt.Errorf("parse target URL: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("target must be an absolute URL: %q", target)
	}
	u.Path = path.Join(u.Path, name)
	u.RawPath = ""

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header = p.header.Clone()

	res, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound || res.StatusCode == http.StatusAccepted {
		return nil, nil
	}
	if res.StatusCode < 300 || res.StatusCode >= 400 {
		return nil, fmt.Errorf("head %s: %s", u.String(), res.Status)
	}

	location := res.Header.Get("Location")
	if location == "" {
		return nil, fmt.Errorf("head %s: missing Location header", u.String())
	}

	locationURL, err := u.Parse(location)
	if err != nil {
		return nil, fmt.Errorf("parse Location header: %w", err)
	}
	file := ledger.ParseSnapshotFileName(path.Base(locationURL.Path))
	if file == nil {
		return nil, fmt.Errorf("parse snapshot filename from Location header: %q", location)
	}
	file.FileName = locationURL.String()
	return file, nil
}
