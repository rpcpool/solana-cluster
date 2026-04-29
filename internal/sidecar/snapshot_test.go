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

package sidecar

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.blockdaemon.com/solana/cluster-manager/internal/ledgertest"
	"go.uber.org/zap/zaptest"
)

func newRouter(h *SnapshotHandler) http.Handler {
	router := gin.Default()
	h.RegisterHandlers(router)
	return router
}

func testRequest(h *SnapshotHandler, req *http.Request) *httptest.ResponseRecorder {
	router := newRouter(h)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func TestHandler_ListSnapshots_Error(t *testing.T) {
	h := NewSnapshotHandler("???/some/nonexistent/path", zaptest.NewLogger(t))

	req, err := http.NewRequest(http.MethodGet, "/snapshots", nil)
	require.NoError(t, err)

	res := testRequest(h, req)
	assert.Equal(t, http.StatusInternalServerError, res.Code)
}

func TestHandler_RedirectsBestSnapshots(t *testing.T) {
	const hash = "AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr"
	root := ledgertest.NewFS(t)
	root.AddFakeFile(t, "snapshot-100-"+hash+".tar.bz2")
	root.AddFakeFile(t, "snapshot-200-"+hash+".tar.zst")
	root.AddFakeFile(t, "incremental-snapshot-200-300-"+hash+".tar.zst")

	h := &SnapshotHandler{
		LedgerDir: root.GetLedgerDir(t),
		Log:       zaptest.NewLogger(t),
	}

	req, err := http.NewRequest(http.MethodHead, "/snapshot.tar.bz2", nil)
	require.NoError(t, err)
	res := testRequest(h, req)
	assert.Equal(t, http.StatusSeeOther, res.Code)
	assert.Equal(t, "/snapshot-200-"+hash+".tar.zst", res.Header().Get("Location"))

	req, err = http.NewRequest(http.MethodHead, "/incremental-snapshot.tar.zst", nil)
	require.NoError(t, err)
	res = testRequest(h, req)
	assert.Equal(t, http.StatusSeeOther, res.Code)
	assert.Equal(t, "/incremental-snapshot-200-300-"+hash+".tar.zst", res.Header().Get("Location"))
}

func TestHandler_DownloadRedirectTarget(t *testing.T) {
	const name = "snapshot-100-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.bz2"
	root := ledgertest.NewFS(t)
	root.AddFakeFile(t, name)

	h := &SnapshotHandler{
		LedgerDir: root.GetLedgerDir(t),
		Log:       zaptest.NewLogger(t),
	}

	req, err := http.NewRequest(http.MethodGet, "/"+name, nil)
	require.NoError(t, err)

	res := testRequest(h, req)
	assert.Equal(t, http.StatusOK, res.Code)
	assert.Equal(t, "1", res.Header().Get("Content-Length"))
}
