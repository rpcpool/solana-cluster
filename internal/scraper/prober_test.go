package scraper

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.blockdaemon.com/solana/cluster-manager/types"
)

func TestProber_ProbeURL(t *testing.T) {
	const hash = "AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodHead, r.Method)

		switch r.URL.Path {
		case "/snapshot.tar.bz2":
			w.Header().Set("Location", "/snapshot-100-"+hash+".tar.zst")
			w.WriteHeader(http.StatusSeeOther)
		case "/incremental-snapshot.tar.bz2":
			w.Header().Set("Location", "/incremental-snapshot-100-200-"+hash+".tar.zst")
			w.WriteHeader(http.StatusSeeOther)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	prober, err := NewProber(&types.TargetGroup{
		Group:       "mainnet",
		HttpTargets: &types.HttpTargets{},
	})
	require.NoError(t, err)

	infos, err := prober.Probe(context.Background(), server.URL)
	require.NoError(t, err)
	require.Len(t, infos, 2)

	assert.Equal(t, uint64(100), infos[0].Slot)
	assert.Equal(t, uint64(100), infos[0].BaseSlot)
	require.Len(t, infos[0].Files, 1)
	assert.Equal(t, server.URL+"/snapshot-100-"+hash+".tar.zst", infos[0].Files[0].FileName)

	assert.Equal(t, uint64(200), infos[1].Slot)
	assert.Equal(t, uint64(100), infos[1].BaseSlot)
	require.Len(t, infos[1].Files, 2)
	assert.Equal(t, server.URL+"/incremental-snapshot-100-200-"+hash+".tar.zst", infos[1].Files[0].FileName)
	assert.Equal(t, server.URL+"/snapshot-100-"+hash+".tar.zst", infos[1].Files[1].FileName)
}

func TestProber_ProbeURLNoSnapshot(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	prober, err := NewProber(&types.TargetGroup{
		Group:       "mainnet",
		HttpTargets: &types.HttpTargets{},
	})
	require.NoError(t, err)

	infos, err := prober.Probe(context.Background(), server.URL)
	require.NoError(t, err)
	assert.Empty(t, infos)
}
