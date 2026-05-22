package types

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	assert.True(t, ok)
	exampleConfig := filepath.Join(filepath.Dir(testFile), "../example-config.yml")

	actual, err := LoadConfig(exampleConfig)
	require.NoError(t, err)

	expected := &Config{
		ScrapeInterval: 15 * time.Second,
		TargetGroups: []*TargetGroup{
			{
				Group:  "mainnet",
				Scheme: "http",
				StaticTargets: &StaticTargets{
					Targets: []string{
						"solana-mainnet-1.example.org:8899",
						"solana-mainnet-2.example.org:8899",
						"solana-mainnet-3.example.org:8899",
					},
				},
			},
		},
	}

	assert.Equal(t, expected, actual)
}

func TestLoadConfigHttpTargets(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "config.yml")
	err := os.WriteFile(configFile, []byte(`
scrape_interval: 15s
target_groups:
  - group: mainnet
    http_targets:
      targets:
        - https://api.mainnet.solana.com
`), 0o600)
	require.NoError(t, err)

	actual, err := LoadConfig(configFile)
	require.NoError(t, err)

	require.Len(t, actual.TargetGroups, 1)
	assert.Equal(t, &HttpTargets{
		Targets: []string{"https://api.mainnet.solana.com"},
	}, actual.TargetGroups[0].HttpTargets)
}

func TestLoadConfigProxySnapshotDownloads(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "config.yml")
	err := os.WriteFile(configFile, []byte(`
scrape_interval: 15s
proxy_snapshot_downloads: true
proxy_snapshot_cache_dir: /var/cache/solana-snapshots
target_groups:
  - group: mainnet
    http_targets:
      targets:
        - https://api.mainnet.solana.com
`), 0o600)
	require.NoError(t, err)

	actual, err := LoadConfig(configFile)
	require.NoError(t, err)

	assert.True(t, actual.ProxySnapshotDownloads)
	assert.Equal(t, "/var/cache/solana-snapshots", actual.ProxySnapshotCacheDir)
}
