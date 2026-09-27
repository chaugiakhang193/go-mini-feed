package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The files the binaries actually ship with must load cleanly.
func TestLoadRepoConfigs(t *testing.T) {
	app, err := LoadApp("../../configs/config.yaml")
	require.NoError(t, err)
	assert.Equal(t, "/lab", app.RabbitMQ.VHost)
	assert.Equal(t, 500*time.Millisecond, app.Gateway.ReadDeadline)
	assert.Equal(t, 16, app.Processor.Partitions)

	mc, err := LoadMulticast("../../configs/multicast/demo.yaml")
	require.NoError(t, err)
	assert.Equal(t, "demo", mc.Market)
	assert.Len(t, mc.Groups, 4)
}

func TestMarketFromPath(t *testing.T) {
	tests := []struct{ path, want string }{
		{"configs/multicast/demo.yaml", "demo"},
		{"/etc/feed/multicast/odd_lot.yml", "odd_lot"},
		{"demo", "demo"},
		{filepath.Join("a", "b", "demo.test.yaml"), "demo.test"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, MarketFromPath(tt.path), tt.path)
	}
}

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	return path
}

func TestLoadMulticastRejectsUnknownKey(t *testing.T) {
	path := writeFile(t, "demo.yaml", `
groups:
  - name: price
    address: 239.255.10.2
    port: 46002
    raw_logs: true
    exchange: demo_all_raw
    routing_key: all
`)
	_, err := LoadMulticast(path)
	assert.ErrorContains(t, err, "field raw_logs not found")
}

func TestLoadMulticastInvalidFileNameIsReported(t *testing.T) {
	path := writeFile(t, "Demo Market.yaml", `
groups:
  - name: price
    address: 239.255.10.2
    port: 46002
    raw_log: true
    exchange: demo_all_raw
    routing_key: all
`)
	_, err := LoadMulticast(path)
	assert.ErrorContains(t, err, `market "Demo Market"`)
}

func TestLoadEmptyFile(t *testing.T) {
	_, err := LoadApp(writeFile(t, "config.yaml", ""))
	assert.ErrorContains(t, err, "file is empty")
}

func TestLoadMissingFile(t *testing.T) {
	_, err := LoadApp(filepath.Join(t.TempDir(), "nope.yaml"))
	assert.ErrorIs(t, err, os.ErrNotExist)
}
