package logger

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/chaugiakhang193/go-mini-feed/internal/multicast"
)

// captureLog sends the standard logger to a buffer for the rest of the test.
// The buffer is read only after Close, once the writer goroutine has exited.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	return &buf
}

func packet(group, at string, data string) multicast.Packet {
	ts, err := time.Parse(time.RFC3339Nano, at)
	if err != nil {
		panic(err)
	}
	return multicast.Packet{Group: group, ReceivedAt: ts, Data: []byte(data)}
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	return strings.SplitAfter(strings.TrimSuffix(string(raw), "\n"), "\n")
}

func TestRawFilesFollowTheReceiveDateAndKeepBytes(t *testing.T) {
	dir := t.TempDir()
	payload := "8=FIX.4.4\x019=5\x01\n"
	r := NewRaw(dir, "demo", 8)

	require.True(t, r.Record(packet("price", "2026-11-21T23:59:59.9Z", payload)))
	require.True(t, r.Record(packet("price", "2026-11-22T00:00:00.1Z", payload)))
	require.NoError(t, r.Close())

	before, err := os.ReadFile(filepath.Join(dir, "2026-11-21", "demo", "price.raw"))
	require.NoError(t, err)
	after, err := os.ReadFile(filepath.Join(dir, "2026-11-22", "demo", "price.raw"))
	require.NoError(t, err)
	// The length field counts every payload byte, SOH and the trailing LF
	// included, so a reader can split the file without scanning for line ends.
	assert.Equal(t, "2026-11-21T23:59:59.9Z 15 "+payload+"\n", string(before))
	assert.Equal(t, "2026-11-22T00:00:00.1Z 15 "+payload+"\n", string(after))
}

func TestRawRecordDropsWhenTheQueueIsFull(t *testing.T) {
	logs := captureLog(t)
	dir := t.TempDir()
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(release)
	r := newRaw(dir, "demo", 2, gate)

	results := make(chan []bool, 1)
	go func() {
		var got []bool
		for i := range 3 {
			got = append(got, r.Record(packet("price", "2026-11-21T09:00:00Z", string(rune('a'+i)))))
		}
		results <- got
	}()
	select {
	case got := <-results:
		assert.Equal(t, []bool{true, true, false}, got)
	case <-time.After(time.Second):
		require.FailNow(t, "Record blocked on a full queue")
	}
	assert.Equal(t, uint64(1), r.Dropped())

	release()
	require.NoError(t, r.Close())
	assert.Len(t, readLines(t, filepath.Join(dir, "2026-11-21", "demo", "price.raw")), 2)
	assert.Contains(t, logs.String(), "op=raw_log_dropped count=1 total=1")
}

func TestRawCloseWritesEverythingQueued(t *testing.T) {
	dir := t.TempDir()
	r := NewRaw(dir, "demo", 128)

	for range 100 {
		require.True(t, r.Record(packet("price", "2026-11-21T09:00:00Z", "x")))
	}
	require.NoError(t, r.Close())

	assert.Len(t, readLines(t, filepath.Join(dir, "2026-11-21", "demo", "price.raw")), 100)
	assert.Zero(t, r.Dropped())
}

func TestRawReportsAFailingFileOnce(t *testing.T) {
	logs := captureLog(t)
	// A regular file where the date directory should go makes every open fail.
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "2026-11-21"), nil, 0o644))
	r := NewRaw(dir, "demo", 8)

	for range 3 {
		require.True(t, r.Record(packet("price", "2026-11-21T09:00:00Z", "x")))
	}
	assert.ErrorIs(t, r.Close(), ErrWrite)

	assert.Equal(t, 1, strings.Count(logs.String(), "op=raw_log_write_failed"))
	assert.Contains(t, logs.String(), "price.raw")
}

func TestRawLogsAFlushErrorOnceAndFailsClose(t *testing.T) {
	if _, err := os.Stat("/dev/full"); err != nil {
		t.Skipf("no /dev/full: %v", err)
	}
	logs := captureLog(t)
	dir := t.TempDir()
	groupDir := filepath.Join(dir, "2026-11-21", "demo")
	require.NoError(t, os.MkdirAll(groupDir, 0o755))
	require.NoError(t, os.Symlink("/dev/full", filepath.Join(groupDir, "price.raw")))
	r := NewRaw(dir, "demo", 8)

	// The payload is larger than the 4096-byte bufio buffer, so the write
	// reaches the file at once and fails with ENOSPC.
	require.True(t, r.Record(packet("price", "2026-11-21T09:00:00Z", strings.Repeat("x", 5000))))
	err := r.Close()

	require.ErrorIs(t, err, ErrWrite)
	assert.NotContains(t, err.Error(), "no space left")
	assert.Equal(t, 1, strings.Count(logs.String(), "op=raw_log_write_failed"))
}
