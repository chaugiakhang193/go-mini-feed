package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/chaugiakhang193/go-mini-feed/internal/config"
	"github.com/chaugiakhang193/go-mini-feed/internal/logger"
	"github.com/chaugiakhang193/go-mini-feed/internal/multicast"
)

func TestPrintConfigNeverPrintsThePassword(t *testing.T) {
	app, err := config.LoadApp("../../configs/config.yaml")
	require.NoError(t, err)
	mc, err := config.LoadMulticast("../../configs/multicast/demo.yaml")
	require.NoError(t, err)
	env := &config.Env{
		RabbitMQHost:     "localhost",
		RabbitMQPort:     5672,
		RabbitMQUser:     "lab",
		RabbitMQPassword: "s3cr3t-value",
		RedisAddr:        "localhost:6379",
	}

	var out bytes.Buffer
	printConfig(&out, app, mc, env)

	assert.NotContains(t, out.String(), "s3cr3t-value")
	assert.Contains(t, out.String(), "lab@localhost:5672 vhost=/lab password=******")
	assert.Contains(t, out.String(), "security_definition  239.255.10.1:46001 raw_log=true")
}

func TestMask(t *testing.T) {
	assert.Equal(t, "(empty)", mask(""))
	assert.Equal(t, "******", mask("x"))
	assert.Equal(t, "******", mask("a much longer secret"))
}

// loopbackListener opens a unicast socket on 127.0.0.1 for group and returns
// its listener and a client connected to it.
func loopbackListener(t *testing.T, group string, rec multicast.Recorder) (*multicast.Listener, *net.UDPConn) {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	client, err := net.DialUDP("udp4", nil, conn.LocalAddr().(*net.UDPAddr))
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	return multicast.NewListener(group, conn, testDeadline, rec), client
}

const testDeadline = 500 * time.Millisecond

func TestRunCountsEachGroupAndStopsOnCancel(t *testing.T) {
	dir := t.TempDir()
	raw := logger.NewRaw(dir, "demo", 16)
	on, off := true, false
	a, clientA := loopbackListener(t, "a", recorderFor(config.Group{RawLog: &on}, raw))
	b, clientB := loopbackListener(t, "b", recorderFor(config.Group{RawLog: &off}, raw))
	c, _ := loopbackListener(t, "c", nil)
	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		counts map[string]uint64
		err    error
	}
	done := make(chan result, 1)
	go func() {
		counts, err := run(ctx, []*multicast.Listener{a, b, c}, 16, raw)
		done <- result{counts, err}
	}()

	for _, msg := range []string{"a1", "a2"} {
		_, err := clientA.Write([]byte(msg))
		require.NoError(t, err)
	}
	_, err := clientB.Write([]byte("b1"))
	require.NoError(t, err)
	// Loopback datagrams reach the sockets at once; the pause lets the
	// readers hand them over before the cancel.
	time.Sleep(200 * time.Millisecond)
	cancel()
	cancelled := time.Now()

	select {
	case res := <-done:
		require.NoError(t, res.err)
		assert.Equal(t, map[string]uint64{"a": 2, "b": 1, "c": 0}, res.counts)
	case <-time.After(2 * time.Second):
		require.FailNow(t, "run did not return after cancel")
	}
	assert.LessOrEqual(t, time.Since(cancelled), testDeadline+100*time.Millisecond)

	// The day directory is matched with a wildcard so the test does not
	// depend on the date at which the packets were received. Two packets that
	// arrive on both sides of midnight UTC land in two files. The lines are
	// summed over every match.
	rawA, err := filepath.Glob(filepath.Join(dir, "*", "demo", "a.raw"))
	require.NoError(t, err)
	require.NotEmpty(t, rawA)
	var lineCount int
	for _, path := range rawA {
		lines, err := os.ReadFile(path)
		require.NoError(t, err)
		lineCount += bytes.Count(lines, []byte("\n"))
	}
	assert.Equal(t, 2, lineCount)
	rawB, err := filepath.Glob(filepath.Join(dir, "*", "demo", "b.raw"))
	require.NoError(t, err)
	assert.Empty(t, rawB)
}

func TestFormatCountsKeepsGroupsApartFromFixedKeys(t *testing.T) {
	got := formatCounts(map[string]uint64{"op": 0, "price": 3})

	assert.Equal(t, "packets.op=0 packets.price=3", got)
}

// brokenConn fails every read with a socket error.
type brokenConn struct{}

func (brokenConn) Read([]byte) (int, error)        { return 0, errors.New("boom") }
func (brokenConn) SetReadDeadline(time.Time) error { return nil }

func TestRunStopsEveryListenerWhenOneFails(t *testing.T) {
	raw := logger.NewRaw(t.TempDir(), "demo", 16)
	healthy, _ := loopbackListener(t, "a", nil)
	broken := multicast.NewListener("b", brokenConn{}, testDeadline, nil)
	done := make(chan error, 1)
	go func() {
		_, err := run(context.Background(), []*multicast.Listener{healthy, broken}, 16, raw)
		done <- err
	}()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "group b")
		assert.Contains(t, err.Error(), "boom")
	case <-time.After(2 * time.Second):
		require.FailNow(t, "run kept going after a listener failed")
	}
}

func TestRecorderForSkipsGroupsWithoutRawLog(t *testing.T) {
	raw := logger.NewRaw(t.TempDir(), "demo", 1)
	t.Cleanup(func() { raw.Close() })
	on, off := true, false

	assert.Same(t, raw, recorderFor(config.Group{RawLog: &on}, raw))
	// testify's Nil also accepts a nil *logger.Raw stored in the interface.
	// The listener compares the interface with nil. Only == nil proves that
	// it will not call Record.
	rec := recorderFor(config.Group{RawLog: &off}, raw)
	assert.True(t, rec == nil, "recorderFor returned %T, not a nil interface", rec)
}
