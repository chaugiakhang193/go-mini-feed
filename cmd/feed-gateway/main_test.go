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
	a, clientA := loopbackListener(t, "a", raw)
	b, clientB := loopbackListener(t, "b", nil)
	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		counts map[string]uint64
		err    error
	}
	done := make(chan result, 1)
	go func() {
		counts, err := run(ctx, []*multicast.Listener{a, b}, 16, raw)
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
		assert.Equal(t, map[string]uint64{"a": 2, "b": 1}, res.counts)
	case <-time.After(2 * time.Second):
		require.FailNow(t, "run did not return after cancel")
	}
	assert.LessOrEqual(t, time.Since(cancelled), testDeadline+100*time.Millisecond)

	today := time.Now().UTC().Format(time.DateOnly)
	lines, err := os.ReadFile(filepath.Join(dir, today, "demo", "a.raw"))
	require.NoError(t, err)
	assert.Equal(t, 2, bytes.Count(lines, []byte("\n")))
	assert.NoFileExists(t, filepath.Join(dir, today, "demo", "b.raw"))
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
	// A nil *logger.Raw stored in the interface would not compare equal to
	// nil, and the listener would call Record on it.
	assert.Nil(t, recorderFor(config.Group{RawLog: &off}, raw))
}
