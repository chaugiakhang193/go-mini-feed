package multicast

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testDeadline = 100 * time.Millisecond

// listenLoopback opens a unicast socket on 127.0.0.1 and a client connected
// to it. Unicast exercises the same read loop as a multicast group without
// depending on the host's multicast routing.
func listenLoopback(t *testing.T) (*net.UDPConn, *net.UDPConn) {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	client, err := net.DialUDP("udp4", nil, conn.LocalAddr().(*net.UDPAddr))
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	return conn, client
}

// start runs l in a goroutine and returns the channel that receives its
// result.
func start(ctx context.Context, l *Listener, out chan<- Packet) <-chan error {
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx, out) }()
	return done
}

// waitRun returns the result of Run, or fails the test when Run has not
// returned within limit.
func waitRun(t *testing.T, done <-chan error, limit time.Duration) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(limit):
		require.FailNow(t, "Run did not return", "limit %s", limit)
		return nil
	}
}

// fakeRecorder keeps every packet it is given.
type fakeRecorder struct {
	mu      sync.Mutex
	packets []Packet
}

func (r *fakeRecorder) Record(p Packet) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.packets = append(r.packets, p)
	return true
}

func (r *fakeRecorder) recorded() []Packet {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Packet(nil), r.packets...)
}

func TestRunGivesEachDatagramItsOwnBytes(t *testing.T) {
	conn, client := listenLoopback(t)
	rec := &fakeRecorder{}
	out := make(chan Packet, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := start(ctx, NewListener("price", conn, testDeadline, rec), out)

	_, err := client.Write([]byte("AAA"))
	require.NoError(t, err)
	_, err = client.Write([]byte("BBB"))
	require.NoError(t, err)
	// Both datagrams are read before the channel is drained, so a packet that
	// shared the read buffer would show the second payload twice.
	require.Eventually(t, func() bool { return len(out) == 2 }, time.Second, 5*time.Millisecond)

	first, second := <-out, <-out
	assert.Equal(t, "AAA", string(first.Data))
	assert.Equal(t, "BBB", string(second.Data))
	assert.Equal(t, "price", first.Group)
	assert.False(t, first.ReceivedAt.IsZero())
	assert.Equal(t, time.UTC, first.ReceivedAt.Location())

	got := rec.recorded()
	require.Len(t, got, 2)
	assert.Equal(t, "AAA", string(got[0].Data))
	assert.Equal(t, "BBB", string(got[1].Data))

	cancel()
	assert.NoError(t, waitRun(t, done, time.Second))
}

func TestRunWithoutRecorderStillForwards(t *testing.T) {
	conn, client := listenLoopback(t)
	out := make(chan Packet, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := start(ctx, NewListener("heartbeat", conn, testDeadline, nil), out)

	_, err := client.Write([]byte("HB"))
	require.NoError(t, err)

	select {
	case p := <-out:
		assert.Equal(t, "HB", string(p.Data))
	case <-time.After(time.Second):
		require.FailNow(t, "no packet forwarded")
	}
	cancel()
	assert.NoError(t, waitRun(t, done, time.Second))
}

func TestRunStopsWithinADeadlineWhenIdle(t *testing.T) {
	conn, _ := listenLoopback(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := start(ctx, NewListener("price", conn, testDeadline, nil), make(chan Packet))

	// Let Read block before the cancel.
	time.Sleep(testDeadline / 2)
	cancel()
	cancelled := time.Now()

	assert.NoError(t, waitRun(t, done, 3*testDeadline))
	assert.LessOrEqual(t, time.Since(cancelled), 3*testDeadline)
}

func TestRunReturnsNilOnceTheSocketIsClosed(t *testing.T) {
	conn, _ := listenLoopback(t)
	// The deadline is far longer than the wait, so only the close can end Run.
	done := start(context.Background(), NewListener("price", conn, 10*time.Second, nil), make(chan Packet))

	time.Sleep(testDeadline / 2)
	require.NoError(t, conn.Close())

	assert.NoError(t, waitRun(t, done, time.Second))
}

// failingReader returns err from every Read until closed is set, then
// net.ErrClosed, so a Run that ignores the error can still be stopped.
type failingReader struct {
	err    error
	closed atomic.Bool
}

func (r *failingReader) Read([]byte) (int, error) {
	if r.closed.Load() {
		return 0, net.ErrClosed
	}
	return 0, r.err
}

func (r *failingReader) SetReadDeadline(time.Time) error { return nil }

func TestRunReturnsAReadErrorOfTheSocket(t *testing.T) {
	r := &failingReader{err: errors.New("boom")}
	t.Cleanup(func() { r.closed.Store(true) })
	done := start(context.Background(), NewListener("price", r, testDeadline, nil), make(chan Packet))

	err := waitRun(t, done, time.Second)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "group price")
	assert.Contains(t, err.Error(), "boom")
}

// countingReader returns one byte from every Read and counts the calls.
type countingReader struct {
	reads atomic.Int64
}

func (r *countingReader) Read(b []byte) (int, error) {
	r.reads.Add(1)
	b[0] = 'x'
	return 1, nil
}

func (r *countingReader) SetReadDeadline(time.Time) error { return nil }

func TestRunDoesNotReadOnceCancelled(t *testing.T) {
	r := &countingReader{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out := make(chan Packet, 1)

	err := NewListener("price", r, testDeadline, nil).Run(ctx, out)

	assert.NoError(t, err)
	assert.Zero(t, r.reads.Load(), "reads after the context ended")
}
