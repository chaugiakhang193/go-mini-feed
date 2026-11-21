package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/chaugiakhang193/go-mini-feed/internal/sim"
)

func heartbeat(t *testing.T, seq int) []byte {
	t.Helper()
	raw, err := sim.Heartbeat(sim.Header{
		Seq:         seq,
		SendingTime: time.Date(2026, time.September, 28, 2, 0, 59, 0, time.UTC),
	})
	require.NoError(t, err)
	return raw
}

func TestDescribeShowsSOHAsPipeAndTheSequenceNumber(t *testing.T) {
	raw := heartbeat(t, 59)

	line := describe(packet{group: "heartbeat", data: raw})

	assert.Equal(t, fmt.Sprintf("group=heartbeat len=%d seq=59 "+
		"8=FIX.4.4|9=51|35=0|49=DEMO|56=LAB|34=59|52=20260928-02:00:59.000|10=000|", len(raw)), line)
}

func TestDescribePrintsBytesThatAreNotFIX(t *testing.T) {
	line := describe(packet{group: "price", data: []byte("hello\x01")})

	assert.Equal(t, "group=price len=6 seq=? hello|", line)
}

// listenLoopback opens a UDP socket on a random port of 127.0.0.1 per group.
func listenLoopback(t *testing.T, groups ...string) []listener {
	t.Helper()
	var out []listener
	for _, g := range groups {
		conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		require.NoError(t, err)
		out = append(out, listener{group: g, conn: conn})
	}
	return out
}

func sendTo(t *testing.T, l listener, packets ...[]byte) {
	t.Helper()
	conn, err := net.DialUDP("udp4", nil, l.conn.LocalAddr().(*net.UDPAddr))
	require.NoError(t, err)
	defer conn.Close()
	for _, p := range packets {
		_, err := conn.Write(p)
		require.NoError(t, err)
	}
}

// dumpAsync runs dump and reports when it returns.
func dumpAsync(ctx context.Context, ls []listener, count int, out *bytes.Buffer) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		dump(ctx, ls, count, out)
	}()
	return done
}

func waitFor(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("dump did not return")
	}
}

func TestDumpPrintsPacketsOfEveryGroupAndStopsAtCount(t *testing.T) {
	ls := listenLoopback(t, "price", "heartbeat")
	var out bytes.Buffer
	done := dumpAsync(context.Background(), ls, 3, &out)

	sendTo(t, ls[0], heartbeat(t, 1), heartbeat(t, 2))
	sendTo(t, ls[1], heartbeat(t, 3))
	waitFor(t, done)

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Len(t, lines, 3)
	text := out.String()
	assert.Equal(t, 2, strings.Count(text, "group=price "))
	assert.Equal(t, 1, strings.Count(text, "group=heartbeat "))
}

func TestDumpKeepsEachPacketIntact(t *testing.T) {
	// The read buffer is reused for the next datagram, so the printer sees
	// the right bytes only if every reader copied its packet first.
	ls := listenLoopback(t, "price")
	var out bytes.Buffer
	done := dumpAsync(context.Background(), ls, 100, &out)

	var packets [][]byte
	for seq := 1; seq <= 100; seq++ {
		packets = append(packets, heartbeat(t, seq))
	}
	sendTo(t, ls[0], packets...)
	waitFor(t, done)

	seen := make(map[string]bool)
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		seen[strings.Fields(line)[2]] = true
	}
	assert.Len(t, seen, 100, "100 different sequence numbers, none printed twice")
}

func TestDumpReturnsWhenTheContextIsCancelled(t *testing.T) {
	ls := listenLoopback(t, "price", "snapshot")
	var out bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	done := dumpAsync(ctx, ls, 0, &out)

	time.Sleep(20 * time.Millisecond)
	cancel()

	waitFor(t, done)
	assert.Empty(t, out.String())
}
