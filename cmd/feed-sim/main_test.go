package main

import (
	"bytes"
	"context"
	"log"
	"math"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/chaugiakhang193/go-mini-feed/internal/config"
	"github.com/chaugiakhang193/go-mini-feed/internal/fix"
	"github.com/chaugiakhang193/go-mini-feed/internal/sim"
)

func TestOptionsValidate(t *testing.T) {
	tests := []struct {
		name    string
		change  func(*options)
		wantErr string
	}{
		{"defaults", func(o *options) {}, ""},
		{"stream", func(o *options) { o.scenario = "stream" }, ""},
		{"unknown scenario", func(o *options) { o.scenario = "burst" }, `-scenario "burst"`},
		{"unknown transport", func(o *options) { o.transport = "broadcast" }, `-transport "broadcast"`},
		{"rate zero", func(o *options) { o.rate = 0 }, "-rate 0 out of range 1..5000"},
		{"rate above the cap", func(o *options) { o.rate = 5001 }, "-rate 5001 out of range 1..5000"},
		{"negative count", func(o *options) { o.count = -1 }, "-count -1"},
		{"no symbols", func(o *options) { o.symbols = 0 }, "-symbols 0 out of range 1..26"},
		{"too many symbols", func(o *options) { o.symbols = 27 }, "-symbols 27 out of range 1..26"},
		{"negative drop", func(o *options) { o.drop = -0.1 }, "-drop"},
		{"duplicate above one", func(o *options) { o.dup = 1.5 }, "-dup"},
		{"reorder not a number", func(o *options) { o.reorder = math.NaN() }, "-reorder"},
		{"all rates at one", func(o *options) { o.drop, o.dup, o.reorder = 1, 1, 1 }, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o := defaultOptions()
			tc.change(&o)

			err := o.validate()
			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func testGroups(ports map[sim.Kind]int) *config.Multicast {
	mc := &config.Multicast{Market: "demo"}
	for i, k := range sim.Kinds() {
		mc.Groups = append(mc.Groups, config.Group{
			Name:    k.String(),
			Address: "239.255.10." + string(rune('1'+i)),
			Port:    ports[k],
		})
	}
	return mc
}

func TestGroupAddresses(t *testing.T) {
	mc := testGroups(map[sim.Kind]int{
		sim.KindSecDef: 46001, sim.KindPrice: 46002, sim.KindSnapshot: 46003, sim.KindHeartbeat: 46004,
	})

	multicast, err := groupAddresses(mc, transportMulticast)
	require.NoError(t, err)
	assert.Equal(t, "239.255.10.2:46002", multicast[sim.KindPrice].String())

	unicast, err := groupAddresses(mc, transportUnicast)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:46002", unicast[sim.KindPrice].String())
}

func TestGroupAddressesNeedEveryGroup(t *testing.T) {
	mc := testGroups(map[sim.Kind]int{})
	mc.Groups = mc.Groups[:3] // no heartbeat

	_, err := groupAddresses(mc, transportMulticast)
	assert.ErrorContains(t, err, `no group "heartbeat"`)
}

// captureLog sends the standard logger to a buffer for the rest of the test.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	flags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(os.Stderr)
		log.SetFlags(flags)
	})
	return &buf
}

// listenLoopback opens one UDP socket per kind on a random port of 127.0.0.1.
func listenLoopback(t *testing.T) (map[sim.Kind]*net.UDPConn, *config.Multicast) {
	t.Helper()
	conns := make(map[sim.Kind]*net.UDPConn)
	ports := make(map[sim.Kind]int)
	for _, k := range sim.Kinds() {
		conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		conns[k] = conn
		ports[k] = conn.LocalAddr().(*net.UDPAddr).Port
	}
	return conns, testGroups(ports)
}

func readPackets(t *testing.T, conn *net.UDPConn, n int) []fix.Message {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	buf := make([]byte, 2048)
	var out []fix.Message
	for range n {
		size, err := conn.Read(buf)
		require.NoError(t, err)
		msg, err := fix.Decode(buf[:size])
		require.NoError(t, err)
		out = append(out, msg)
	}
	return out
}

func TestRunSendsOneTickOverUnicast(t *testing.T) {
	logs := captureLog(t)
	conns, mc := listenLoopback(t)
	o := defaultOptions()
	o.scenario = "one-tick"
	o.transport = transportUnicast
	o.symbols = 2

	require.NoError(t, run(context.Background(), o, mc))

	definition := readPackets(t, conns[sim.KindSecDef], 1)
	assert.Equal(t, "d", definition[0].MsgType())
	prices := readPackets(t, conns[sim.KindPrice], 2)
	assert.Equal(t, "X", prices[0].MsgType())
	assert.Equal(t, "X", prices[1].MsgType())

	assert.Contains(t, logs.String(),
		"op=sim_start scenario=one-tick transport=unicast rate=50 symbols=2 seed=1")
	assert.Contains(t, logs.String(),
		"op=sim_done kind=security_definition built=1 sent=1 dropped=0 duplicated=0 reordered=0 errors=0")
	assert.Contains(t, logs.String(),
		"op=sim_done kind=price built=2 sent=2 dropped=0 duplicated=0 reordered=0 errors=0")
	assert.Contains(t, logs.String(), "op=sim_done kind=heartbeat built=0")
}

func TestRunStreamStopsAfterCount(t *testing.T) {
	captureLog(t)
	conns, mc := listenLoopback(t)
	o := defaultOptions()
	o.scenario = "stream"
	o.transport = transportUnicast
	o.symbols = 1
	o.rate = 1000
	o.count = 20

	require.NoError(t, run(context.Background(), o, mc))

	readPackets(t, conns[sim.KindSecDef], 1)
	readPackets(t, conns[sim.KindPrice], 20)
}

func TestSenderLogsTheFirstFailureOfEachGroupOnly(t *testing.T) {
	var buf bytes.Buffer
	_, mc := listenLoopback(t)
	addrs, err := groupAddresses(mc, transportUnicast)
	require.NoError(t, err)
	delete(addrs, sim.KindSecDef)
	delete(addrs, sim.KindSnapshot)

	sender, err := dialSender(addrs, log.New(&buf, "", 0))
	require.NoError(t, err)
	sender.Close() // every later write fails

	for range 3 {
		assert.Error(t, sender.Send(sim.KindPrice, []byte("x")))
	}
	assert.Error(t, sender.Send(sim.KindHeartbeat, []byte("x")))

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, lines, 2, "one line per group, not one per failed packet")
	assert.Contains(t, lines[0], "op=send_failed group=price")
	assert.Contains(t, lines[1], "op=send_failed group=heartbeat")
}
