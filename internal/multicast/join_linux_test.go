//go:build linux

package multicast

import (
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/chaugiakhang193/go-mini-feed/internal/config"
)

// drain counts the datagrams that reach conn until a read times out.
func drain(t *testing.T, conn *net.UDPConn) int {
	t.Helper()
	buf := make([]byte, 64)
	n := 0
	for {
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(300*time.Millisecond)))
		if _, _, err := conn.ReadFromUDP(buf); err != nil {
			require.True(t, errors.Is(err, os.ErrDeadlineExceeded), "read: %v", err)
			return n
		}
		n++
	}
}

func TestJoinReceivesOnlyItsOwnGroup(t *testing.T) {
	port := freePort(t)
	first := config.Group{Name: "first", Address: "239.255.10.251", Port: port}
	second := config.Group{Name: "second", Address: "239.255.10.252", Port: port}
	connFirst, _, err := Join(first, nil, 65536)
	require.NoError(t, err)
	t.Cleanup(func() { connFirst.Close() })
	connSecond, _, err := Join(second, nil, 65536)
	require.NoError(t, err)
	t.Cleanup(func() { connSecond.Close() })

	sender, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: net.ParseIP(first.Address), Port: port})
	require.NoError(t, err)
	t.Cleanup(func() { sender.Close() })
	for i := 0; i < 3; i++ {
		_, err := sender.Write([]byte("x"))
		require.NoError(t, err)
	}

	assert.Equal(t, 3, drain(t, connFirst), "the group that was sent to")
	assert.Equal(t, 0, drain(t, connSecond), "a group sharing the port")
}
