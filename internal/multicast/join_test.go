package multicast

import (
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/chaugiakhang193/go-mini-feed/internal/config"
)

func skipUnlessLinux(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("SO_RCVBUF is read back only on Linux")
	}
}

func loopbackSocket(t *testing.T) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestSetReadBufferReportsTheKernelSizeWithoutDoubling(t *testing.T) {
	skipUnlessLinux(t)

	buf, err := setReadBuffer(loopbackSocket(t), 65536)

	require.NoError(t, err)
	assert.Equal(t, ReadBuffer{Requested: 65536, Granted: 65536, Measured: true}, buf)
	assert.False(t, buf.Short())
}

func TestSetReadBufferAboveTheLimitIsShort(t *testing.T) {
	skipUnlessLinux(t)
	raw, err := os.ReadFile("/proc/sys/net/core/rmem_max")
	if err != nil {
		t.Skipf("read rmem_max: %v", err)
	}
	rmemMax, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	require.NoError(t, err)

	buf, err := setReadBuffer(loopbackSocket(t), 2*rmemMax)

	require.NoError(t, err)
	assert.Equal(t, 2*rmemMax, buf.Requested)
	assert.Equal(t, rmemMax, buf.Granted)
	assert.True(t, buf.Short())
}

func TestReadBufferShort(t *testing.T) {
	assert.True(t, ReadBuffer{Requested: 8, Granted: 4, Measured: true}.Short())
	assert.False(t, ReadBuffer{Requested: 8, Granted: 8, Measured: true}.Short())
	assert.False(t, ReadBuffer{Requested: 8}.Short(), "an unmeasured buffer is never reported short")
}

// freePort returns a UDP port that was free a moment ago.
func freePort(t *testing.T) int {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{})
	require.NoError(t, err)
	port := conn.LocalAddr().(*net.UDPAddr).Port
	require.NoError(t, conn.Close())
	return port
}

func TestJoinOpensTheGroupPort(t *testing.T) {
	port := freePort(t)
	g := config.Group{Name: "test", Address: "239.255.10.250", Port: port}

	conn, buf, err := Join(g, nil, 65536)

	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	assert.Equal(t, port, conn.LocalAddr().(*net.UDPAddr).Port)
	assert.Equal(t, 65536, buf.Requested)
	assert.Equal(t, runtime.GOOS == "linux", buf.Measured)
}

func TestJoinRejectsAnInvalidAddress(t *testing.T) {
	g := config.Group{Name: "test", Address: "239.255.10", Port: 46099}

	conn, _, err := Join(g, nil, 65536)

	require.Error(t, err)
	assert.Nil(t, conn)
	assert.Contains(t, err.Error(), "group test")
}
