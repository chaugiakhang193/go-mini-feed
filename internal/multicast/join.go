package multicast

import (
	"fmt"
	"net"

	"github.com/chaugiakhang193/go-mini-feed/internal/config"
)

// ReadBuffer is the socket receive buffer asked for and the size the kernel
// actually granted. SetReadBuffer succeeds even when the kernel caps the
// size. Only the read-back value shows a short buffer. Measured is false on
// systems where the granted size is not read back.
type ReadBuffer struct {
	Requested int
	Granted   int
	Measured  bool
}

// Short reports whether the kernel granted less than requested.
func (b ReadBuffer) Short() bool {
	return b.Measured && b.Granted < b.Requested
}

// Join opens a socket on the port of g, joins its group on ifi (nil lets the
// kernel pick the interface) and asks for a receive buffer of readBuffer
// bytes. The socket receives only the group of g, even when other groups use
// the same port. Join does not log. The caller reports the returned ReadBuffer.
func Join(g config.Group, ifi *net.Interface, readBuffer int) (*net.UDPConn, ReadBuffer, error) {
	ip := net.ParseIP(g.Address)
	if ip == nil {
		return nil, ReadBuffer{}, fmt.Errorf("group %s: invalid address %q", g.Name, g.Address)
	}
	conn, err := net.ListenMulticastUDP("udp4", ifi, &net.UDPAddr{IP: ip, Port: g.Port})
	if err != nil {
		return nil, ReadBuffer{}, fmt.Errorf("group %s: join %s:%d: %w", g.Name, g.Address, g.Port, err)
	}
	if err := joinedGroupsOnly(conn); err != nil {
		conn.Close()
		return nil, ReadBuffer{}, fmt.Errorf("group %s: %w", g.Name, err)
	}
	buf, err := setReadBuffer(conn, readBuffer)
	if err != nil {
		conn.Close()
		return nil, ReadBuffer{}, fmt.Errorf("group %s: %w", g.Name, err)
	}
	return conn, buf, nil
}

// setReadBuffer asks for an n-byte receive buffer and reads back what the
// kernel granted.
func setReadBuffer(conn *net.UDPConn, n int) (ReadBuffer, error) {
	if err := conn.SetReadBuffer(n); err != nil {
		return ReadBuffer{}, fmt.Errorf("set read buffer %d: %w", n, err)
	}
	granted, measured, err := grantedReadBuffer(conn)
	if err != nil {
		return ReadBuffer{}, err
	}
	return ReadBuffer{Requested: n, Granted: granted, Measured: measured}, nil
}
