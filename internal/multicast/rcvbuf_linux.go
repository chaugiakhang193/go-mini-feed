//go:build linux

package multicast

import (
	"fmt"
	"net"
	"syscall"
)

// grantedReadBuffer reads SO_RCVBUF back from the socket. Linux doubles the
// value set with SO_RCVBUF to leave room for its own bookkeeping, caps the
// request at net.core.rmem_max before doubling, and returns the doubled value
// from getsockopt (man 7 socket). Half of it is the usable size granted.
func grantedReadBuffer(conn *net.UDPConn) (int, bool, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, false, fmt.Errorf("read back SO_RCVBUF: %w", err)
	}
	var size int
	var sockErr error
	err = raw.Control(func(fd uintptr) {
		size, sockErr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF)
	})
	if err != nil {
		return 0, false, fmt.Errorf("read back SO_RCVBUF: %w", err)
	}
	if sockErr != nil {
		return 0, false, fmt.Errorf("read back SO_RCVBUF: %w", sockErr)
	}
	return size / 2, true, nil
}
