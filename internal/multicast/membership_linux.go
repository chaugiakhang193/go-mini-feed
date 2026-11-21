//go:build linux

package multicast

import (
	"fmt"
	"net"
	"syscall"
)

// ipMulticastAll is IP_MULTICAST_ALL from linux/in.h. The syscall package
// does not define it.
const ipMulticastAll = 0x31

// joinedGroupsOnly turns IP_MULTICAST_ALL off. A socket bound to 0.0.0.0:port
// receives the datagrams of every group that any socket on the host joined on
// that port while the option is on, and Linux turns it on by default
// (man 7 ip). With it off, the socket receives only the groups joined on it.
func joinedGroupsOnly(conn *net.UDPConn) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return fmt.Errorf("clear IP_MULTICAST_ALL: %w", err)
	}
	var sockErr error
	err = raw.Control(func(fd uintptr) {
		sockErr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, ipMulticastAll, 0)
	})
	if err != nil {
		return fmt.Errorf("clear IP_MULTICAST_ALL: %w", err)
	}
	if sockErr != nil {
		return fmt.Errorf("clear IP_MULTICAST_ALL: %w", sockErr)
	}
	return nil
}
