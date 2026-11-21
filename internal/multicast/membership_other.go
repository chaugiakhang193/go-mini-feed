//go:build !linux

package multicast

import "net"

// joinedGroupsOnly does nothing. IP_MULTICAST_ALL is a Linux socket option.
func joinedGroupsOnly(*net.UDPConn) error {
	return nil
}
