//go:build !linux

package multicast

import "net"

// grantedReadBuffer reports the buffer as unmeasured. Other systems store
// and return SO_RCVBUF by their own rules. The Linux halving does not apply
// there.
func grantedReadBuffer(*net.UDPConn) (int, bool, error) {
	return 0, false, nil
}
