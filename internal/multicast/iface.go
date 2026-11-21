// Package multicast receives UDP multicast datagrams: it picks the network
// interface that joins the groups and reads each group's socket until the
// context ends.
package multicast

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
)

// Errors returned by FindInterface, wrapped with the address and the
// interface name. A host that joins a group on the wrong card gets no error
// from the kernel and receives nothing, so these are reported at startup.
var (
	ErrNoInterface   = errors.New("no interface has this address")
	ErrNotMulticast  = errors.New("interface has no multicast flag")
	ErrInterfaceDown = errors.New("interface is down")
)

// candidate is one interface and its addresses, the input of match.
type candidate struct {
	ifi   net.Interface
	addrs []net.Addr
}

// FindInterface returns the interface that owns the IPv4 address ip. An empty
// ip returns a nil interface and no error, which lets the kernel pick the card
// from its routing table.
func FindInterface(ip string) (*net.Interface, error) {
	if ip == "" {
		return nil, nil
	}
	want, err := netip.ParseAddr(ip)
	if err != nil {
		return nil, fmt.Errorf("interface address: %w", err)
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("list interfaces: %w", err)
	}
	cands := make([]candidate, 0, len(ifaces))
	for _, ifi := range ifaces {
		addrs, err := ifi.Addrs()
		if err != nil {
			return nil, fmt.Errorf("interface %s: list addresses: %w", ifi.Name, err)
		}
		cands = append(cands, candidate{ifi: ifi, addrs: addrs})
	}
	return match(want, cands)
}

// match picks the candidate that owns want and checks that it can receive
// multicast. It reads no host state, so tests pass fake interfaces.
func match(want netip.Addr, cands []candidate) (*net.Interface, error) {
	for _, c := range cands {
		if !hasAddr(c.addrs, want) {
			continue
		}
		if c.ifi.Flags&net.FlagUp == 0 {
			return nil, fmt.Errorf("address %s is on interface %s: %w", want, c.ifi.Name, ErrInterfaceDown)
		}
		if c.ifi.Flags&net.FlagMulticast == 0 {
			return nil, fmt.Errorf("address %s is on interface %s: %w", want, c.ifi.Name, ErrNotMulticast)
		}
		ifi := c.ifi
		return &ifi, nil
	}
	return nil, fmt.Errorf("address %s: %w", want, ErrNoInterface)
}

// hasAddr reports whether one of addrs is want, ignoring the prefix length.
// Interface addresses come as 16-byte IPv4-mapped values, so they are unmapped
// before the comparison.
func hasAddr(addrs []net.Addr, want netip.Addr) bool {
	for _, a := range addrs {
		var ip net.IP
		switch v := a.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		default:
			continue
		}
		got, ok := netip.AddrFromSlice(ip)
		if ok && got.Unmap() == want {
			return true
		}
	}
	return false
}
