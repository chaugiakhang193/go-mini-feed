package multicast

import (
	"net"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ipNet(ip net.IP, bits int) *net.IPNet {
	return &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, 32)}
}

// fakeInterfaces mirrors the shape of a WSL guest: a loopback card without
// the multicast flag, an Ethernet card that can join groups, and a card that
// is down. net.IPv4 returns the 16-byte form, as net.Interface.Addrs does.
func fakeInterfaces() []candidate {
	return []candidate{
		{
			ifi:   net.Interface{Index: 1, Name: "lo", Flags: net.FlagUp | net.FlagLoopback},
			addrs: []net.Addr{ipNet(net.IPv4(127, 0, 0, 1), 8)},
		},
		{
			ifi:   net.Interface{Index: 2, Name: "eth0", Flags: net.FlagUp | net.FlagBroadcast | net.FlagMulticast},
			addrs: []net.Addr{ipNet(net.IPv4(192, 0, 2, 11), 24)},
		},
		{
			ifi:   net.Interface{Index: 3, Name: "eth1", Flags: net.FlagBroadcast | net.FlagMulticast},
			addrs: []net.Addr{&net.IPAddr{IP: net.IPv4(192, 0, 2, 20)}},
		},
	}
}

func TestFindInterfaceWithoutAddressLeavesTheChoiceToTheOS(t *testing.T) {
	ifi, err := FindInterface("")

	require.NoError(t, err)
	assert.Nil(t, ifi)
}

func TestMatchInterface(t *testing.T) {
	tests := []struct {
		name     string
		ip       string
		wantName string
		wantErr  error
		errText  string
	}{
		{name: "multicast card", ip: "192.0.2.11", wantName: "eth0"},
		{name: "loopback card", ip: "127.0.0.1", wantErr: ErrNotMulticast, errText: "interface lo"},
		{name: "card down", ip: "192.0.2.20", wantErr: ErrInterfaceDown, errText: "interface eth1"},
		{name: "address on no card", ip: "192.0.2.10", wantErr: ErrNoInterface, errText: "192.0.2.10"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ifi, err := match(netip.MustParseAddr(tt.ip), fakeInterfaces())

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Contains(t, err.Error(), tt.errText)
				assert.Nil(t, ifi)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, ifi)
			assert.Equal(t, tt.wantName, ifi.Name)
		})
	}
}
