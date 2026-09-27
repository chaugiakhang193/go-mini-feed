package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validGroup() Group {
	rawLog := true
	return Group{
		Name:       "price",
		Address:    "239.255.10.2",
		Port:       46002,
		RawLog:     &rawLog,
		Exchange:   "demo_all_raw",
		RoutingKey: "all",
	}
}

func TestMulticastValidate(t *testing.T) {
	tests := []struct {
		name    string
		market  string
		groups  func() []Group
		wantErr []string // substrings that must all appear; nil = valid
	}{
		{
			name:   "valid single group",
			market: "demo",
			groups: func() []Group { return []Group{validGroup()} },
		},
		{
			name:   "lowest multicast address 224.0.0.0 is accepted",
			market: "demo",
			groups: func() []Group { g := validGroup(); g.Address = "224.0.0.0"; return []Group{g} },
		},
		{
			name:   "highest multicast address 239.255.255.255 is accepted",
			market: "demo",
			groups: func() []Group { g := validGroup(); g.Address = "239.255.255.255"; return []Group{g} },
		},
		{
			name:    "unicast address is rejected",
			market:  "demo",
			groups:  func() []Group { g := validGroup(); g.Address = "10.0.0.5"; return []Group{g} },
			wantErr: []string{`groups[0].address "10.0.0.5" is not a multicast address`},
		},
		{
			name:    "first address above the multicast range is rejected",
			market:  "demo",
			groups:  func() []Group { g := validGroup(); g.Address = "240.0.0.1"; return []Group{g} },
			wantErr: []string{"is not a multicast address"},
		},
		{
			name:    "IPv6 multicast is rejected",
			market:  "demo",
			groups:  func() []Group { g := validGroup(); g.Address = "ff02::1"; return []Group{g} },
			wantErr: []string{"is not IPv4"},
		},
		{
			name:    "garbage address",
			market:  "demo",
			groups:  func() []Group { g := validGroup(); g.Address = "239.255.10"; return []Group{g} },
			wantErr: []string{"is not an IP address"},
		},
		{
			name:    "port out of range",
			market:  "demo",
			groups:  func() []Group { g := validGroup(); g.Port = 70000; return []Group{g} },
			wantErr: []string{"groups[0].port 70000 is out of range"},
		},
		{
			name:   "duplicate name and duplicate address:port",
			market: "demo",
			groups: func() []Group {
				a, b := validGroup(), validGroup()
				return []Group{a, b}
			},
			wantErr: []string{
				`groups[1].name "price" duplicates groups[0]`,
				"groups[1].address:port 239.255.10.2:46002 duplicates groups[0]",
			},
		},
		{
			name:   "same address on another port is a different stream",
			market: "demo",
			groups: func() []Group {
				a, b := validGroup(), validGroup()
				b.Name, b.Port = "price_b", 46099
				return []Group{a, b}
			},
		},
		{
			name:    "missing exchange and routing key",
			market:  "demo",
			groups:  func() []Group { g := validGroup(); g.Exchange, g.RoutingKey = "", ""; return []Group{g} },
			wantErr: []string{"groups[0].exchange is required", "groups[0].routing_key is required"},
		},
		{
			name:    "missing raw_log is an error, not a silent false",
			market:  "demo",
			groups:  func() []Group { g := validGroup(); g.RawLog = nil; return []Group{g} },
			wantErr: []string{"groups[0].raw_log is required (true or false)"},
		},
		{
			name:   "explicit raw_log false is valid",
			market: "demo",
			groups: func() []Group {
				off := false
				g := validGroup()
				g.RawLog = &off
				return []Group{g}
			},
		},
		{
			name:    "no groups",
			market:  "demo",
			groups:  func() []Group { return nil },
			wantErr: []string{"at least one group is required"},
		},
		{
			name:    "market name from a badly named file",
			market:  "Demo-Market",
			groups:  func() []Group { return []Group{validGroup()} },
			wantErr: []string{`market "Demo-Market"`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := Multicast{Market: tt.market, Groups: tt.groups()}
			err := mc.Validate()
			if tt.wantErr == nil {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			for _, want := range tt.wantErr {
				assert.Contains(t, err.Error(), want)
			}
		})
	}
}

func TestAppValidateReportsAllProblemsAtOnce(t *testing.T) {
	app := App{} // every field at its zero value
	err := app.Validate()
	require.Error(t, err)

	for _, want := range []string{
		"rabbitmq.vhost is required",
		"rabbitmq.prefetch must be > 0, got 0",
		"gateway.read_deadline must be > 0, got 0s",
		"processor.partitions must be > 0, got 0",
	} {
		assert.Contains(t, err.Error(), want)
	}
}

func TestAppValidateReplicaMustDifferFromPrimary(t *testing.T) {
	app, err := LoadApp("../../configs/config.yaml")
	require.NoError(t, err)

	app.Multiplexer.Replicas = []Replica{{VHost: app.RabbitMQ.VHost}}
	assert.ErrorContains(t, app.Validate(), "equals the primary vhost")
}
