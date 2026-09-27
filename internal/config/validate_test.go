package config

import (
	"fmt"
	"strings"
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

// With every field at its zero value, each required check must fire exactly
// once. Removing any check makes the list differ.
func TestAppValidateReportsAllProblemsAtOnce(t *testing.T) {
	err := (&App{}).Validate()
	require.Error(t, err)

	assert.Equal(t, []string{
		"rabbitmq.vhost is required",
		"rabbitmq.exchanges.raw is required",
		"rabbitmq.exchanges.matched is required",
		"rabbitmq.exchanges.bid_offer is required",
		"rabbitmq.exchanges.index is required",
		"rabbitmq.input_queue is required",
		"rabbitmq.prefetch must be > 0, got 0",
		"gateway.queue_size must be > 0, got 0",
		"gateway.read_buffer_bytes must be > 0, got 0",
		"gateway.read_deadline must be > 0, got 0s",
		"gateway.raw_log.dir is required",
		"gateway.raw_log.queue_size must be > 0, got 0",
		"processor.partitions must be > 0, got 0",
		"processor.partition_buffer must be > 0, got 0",
	}, strings.Split(err.Error(), "\n"))
}

func setRedisDB(t *testing.T, app *App, key string, db int) {
	t.Helper()
	switch key {
	case "redis.secdef_db":
		app.Redis.SecDefDB = db
	case "redis.stock_info_db":
		app.Redis.StockInfoDB = db
	case "redis.pubsub_db":
		app.Redis.PubSubDB = db
	default:
		t.Fatalf("unknown Redis key %q", key)
	}
}

// Every Redis database field is checked on its own, so dropping the check of
// any single field makes a case fail.
func TestAppValidateRedisDB(t *testing.T) {
	keys := []string{"redis.secdef_db", "redis.stock_info_db", "redis.pubsub_db"}
	tests := []struct {
		db      int
		wantErr bool
	}{
		{db: 0},
		{db: 15},
		{db: -1, wantErr: true},
		{db: 16, wantErr: true},
	}
	for _, key := range keys {
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%s=%d", key, tt.db), func(t *testing.T) {
				app, err := LoadApp("../../configs/config.yaml")
				require.NoError(t, err)

				setRedisDB(t, app, key, tt.db)
				err = app.Validate()
				if !tt.wantErr {
					assert.NoError(t, err)
					return
				}
				assert.EqualError(t, err, fmt.Sprintf("%s must be between 0 and 15, got %d", key, tt.db))
			})
		}
	}
}

func TestGroupRawLogEnabled(t *testing.T) {
	on, off := true, false
	assert.False(t, Group{RawLog: nil}.RawLogEnabled(), "missing key")
	assert.False(t, Group{RawLog: &off}.RawLogEnabled(), "explicit false")
	assert.True(t, Group{RawLog: &on}.RawLogEnabled(), "explicit true")
}

func TestAppValidateReplicaMustDifferFromPrimary(t *testing.T) {
	app, err := LoadApp("../../configs/config.yaml")
	require.NoError(t, err)

	app.Multiplexer.Replicas = []Replica{{VHost: app.RabbitMQ.VHost}}
	assert.ErrorContains(t, app.Validate(), "equals the primary vhost")
}
