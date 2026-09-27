package config

import (
	"errors"
	"fmt"
	"net/netip"
	"regexp"
)

var marketPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// Validate reports every problem at once (errors.Join) rather than stopping
// at the first one, so a broken file can be fixed in a single pass.
func (a *App) Validate() error {
	var errs []error
	positive := func(name string, v int) {
		if v <= 0 {
			errs = append(errs, fmt.Errorf("%s must be > 0, got %d", name, v))
		}
	}
	nonEmpty := func(name, v string) {
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", name))
		}
	}
	// A default Redis server has 16 logical databases, numbered 0-15.
	redisDB := func(name string, v int) {
		if v < 0 || v > 15 {
			errs = append(errs, fmt.Errorf("%s must be between 0 and 15, got %d", name, v))
		}
	}

	nonEmpty("rabbitmq.vhost", a.RabbitMQ.VHost)
	nonEmpty("rabbitmq.exchanges.raw", a.RabbitMQ.Exchanges.Raw)
	nonEmpty("rabbitmq.exchanges.matched", a.RabbitMQ.Exchanges.Matched)
	nonEmpty("rabbitmq.exchanges.bid_offer", a.RabbitMQ.Exchanges.BidOffer)
	nonEmpty("rabbitmq.exchanges.index", a.RabbitMQ.Exchanges.Index)
	nonEmpty("rabbitmq.input_queue", a.RabbitMQ.InputQueue)
	positive("rabbitmq.prefetch", a.RabbitMQ.Prefetch)

	if a.Multiplexer.Enabled {
		positive("multiplexer.pool_size", a.Multiplexer.PoolSize)
		for i, r := range a.Multiplexer.Replicas {
			nonEmpty(fmt.Sprintf("multiplexer.replicas[%d].vhost", i), r.VHost)
			if r.VHost == a.RabbitMQ.VHost {
				errs = append(errs, fmt.Errorf("multiplexer.replicas[%d].vhost %q equals the primary vhost", i, r.VHost))
			}
		}
	}

	redisDB("redis.secdef_db", a.Redis.SecDefDB)
	redisDB("redis.stock_info_db", a.Redis.StockInfoDB)
	redisDB("redis.pubsub_db", a.Redis.PubSubDB)

	positive("gateway.queue_size", a.Gateway.QueueSize)
	positive("gateway.read_buffer_bytes", a.Gateway.ReadBufferBytes)
	if a.Gateway.ReadDeadline <= 0 {
		errs = append(errs, fmt.Errorf("gateway.read_deadline must be > 0, got %s", a.Gateway.ReadDeadline))
	}
	nonEmpty("gateway.raw_log.dir", a.Gateway.RawLog.Dir)
	positive("gateway.raw_log.queue_size", a.Gateway.RawLog.QueueSize)

	positive("processor.partitions", a.Processor.Partitions)
	positive("processor.partition_buffer", a.Processor.PartitionBuffer)

	return errors.Join(errs...)
}

// Validate checks the market name and every group: the address must be an
// IPv4 multicast address (224.0.0.0/4), and names and address:port pairs
// must be unique so two listeners never read the same stream.
func (m *Multicast) Validate() error {
	var errs []error

	if !marketPattern.MatchString(m.Market) {
		errs = append(errs, fmt.Errorf("market %q (from the file name) must match %s", m.Market, marketPattern))
	}
	if len(m.Groups) == 0 {
		errs = append(errs, errors.New("at least one group is required"))
	}

	names := make(map[string]int)
	endpoints := make(map[string]int)
	for i, g := range m.Groups {
		field := func(f string) string { return fmt.Sprintf("groups[%d].%s", i, f) }

		if g.Name == "" {
			errs = append(errs, fmt.Errorf("%s is required", field("name")))
		} else if j, dup := names[g.Name]; dup {
			errs = append(errs, fmt.Errorf("%s %q duplicates groups[%d]", field("name"), g.Name, j))
		} else {
			names[g.Name] = i
		}

		addr, err := netip.ParseAddr(g.Address)
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("%s %q is not an IP address", field("address"), g.Address))
		case !addr.Is4():
			errs = append(errs, fmt.Errorf("%s %q is not IPv4", field("address"), g.Address))
		case !addr.IsMulticast():
			errs = append(errs, fmt.Errorf("%s %q is not a multicast address (224.0.0.0/4)", field("address"), g.Address))
		}

		if g.Port < 1 || g.Port > 65535 {
			errs = append(errs, fmt.Errorf("%s %d is out of range 1-65535", field("port"), g.Port))
		}

		endpoint := fmt.Sprintf("%s:%d", g.Address, g.Port)
		if j, dup := endpoints[endpoint]; dup {
			errs = append(errs, fmt.Errorf("%s %s duplicates groups[%d]", field("address:port"), endpoint, j))
		} else {
			endpoints[endpoint] = i
		}

		if g.RawLog == nil {
			errs = append(errs, fmt.Errorf("%s is required (true or false)", field("raw_log")))
		}
		if g.Exchange == "" {
			errs = append(errs, fmt.Errorf("%s is required", field("exchange")))
		}
		if g.RoutingKey == "" {
			errs = append(errs, fmt.Errorf("%s is required", field("routing_key")))
		}
	}

	return errors.Join(errs...)
}
