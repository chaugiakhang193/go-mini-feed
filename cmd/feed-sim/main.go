// Command feed-sim sends a synthetic DEMO market over UDP: security
// definitions, order books, trades, snapshots and heartbeats, each kind to the
// multicast group of the same name in the group file. Drop, duplicate and
// reorder rates make the receiver see the gaps, repeats and swaps of a lossy
// network.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/chaugiakhang193/go-mini-feed/internal/config"
	"github.com/chaugiakhang193/go-mini-feed/internal/sim"
)

const (
	transportMulticast = "multicast"
	transportUnicast   = "unicast"

	// unicastHost receives the packets when the multicast groups are not used.
	unicastHost = "127.0.0.1"
)

type options struct {
	multicast string
	scenario  string
	transport string
	rate      int
	count     int
	symbols   int
	seed      uint64
	drop      float64
	dup       float64
	reorder   float64
}

func defaultOptions() options {
	return options{
		multicast: "configs/multicast/demo.yaml",
		scenario:  string(sim.Stream),
		transport: transportMulticast,
		rate:      50,
		symbols:   20,
		seed:      1,
	}
}

func main() {
	o := defaultOptions()
	flag.StringVar(&o.multicast, "multicast", o.multicast, "multicast group file; each simulator stream goes to the group of the same name")
	flag.StringVar(&o.scenario, "scenario", o.scenario, "one-tick (a definition, a book and a trade) or stream")
	flag.StringVar(&o.transport, "transport", o.transport, "multicast (the groups of the file) or unicast (127.0.0.1, same ports)")
	flag.IntVar(&o.rate, "rate", o.rate, fmt.Sprintf("price packets per second, 1..%d (stream)", sim.MaxRate))
	flag.IntVar(&o.count, "count", o.count, "price packets to send, 0 for no limit (stream)")
	flag.IntVar(&o.symbols, "symbols", o.symbols, "number of instruments, 1..26")
	flag.Uint64Var(&o.seed, "seed", o.seed, "seed of the random market and faults")
	flag.Float64Var(&o.drop, "drop", o.drop, "probability that a packet is lost, 0..1")
	flag.Float64Var(&o.dup, "dup", o.dup, "probability that a packet is delivered twice, 0..1")
	flag.Float64Var(&o.reorder, "reorder", o.reorder, "probability that a packet arrives behind the next one, 0..1")
	flag.Parse()

	if err := o.validate(); err != nil {
		log.Fatal(err)
	}
	mc, err := config.LoadMulticast(o.multicast)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, o, mc); err != nil {
		log.Fatal(err)
	}
}

// validate checks every flag, so the run never starts with a value the
// simulator would reject half-way.
func (o options) validate() error {
	switch o.scenario {
	case string(sim.OneTick), string(sim.Stream):
	default:
		return fmt.Errorf("-scenario %q: want %s or %s", o.scenario, sim.OneTick, sim.Stream)
	}
	switch o.transport {
	case transportMulticast, transportUnicast:
	default:
		return fmt.Errorf("-transport %q: want %s or %s", o.transport, transportMulticast, transportUnicast)
	}
	if o.rate < 1 || o.rate > sim.MaxRate {
		return fmt.Errorf("-rate %d out of range 1..%d", o.rate, sim.MaxRate)
	}
	if o.count < 0 {
		return fmt.Errorf("-count %d is negative", o.count)
	}
	if o.symbols < 1 || o.symbols > 26 {
		return fmt.Errorf("-symbols %d out of range 1..26", o.symbols)
	}
	for _, p := range []probability{{"-drop", o.drop}, {"-dup", o.dup}, {"-reorder", o.reorder}} {
		// Written as a positive range so that NaN fails it.
		if !(p.value >= 0 && p.value <= 1) {
			return fmt.Errorf("%s %v out of range 0..1", p.flag, p.value)
		}
	}
	return nil
}

// probability pairs a flag name with its value for validation.
type probability struct {
	flag  string
	value float64
}

// run sends the scenario and logs a line per packet kind when it stops.
func run(ctx context.Context, o options, mc *config.Multicast) error {
	addrs, err := groupAddresses(mc, o.transport)
	if err != nil {
		return err
	}
	sender, err := dialSender(addrs, log.Default())
	if err != nil {
		return err
	}
	defer sender.Close()

	instruments, err := sim.Instruments(o.symbols, rand.New(rand.NewPCG(o.seed, 0)))
	if err != nil {
		return err
	}
	cfg := sim.Config{
		Scenario:    sim.Scenario(o.scenario),
		Instruments: instruments,
		Seed:        o.seed,
		Rate:        o.rate,
		Count:       o.count,
		Drop:        o.drop,
		Dup:         o.dup,
		Reorder:     o.reorder,
	}

	log.Printf("op=sim_start scenario=%s transport=%s rate=%d symbols=%d seed=%d",
		o.scenario, o.transport, o.rate, o.symbols, o.seed)
	stats, err := sim.Run(ctx, cfg, sender)
	for _, k := range sim.Kinds() {
		s := stats[k]
		log.Printf("op=sim_done kind=%s built=%d sent=%d dropped=%d duplicated=%d reordered=%d errors=%d",
			k, s.Built, s.Sent, s.Dropped, s.Duplicated, s.Reordered, s.SendErrors)
	}
	return err
}

// groupAddresses maps every packet kind to the UDP address of its group. With
// the unicast transport the ports stay and the host becomes the loopback
// address.
func groupAddresses(mc *config.Multicast, transport string) (map[sim.Kind]*net.UDPAddr, error) {
	addrs := make(map[sim.Kind]*net.UDPAddr)
	for _, k := range sim.Kinds() {
		var group *config.Group
		for i := range mc.Groups {
			if mc.Groups[i].Name == k.String() {
				group = &mc.Groups[i]
			}
		}
		if group == nil {
			return nil, fmt.Errorf("%s has no group %q", mc.Market, k)
		}

		host := group.Address
		if transport == transportUnicast {
			host = unicastHost
		}
		ip := net.ParseIP(host)
		if ip == nil {
			return nil, fmt.Errorf("group %q: invalid address %q", group.Name, host)
		}
		addrs[k] = &net.UDPAddr{IP: ip, Port: group.Port}
	}
	return addrs, nil
}

// udpSender writes each packet kind to its own connected UDP socket.
type udpSender struct {
	conns  map[sim.Kind]*net.UDPConn
	logger *log.Logger
	failed map[sim.Kind]bool
}

func dialSender(addrs map[sim.Kind]*net.UDPAddr, logger *log.Logger) (*udpSender, error) {
	s := &udpSender{
		conns:  make(map[sim.Kind]*net.UDPConn),
		logger: logger,
		failed: make(map[sim.Kind]bool),
	}
	for k, addr := range addrs {
		conn, err := net.DialUDP("udp4", nil, addr)
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("dial group %s: %w", k, err)
		}
		s.conns[k] = conn
	}
	return s, nil
}

// Send writes pkt to the socket of kind. Only the first failure of a group is
// logged, since a down network would otherwise log once per packet. Every
// failure is returned and counted by the caller.
func (s *udpSender) Send(kind sim.Kind, pkt []byte) error {
	_, err := s.conns[kind].Write(pkt)
	if err != nil && !s.failed[kind] {
		s.failed[kind] = true
		s.logger.Printf("op=send_failed group=%s err=%v", kind, err)
	}
	return err
}

// Close closes every socket.
func (s *udpSender) Close() {
	for _, conn := range s.conns {
		conn.Close()
	}
}
