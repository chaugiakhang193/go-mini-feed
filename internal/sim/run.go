package sim

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

// Kind is the stream a packet belongs to. Each kind travels on its own
// multicast group and has its own sequence numbers.
type Kind int

const (
	KindSecDef Kind = iota
	KindPrice
	KindSnapshot
	KindHeartbeat
	kindCount
)

// kindNames are the group names of the multicast configuration.
var kindNames = [kindCount]string{"security_definition", "price", "snapshot", "heartbeat"}

// String returns the multicast group name of k.
func (k Kind) String() string {
	return kindNames[k]
}

// Kinds lists every Kind in definition order.
func Kinds() []Kind {
	return []Kind{KindSecDef, KindPrice, KindSnapshot, KindHeartbeat}
}

// Scenario selects what Run sends.
type Scenario string

const (
	// OneTick sends one definition, one book and one trade of the first
	// instrument, without faults.
	OneTick Scenario = "one-tick"
	// Stream sends a definition per instrument, then prices, snapshots and
	// heartbeats on their own timers.
	Stream Scenario = "stream"
)

// MaxRate is the highest price packet rate per second. One packet goes out per
// tick.
const MaxRate = 5000

const (
	defaultSnapshotEvery  = 5 * time.Second
	defaultHeartbeatEvery = time.Second
)

// Sender delivers one packet of the given kind. Run never calls it from more
// than one goroutine. The packet must not be modified, since a duplicated
// packet is the same slice sent twice.
type Sender interface {
	Send(kind Kind, pkt []byte) error
}

// Config describes one run.
type Config struct {
	Scenario    Scenario
	Instruments []Instrument
	Seed        uint64 // drives the market and the faults

	// Rate is price packets per second and Count how many to send; zero
	// Count runs until the context ends. Both apply to Stream only.
	Rate  int
	Count int

	// Faults apply to Stream only: probabilities in [0, 1] for each packet.
	Drop, Dup, Reorder float64

	// SnapshotEvery and HeartbeatEvery default to 5s and 1s.
	SnapshotEvery  time.Duration
	HeartbeatEvery time.Duration

	// Now supplies SendingTime; time.Now when nil.
	Now func() time.Time
}

// KindStats counts what happened to the packets of one Kind. Built packets
// that were dropped never reach the sender; a duplicate adds one to Sent, and
// a send that fails adds one to SendErrors instead.
type KindStats struct {
	Built      int
	Sent       int
	Dropped    int
	Duplicated int
	Reordered  int
	SendErrors int
}

// Stats is indexed by Kind.
type Stats [kindCount]KindStats

// Run sends the scenario to sender. A failed send is counted in Stats and the
// run goes on. Run returns early with an error only for an invalid Config or
// a packet it cannot build. Cancelling ctx ends a stream normally and returns
// a nil error.
func Run(ctx context.Context, cfg Config, sender Sender) (Stats, error) {
	if err := cfg.validate(); err != nil {
		return Stats{}, err
	}
	r := newRunner(cfg, sender)

	var err error
	switch cfg.Scenario {
	case OneTick:
		err = r.oneTick()
	case Stream:
		err = r.stream(ctx)
	}
	r.flushHeld()
	return r.collect(), err
}

func (c Config) validate() error {
	switch c.Scenario {
	case OneTick:
	case Stream:
		if c.Rate < 1 || c.Rate > MaxRate {
			return fmt.Errorf("rate %d out of range 1..%d", c.Rate, MaxRate)
		}
	default:
		return fmt.Errorf("unknown scenario %q", c.Scenario)
	}
	if len(c.Instruments) == 0 {
		return errors.New("no instruments to simulate")
	}
	return nil
}

// runner holds the state of one run. One goroutine calls every method, so the
// market, the faults and the sender need no locks.
type runner struct {
	cfg    Config
	sender Sender
	market *Market
	faults [kindCount]*Faults
	seq    [kindCount]int
	stats  Stats
	now    func() time.Time
}

func newRunner(cfg Config, sender Sender) *runner {
	r := &runner{cfg: cfg, sender: sender, now: cfg.Now}
	if r.now == nil {
		r.now = time.Now
	}
	// NewMarket only fails without instruments, which validate has ruled out.
	r.market, _ = NewMarket(cfg.Instruments, rand.New(rand.NewPCG(cfg.Seed, 1)))

	drop, dup, reorder := cfg.Drop, cfg.Dup, cfg.Reorder
	if cfg.Scenario == OneTick {
		drop, dup, reorder = 0, 0, 0
	}
	for _, k := range Kinds() {
		r.faults[k] = NewFaults(drop, dup, reorder, rand.New(rand.NewPCG(cfg.Seed, 2+uint64(k))))
	}
	return r
}

func (r *runner) oneTick() error {
	if err := r.emit(KindSecDef, func(h Header) ([]byte, error) {
		return SecurityDefinition(h, r.cfg.Instruments[0])
	}); err != nil {
		return err
	}
	if err := r.emitEvent(r.market.Book(0)); err != nil {
		return err
	}
	trade, err := r.market.Trade(0, Buy)
	if err != nil {
		return err
	}
	return r.emitEvent(trade)
}

func (r *runner) stream(ctx context.Context) error {
	for _, in := range r.cfg.Instruments {
		if err := r.emit(KindSecDef, func(h Header) ([]byte, error) {
			return SecurityDefinition(h, in)
		}); err != nil {
			return err
		}
	}

	snapshotEvery := durationOr(r.cfg.SnapshotEvery, defaultSnapshotEvery)
	heartbeatEvery := durationOr(r.cfg.HeartbeatEvery, defaultHeartbeatEvery)
	prices := time.NewTicker(time.Second / time.Duration(r.cfg.Rate))
	defer prices.Stop()
	snapshots := time.NewTicker(snapshotEvery)
	defer snapshots.Stop()
	heartbeats := time.NewTicker(heartbeatEvery)
	defer heartbeats.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-prices.C:
			if err := r.emitEvent(r.market.Next()); err != nil {
				return err
			}
			if r.cfg.Count > 0 && r.stats[KindPrice].Built >= r.cfg.Count {
				return nil
			}
		case <-snapshots.C:
			if err := r.emitSnapshots(); err != nil {
				return err
			}
		case <-heartbeats.C:
			if err := r.emit(KindHeartbeat, Heartbeat); err != nil {
				return err
			}
		}
	}
}

func durationOr(d, fallback time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return fallback
}

// emitEvent builds the price packet for a market event.
func (r *runner) emitEvent(ev Event) error {
	return r.emit(KindPrice, func(h Header) ([]byte, error) {
		id := ev.Instrument.SecurityID
		if ev.Kind == EventBook {
			return OrderBook(h, id, h.SendingTime, ev.Bids, ev.Asks)
		}
		return Trade(h, id, h.SendingTime, ev.Fill)
	})
}

// emitSnapshots sends the current book of every instrument that has one.
func (r *runner) emitSnapshots() error {
	for i, in := range r.cfg.Instruments {
		bids, asks, ok := r.market.Current(i)
		if !ok {
			continue
		}
		if err := r.emit(KindSnapshot, func(h Header) ([]byte, error) {
			return Snapshot(h, in.SecurityID, h.SendingTime, bids, asks)
		}); err != nil {
			return err
		}
	}
	return nil
}

// emit numbers a packet, builds it, and passes it through the faults of its
// kind. The sequence number is assigned before the faults, so a dropped packet
// leaves a gap and a held one arrives out of order.
func (r *runner) emit(kind Kind, build func(Header) ([]byte, error)) error {
	r.seq[kind]++
	pkt, err := build(Header{Seq: r.seq[kind], SendingTime: r.now()})
	if err != nil {
		return fmt.Errorf("build %s packet: %w", kind, err)
	}
	r.stats[kind].Built++
	r.deliver(kind, r.faults[kind].Apply(pkt))
	return nil
}

func (r *runner) deliver(kind Kind, packets [][]byte) {
	for _, pkt := range packets {
		if err := r.sender.Send(kind, pkt); err != nil {
			r.stats[kind].SendErrors++
			continue
		}
		r.stats[kind].Sent++
	}
}

// flushHeld sends the packets the faults are still holding back.
func (r *runner) flushHeld() {
	for _, k := range Kinds() {
		r.deliver(k, r.faults[k].Flush())
	}
}

func (r *runner) collect() Stats {
	for _, k := range Kinds() {
		counts := r.faults[k].Counts()
		r.stats[k].Dropped = counts.Dropped
		r.stats[k].Duplicated = counts.Duplicated
		r.stats[k].Reordered = counts.Reordered
	}
	return r.stats
}
