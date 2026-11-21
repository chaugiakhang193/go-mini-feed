package sim

import (
	"errors"
	"fmt"
	"math/rand/v2"
)

const (
	// lot is the trading unit: every size is a whole number of lots.
	lot = 100

	// bookDepth is the number of levels on each side of a published book.
	bookDepth = 3

	// maxLevelLots caps the size of one book level.
	maxLevelLots = 50

	// bookShare is the chance that Next publishes a book rather than a trade
	// once the chosen instrument already has one.
	bookShare = 0.7
)

// EventKind tells which of the Event fields is filled.
type EventKind int

const (
	// EventBook carries Bids and Asks.
	EventBook EventKind = iota
	// EventTrade carries Fill.
	EventTrade
)

// Side is the aggressor of a trade.
type Side int

const (
	// Buy takes the best ask.
	Buy Side = iota
	// Sell hits the best bid.
	Sell
)

// Event is one market change for one instrument. The Bids and Asks slices are
// never modified after the Market returns them, so they can be kept.
type Event struct {
	Kind       EventKind
	Instrument Instrument
	Bids, Asks []Level // best level first
	Fill       Fill
}

// Market is the state of every instrument and the source of random changes.
// It is not safe for concurrent use: one goroutine owns it.
type Market struct {
	symbols []*symbolState
	rng     *rand.Rand
}

// symbolState is everything the market remembers about one instrument.
type symbolState struct {
	instrument Instrument

	// bestBid moves with every book. The best ask is always one tick above.
	bestBid int64

	// bids and asks are the last published book, nil before the first.
	bids, asks []Level

	traded    bool
	high, low int64 // extremes of today's trades, valid once traded

	totalVolume int64
	totalValue  int64
}

// NewMarket starts every instrument with its best bid at the reference price.
func NewMarket(instruments []Instrument, rng *rand.Rand) (*Market, error) {
	if len(instruments) == 0 {
		return nil, errors.New("no instruments given")
	}
	m := &Market{rng: rng}
	for _, in := range instruments {
		s := &symbolState{instrument: in, bestBid: in.RefPrice}
		s.bestBid = s.clampBid(s.bestBid)
		m.symbols = append(m.symbols, s)
	}
	return m, nil
}

// clampBid keeps the whole book inside the price band: the lowest bid level
// sits (bookDepth-1) ticks under the best bid, and the highest ask level
// bookDepth ticks above it.
func (s *symbolState) clampBid(bid int64) int64 {
	lowest := s.instrument.Low + (bookDepth-1)*priceStep
	highest := s.instrument.High - bookDepth*priceStep
	return min(max(bid, lowest), highest)
}

// Next picks an instrument at random and returns a new book or a trade for
// it. An instrument without a book always gets a book first, since a trade
// takes its price from the book.
func (m *Market) Next() Event {
	i := m.rng.IntN(len(m.symbols))
	s := m.symbols[i]
	if s.bids == nil || m.rng.Float64() < bookShare {
		return m.Book(i)
	}
	side := Buy
	if m.rng.IntN(2) == 0 {
		side = Sell
	}
	return m.trade(s, side)
}

// Book publishes a new book for instrument i. The best bid moves by one tick
// up, one tick down or not at all.
func (m *Market) Book(i int) Event {
	s := m.symbols[i]
	s.bestBid = s.clampBid(s.bestBid + int64(m.rng.IntN(3)-1)*priceStep)
	s.bids = m.levels(s.bestBid, -priceStep)
	s.asks = m.levels(s.bestBid+priceStep, priceStep)
	return Event{Kind: EventBook, Instrument: s.instrument, Bids: s.bids, Asks: s.asks}
}

// levels returns bookDepth levels starting at first and stepping by step.
func (m *Market) levels(first, step int64) []Level {
	out := make([]Level, bookDepth)
	for k := range out {
		out[k] = Level{
			Price: first + int64(k)*step,
			Size:  (m.rng.Int64N(maxLevelLots) + 1) * lot,
		}
	}
	return out
}

// Trade executes one trade for instrument i against the top of its last
// published book. An instrument without a book has nothing to trade against.
func (m *Market) Trade(i int, side Side) (Event, error) {
	s := m.symbols[i]
	if s.bids == nil {
		return Event{}, fmt.Errorf("instrument %s has no published book", s.instrument.Symbol)
	}
	return m.trade(s, side), nil
}

// trade needs s.bids and s.asks to be set.
func (m *Market) trade(s *symbolState, side Side) Event {
	price, available := s.asks[0].Price, s.asks[0].Size
	if side == Sell {
		price, available = s.bids[0].Price, s.bids[0].Size
	}
	size := (m.rng.Int64N(available/lot) + 1) * lot

	s.totalVolume += size
	s.totalValue += price * size

	var extras []PriceEntry
	if !s.traded {
		s.traded = true
		s.high, s.low = price, price
		extras = append(extras, PriceEntry{Type: entryOpen, Price: price})
	} else {
		if price > s.high {
			s.high = price
			extras = append(extras, PriceEntry{Type: entryHigh, Price: price})
		}
		if price < s.low {
			s.low = price
			extras = append(extras, PriceEntry{Type: entryLow, Price: price})
		}
	}

	return Event{
		Kind:       EventTrade,
		Instrument: s.instrument,
		Fill: Fill{
			Price:       price,
			Size:        size,
			TotalVolume: s.totalVolume,
			TotalValue:  s.totalValue,
			Extras:      extras,
		},
	}
}

// Current returns the last published book of instrument i, and false before
// the first one.
func (m *Market) Current(i int) (bids, asks []Level, ok bool) {
	s := m.symbols[i]
	return s.bids, s.asks, s.bids != nil
}
