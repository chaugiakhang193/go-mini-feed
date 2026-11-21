package sim

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const eventCount = 10000

func newTestMarket(t *testing.T, seed uint64) (*Market, []Instrument) {
	t.Helper()
	instruments, err := Instruments(3, newRand(seed))
	require.NoError(t, err)
	market, err := NewMarket(instruments, newRand(seed+100))
	require.NoError(t, err)
	return market, instruments
}

// runMarket draws eventCount events from a fresh market.
func runMarket(t *testing.T, seed uint64) ([]Event, []Instrument) {
	t.Helper()
	market, instruments := newTestMarket(t, seed)
	events := make([]Event, eventCount)
	for i := range events {
		events[i] = market.Next()
	}
	return events, instruments
}

func TestTradeIsAtTheTopOfTheLastBook(t *testing.T) {
	events, _ := runMarket(t, 1)

	lastBook := make(map[string]Event)
	var trades int
	for _, ev := range events {
		symbol := ev.Instrument.Symbol
		if ev.Kind == EventBook {
			lastBook[symbol] = ev
			continue
		}
		trades++
		book, ok := lastBook[symbol]
		require.True(t, ok, "%s traded before it published a book", symbol)

		fill := ev.Fill
		var available int64
		switch fill.Price {
		case book.Asks[0].Price:
			available = book.Asks[0].Size
		case book.Bids[0].Price:
			available = book.Bids[0].Size
		default:
			require.Failf(t, "trade off the top of the book", "%s price %d, bid %d, ask %d",
				symbol, fill.Price, book.Bids[0].Price, book.Asks[0].Price)
		}
		assert.Zero(t, fill.Size%lot, "size %d is a whole number of lots", fill.Size)
		assert.Positive(t, fill.Size)
		assert.LessOrEqual(t, fill.Size, available, "%s trade larger than its level", symbol)
	}
	assert.Positive(t, trades)
}

func TestTradeSideDecidesThePrice(t *testing.T) {
	market, _ := newTestMarket(t, 2)
	book := market.Book(0)

	buy, err := market.Trade(0, Buy)
	require.NoError(t, err)
	assert.Equal(t, book.Asks[0].Price, buy.Fill.Price, "a buyer takes the best ask")

	sell, err := market.Trade(0, Sell)
	require.NoError(t, err)
	assert.Equal(t, book.Bids[0].Price, sell.Fill.Price, "a seller hits the best bid")
}

func TestTradeNeedsABook(t *testing.T) {
	market, _ := newTestMarket(t, 2)

	_, err := market.Trade(1, Buy)
	assert.ErrorContains(t, err, "BBB")
}

func TestFirstEventOfEachSymbolIsABook(t *testing.T) {
	events, instruments := runMarket(t, 3)

	seen := make(map[string]bool)
	for _, ev := range events {
		symbol := ev.Instrument.Symbol
		if !seen[symbol] {
			assert.Equal(t, EventBook, ev.Kind, "first event of %s", symbol)
			seen[symbol] = true
		}
	}
	assert.Len(t, seen, len(instruments))
}

func TestTotalsFollowTheTrades(t *testing.T) {
	events, _ := runMarket(t, 4)

	volume := make(map[string]int64)
	value := make(map[string]int64)
	for _, ev := range events {
		if ev.Kind != EventTrade {
			continue
		}
		symbol := ev.Instrument.Symbol
		fill := ev.Fill
		previous := volume[symbol]
		volume[symbol] += fill.Size
		value[symbol] += fill.Price * fill.Size

		assert.Greater(t, fill.TotalVolume, previous, "%s total volume rises with every trade", symbol)
		assert.Equal(t, volume[symbol], fill.TotalVolume, "%s total volume", symbol)
		assert.Equal(t, value[symbol], fill.TotalValue, "%s total value", symbol)
	}
}

func TestPricesStayInTheBandOnTheTickGrid(t *testing.T) {
	events, _ := runMarket(t, 5)

	for _, ev := range events {
		var prices []int64
		for _, l := range append(append([]Level{}, ev.Bids...), ev.Asks...) {
			prices = append(prices, l.Price)
		}
		if ev.Kind == EventTrade {
			prices = append(prices, ev.Fill.Price)
			for _, e := range ev.Fill.Extras {
				prices = append(prices, e.Price)
			}
		}
		for _, p := range prices {
			assert.Zero(t, p%priceStep, "%s price %d is on the tick grid", ev.Instrument.Symbol, p)
			assert.GreaterOrEqual(t, p, ev.Instrument.Low, ev.Instrument.Symbol)
			assert.LessOrEqual(t, p, ev.Instrument.High, ev.Instrument.Symbol)
		}
	}
}

func TestOpeningPriceAndNewExtremes(t *testing.T) {
	events, _ := runMarket(t, 6)

	type extremes struct{ high, low int64 }
	traded := make(map[string]extremes)
	var highs, lows int
	for _, ev := range events {
		if ev.Kind != EventTrade {
			continue
		}
		symbol := ev.Instrument.Symbol
		price := ev.Fill.Price
		so, seen := traded[symbol]
		if !seen {
			assert.Equal(t, []PriceEntry{{entryOpen, price}}, ev.Fill.Extras,
				"the first trade of %s reports the opening price only", symbol)
			traded[symbol] = extremes{price, price}
			continue
		}

		var want []PriceEntry
		if price > so.high {
			want = append(want, PriceEntry{entryHigh, price})
			so.high = price
			highs++
		}
		if price < so.low {
			want = append(want, PriceEntry{entryLow, price})
			so.low = price
			lows++
		}
		traded[symbol] = so
		assert.Equal(t, want, ev.Fill.Extras, "%s extremes at price %d", symbol, price)
	}
	assert.Positive(t, highs, "the run sets at least one new high")
	assert.Positive(t, lows, "the run sets at least one new low")
}

func TestBookShape(t *testing.T) {
	events, _ := runMarket(t, 7)

	lastBid := make(map[string]int64)
	for _, ev := range events {
		if ev.Kind != EventBook {
			assert.Empty(t, ev.Bids)
			assert.Empty(t, ev.Asks)
			continue
		}
		require.Len(t, ev.Bids, bookDepth)
		require.Len(t, ev.Asks, bookDepth)
		assert.Equal(t, ev.Bids[0].Price+priceStep, ev.Asks[0].Price, "spread is one tick")
		for k := 1; k < bookDepth; k++ {
			assert.Equal(t, ev.Bids[k-1].Price-priceStep, ev.Bids[k].Price, "bids step down")
			assert.Equal(t, ev.Asks[k-1].Price+priceStep, ev.Asks[k].Price, "asks step up")
		}
		for _, l := range append(append([]Level{}, ev.Bids...), ev.Asks...) {
			assert.Zero(t, l.Size%lot)
			assert.GreaterOrEqual(t, l.Size, int64(lot))
			assert.LessOrEqual(t, l.Size, int64(maxLevelLots*lot))
		}

		symbol := ev.Instrument.Symbol
		if previous, ok := lastBid[symbol]; ok {
			move := ev.Bids[0].Price - previous
			assert.LessOrEqual(t, move, int64(priceStep), "%s moves at most one tick", symbol)
			assert.GreaterOrEqual(t, move, -int64(priceStep), "%s moves at most one tick", symbol)
		}
		lastBid[symbol] = ev.Bids[0].Price
	}
}

func TestAboutThirtyPercentOfEventsAreTrades(t *testing.T) {
	events, _ := runMarket(t, 8)

	var trades int
	for _, ev := range events {
		if ev.Kind == EventTrade {
			trades++
		}
	}
	assert.InDelta(t, 0.30, float64(trades)/float64(len(events)), 0.03)
}

func TestSameSeedSameEvents(t *testing.T) {
	a, _ := runMarket(t, 9)
	b, _ := runMarket(t, 9)
	c, _ := runMarket(t, 10)

	assert.Equal(t, a, b)
	assert.NotEqual(t, a, c)
}

func TestCurrentReturnsTheLastPublishedBook(t *testing.T) {
	market, _ := newTestMarket(t, 11)

	_, _, ok := market.Current(0)
	assert.False(t, ok, "nothing published yet")

	book := market.Book(0)
	bids, asks, ok := market.Current(0)
	require.True(t, ok)
	assert.Equal(t, book.Bids, bids)
	assert.Equal(t, book.Asks, asks)
}

func TestNewMarketNeedsAnInstrument(t *testing.T) {
	market, err := NewMarket(nil, newRand(1))

	assert.Nil(t, market)
	assert.ErrorContains(t, err, "no instruments")
}
