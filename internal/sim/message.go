package sim

import (
	"strconv"
	"time"

	"github.com/chaugiakhang193/go-mini-feed/internal/fix"
)

// Identity of the simulated market and the receiver, written to every packet.
const (
	senderCompID = "DEMO"
	targetCompID = "LAB"

	boardMain         = "MAIN"
	sessionContinuous = "CONT"

	// securityIDSourceISIN is FIX SecurityIDSource 4.
	securityIDSourceISIN = "4"

	// actionNew is MDUpdateAction 0. The simulator only adds entries.
	actionNew = "0"
)

// MDEntryType values the simulator writes.
const (
	entryBid   = "0"
	entryOffer = "1"
	entryTrade = "2"
	entryOpen  = "4"
	entryHigh  = "7"
	entryLow   = "8"
)

// timeLayout is the FIX UTCTimestamp with milliseconds. It carries the date;
// a time of day alone leaves the receiver unable to order packets across
// midnight.
const timeLayout = "20060102-15:04:05.000"

func formatTime(t time.Time) string {
	return t.UTC().Format(timeLayout)
}

// Header carries the fields every packet starts with after the framing.
type Header struct {
	Seq         int
	SendingTime time.Time
}

func (h Header) fields() []fix.Field {
	return []fix.Field{
		field(fix.TagSenderCompID, senderCompID),
		field(fix.TagTargetCompID, targetCompID),
		intField(fix.TagMsgSeqNum, int64(h.Seq)),
		field(fix.TagSendingTime, formatTime(h.SendingTime)),
	}
}

// Level is one price level of an order book side.
type Level struct {
	Price int64
	Size  int64
}

// PriceEntry is a price-only entry of a trade message: the opening price or a
// new high or low.
type PriceEntry struct {
	Type  string
	Price int64
}

// Fill is one trade with the running totals of its instrument.
type Fill struct {
	Price       int64
	Size        int64
	TotalVolume int64 // sum of sizes so far today
	TotalValue  int64 // sum of price times size so far today
	Extras      []PriceEntry
}

func field(tag int, value string) fix.Field {
	return fix.Field{Tag: tag, Value: value}
}

func intField(tag int, value int64) fix.Field {
	return fix.Field{Tag: tag, Value: strconv.FormatInt(value, 10)}
}

// SecurityDefinition builds a 35=d packet that announces an instrument and
// its price band.
func SecurityDefinition(h Header, in Instrument) ([]byte, error) {
	body := h.fields()
	body = append(body,
		field(fix.TagSymbol, in.Symbol),
		field(fix.TagSecurityID, in.SecurityID),
		field(fix.TagSecurityIDSource, securityIDSourceISIN),
		field(fix.TagMarketSegmentID, boardMain),
		intField(fix.TagPrevClosePx, in.RefPrice),
		intField(fix.TagHighLimitPrice, in.High),
		intField(fix.TagLowLimitPrice, in.Low),
	)
	return fix.Encode("d", body)
}

// marketFields are the fields that follow the header in every market data
// packet. A 35=X or 35=W packet names its instrument by SecurityID alone, so
// the receiver has to remember the symbol from the 35=d packet.
func marketFields(securityID string, at time.Time) []fix.Field {
	return []fix.Field{
		field(fix.TagSecurityID, securityID),
		field(fix.TagMarketSegmentID, boardMain),
		field(fix.TagTradingSessionID, sessionContinuous),
		field(fix.TagTransactTime, formatTime(at)),
	}
}

// OrderBook builds a 35=X packet with the given bid levels followed by the
// ask levels. Every entry opens with MDUpdateAction.
func OrderBook(h Header, securityID string, at time.Time, bids, asks []Level) ([]byte, error) {
	body := h.fields()
	body = append(body, marketFields(securityID, at)...)
	body = append(body, intField(fix.TagNoMDEntries, int64(len(bids)+len(asks))))
	body = appendLevels(body, true, entryBid, bids)
	body = appendLevels(body, true, entryOffer, asks)
	return fix.Encode("X", body)
}

// Snapshot builds a 35=W packet with the same levels as OrderBook. A snapshot
// entry has no MDUpdateAction and opens with MDEntryType.
func Snapshot(h Header, securityID string, at time.Time, bids, asks []Level) ([]byte, error) {
	body := h.fields()
	body = append(body, marketFields(securityID, at)...)
	body = append(body, intField(fix.TagNoMDEntries, int64(len(bids)+len(asks))))
	body = appendLevels(body, false, entryBid, bids)
	body = appendLevels(body, false, entryOffer, asks)
	return fix.Encode("W", body)
}

// appendLevels writes one entry per level. MDEntryPositionNo counts from 1
// within the side.
func appendLevels(body []fix.Field, withAction bool, entryType string, levels []Level) []fix.Field {
	for i, l := range levels {
		if withAction {
			body = append(body, field(fix.TagMDUpdateAction, actionNew))
		}
		body = append(body,
			field(fix.TagMDEntryType, entryType),
			intField(fix.TagMDEntryPositionNo, int64(i+1)),
			intField(fix.TagMDEntryPx, l.Price),
			intField(fix.TagMDEntrySize, l.Size),
		)
	}
	return body
}

// Trade builds a 35=X packet for one trade. The trade entry carries price and
// size. Price-only entries for the opening price and for a new high or low
// follow it when fill.Extras lists them.
func Trade(h Header, securityID string, at time.Time, fill Fill) ([]byte, error) {
	body := h.fields()
	body = append(body, marketFields(securityID, at)...)
	body = append(body,
		intField(fix.TagTotalVolumeTraded, fill.TotalVolume),
		intField(fix.TagGrossTradeAmt, fill.TotalValue),
		intField(fix.TagNoMDEntries, int64(1+len(fill.Extras))),
		field(fix.TagMDUpdateAction, actionNew),
		field(fix.TagMDEntryType, entryTrade),
		intField(fix.TagMDEntryPx, fill.Price),
		intField(fix.TagMDEntrySize, fill.Size),
	)
	for _, e := range fill.Extras {
		body = append(body,
			field(fix.TagMDUpdateAction, actionNew),
			field(fix.TagMDEntryType, e.Type),
			intField(fix.TagMDEntryPx, e.Price),
		)
	}
	return fix.Encode("X", body)
}

// Heartbeat builds a 35=0 packet.
func Heartbeat(h Header) ([]byte, error) {
	return fix.Encode("0", h.fields())
}
