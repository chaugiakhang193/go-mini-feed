package sim

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/chaugiakhang193/go-mini-feed/internal/fix"
)

// wantLevel is one order book entry as the tests expect to read it back.
type wantLevel struct{ entryType, position, price, size string }

// wantExtra is one price-only entry of a trade message.
type wantExtra struct{ entryType, price string }

// tagValue pairs a tag with the value an entry should hold for it.
type tagValue struct {
	tag  int
	want string
}

func readable(b []byte) string {
	return strings.ReplaceAll(string(b), string(fix.SOH), "|")
}

func mustDecode(t *testing.T, raw []byte, err error) fix.Message {
	t.Helper()
	require.NoError(t, err)
	msg, err := fix.Decode(raw)
	require.NoError(t, err, readable(raw))
	return msg
}

func at(hour, min, sec, millis int) time.Time {
	return time.Date(2026, time.September, 28, hour, min, sec, millis*int(time.Millisecond), time.UTC)
}

var (
	bids = []Level{{24900, 1000}, {24800, 2500}, {24700, 500}}
	asks = []Level{{25000, 1200}, {25100, 800}, {25200, 3000}}

	testInstrument = Instrument{
		Symbol: "AAA", SecurityID: "XX000000AAA9",
		RefPrice: 25000, High: 26700, Low: 23300,
	}
	bookHeader = Header{Seq: 2, SendingTime: at(2, 15, 0, 123)}
)

// The golden messages were encoded and checksummed outside Go.
func TestSecurityDefinitionGolden(t *testing.T) {
	raw, err := SecurityDefinition(Header{Seq: 1, SendingTime: at(1, 30, 0, 0)}, testInstrument)
	require.NoError(t, err)

	assert.Equal(t, "8=FIX.4.4|9=120|35=d|49=DEMO|56=LAB|34=1|52=20260928-01:30:00.000|"+
		"55=AAA|48=XX000000AAA9|22=4|1300=MAIN|140=25000|1149=26700|1148=23300|10=208|", readable(raw))
}

func TestHeartbeatGolden(t *testing.T) {
	raw, err := Heartbeat(Header{Seq: 59, SendingTime: at(2, 0, 59, 0)})
	require.NoError(t, err)

	assert.Equal(t, "8=FIX.4.4|9=51|35=0|49=DEMO|56=LAB|34=59|52=20260928-02:00:59.000|10=000|", readable(raw))
}

func TestSendingTimeIsUTCWithDate(t *testing.T) {
	local := time.FixedZone("UTC+7", 7*60*60)
	sent := time.Date(2026, time.September, 28, 9, 5, 7, 8*int(time.Millisecond), local)

	assert.Equal(t, "20260928-02:05:07.008", formatTime(sent))
}

func TestOrderBook(t *testing.T) {
	raw, err := OrderBook(bookHeader, testInstrument.SecurityID, at(2, 15, 0, 120), bids, asks)
	msg := mustDecode(t, raw, err)

	assert.Equal(t, "X", msg.MsgType())
	assertHeader(t, msg, "2", "20260928-02:15:00.123")
	assertValue(t, msg, fix.TagSecurityID, "XX000000AAA9")
	assertValue(t, msg, fix.TagMarketSegmentID, "MAIN")
	assertValue(t, msg, fix.TagTradingSessionID, "CONT")
	assertValue(t, msg, fix.TagTransactTime, "20260928-02:15:00.120")
	assertValue(t, msg, fix.TagNoMDEntries, "6")
	_, hasSymbol := msg.Get(fix.TagSymbol)
	assert.False(t, hasSymbol, "an incremental refresh names the instrument by tag 48 only")

	entries, err := msg.Entries(fix.IncrementalEntries)
	require.NoError(t, err)
	require.Len(t, entries, 6)
	for i, want := range []wantLevel{
		{"0", "1", "24900", "1000"},
		{"0", "2", "24800", "2500"},
		{"0", "3", "24700", "500"},
		{"1", "1", "25000", "1200"},
		{"1", "2", "25100", "800"},
		{"1", "3", "25200", "3000"},
	} {
		assertEntry(t, entries[i], "0", want.entryType, want.position, want.price, want.size)
		assert.Len(t, entries[i], 5, "entry %d: 279 269 290 270 271", i+1)
	}
}

func TestTradeWithPriceExtremes(t *testing.T) {
	fill := Fill{
		Price: 25000, Size: 300, TotalVolume: 15300, TotalValue: 382350000,
		Extras: []PriceEntry{{entryOpen, 24900}, {entryHigh, 25100}, {entryLow, 24800}},
	}
	raw, err := Trade(bookHeader, testInstrument.SecurityID, at(2, 15, 0, 120), fill)
	msg := mustDecode(t, raw, err)

	assert.Equal(t, "X", msg.MsgType())
	assertHeader(t, msg, "2", "20260928-02:15:00.123")
	assertValue(t, msg, fix.TagTotalVolumeTraded, "15300")
	assertValue(t, msg, fix.TagGrossTradeAmt, "382350000")
	assertValue(t, msg, fix.TagNoMDEntries, "4")

	entries, err := msg.Entries(fix.IncrementalEntries)
	require.NoError(t, err)
	require.Len(t, entries, 4)
	assertEntry(t, entries[0], "0", "2", "", "25000", "300")
	assert.Len(t, entries[0], 4, "trade entry: 279 269 270 271")
	for i, want := range []wantExtra{{"4", "24900"}, {"7", "25100"}, {"8", "24800"}} {
		e := entries[i+1]
		assertEntry(t, e, "0", want.entryType, "", want.price, "")
		assert.Len(t, e, 3, "price entry: 279 269 270")
	}
}

func TestTradeWithoutExtras(t *testing.T) {
	fill := Fill{Price: 25000, Size: 300, TotalVolume: 300, TotalValue: 7500000}
	raw, err := Trade(bookHeader, testInstrument.SecurityID, at(2, 15, 0, 120), fill)
	msg := mustDecode(t, raw, err)

	entries, err := msg.Entries(fix.IncrementalEntries)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assertValue(t, msg, fix.TagNoMDEntries, "1")
}

func TestSnapshot(t *testing.T) {
	raw, err := Snapshot(bookHeader, testInstrument.SecurityID, at(2, 15, 0, 120), bids, asks)
	msg := mustDecode(t, raw, err)

	assert.Equal(t, "W", msg.MsgType())
	assertHeader(t, msg, "2", "20260928-02:15:00.123")
	assertValue(t, msg, fix.TagNoMDEntries, "6")
	_, hasAction := msg.Get(fix.TagMDUpdateAction)
	assert.False(t, hasAction, "a snapshot has no update action")

	entries, err := msg.Entries(fix.SnapshotEntries)
	require.NoError(t, err)
	require.Len(t, entries, 6)
	assertEntry(t, entries[0], "", "0", "1", "24900", "1000")
	assertEntry(t, entries[5], "", "1", "3", "25200", "3000")
	assert.Len(t, entries[0], 4, "snapshot entry: 269 290 270 271")
}

// Entries stops at the first tag outside the group, so a stray tag in an entry
// shows up as a count that no longer matches.
func TestEveryMarketDataPacketPassesEntries(t *testing.T) {
	extras := []PriceEntry{{entryOpen, 24900}, {entryHigh, 25100}, {entryLow, 24800}}
	tests := []struct {
		name  string
		build func() ([]byte, error)
		spec  fix.GroupSpec
		count int
	}{
		{"book", func() ([]byte, error) {
			return OrderBook(bookHeader, "XX000000AAA9", at(2, 15, 0, 120), bids, asks)
		}, fix.IncrementalEntries, 6},
		{"one level per side", func() ([]byte, error) {
			return OrderBook(bookHeader, "XX000000AAA9", at(2, 15, 0, 120), bids[:1], asks[:1])
		}, fix.IncrementalEntries, 2},
		{"first trade of the day", func() ([]byte, error) {
			return Trade(bookHeader, "XX000000AAA9", at(2, 15, 0, 120), Fill{25000, 300, 300, 7500000, extras})
		}, fix.IncrementalEntries, 4},
		{"later trade", func() ([]byte, error) {
			return Trade(bookHeader, "XX000000AAA9", at(2, 15, 0, 120), Fill{25000, 300, 600, 15000000, nil})
		}, fix.IncrementalEntries, 1},
		{"snapshot", func() ([]byte, error) {
			return Snapshot(bookHeader, "XX000000AAA9", at(2, 15, 0, 120), bids, asks)
		}, fix.SnapshotEntries, 6},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := tc.build()
			msg := mustDecode(t, raw, err)

			entries, err := msg.Entries(tc.spec)
			require.NoError(t, err, readable(raw))
			assert.Len(t, entries, tc.count)
			declared, _ := msg.Get(fix.TagNoMDEntries)
			assert.Equal(t, strconv.Itoa(tc.count), declared)
		})
	}
}

func assertHeader(t *testing.T, msg fix.Message, seq, sendingTime string) {
	t.Helper()
	assertValue(t, msg, fix.TagSenderCompID, "DEMO")
	assertValue(t, msg, fix.TagTargetCompID, "LAB")
	assertValue(t, msg, fix.TagMsgSeqNum, seq)
	assertValue(t, msg, fix.TagSendingTime, sendingTime)
}

func assertValue(t *testing.T, msg fix.Message, tag int, want string) {
	t.Helper()
	got, ok := msg.Get(tag)
	assert.True(t, ok, "tag %d present", tag)
	assert.Equal(t, want, got, "tag %d", tag)
}

// assertEntry compares the entry's fields with the wanted values; "" means the
// entry does not carry that tag.
func assertEntry(t *testing.T, e fix.Entry, action, entryType, position, price, size string) {
	t.Helper()
	for _, c := range []tagValue{
		{fix.TagMDUpdateAction, action}, {fix.TagMDEntryType, entryType},
		{fix.TagMDEntryPositionNo, position}, {fix.TagMDEntryPx, price}, {fix.TagMDEntrySize, size},
	} {
		got, ok := e.Get(c.tag)
		if c.want == "" {
			assert.False(t, ok, "tag %d absent", c.tag)
			continue
		}
		assert.True(t, ok, "tag %d present", c.tag)
		assert.Equal(t, c.want, got, "tag %d", c.tag)
	}
}
