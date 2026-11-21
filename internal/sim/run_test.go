package sim

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/chaugiakhang193/go-mini-feed/internal/fix"
)

type sentPacket struct {
	kind Kind
	pkt  []byte
}

// recorder keeps every packet it is asked to send. Run calls Send from one
// goroutine, so no lock is needed.
type recorder struct {
	got []sentPacket
}

func (r *recorder) Send(kind Kind, pkt []byte) error {
	r.got = append(r.got, sentPacket{kind, pkt})
	return nil
}

func (r *recorder) kinds() []Kind {
	var out []Kind
	for _, p := range r.got {
		out = append(out, p.kind)
	}
	return out
}

// failingSender refuses every packet of one kind.
type failingSender struct {
	recorder
	failKind Kind
}

func (f *failingSender) Send(kind Kind, pkt []byte) error {
	if kind == f.failKind {
		return errors.New("network unreachable")
	}
	return f.recorder.Send(kind, pkt)
}

func testConfig(t *testing.T, scenario Scenario) Config {
	t.Helper()
	instruments, err := Instruments(2, newRand(1))
	require.NoError(t, err)
	return Config{
		Scenario:    scenario,
		Instruments: instruments,
		Seed:        1,
		Rate:        1000,
		Now:         func() time.Time { return at(2, 15, 0, 123) },
	}
}

func decodeAll(t *testing.T, packets []sentPacket) []fix.Message {
	t.Helper()
	var out []fix.Message
	for _, p := range packets {
		msg, err := fix.Decode(p.pkt)
		require.NoError(t, err, readable(p.pkt))
		out = append(out, msg)
	}
	return out
}

func seqOf(t *testing.T, msg fix.Message) int {
	t.Helper()
	text, ok := msg.Get(fix.TagMsgSeqNum)
	require.True(t, ok)
	n, err := strconv.Atoi(text)
	require.NoError(t, err)
	return n
}

func TestRunOneTickSendsDefinitionBookAndTrade(t *testing.T) {
	cfg := testConfig(t, OneTick)
	cfg.Drop, cfg.Dup, cfg.Reorder = 1, 1, 1 // one-tick ignores faults
	var rec recorder

	stats, err := Run(context.Background(), cfg, &rec)
	require.NoError(t, err)

	assert.Equal(t, []Kind{KindSecDef, KindPrice, KindPrice}, rec.kinds())
	msgs := decodeAll(t, rec.got)
	assert.Equal(t, "d", msgs[0].MsgType())
	assert.Equal(t, "X", msgs[1].MsgType())
	assert.Equal(t, "X", msgs[2].MsgType())
	assert.Equal(t, []int{1, 1, 2}, []int{seqOf(t, msgs[0]), seqOf(t, msgs[1]), seqOf(t, msgs[2])},
		"34 counts each kind on its own")

	symbol, _ := msgs[0].Get(fix.TagSymbol)
	assert.Equal(t, cfg.Instruments[0].Symbol, symbol, "the first instrument")

	book, err := msgs[1].Entries(fix.IncrementalEntries)
	require.NoError(t, err)
	require.Len(t, book, 6)
	bestAsk, _ := book[3].Get(fix.TagMDEntryPx)
	trade, err := msgs[2].Entries(fix.IncrementalEntries)
	require.NoError(t, err)
	require.NotEmpty(t, trade)
	tradePrice, _ := trade[0].Get(fix.TagMDEntryPx)
	assert.Equal(t, bestAsk, tradePrice, "the trade is at the best ask")

	assert.Equal(t, 1, stats[KindSecDef].Sent)
	assert.Equal(t, 2, stats[KindPrice].Sent)
	assert.Zero(t, stats[KindPrice].Dropped)
}

func TestRunStreamSendsTheRequestedNumberOfPricePackets(t *testing.T) {
	cfg := testConfig(t, Stream)
	cfg.Count = 50
	var rec recorder

	stats, err := Run(context.Background(), cfg, &rec)
	require.NoError(t, err)

	assert.Equal(t, 50, stats[KindPrice].Built)
	assert.Equal(t, 50, stats[KindPrice].Sent)
	assert.Zero(t, stats[KindPrice].SendErrors)
	assert.Equal(t, len(cfg.Instruments), stats[KindSecDef].Sent, "one definition per instrument")

	var prices int
	for _, k := range rec.kinds() {
		if k == KindPrice {
			prices++
		}
	}
	assert.Equal(t, 50, prices)
	assert.Equal(t, []Kind{KindSecDef, KindSecDef}, rec.kinds()[:2], "definitions come first")
	decodeAll(t, rec.got)
}

func TestRunStreamSendsSnapshotsAndHeartbeats(t *testing.T) {
	cfg := testConfig(t, Stream)
	cfg.Rate = 100
	cfg.Count = 20
	cfg.SnapshotEvery = 10 * time.Millisecond
	cfg.HeartbeatEvery = 10 * time.Millisecond
	var rec recorder

	stats, err := Run(context.Background(), cfg, &rec)
	require.NoError(t, err)

	assert.Positive(t, stats[KindSnapshot].Sent)
	assert.Positive(t, stats[KindHeartbeat].Sent)
	var heartbeats, snapshots int
	for i, msg := range decodeAll(t, rec.got) {
		switch rec.got[i].kind {
		case KindHeartbeat:
			assert.Equal(t, "0", msg.MsgType())
			heartbeats++
		case KindSnapshot:
			assert.Equal(t, "W", msg.MsgType())
			entries, err := msg.Entries(fix.SnapshotEntries)
			assert.NoError(t, err)
			assert.Len(t, entries, 6)
			snapshots++
		}
	}
	assert.Equal(t, stats[KindHeartbeat].Sent, heartbeats)
	assert.Equal(t, stats[KindSnapshot].Sent, snapshots)
}

func TestRunStreamFaultsShowUpInSequenceNumbers(t *testing.T) {
	cfg := testConfig(t, Stream)
	cfg.Count = 200
	cfg.Drop, cfg.Dup, cfg.Reorder = 0.1, 0.05, 0.05
	var rec recorder

	stats, err := Run(context.Background(), cfg, &rec)
	require.NoError(t, err)

	price := stats[KindPrice]
	assert.Equal(t, 200, price.Built)
	assert.Positive(t, price.Dropped)
	assert.Positive(t, price.Duplicated)
	assert.Positive(t, price.Reordered)
	assert.Equal(t, price.Built-price.Dropped+price.Duplicated, price.Sent)

	var seqs []int
	for i, msg := range decodeAll(t, rec.got) {
		if rec.got[i].kind == KindPrice {
			seqs = append(seqs, seqOf(t, msg))
		}
	}
	distinct := make(map[int]bool)
	var outOfOrder int
	for i, s := range seqs {
		distinct[s] = true
		if i > 0 && s < seqs[i-1] {
			outOfOrder++
		}
	}
	assert.Len(t, distinct, price.Built-price.Dropped, "a dropped packet leaves a gap in 34")
	assert.Positive(t, outOfOrder, "a reordered packet arrives behind a higher 34")
}

func TestRunFlushesHeldPacketsAtTheEnd(t *testing.T) {
	cfg := testConfig(t, Stream)
	cfg.Count = 5
	cfg.Reorder = 1
	var rec recorder

	stats, err := Run(context.Background(), cfg, &rec)
	require.NoError(t, err)

	assert.Equal(t, 5, stats[KindPrice].Sent, "the fifth packet was held and is released at the end")
}

func TestRunCountsSendErrorsAndKeepsGoing(t *testing.T) {
	cfg := testConfig(t, Stream)
	cfg.Count = 10
	sender := &failingSender{failKind: KindPrice}

	stats, err := Run(context.Background(), cfg, sender)
	require.NoError(t, err, "a send error is counted, not returned")

	assert.Equal(t, 10, stats[KindPrice].Built)
	assert.Equal(t, 10, stats[KindPrice].SendErrors)
	assert.Zero(t, stats[KindPrice].Sent)
	assert.Equal(t, len(cfg.Instruments), stats[KindSecDef].Sent)
}

func TestRunStopsPromptlyWhenTheContextIsCancelled(t *testing.T) {
	cfg := testConfig(t, Stream)
	cfg.Rate = 1 // one price packet per second: only the context can end the run
	var rec recorder
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := Run(ctx, cfg, &rec)
		assert.NoError(t, err)
	}()
	time.Sleep(20 * time.Millisecond)
	cancelled := time.Now()
	cancel()

	select {
	case <-done:
		assert.Less(t, time.Since(cancelled), 100*time.Millisecond)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after the context was cancelled")
	}
}

func TestRunRejectsBadConfig(t *testing.T) {
	tests := []struct {
		name    string
		change  func(*Config)
		wantErr string
	}{
		{"unknown scenario", func(c *Config) { c.Scenario = "burst" }, `unknown scenario "burst"`},
		{"no instruments", func(c *Config) { c.Instruments = nil }, "no instruments"},
		{"rate zero", func(c *Config) { c.Scenario = Stream; c.Rate = 0 }, "rate 0 out of range"},
		{"rate above the cap", func(c *Config) { c.Scenario = Stream; c.Rate = MaxRate + 1 }, "out of range"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t, OneTick)
			tc.change(&cfg)
			var rec recorder

			_, err := Run(context.Background(), cfg, &rec)
			assert.ErrorContains(t, err, tc.wantErr)
			assert.Empty(t, rec.got)
		})
	}
}

func TestKindNamesMatchTheMulticastGroups(t *testing.T) {
	var names []string
	for _, k := range Kinds() {
		names = append(names, k.String())
	}
	assert.Equal(t, []string{"security_definition", "price", "snapshot", "heartbeat"}, names)
}
