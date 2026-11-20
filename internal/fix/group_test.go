package fix

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mustDecode encodes body and decodes it back, failing the test on any error.
func mustDecode(t *testing.T, msgType string, body []Field) Message {
	t.Helper()
	raw, err := Encode(msgType, body)
	require.NoError(t, err)
	msg, err := Decode(raw)
	require.NoError(t, err, readable(raw))
	return msg
}

// level is the part of an order book or trade entry the tests compare.
type level struct {
	entryType, position, price, size string
}

func levels(entries []Entry) []level {
	var out []level
	for _, e := range entries {
		entryType, _ := e.Get(TagMDEntryType)
		position, _ := e.Get(TagMDEntryPositionNo)
		price, _ := e.Get(TagMDEntryPx)
		size, _ := e.Get(TagMDEntrySize)
		out = append(out, level{entryType, position, price, size})
	}
	return out
}

func TestEntriesOrderBook(t *testing.T) {
	msg := mustDecode(t, "X", orderBookBody())

	entries, err := msg.Entries(IncrementalEntries)
	require.NoError(t, err)
	assert.Equal(t, []level{
		{"0", "1", "24900", "1000"},
		{"0", "2", "24800", "2500"},
		{"0", "3", "24700", "500"},
		{"1", "1", "25000", "1200"},
		{"1", "2", "25100", "800"},
		{"1", "3", "25200", "3000"},
	}, levels(entries))
}

func TestEntriesTrade(t *testing.T) {
	msg := mustDecode(t, "X", []Field{
		{TagSecurityID, "XX000000AAA9"},
		{TagTotalVolumeTraded, "15300"},
		{TagGrossTradeAmt, "382350000"},
		{TagNoMDEntries, "4"},
		{TagMDUpdateAction, "0"}, {TagMDEntryType, "2"}, {TagMDEntryPx, "25000"}, {TagMDEntrySize, "300"},
		{TagMDUpdateAction, "0"}, {TagMDEntryType, "4"}, {TagMDEntryPx, "24900"},
		{TagMDUpdateAction, "0"}, {TagMDEntryType, "7"}, {TagMDEntryPx, "25100"},
		{TagMDUpdateAction, "0"}, {TagMDEntryType, "8"}, {TagMDEntryPx, "24800"},
	})

	entries, err := msg.Entries(IncrementalEntries)
	require.NoError(t, err)
	assert.Equal(t, []level{
		{"2", "", "25000", "300"},
		{"4", "", "24900", ""},
		{"7", "", "25100", ""},
		{"8", "", "24800", ""},
	}, levels(entries), "entries of different sizes stay separate")
}

func TestEntriesSnapshotUsesItsOwnDelimiter(t *testing.T) {
	msg := mustDecode(t, "W", []Field{
		{TagSecurityID, "XX000000AAA9"},
		{TagNoMDEntries, "2"},
		{TagMDEntryType, "0"}, {TagMDEntryPx, "24900"}, {TagMDEntrySize, "1000"},
		{TagMDEntryType, "1"}, {TagMDEntryPx, "25000"}, {TagMDEntrySize, "1200"},
	})

	entries, err := msg.Entries(SnapshotEntries)
	require.NoError(t, err)
	assert.Equal(t, []level{
		{"0", "", "24900", "1000"},
		{"1", "", "25000", "1200"},
	}, levels(entries))
}

func TestEntriesEndAtFirstNonMember(t *testing.T) {
	msg := mustDecode(t, "X", []Field{
		{TagNoMDEntries, "1"},
		{TagMDUpdateAction, "0"}, {TagMDEntryType, "2"}, {TagMDEntryPx, "25000"},
		{TagTransactTime, "20260928-02:15:00.120"},
	})

	entries, err := msg.Entries(IncrementalEntries)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Len(t, entries[0], 3, "tag 60 belongs to the message, not to the entry")
	_, inEntry := entries[0].Get(TagTransactTime)
	assert.False(t, inEntry)
}

func TestEntriesWithoutGroup(t *testing.T) {
	msg, err := Decode(wire(goldenSecurityDefinition))
	require.NoError(t, err)

	entries, err := msg.Entries(IncrementalEntries)
	assert.NoError(t, err)
	assert.Nil(t, entries)
}

func TestEntriesEmptyGroup(t *testing.T) {
	// FIX 4.4 defines NumInGroup as positive; a zero count still carries no
	// entries, so it reads as an empty group rather than an error.
	msg := mustDecode(t, "X", []Field{{TagSecurityID, "XX000000AAA9"}, {TagNoMDEntries, "0"}})

	entries, err := msg.Entries(IncrementalEntries)
	assert.NoError(t, err)
	assert.Empty(t, entries)
}

func TestEntriesCountWithLeadingZero(t *testing.T) {
	msg := mustDecode(t, "X", []Field{
		{TagNoMDEntries, "01"},
		{TagMDUpdateAction, "0"}, {TagMDEntryType, "2"}, {TagMDEntryPx, "25000"},
	})

	entries, err := msg.Entries(IncrementalEntries)
	require.NoError(t, err)
	assert.Len(t, entries, 1)
}

func TestEntriesRejects(t *testing.T) {
	tests := []struct {
		name    string
		body    []Field
		wantErr string
	}{
		{
			name: "fewer entries than declared",
			body: []Field{
				{TagNoMDEntries, "3"},
				{TagMDUpdateAction, "0"}, {TagMDEntryType, "0"}, {TagMDEntryPx, "24900"},
				{TagMDUpdateAction, "0"}, {TagMDEntryType, "1"}, {TagMDEntryPx, "25000"},
			},
			wantErr: "tag 268 declares 3 entries, found 2",
		},
		{
			name: "more entries than declared",
			body: []Field{
				{TagNoMDEntries, "1"},
				{TagMDUpdateAction, "0"}, {TagMDEntryType, "0"}, {TagMDEntryPx, "24900"},
				{TagMDUpdateAction, "0"}, {TagMDEntryType, "1"}, {TagMDEntryPx, "25000"},
			},
			wantErr: "tag 268 declares 1 entries, found 2",
		},
		{
			name: "first entry does not start with the delimiter",
			body: []Field{
				{TagNoMDEntries, "1"},
				{TagMDEntryType, "0"}, {TagMDUpdateAction, "0"}, {TagMDEntryPx, "24900"},
			},
			wantErr: "tag 268 must be followed by tag 279, got 269",
		},
		{
			name: "second entry lost its delimiter",
			body: []Field{
				{TagNoMDEntries, "2"},
				{TagMDUpdateAction, "0"}, {TagMDEntryType, "0"}, {TagMDEntryPx, "24900"},
				{TagMDEntryType, "1"}, {TagMDEntryPx, "25000"},
			},
			wantErr: "entry 1 repeats tag 269, delimiter 279 missing",
		},
		{
			name:    "count is not a number",
			body:    []Field{{TagNoMDEntries, "six"}},
			wantErr: `tag 268 count "six" is not a number`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := mustDecode(t, "X", tc.body)
			entries, err := msg.Entries(IncrementalEntries)
			assert.Nil(t, entries)
			assert.ErrorIs(t, err, ErrGroup)
			assert.ErrorContains(t, err, tc.wantErr)
		})
	}
}
