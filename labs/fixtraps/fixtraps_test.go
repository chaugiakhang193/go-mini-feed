package fixtraps

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/chaugiakhang193/go-mini-feed/internal/fix"
)

// tagRptSeq is RptSeq (83). The lab message deliberately omits it.
const tagRptSeq = 83

func field(tag int, value string) fix.Field {
	return fix.Field{Tag: tag, Value: value}
}

func entryPrices(t *testing.T, raw []byte) []string {
	t.Helper()
	msg, err := fix.Decode(raw)
	require.NoError(t, err)
	entries, err := msg.Entries(fix.IncrementalEntries)
	require.NoError(t, err)

	var prices []string
	for _, e := range entries {
		price, _ := e.Get(fix.TagMDEntryPx)
		prices = append(prices, price)
	}
	return prices
}

func TestMapParseKeepsOnlyTheLastEntry(t *testing.T) {
	// Three bid levels (269=0) followed by three ask levels (269=1).
	raw, err := fix.Encode("X", []fix.Field{
		field(fix.TagSecurityID, "XX000000AAA9"),
		field(fix.TagNoMDEntries, "6"),
		field(fix.TagMDUpdateAction, "0"), field(fix.TagMDEntryType, "0"), field(fix.TagMDEntryPx, "24900"),
		field(fix.TagMDUpdateAction, "0"), field(fix.TagMDEntryType, "0"), field(fix.TagMDEntryPx, "24800"),
		field(fix.TagMDUpdateAction, "0"), field(fix.TagMDEntryType, "0"), field(fix.TagMDEntryPx, "24700"),
		field(fix.TagMDUpdateAction, "0"), field(fix.TagMDEntryType, "1"), field(fix.TagMDEntryPx, "25000"),
		field(fix.TagMDUpdateAction, "0"), field(fix.TagMDEntryType, "1"), field(fix.TagMDEntryPx, "25100"),
		field(fix.TagMDUpdateAction, "0"), field(fix.TagMDEntryType, "1"), field(fix.TagMDEntryPx, "25200"),
	})
	require.NoError(t, err)

	fields := MapParse(raw)
	assert.Equal(t, "25200", fields[fix.TagMDEntryPx], "only the third ask price survives")
	assert.Equal(t, "1", fields[fix.TagMDEntryType], "every level now looks like an ask")

	assert.Equal(t, []string{"24900", "24800", "24700", "25000", "25100", "25200"}, entryPrices(t, raw))
}

func TestFindTagMatchesInsideALongerTag(t *testing.T) {
	raw, err := fix.Encode("X", []fix.Field{
		field(fix.TagSecurityID, "XX000000AAA9"),
		field(fix.TagNoMDEntries, "1"),
		field(fix.TagMDUpdateAction, "0"),
		field(fix.TagMDEntryType, "0"),
		field(fix.TagLocationID, "L1"),
		field(fix.TagMDEntryPx, "24900"),
	})
	require.NoError(t, err)

	value, found := FindTag(raw, tagRptSeq)
	assert.True(t, found, "the message has no tag 83, yet the substring search finds one")
	assert.Equal(t, "L1", value, "the value of LocationID (283)")

	msg, err := fix.Decode(raw)
	require.NoError(t, err)
	_, found = msg.Get(tagRptSeq)
	assert.False(t, found, "comparing whole tags finds no tag 83")
	assert.Equal(t, []string{"24900"}, entryPrices(t, raw))
}
