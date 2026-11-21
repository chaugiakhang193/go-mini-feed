package fix

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// bodyBytes writes fields as wire bytes without any framing, the shape of the
// input FuzzEntries expects.
func bodyBytes(fields []Field) []byte {
	var b []byte
	for _, f := range fields {
		b = appendField(b, f.Tag, f.Value)
	}
	return b
}

// tradeBody is an incremental refresh whose entries differ in size: a trade
// followed by the opening, high and low prices.
func tradeBody() []Field {
	return []Field{
		{TagSecurityID, "XX000000AAA9"},
		{TagTotalVolumeTraded, "15300"},
		{TagGrossTradeAmt, "382350000"},
		{TagNoMDEntries, "4"},
		{TagMDUpdateAction, "0"}, {TagMDEntryType, "2"}, {TagMDEntryPx, "25000"}, {TagMDEntrySize, "300"},
		{TagMDUpdateAction, "0"}, {TagMDEntryType, "4"}, {TagMDEntryPx, "24900"},
		{TagMDUpdateAction, "0"}, {TagMDEntryType, "7"}, {TagMDEntryPx, "25100"},
		{TagMDUpdateAction, "0"}, {TagMDEntryType, "8"}, {TagMDEntryPx, "24800"},
	}
}

// snapshotBody is a full refresh (35=W) whose entries start at tag 269.
func snapshotBody() []Field {
	return []Field{
		{TagSecurityID, "XX000000AAA9"},
		{TagNoMDEntries, "2"},
		{TagMDEntryType, "0"}, {TagMDEntryPx, "24900"}, {TagMDEntrySize, "1000"},
		{TagMDEntryType, "1"}, {TagMDEntryPx, "25000"}, {TagMDEntrySize, "1200"},
	}
}

// FuzzDecode feeds raw bytes to Decode. A random input almost never carries a
// BodyLength and CheckSum that match its bytes, so the fuzzer mostly exercises
// the field splitting that runs before those checks. What it can reach is
// held to one property: a message Decode accepts survives Encode and Decode
// again with the same fields from tag 35 up to CheckSum. The comparison starts
// after BodyLength because Decode accepts leading zeros there and Encode never
// writes them.
func FuzzDecode(f *testing.F) {
	f.Add(wire(goldenSecurityDefinition))
	f.Add(wire(goldenHeartbeat))
	book, err := Encode("X", orderBookBody())
	require.NoError(f, err)
	f.Add(book)

	f.Fuzz(func(t *testing.T, raw []byte) {
		msg, err := Decode(raw)
		if err != nil {
			return
		}

		last := len(msg.fields) - 1
		again, err := Encode(msg.MsgType(), msg.fields[3:last])
		require.NoError(t, err, "re-encoding a decoded message")
		msg2, err := Decode(again)
		require.NoError(t, err, readable(again))
		assert.True(t, slices.Equal(msg.fields[2:last], msg2.fields[2:len(msg2.fields)-1]),
			"fields changed through Encode and Decode")
	})
}

// FuzzEntries feeds the part of a message after tag 35 to Entries. Raw bytes
// fed to Decode hardly ever get past BodyLength and CheckSum, so the repeating
// groups would stay untouched. Splitting the body directly puts every
// mutation in front of the group logic. Both specs run on each input, because
// the two groups read the same fields with different delimiters.
func FuzzEntries(f *testing.F) {
	f.Add(bodyBytes(orderBookBody()))
	f.Add(bodyBytes(tradeBody()))
	f.Add(bodyBytes(snapshotBody()))

	f.Fuzz(func(t *testing.T, body []byte) {
		fields, err := Split(body)
		if err != nil {
			return
		}
		msg := Message{fields: fields}

		for _, spec := range []GroupSpec{IncrementalEntries, SnapshotEntries} {
			entries, err := msg.Entries(spec)
			if err != nil {
				require.ErrorIs(t, err, ErrGroup)
				continue
			}
			checkGroup(t, fields, spec, entries)
		}
	})
}

// checkGroup holds entries, returned without error, to what Entries promises:
// as many entries as the count field declares, each opening at the delimiter
// tag, built only from member tags without repeating one, and together
// exactly the run of fields right after the count field.
func checkGroup(t *testing.T, fields []Field, spec GroupSpec, entries []Entry) {
	t.Helper()
	countAt := slices.IndexFunc(fields, func(f Field) bool { return f.Tag == spec.CountTag })
	if countAt < 0 {
		assert.Nil(t, entries, "no count field, so no group")
		return
	}
	declared, ok := parseCount(fields[countAt].Value)
	require.True(t, ok, "Entries accepted count %q", fields[countAt].Value)
	assert.Len(t, entries, declared)

	var rebuilt []Field
	for i, e := range entries {
		require.NotEmpty(t, e, "entry %d", i+1)
		assert.Equal(t, spec.DelimiterTag, e[0].Tag, "entry %d opens at the delimiter", i+1)
		seen := make(map[int]bool)
		for _, f := range e {
			assert.Contains(t, spec.Members, f.Tag, "entry %d", i+1)
			assert.False(t, seen[f.Tag], "entry %d repeats tag %d", i+1, f.Tag)
			seen[f.Tag] = true
		}
		rebuilt = append(rebuilt, e...)
	}

	after := fields[countAt+1:]
	require.LessOrEqual(t, len(rebuilt), len(after), "entries hold more fields than follow the count")
	assert.True(t, slices.Equal(after[:len(rebuilt)], rebuilt),
		"entries are not the run of fields after the count")
}
