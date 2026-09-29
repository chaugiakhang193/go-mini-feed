package fix

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeGolden(t *testing.T) {
	msg, err := Decode(wire(goldenSecurityDefinition))
	require.NoError(t, err)

	assert.Equal(t, "d", msg.MsgType())
	symbol, ok := msg.Get(TagSymbol)
	assert.True(t, ok)
	assert.Equal(t, "AAA", symbol)
	bodyLength, _ := msg.Get(TagBodyLength)
	assert.Equal(t, "120", bodyLength)
	_, ok = msg.Get(TagMDEntryPx)
	assert.False(t, ok, "a tag the message does not carry is reported as missing")
}

func TestDecodeAcceptsLeadingZerosInBodyLength(t *testing.T) {
	// FIX 4.4 int fields may carry leading zeros; only tag numbers may not.
	// The extra "0" adds 48 to the byte sum: 208 + 48 = 256, so CheckSum 000.
	raw := strings.Replace(goldenSecurityDefinition, "|9=120|", "|9=0120|", 1)
	raw = strings.Replace(raw, "|10=208|", "|10=000|", 1)

	msg, err := Decode(wire(raw))
	require.NoError(t, err)
	assert.Equal(t, "d", msg.MsgType())
}

func TestGetReturnsTheFirstOccurrence(t *testing.T) {
	raw, err := Encode("X", orderBookBody())
	require.NoError(t, err)
	msg, err := Decode(raw)
	require.NoError(t, err)

	price, ok := msg.Get(TagMDEntryPx)
	assert.True(t, ok)
	assert.Equal(t, "24900", price, "the first of six MDEntryPx values")
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	body := orderBookBody()
	raw, err := Encode("X", body)
	require.NoError(t, err)

	msg, err := Decode(raw)
	require.NoError(t, err, readable(raw))
	assert.Equal(t, "X", msg.MsgType())
	// Framing aside (8, 9, 35 at the front, 10 at the end), the fields come
	// back in the same order with every repeated tag kept.
	assert.Equal(t, body, msg.fields[3:len(msg.fields)-1])
}

func TestSplitKeepsRepeatedTags(t *testing.T) {
	raw, err := Encode("X", orderBookBody())
	require.NoError(t, err)

	fields, err := Split(raw)
	require.NoError(t, err)
	var prices []string
	for _, f := range fields {
		if f.Tag == TagMDEntryPx {
			prices = append(prices, f.Value)
		}
	}
	assert.Equal(t, []string{"24900", "24800", "24700", "25000", "25100", "25200"}, prices)
}

func TestDecodeRejects(t *testing.T) {
	golden := goldenSecurityDefinition
	tests := []struct {
		name     string
		input    string
		wantKind error
		wantErr  string
	}{
		{
			name:     "empty input",
			input:    "",
			wantKind: ErrMalformed,
			wantErr:  "empty input",
		},
		{
			name:     "truncated: last SOH missing",
			input:    strings.TrimSuffix(golden, "|"),
			wantKind: ErrMalformed,
			wantErr:  "last field is not terminated by SOH",
		},
		{
			name:     "field without '='",
			input:    strings.Replace(golden, "|22=4|", "|224|", 1),
			wantKind: ErrMalformed,
			wantErr:  `missing '=' in "224"`,
		},
		{
			name:     "non-numeric tag",
			input:    strings.Replace(golden, "|22=4|", "|2a=4|", 1),
			wantKind: ErrMalformed,
			wantErr:  `invalid tag "2a"`,
		},
		{
			name:     "tag with a leading zero",
			input:    strings.Replace(golden, "|22=4|", "|022=4|", 1),
			wantKind: ErrMalformed,
			wantErr:  `invalid tag "022"`,
		},
		{
			name:     "tag with a sign",
			input:    strings.Replace(golden, "|22=4|", "|+22=4|", 1),
			wantKind: ErrMalformed,
			wantErr:  `invalid tag "+22"`,
		},
		{
			name:     "negative tag",
			input:    strings.Replace(golden, "|22=4|", "|-22=4|", 1),
			wantKind: ErrMalformed,
			wantErr:  `invalid tag "-22"`,
		},
		{
			name:     "tag zero",
			input:    strings.Replace(golden, "|22=4|", "|0=4|", 1),
			wantKind: ErrMalformed,
			wantErr:  `invalid tag "0"`,
		},
		{
			name:     "empty value",
			input:    strings.Replace(golden, "|22=4|", "|22=|", 1),
			wantKind: ErrMalformed,
			wantErr:  "tag 22 has an empty value",
		},
		{
			name:     "too few fields",
			input:    "8=FIX.4.4|9=5|35=0|",
			wantKind: ErrMalformed,
			wantErr:  "3 fields, need at least tags 8, 9, 35 and 10",
		},
		{
			name:     "first field is not BeginString",
			input:    strings.Replace(golden, "8=FIX.4.4|", "7=FIX.4.4|", 1),
			wantKind: ErrMalformed,
			wantErr:  "field 1 is tag 7, want 8",
		},
		{
			name:     "MsgType before BodyLength",
			input:    strings.Replace(golden, "|9=120|35=d|", "|35=d|9=120|", 1),
			wantKind: ErrMalformed,
			wantErr:  "field 2 is tag 35, want 9",
		},
		{
			name:     "MsgType not third",
			input:    strings.Replace(golden, "|35=d|49=DEMO|", "|49=DEMO|35=d|", 1),
			wantKind: ErrMalformed,
			wantErr:  "field 3 is tag 49, want 35",
		},
		// The four inputs below carry correct BodyLength and CheckSum values,
		// computed outside Go, so only the repeated framing tag is wrong. They
		// cover both ends of the body: right after 35 and right before 10.
		{
			name:     "BeginString repeated in the body",
			input:    "8=FIX.4.4|9=22|35=d|8=FIX.4.4|55=AAA|10=146|",
			wantKind: ErrMalformed,
			wantErr:  "framing tag 8 repeated at field 4",
		},
		{
			name:     "BodyLength repeated in the body",
			input:    "8=FIX.4.4|9=16|35=d|9=5|55=AAA|10=032|",
			wantKind: ErrMalformed,
			wantErr:  "framing tag 9 repeated at field 4",
		},
		{
			name:     "MsgType repeated in the body",
			input:    "8=FIX.4.4|9=17|35=d|35=X|55=AAA|10=115|",
			wantKind: ErrMalformed,
			wantErr:  "framing tag 35 repeated at field 4",
		},
		{
			name:     "CheckSum repeated before the real one",
			input:    "8=FIX.4.4|9=19|35=d|55=AAA|10=000|10=166|",
			wantKind: ErrMalformed,
			wantErr:  "framing tag 10 repeated at field 5",
		},
		{
			name:     "field after CheckSum",
			input:    golden + "58=late|",
			wantKind: ErrMalformed,
			wantErr:  "last field is tag 58, want 10",
		},
		{
			name:     "other FIX version",
			input:    strings.Replace(golden, "8=FIX.4.4|", "8=FIX.4.2|", 1),
			wantKind: ErrMalformed,
			wantErr:  `BeginString "FIX.4.2", want "FIX.4.4"`,
		},
		{
			name:     "BodyLength with a sign",
			input:    strings.Replace(golden, "|9=120|", "|9=+120|", 1),
			wantKind: ErrMalformed,
			wantErr:  `BodyLength "+120" is not a valid byte count`,
		},
		{
			name:     "BodyLength beyond int range",
			input:    strings.Replace(golden, "|9=120|", "|9=9223372036854775808|", 1),
			wantKind: ErrMalformed,
			wantErr:  `BodyLength "9223372036854775808" is not a valid byte count`,
		},
		{
			name:     "BodyLength one byte too long",
			input:    strings.Replace(golden, "|9=120|", "|9=121|", 1),
			wantKind: ErrBodyLength,
			wantErr:  "declared 121, counted 120",
		},
		{
			name:     "body one byte longer than declared",
			input:    strings.Replace(golden, "|55=AAA|", "|55=AAAA|", 1),
			wantKind: ErrBodyLength,
			wantErr:  "declared 120, counted 121",
		},
		{
			name:     "CheckSum off by one",
			input:    strings.Replace(golden, "|10=208|", "|10=209|", 1),
			wantKind: ErrChecksum,
			wantErr:  "declared 209, computed 208",
		},
		{
			name:     "CheckSum with four digits",
			input:    strings.Replace(golden, "|10=208|", "|10=0208|", 1),
			wantKind: ErrChecksum,
			wantErr:  "declared 0208, computed 208",
		},
		{
			name:     "CheckSum without zero padding",
			input:    strings.Replace(goldenHeartbeat, "|10=000|", "|10=0|", 1),
			wantKind: ErrChecksum,
			wantErr:  "declared 0, computed 000",
		},
		{
			name:     "one body byte changed, length unchanged",
			input:    strings.Replace(golden, "|55=AAA|", "|55=AAB|", 1),
			wantKind: ErrChecksum,
			wantErr:  "declared 208, computed 209",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Decode(wire(tc.input))
			require.Error(t, err)
			assert.ErrorIs(t, err, tc.wantKind)
			assert.ErrorContains(t, err, tc.wantErr)
		})
	}
}
