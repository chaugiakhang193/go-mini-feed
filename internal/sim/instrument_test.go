package sim

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newRand(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, seed))
}

func TestSecurityID(t *testing.T) {
	// The check digit comes from the Luhn algorithm with each letter replaced
	// by its two-digit value (A=10 .. Z=35). The expected values were worked
	// out by hand, so they do not check the code against itself.
	tests := []struct {
		symbol string
		want   string
	}{
		{"AAA", "XX000000AAA9"},
		{"BBB", "XX000000BBB3"},
		{"CCC", "XX000000CCC7"},
	}
	for _, tc := range tests {
		t.Run(tc.symbol, func(t *testing.T) {
			assert.Equal(t, tc.want, securityID(tc.symbol))
		})
	}
}

func TestPriceLimits(t *testing.T) {
	tests := []struct {
		name              string
		ref               int64
		wantHigh, wantLow int64
	}{
		{"typical", 25000, 26700, 23300},
		{"high rounds down and low rounds up", 10100, 10800, 9400},
		{"top of the reference range", 99900, 106800, 93000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			high, low := priceLimits(tc.ref)
			assert.Equal(t, tc.wantHigh, high, "high")
			assert.Equal(t, tc.wantLow, low, "low")
		})
	}
}

func TestInstruments(t *testing.T) {
	list, err := Instruments(3, newRand(1))
	require.NoError(t, err)
	require.Len(t, list, 3)

	assert.Equal(t, "AAA", list[0].Symbol)
	assert.Equal(t, "XX000000AAA9", list[0].SecurityID)
	assert.Equal(t, "BBB", list[1].Symbol)
	assert.Equal(t, "CCC", list[2].Symbol)

	for _, in := range list {
		assert.Zero(t, in.RefPrice%priceStep, "%s ref price is a multiple of the tick", in.Symbol)
		assert.GreaterOrEqual(t, in.RefPrice, int64(10000))
		assert.LessOrEqual(t, in.RefPrice, int64(99900))
		high, low := priceLimits(in.RefPrice)
		assert.Equal(t, high, in.High)
		assert.Equal(t, low, in.Low)
	}
}

func TestInstrumentsSameSeedSameList(t *testing.T) {
	a, err := Instruments(26, newRand(7))
	require.NoError(t, err)
	b, err := Instruments(26, newRand(7))
	require.NoError(t, err)
	c, err := Instruments(26, newRand(8))
	require.NoError(t, err)

	assert.Equal(t, a, b)
	assert.NotEqual(t, a, c)
	assert.Equal(t, "ZZZ", a[25].Symbol)
}

func TestInstrumentsRejectsCountOutsideAlphabet(t *testing.T) {
	for _, n := range []int{-1, 0, 27} {
		list, err := Instruments(n, newRand(1))
		assert.Nil(t, list)
		assert.ErrorContains(t, err, "out of range", "n=%d", n)
	}
}
