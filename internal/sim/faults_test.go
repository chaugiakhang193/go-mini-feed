package sim

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// applyAll sends one-byte packets 1..n through f and returns what comes out,
// in order, with whatever Flush releases at the end.
func applyAll(f *Faults, n int) []byte {
	var out []byte
	for i := 1; i <= n; i++ {
		for _, pkt := range f.Apply([]byte{byte(i)}) {
			out = append(out, pkt[0])
		}
	}
	for _, pkt := range f.Flush() {
		out = append(out, pkt[0])
	}
	return out
}

func TestFaultsPassThroughWithoutRates(t *testing.T) {
	f := NewFaults(0, 0, 0, newRand(1))

	assert.Equal(t, []byte{1, 2, 3, 4, 5}, applyAll(f, 5))
	assert.Equal(t, FaultCounts{}, f.Counts())
}

func TestFaultsDropEverything(t *testing.T) {
	f := NewFaults(1, 0, 0, newRand(1))

	assert.Empty(t, applyAll(f, 10))
	assert.Equal(t, 10, f.Counts().Dropped)
}

func TestFaultsDuplicateEverything(t *testing.T) {
	f := NewFaults(0, 1, 0, newRand(1))

	assert.Equal(t, []byte{1, 1, 2, 2, 3, 3}, applyAll(f, 3))
	assert.Equal(t, 3, f.Counts().Duplicated)
}

func TestFaultsReorderSwapsNeighboursOfTheSameStream(t *testing.T) {
	f := NewFaults(0, 0, 1, newRand(1))

	assert.Equal(t, []byte{2, 1, 4, 3}, applyAll(f, 4))
	assert.Equal(t, 2, f.Counts().Reordered)
}

func TestFaultsFlushReleasesTheHeldPacket(t *testing.T) {
	f := NewFaults(0, 0, 1, newRand(1))

	assert.Equal(t, []byte{2, 1, 3}, applyAll(f, 3), "the third packet is held until Flush")
	assert.Nil(t, f.Flush(), "nothing is left to release")
}

func TestFaultsDropRateIsRoughlyTheRequestedShare(t *testing.T) {
	f := NewFaults(0.1, 0, 0, newRand(3))

	out := applyAll(f, 10000)
	dropped := f.Counts().Dropped
	assert.Equal(t, 10000-dropped, len(out))
	assert.InDelta(t, 1000, dropped, 100)
}

func TestFaultsNeverDuplicateOrHoldADroppedPacket(t *testing.T) {
	f := NewFaults(1, 1, 1, newRand(1))

	assert.Empty(t, applyAll(f, 5))
	counts := f.Counts()
	assert.Zero(t, counts.Duplicated)
	assert.Zero(t, counts.Reordered)
}

func TestFaultsSameSeedSameOutcome(t *testing.T) {
	a := applyAll(NewFaults(0.2, 0.2, 0.2, newRand(5)), 200)
	b := applyAll(NewFaults(0.2, 0.2, 0.2, newRand(5)), 200)
	c := applyAll(NewFaults(0.2, 0.2, 0.2, newRand(6)), 200)

	assert.Equal(t, a, b)
	assert.NotEqual(t, a, c)
}
