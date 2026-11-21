package sim

import "math/rand/v2"

// FaultCounts is how often a Faults instance dropped, duplicated and held
// back packets.
type FaultCounts struct {
	Dropped    int
	Duplicated int
	Reordered  int
}

// Faults imitates a lossy network for one stream of packets. Each packet can
// be lost, delivered twice, or delayed until the next packet of the same
// stream has gone out. One Faults value serves one stream, so a held packet
// is only ever overtaken by a neighbour from its own stream. Not safe for
// concurrent use.
type Faults struct {
	drop, dup, reorder float64
	rng                *rand.Rand
	held               [][]byte
	counts             FaultCounts
}

// NewFaults takes the three probabilities, each in [0, 1]. Zero never fires
// and one always does.
func NewFaults(drop, dup, reorder float64, rng *rand.Rand) *Faults {
	return &Faults{drop: drop, dup: dup, reorder: reorder, rng: rng}
}

// Apply takes the next packet of the stream and returns what reaches the
// receiver now, in arrival order. A dropped packet is neither duplicated nor
// held. A held packet is released right behind the next packet that gets
// through, which then is not held itself.
func (f *Faults) Apply(pkt []byte) [][]byte {
	if f.rng.Float64() < f.drop {
		f.counts.Dropped++
		return nil
	}
	out := [][]byte{pkt}
	if f.rng.Float64() < f.dup {
		f.counts.Duplicated++
		out = append(out, pkt)
	}

	if f.held != nil {
		out = append(out, f.held...)
		f.held = nil
		return out
	}
	if f.rng.Float64() < f.reorder {
		f.counts.Reordered++
		f.held = out
		return nil
	}
	return out
}

// Flush returns the packet that is still held, once the stream has ended.
func (f *Faults) Flush() [][]byte {
	held := f.held
	f.held = nil
	return held
}

// Counts reports what Apply has done so far.
func (f *Faults) Counts() FaultCounts {
	return f.counts
}
