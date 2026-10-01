package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTallyAdd(t *testing.T) {
	tests := []struct {
		name           string
		seqs           []int
		wantOutOfOrder int
		wantDuplicate  int
	}{
		{name: "in order", seqs: []int{1, 2, 3}},
		{name: "swapped pair", seqs: []int{1, 3, 2}, wantOutOfOrder: 1},
		{name: "repeat of the latest", seqs: []int{1, 1}, wantDuplicate: 1},
		{name: "repeat of an earlier seq", seqs: []int{1, 2, 1}, wantDuplicate: 1},
		{name: "late seq then its repeat", seqs: []int{2, 1, 1}, wantOutOfOrder: 1, wantDuplicate: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newTally()
			for _, seq := range tt.seqs {
				got.add(seq)
			}
			assert.Equal(t, len(tt.seqs), got.total)
			assert.Equal(t, tt.wantOutOfOrder, got.outOfOrder, "out of order")
			assert.Equal(t, tt.wantDuplicate, got.duplicate, "duplicate")
		})
	}
}

func TestLaneForIsStable(t *testing.T) {
	for _, key := range []string{"AAA", "BBB", "CCC", "DDD"} {
		first := laneFor(key, 4)
		for range 100 {
			assert.Equal(t, first, laneFor(key, 4), key)
		}
		assert.GreaterOrEqual(t, first, 0)
		assert.Less(t, first, 4)
	}
}

func TestField(t *testing.T) {
	body := []byte("key=AAA|seq=12|worker=2")
	assert.Equal(t, "AAA", field(body, "key"))
	assert.Equal(t, "12", field(body, "seq"))
	assert.Equal(t, "", field(body, "lane"))
	assert.Equal(t, "", field([]byte("xkey=AAA"), "key"))
}
