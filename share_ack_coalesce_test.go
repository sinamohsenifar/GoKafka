package gokafka

import (
	"testing"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

func TestCoalesceAckBatches(t *testing.T) {
	at := protocol.ShareAckAccept
	b := func(first, last int64) protocol.ShareAckBatch {
		return protocol.ShareAckBatch{FirstOffset: first, LastOffset: last, Type: at}
	}
	cases := []struct {
		name    string
		offsets []int64
		want    []protocol.ShareAckBatch
	}{
		{"empty", nil, nil},
		{"single", []int64{5}, []protocol.ShareAckBatch{b(5, 5)}},
		{"contiguous", []int64{0, 1, 2, 3}, []protocol.ShareAckBatch{b(0, 3)}},
		{"unsorted contiguous", []int64{2, 0, 3, 1}, []protocol.ShareAckBatch{b(0, 3)}},
		{"gap", []int64{0, 1, 3, 4}, []protocol.ShareAckBatch{b(0, 1), b(3, 4)}},
		{"duplicates", []int64{5, 5, 6, 6, 7}, []protocol.ShareAckBatch{b(5, 7)}},
		{"two singles", []int64{10, 20}, []protocol.ShareAckBatch{b(10, 10), b(20, 20)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := coalesceAckBatches(append([]int64(nil), c.offsets...), at)
			if len(got) != len(c.want) {
				t.Fatalf("offsets %v: got %v, want %v", c.offsets, got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("offsets %v: batch %d = %+v, want %+v", c.offsets, i, got[i], c.want[i])
				}
			}
		})
	}
}
