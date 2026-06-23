package srwire

import "testing"

func TestProtobufIndexOptimization(t *testing.T) {
	idx := encodeMessageIndexes([]int{0})
	if len(idx) != 1 || idx[0] != 0 {
		t.Fatalf("got %v", idx)
	}
	round, n, err := decodeMessageIndexes(append(idx, 1, 2, 3))
	if err != nil || n != 1 || len(round) != 1 || round[0] != 0 {
		t.Fatalf("round=%v n=%d err=%v", round, n, err)
	}
}

func TestConfluentRoundTrip(t *testing.T) {
	payload := []byte("data")
	w := EncodeConfluent(99, payload)
	h, raw, err := DecodeConfluent(w)
	if err != nil || h.SchemaID != 99 || string(raw) != "data" {
		t.Fatalf("h=%+v raw=%q err=%v", h, raw, err)
	}
}
