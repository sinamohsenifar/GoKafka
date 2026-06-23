package schema_test

import (
	"testing"

	"github.com/sinamohsenifar/gokafka/schema"
)

func TestWireRoundTrip(t *testing.T) {
	payload := []byte(`{"x":1}`)
	wired := schema.EncodeWire(42, payload)
	id, raw, err := schema.DecodeWire(wired)
	if err != nil {
		t.Fatal(err)
	}
	if id != 42 {
		t.Fatalf("id=%d", id)
	}
	if string(raw) != string(payload) {
		t.Fatalf("payload mismatch")
	}
}
