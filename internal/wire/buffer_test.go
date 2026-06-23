package wire_test

import (
	"testing"

	"github.com/sinamohsenifar/gokafka/internal/wire"
)

func TestCompactStringRoundTrip(t *testing.T) {
	buf := wire.NewBuffer(16)
	buf.WriteCompactString("hello")
	r := wire.FromBytes(buf.Bytes())
	got, err := r.ReadCompactString()
	if err != nil {
		t.Fatal(err)
	}
	if got != "hello" {
		t.Fatalf("got %q", got)
	}
}

func TestPrependLength(t *testing.T) {
	body := []byte{1, 2, 3}
 framed := wire.PrependLength(body)
	if len(framed) != 7 {
		t.Fatalf("len=%d", len(framed))
	}
}
