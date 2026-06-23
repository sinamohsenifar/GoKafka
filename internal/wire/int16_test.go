package wire_test

import (
	"encoding/binary"
	"testing"

	"github.com/sinamohsenifar/gokafka/internal/wire"
)

func TestWriteInt16NegativeOne(t *testing.T) {
	buf := wire.NewBuffer(4)
	buf.WriteInt16(-1)
	if len(buf.Bytes()) != 2 {
		t.Fatalf("len=%d", len(buf.Bytes()))
	}
	got := int16(binary.BigEndian.Uint16(buf.Bytes()))
	if got != -1 {
		t.Fatalf("got %d", got)
	}
}
