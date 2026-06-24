package protocol_test

import (
	"encoding/hex"
	"testing"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

func TestDecodeProduceResponseFlexCaptured(t *testing.T) {
	raw, err := hex.DecodeString("0212676f6b61666b612d69742d313333383335020000000000000000000000000000ffffffffffffffff0000000000000000010000000000000000")
	if err != nil {
		t.Fatal(err)
	}
	res, err := protocol.DecodeProduceResponse(9, raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].Offset != 0 {
		t.Fatalf("res=%+v", res)
	}
}
