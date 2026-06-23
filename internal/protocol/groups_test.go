package protocol_test

import (
	"testing"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

func TestNegotiateVersion(t *testing.T) {
	versions := []protocol.ApiVersion{{APIKey: protocol.APIProduce, MinVersion: 3, MaxVersion: 9}}
	if got := protocol.NegotiateVersion(versions, protocol.APIProduce, 9); got != 9 {
		t.Fatalf("got=%d", got)
	}
	if got := protocol.NegotiateVersion(versions, protocol.APIProduce, 12); got != 9 {
		t.Fatalf("capped got=%d", got)
	}
	if got := protocol.NegotiateVersion(versions, protocol.APIProduce, 2); got != 0 {
		t.Fatalf("unsupported got=%d", got)
	}
}

func TestEncodeJoinGroupRequestV9(t *testing.T) {
	body := protocol.EncodeJoinGroupRequest("g1", "m1", "range", "", []string{"t1"}, 45000, 45000, false)
	if len(body) == 0 {
		t.Fatal("empty join group body")
	}
}
