package protocol_test

import (
	"testing"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

func TestEncodeDescribeGroupsRequest(t *testing.T) {
	body := protocol.EncodeDescribeGroupsRequest([]string{"g1", "g2"})
	if len(body) == 0 {
		t.Fatal("empty body")
	}
}
