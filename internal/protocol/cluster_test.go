package protocol_test

import (
	"testing"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

func TestEncodeDescribeClusterRequest(t *testing.T) {
	body := protocol.EncodeDescribeClusterRequest()
	if len(body) == 0 {
		t.Fatal("empty body")
	}
}
