package protocol_test

import (
	"testing"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

func TestFirstTopicError(t *testing.T) {
	results := []protocol.TopicMutationResult{
		{Topic: "ok", ErrorCode: 0},
		{Topic: "bad", ErrorCode: 3},
	}
	r, ok := protocol.FirstTopicError(results)
	if !ok || r.Topic != "bad" {
		t.Fatalf("got %+v ok=%v", r, ok)
	}
}

func TestEncodeIncrementalAlterConfigsRequest(t *testing.T) {
	val := "1"
	body := protocol.EncodeIncrementalAlterConfigsRequest(0, map[string][]protocol.ConfigAlteration{
		"t": {{Name: "retention.ms", Value: &val}},
	})
	if len(body) == 0 {
		t.Fatal("empty body")
	}
}

func TestEncodeDeleteGroupsRequest(t *testing.T) {
	body := protocol.EncodeDeleteGroupsRequest([]string{"g1"})
	if len(body) == 0 {
		t.Fatal("empty body")
	}
}
