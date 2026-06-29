package protocol

import (
	"testing"
)

func TestEncodeCreatePartitionsV2Wire(t *testing.T) {
	body := EncodeCreatePartitionsRequest(2, []CreatePartitionsSpec{{
		Topic: "test-topic",
		Count: 4,
	}}, 30000)
	frame := EncodeRequest(RequestHeader{
		APIKey: APICreatePartitions, APIVersion: 2, CorrelationID: 1, ClientID: "gokafka",
	}, body)
	if RequestHeaderVersion(APICreatePartitions, 2) != 2 {
		t.Fatal("expected flex request header v2")
	}
	if len(frame) < 20 {
		t.Fatal("frame too short")
	}
}
