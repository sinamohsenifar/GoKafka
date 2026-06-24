package protocol_test

import (
	"testing"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
	"github.com/sinamohsenifar/gokafka/internal/wire"
)

func TestRequestHeaderVersionMetadataV12(t *testing.T) {
	if got := protocol.RequestHeaderVersion(protocol.APIMetadata, 12); got != 2 {
		t.Fatalf("got %d want 2", got)
	}
}

func TestRequestHeaderVersionMetadataV8(t *testing.T) {
	if got := protocol.RequestHeaderVersion(protocol.APIMetadata, 8); got != 1 {
		t.Fatalf("got %d want 1", got)
	}
}

func TestResponseBodyForAPIProduceV9StripsHeaderTags(t *testing.T) {
	inner := wire.NewBuffer(16)
	inner.WriteInt32(42)
	inner.WriteEmptyTagSection()
	inner.WriteInt8(0x02)
	frame := wire.PrependLength(inner.Bytes())
	body, err := protocol.ResponseBodyForAPI(frame, protocol.APIProduce, 9)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) != 1 || body[0] != 0x02 {
		t.Fatalf("body=%x want [02]", body)
	}
}

func TestRequestHeaderVersionSaslHandshakeV1(t *testing.T) {
	if got := protocol.RequestHeaderVersion(protocol.APISaslHandshake, protocol.VerSaslHandshake); got != 1 {
		t.Fatalf("got %d want 1", got)
	}
}
