package protocol_test

import (
	"testing"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
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

func TestRequestHeaderVersionSaslHandshakeV1(t *testing.T) {
	if got := protocol.RequestHeaderVersion(protocol.APISaslHandshake, protocol.VerSaslHandshake); got != 1 {
		t.Fatalf("got %d want 1", got)
	}
}
