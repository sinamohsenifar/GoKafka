package protocol

import (
	"encoding/hex"
	"testing"
)

// realShareAckV1 is a ShareAcknowledge v1 response body captured from a real
// apache/kafka 4.1.2 broker (share.version=2): throttle=0, top error_code=0,
// one topic (id c5967165…), one partition (index 0, error_code 0), then the
// node_endpoints array (empty) and response tag. Locks the wire layout.
const realShareAckV1 = "0000000000000002c596716554174f099ef18dbb4c2ac9c0020000000000000000000000000000000000000100"

func TestDecodeShareAcknowledgeResponse_RealBytes(t *testing.T) {
	raw, err := hex.DecodeString(realShareAckV1)
	if err != nil {
		t.Fatal(err)
	}
	code, err := DecodeShareAcknowledgeResponse(1, raw)
	if err != nil || code != 0 {
		t.Fatalf("real v1 success response should decode cleanly: code=%d err=%v", code, err)
	}
}

// v2 (KIP-1222) inserts AcquisitionLockTimeoutMs (int32) right after error_message.
// Synthesize it from the captured v1 body and confirm the version-aware decoder
// handles it (this is the layout the Kafka 4.3+ brokers return — the v2 regression).
func TestDecodeShareAcknowledgeResponse_V2(t *testing.T) {
	v1, _ := hex.DecodeString(realShareAckV1)
	// error_message (null, 1 byte) ends at offset 7; insert 4-byte lock timeout there.
	v2 := append([]byte{}, v1[:7]...)
	v2 = append(v2, 0x00, 0x00, 0x75, 0x30) // acquisition_lock_timeout_ms = 30000
	v2 = append(v2, v1[7:]...)
	code, err := DecodeShareAcknowledgeResponse(2, v2)
	if err != nil || code != 0 {
		t.Fatalf("v2 response should decode cleanly: code=%d err=%v", code, err)
	}
	// Decoding v2 bytes as v1 must fail (proves the version field matters).
	if _, err := DecodeShareAcknowledgeResponse(1, v2); err == nil {
		t.Fatal("v2 bytes decoded as v1 should not silently succeed")
	}
}

// TestDecodeShareAcknowledgeResponse_PartitionError is the regression for the bug:
// a per-partition error (here INVALID_RECORD_STATE=36, e.g. an expired acquisition
// lock) must be surfaced, not swallowed as success. The partition error_code int16
// sits at byte offset 29 in the captured body.
func TestDecodeShareAcknowledgeResponse_PartitionError(t *testing.T) {
	raw, _ := hex.DecodeString(realShareAckV1)
	raw[29], raw[30] = 0x00, 0x24 // partition error_code = 36
	code, err := DecodeShareAcknowledgeResponse(1, raw)
	if err == nil {
		t.Fatal("per-partition ack error must be surfaced, not reported as success")
	}
	if code != 36 {
		t.Fatalf("expected code 36 (INVALID_RECORD_STATE), got %d", code)
	}
}

// Sanity: the top-level error path still works.
func TestDecodeShareAcknowledgeResponse_TopLevelError(t *testing.T) {
	raw, _ := hex.DecodeString(realShareAckV1)
	raw[4], raw[5] = 0x00, 0x23 // top error_code = 35 (UNSUPPORTED_VERSION)
	code, err := DecodeShareAcknowledgeResponse(1, raw)
	if err == nil || code != 35 {
		t.Fatalf("top-level error should surface: code=%d err=%v", code, err)
	}
}
