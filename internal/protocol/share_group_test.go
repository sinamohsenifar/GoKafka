package protocol_test

import (
	"testing"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
	"github.com/sinamohsenifar/gokafka/internal/wire"
)

// readShareAckHead reads group_id, member_id, share_session_epoch and returns
// the buffer positioned exactly where is_renew_ack (v2) / the topics array
// begins, so a test can assert the authoritative field order (is_renew_ack
// precedes topics in ShareAcknowledgeRequest v2, KIP-1222).
func readShareAckHead(t *testing.T, body []byte) *wire.Buffer {
	t.Helper()
	buf := wire.FromBytes(body)
	if _, err := buf.ReadCompactString(); err != nil { // group_id
		t.Fatal(err)
	}
	if _, err := buf.ReadCompactString(); err != nil { // member_id
		t.Fatal(err)
	}
	if _, err := buf.ReadInt32(); err != nil { // share_session_epoch
		t.Fatal(err)
	}
	return buf
}

func TestEncodeShareAcknowledgeRenewV2(t *testing.T) {
	req := protocol.ShareAcknowledgeRequest{
		GroupID: "g", MemberID: "m", ShareSessionEpoch: 0,
		Partitions: []protocol.ShareFetchPartition{{
			TopicID:   wire.UUID{1},
			Partition: 0,
			AckBatches: []protocol.ShareAckBatch{{
				FirstOffset: 5, LastOffset: 5, Type: protocol.ShareAckRenew,
			}},
		}},
	}
	// v2: is_renew_ack appears immediately after share_session_epoch (before the
	// topics array) and must be true when a Renew batch is present.
	buf := readShareAckHead(t, protocol.EncodeShareAcknowledgeRequest(2, req))
	renew, err := buf.ReadInt8()
	if err != nil {
		t.Fatal(err)
	}
	if renew != 1 {
		t.Fatalf("v2 is_renew_ack = %d, want 1", renew)
	}
	if _, err := buf.ReadUvarint(); err != nil { // topics array must follow
		t.Fatalf("topics array must follow is_renew_ack at v2: %v", err)
	}

	// v1: no is_renew_ack — the topics array follows share_session_epoch directly.
	// A single topic encodes as compact-array length 2 (uvarint).
	buf1 := readShareAckHead(t, protocol.EncodeShareAcknowledgeRequest(1, req))
	n, err := buf1.ReadUvarint()
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("v1: expected topics compact-array len 2 directly after epoch, got %d (is_renew_ack must not be written at v1)", n)
	}
}

func TestEncodeShareGroupHeartbeatRequest(t *testing.T) {
	body := protocol.EncodeShareGroupHeartbeatRequest(protocol.ShareGroupHeartbeatRequest{
		GroupID:              "share-grp",
		MemberID:             "550e8400-e29b-41d4-a716-446655440000",
		MemberEpoch:          0,
		SubscribedTopicNames: []string{"events"},
	})
	if len(body) == 0 {
		t.Fatal("empty body")
	}
}

func TestEncodeShareFetchRequest(t *testing.T) {
	body := protocol.EncodeShareFetchRequest(protocol.VerShareFetch, protocol.ShareFetchRequest{
		GroupID: "g", MemberID: "m", ShareSessionEpoch: 0,
		MaxWaitMs: 500, MinBytes: 1, MaxBytes: 1 << 20, MaxRecords: 100, BatchSize: 1,
	})
	if len(body) == 0 {
		t.Fatal("empty body")
	}
}
