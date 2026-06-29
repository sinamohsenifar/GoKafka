package protocol_test

import (
	"testing"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
	"github.com/sinamohsenifar/gokafka/internal/wire"
)

func TestEncodeAddPartitionsToTxnRoundTrip(t *testing.T) {
	body := protocol.EncodeAddPartitionsToTxn("txn-1", 42, 1, []protocol.TxnTopicPartitions{
		{Topic: "events", Partitions: []int32{0, 1}},
	})
	if len(body) == 0 {
		t.Fatal("empty body")
	}
	resp := wire.NewBuffer(64)
	resp.WriteInt32(0)
	resp.WriteInt32(1)
	resp.WriteString("events")
	resp.WriteInt32(2)
	resp.WriteInt32(0)
	resp.WriteInt16(0)
	resp.WriteInt32(1)
	resp.WriteInt16(0)
	code, err := protocol.DecodeAddPartitionsToTxn(resp.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("code=%d", code)
	}
}

func TestEncodeEndTxnFlexCommittedByte(t *testing.T) {
	// Regression: the flex (v3+) encoder must write committed=1 for commit and 0
	// for abort. An earlier dormant version inverted this, which turned every
	// commit into an abort once VerEndTxn was bumped to the flex range.
	for _, tc := range []struct {
		commit bool
		want   byte
	}{{true, 1}, {false, 0}} {
		body := protocol.EncodeEndTxn(5, "txn-1", 42, 7, tc.commit)
		buf := wire.FromBytes(body)
		if _, err := buf.ReadCompactString(); err != nil { // transactional_id
			t.Fatal(err)
		}
		if _, err := buf.ReadInt64(); err != nil { // producer_id
			t.Fatal(err)
		}
		if _, err := buf.ReadInt16(); err != nil { // producer_epoch
			t.Fatal(err)
		}
		got, err := buf.ReadInt8() // committed
		if err != nil {
			t.Fatal(err)
		}
		if byte(got) != tc.want {
			t.Fatalf("commit=%v: committed byte=%d, want %d", tc.commit, got, tc.want)
		}
	}
}

func TestDecodeEndTxnV5ReturnsBumpedEpoch(t *testing.T) {
	// EndTxn v5 response: throttle_time_ms, error_code, producer_id, producer_epoch, tags.
	resp := wire.NewBuffer(32)
	resp.WriteInt32(0)   // throttle_time_ms
	resp.WriteInt16(0)   // error_code
	resp.WriteInt64(185) // producer_id
	resp.WriteInt16(9)   // producer_epoch (bumped)
	resp.WriteEmptyTagSection()
	res, err := protocol.DecodeEndTxn(5, resp.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != 0 || res.ProducerID != 185 || res.ProducerEpoch != 9 {
		t.Fatalf("got %+v, want code=0 id=185 epoch=9", res)
	}

	// Pre-v5 responses carry no producer id/epoch; they decode as -1.
	old := wire.NewBuffer(16)
	old.WriteInt32(0)
	old.WriteInt16(0)
	old.WriteEmptyTagSection()
	res4, err := protocol.DecodeEndTxn(4, old.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if res4.Code != 0 || res4.ProducerID != -1 || res4.ProducerEpoch != -1 {
		t.Fatalf("v4: got %+v, want code=0 id=-1 epoch=-1", res4)
	}
}

func TestEncodeAddOffsetsToTxnRoundTrip(t *testing.T) {
	body := protocol.EncodeAddOffsetsToTxn(3, "txn-1", 42, 1, "g1")
	if len(body) == 0 {
		t.Fatal("empty body")
	}
	resp := wire.NewBuffer(16)
	resp.WriteInt32(0)
	resp.WriteInt16(0)
	resp.WriteEmptyTagSection()
	code, err := protocol.DecodeAddOffsetsToTxn(3, resp.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("code=%d", code)
	}
}

func TestEncodeTxnOffsetCommitRoundTrip(t *testing.T) {
	body := protocol.EncodeTxnOffsetCommit(3, "txn-1", "g1", 42, 1, protocol.TxnOffsetCommitMeta{
		Generation: -1,
	}, []protocol.TxnCommittedOffset{
		{Topic: "t", Partition: 0, Offset: 100},
	})
	if len(body) == 0 {
		t.Fatal("empty body")
	}
	resp := wire.NewBuffer(32)
	resp.WriteInt32(0)
	resp.WriteUvarint(2)
	resp.WriteCompactString("t")
	resp.WriteUvarint(2)
	resp.WriteInt32(0)
	resp.WriteInt16(0)
	resp.WriteEmptyTagSection()
	resp.WriteEmptyTagSection()
	resp.WriteEmptyTagSection()
	code, err := protocol.DecodeTxnOffsetCommit(3, resp.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("code=%d", code)
	}
}

func TestDecodeAddOffsetsToTxnFlexFallback(t *testing.T) {
	resp := wire.NewBuffer(16)
	resp.WriteInt32(0)
	resp.WriteInt16(0)
	resp.WriteEmptyTagSection()
	code, err := protocol.DecodeAddOffsetsToTxn(2, resp.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("code=%d", code)
	}
}

func TestDecodeTxnOffsetCommitFlexFallback(t *testing.T) {
	resp := wire.NewBuffer(32)
	resp.WriteInt32(0)
	resp.WriteUvarint(2)
	resp.WriteCompactString("t")
	resp.WriteUvarint(2)
	resp.WriteInt32(0)
	resp.WriteInt16(0)
	resp.WriteEmptyTagSection()
	resp.WriteEmptyTagSection()
	resp.WriteEmptyTagSection()
	code, err := protocol.DecodeTxnOffsetCommit(2, resp.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("code=%d", code)
	}
}
