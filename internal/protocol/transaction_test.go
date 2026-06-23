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
