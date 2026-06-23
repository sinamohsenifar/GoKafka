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
	body := protocol.EncodeAddOffsetsToTxn("txn-1", 42, 1, []protocol.TxnGroupOffsets{{
		GroupID: "g1",
		Topics:  []protocol.TxnTopicPartitions{{Topic: "t", Partitions: []int32{0}}},
	}})
	if len(body) == 0 {
		t.Fatal("empty body")
	}
	resp := wire.NewBuffer(32)
	resp.WriteInt32(0)
	resp.WriteUvarint(2) // 1 group + 1
	resp.WriteCompactString("g1")
	resp.WriteUvarint(2) // 1 topic + 1
	resp.WriteCompactString("t")
	resp.WriteUvarint(2) // 1 partition + 1
	resp.WriteInt32(0)
	resp.WriteInt16(0)
	resp.WriteEmptyTagSection()
	resp.WriteEmptyTagSection()
	resp.WriteEmptyTagSection()
	code, err := protocol.DecodeAddOffsetsToTxn(resp.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("code=%d", code)
	}
}

func TestEncodeTxnOffsetCommitRoundTrip(t *testing.T) {
	body := protocol.EncodeTxnOffsetCommit("txn-1", "g1", 42, 1, []protocol.TxnCommittedOffset{
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
	code, err := protocol.DecodeTxnOffsetCommit(resp.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("code=%d", code)
	}
}
