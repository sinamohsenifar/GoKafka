package protocol_test

import (
	"encoding/binary"
	"testing"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

func TestProduceRequestAcksEncoding(t *testing.T) {
	settings := protocol.ProduceSettings{Acks: -1, TimeoutMs: 30000}
	body, err := protocol.EncodeProduceRequest([]protocol.ProduceRecord{
		{Topic: "t", Partition: 0, Value: []byte("x")},
	}, settings)
	if err != nil {
		t.Fatal(err)
	}
	// null transactional_id (int16 -1) then acks int16
	acks := int16(binary.BigEndian.Uint16(body[2:4]))
	if acks != -1 {
		t.Fatalf("acks=%d want -1, body hex=%x", acks, body[:12])
	}
}

func TestProduceRequestTransactionalIDEncoding(t *testing.T) {
	settings := protocol.ProduceSettings{
		Acks: -1, TimeoutMs: 30000,
		Transactional: true, TransactionalID: "my-txn",
	}
	body, err := protocol.EncodeProduceRequest([]protocol.ProduceRecord{
		{Topic: "t", Partition: 0, Value: []byte("x")},
	}, settings)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) < 8 || body[0] != 0 || body[1] != 6 {
		t.Fatalf("unexpected transactional id prefix: %x", body[:8])
	}
	if string(body[2:8]) != "my-txn" {
		t.Fatalf("txn id=%q", body[2:8])
	}
}
