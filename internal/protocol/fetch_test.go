package protocol

import (
	"encoding/binary"
	"strings"
	"testing"
)

// v2RecordBatchHeader writes the fixed 61-byte RecordBatch header into batch.
func v2RecordBatchHeader(batch []byte, baseOffset int64, attributes int16, lastOffsetDelta int32) {
	binary.BigEndian.PutUint64(batch[0:8], uint64(baseOffset))
	batch[16] = 2 // magic
	binary.BigEndian.PutUint16(batch[21:23], uint16(attributes))
	binary.BigEndian.PutUint32(batch[23:27], uint32(lastOffsetDelta))
}

// A control batch (isControl bit 0x20) must not be parsed as data; it yields a
// single Control marker carrying the batch's absolute last offset so the
// consumer can advance past it.
func TestDecodeOneRecordBatchControlMarker(t *testing.T) {
	batch := make([]byte, 80)
	v2RecordBatchHeader(batch, 41, 0x20, 0)
	recs, err := decodeOneRecordBatch("t", 3, batch)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || !recs[0].Control || recs[0].Offset != 41 {
		t.Fatalf("expected one control marker at offset 41, got %+v", recs)
	}
}

// A v0/v1 message set (magic 0 or 1) must be rejected, not silently misparsed
// as a v2 RecordBatch.
func TestDecodeOneRecordBatchRejectsLegacyMagic(t *testing.T) {
	batch := make([]byte, 80)
	batch[16] = 1 // magic byte position: baseOffset(8)+batchLength(4)+leaderEpoch(4)
	_, err := decodeOneRecordBatch("t", 0, batch)
	if err == nil || !strings.Contains(err.Error(), "magic") {
		t.Fatalf("expected magic error, got %v", err)
	}
}

// A v2 batch that is too short to contain the fixed header is ignored (nil, nil),
// matching Kafka's partial-trailing-batch semantics.
func TestDecodeOneRecordBatchShortIsIgnored(t *testing.T) {
	recs, err := decodeOneRecordBatch("t", 0, make([]byte, 10))
	if err != nil || recs != nil {
		t.Fatalf("expected (nil,nil) for short batch, got (%v,%v)", recs, err)
	}
}
