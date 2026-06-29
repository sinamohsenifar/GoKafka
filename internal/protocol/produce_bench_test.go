package protocol

import (
	"testing"
	"time"
)

func benchRecords(n int) []ProduceRecord {
	recs := make([]ProduceRecord, n)
	val := []byte("a representative kafka record value of moderate size ~ 64 bytes!!")
	ts := time.Unix(1700000000, 0)
	for i := range recs {
		recs[i] = ProduceRecord{
			Topic:     "bench-topic",
			Partition: 0,
			Key:       []byte("key-0000"),
			Value:     val,
			Timestamp: ts,
		}
	}
	return recs
}

func BenchmarkEncodeProduceRequestSingle(b *testing.B) {
	recs := benchRecords(1)
	settings := DefaultProduceSettings()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := EncodeProduceRequest(VerProduce, recs, settings); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeProduceRequestBatch1000(b *testing.B) {
	recs := benchRecords(1000)
	settings := DefaultProduceSettings()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := EncodeProduceRequest(VerProduce, recs, settings); err != nil {
			b.Fatal(err)
		}
	}
}
