package schema

import (
	"encoding/json"
	"testing"

	"github.com/sinamohsenifar/gokafka/internal/avro"
	srwire "github.com/sinamohsenifar/gokafka/internal/schema/wire"
)

func BenchmarkSerdeEncodeAvro(b *testing.B) {
	schemaText := `{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"msg","type":"string"}]}`
	s, err := avro.ParseRecordSchema(schemaText)
	if err != nil {
		b.Fatal(err)
	}
	values := map[string]any{"id": int64(42), "msg": "hello world"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload, err := avro.EncodeRecord(s, values)
		if err != nil {
			b.Fatal(err)
		}
		_ = srwire.EncodeConfluent(1, payload)
	}
}

func BenchmarkSerdeEncodeJSON(b *testing.B) {
	v := map[string]string{"msg": "hello"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload, err := json.Marshal(v)
		if err != nil {
			b.Fatal(err)
		}
		_ = srwire.EncodeConfluent(1, payload)
	}
}
