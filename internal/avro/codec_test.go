package avro

import (
	"testing"
)

func TestEncodeDecodeRecord(t *testing.T) {
	schemaJSON := `{"type":"record","name":"Event","fields":[{"name":"msg","type":"string"},{"name":"n","type":"int"}]}`
	s, err := ParseRecordSchema(schemaJSON)
	if err != nil {
		t.Fatal(err)
	}
	in := map[string]any{"msg": "hello", "n": 42}
	enc, err := EncodeRecord(s, in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := DecodeRecord(s, enc)
	if err != nil {
		t.Fatal(err)
	}
	if out["msg"] != "hello" || out["n"] != 42 {
		t.Fatalf("got %+v", out)
	}
}
