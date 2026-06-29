package schema

import (
	"context"
	"fmt"
	"testing"
)

func TestSubjectNameStrategies(t *testing.T) {
	if got := SubjectForTopic("orders", false); got != "orders-value" {
		t.Errorf("TopicNameStrategy value = %q", got)
	}
	if got := SubjectForTopic("orders", true); got != "orders-key" {
		t.Errorf("TopicNameStrategy key = %q", got)
	}
	if got := SubjectForRecord("com.example.User"); got != "com.example.User" {
		t.Errorf("RecordNameStrategy = %q", got)
	}
	if got := SubjectForTopicRecord("orders", "com.example.User"); got != "orders-com.example.User" {
		t.Errorf("TopicRecordNameStrategy = %q", got)
	}
	// The pluggable strategy funcs match their helpers.
	var s SubjectNameStrategy = TopicRecordNameStrategy
	if got := s("orders", "com.example.User", false); got != "orders-com.example.User" {
		t.Errorf("strategy func = %q", got)
	}
}

func TestMockRegistryDedupAndVersions(t *testing.T) {
	ctx := context.Background()
	m := NewMockRegistry()
	id1, _ := m.RegisterAvro(ctx, "orders-value", `{"type":"string"}`)
	id2, _ := m.RegisterAvro(ctx, "orders-value", `{"type":"string"}`) // identical -> same id
	if id1 != id2 {
		t.Fatalf("identical schema should dedup: %d != %d", id1, id2)
	}
	id3, _ := m.RegisterAvro(ctx, "orders-value", `{"type":"long"}`) // new schema -> new id + version
	if id3 == id1 {
		t.Fatal("different schema must get a new id")
	}
	if v := m.ListVersions("orders-value"); len(v) != 2 {
		t.Fatalf("expected 2 versions, got %v", v)
	}
	if _, err := m.SchemaByID(ctx, 999); err == nil {
		t.Fatal("unknown id should error")
	}
}

// TestSerdeRoundTripWithMock proves a Serde can encode and decode through the
// in-memory registry with no live Schema Registry — the core testing win.
func TestSerdeRoundTripWithMock(t *testing.T) {
	ctx := context.Background()
	reg := NewMockRegistry()

	// JSON round-trip.
	js := NewSerde(reg, SerdeConfig{Subject: "events-value", Format: FormatJSON})
	type evt struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	enc, err := js.EncodeJSON(ctx, `{"type":"object"}`, evt{ID: 7, Name: "go"})
	if err != nil {
		t.Fatal(err)
	}
	var out evt
	if err := js.DecodeJSON(ctx, enc, &out); err != nil {
		t.Fatal(err)
	}
	if out.ID != 7 || out.Name != "go" {
		t.Fatalf("JSON round-trip mismatch: %+v", out)
	}

	// Avro round-trip (DecodeAvro fetches the schema text back from the mock by id).
	av := NewSerde(reg, SerdeConfig{Subject: "users-value", Format: FormatAvro})
	schema := `{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}`
	abytes, err := av.EncodeAvro(ctx, schema, map[string]any{"id": int32(42), "name": "ada"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := av.DecodeAvro(ctx, abytes)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got["id"]) != "42" || fmt.Sprint(got["name"]) != "ada" {
		t.Fatalf("Avro round-trip mismatch: %+v", got)
	}
}
