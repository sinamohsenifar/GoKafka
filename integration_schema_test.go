//go:build integration

package gokafka_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
	"github.com/sinamohsenifar/gokafka/schema"
)

func TestIntegrationSchemaRegistry(t *testing.T) {
	url := os.Getenv("SCHEMA_REGISTRY_URL")
	if url == "" {
		url = "http://127.0.0.1:8081/apis/ccompat/v6"
	}
	_ = integrationBrokers(t) // skip if kafka env not configured

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	reg, err := schema.New(schema.Config{URL: url})
	if err != nil {
		t.Fatal(err)
	}

	subject := "gokafka-it-" + time.Now().Format("150405.000")
	schemaJSON := `{"type":"object","properties":{"msg":{"type":"string"}}}`
	id, err := reg.RegisterJSON(ctx, subject, schemaJSON)
	if err != nil {
		t.Fatal(err)
	}
	if id <= 0 {
		t.Fatalf("schema id=%d", id)
	}

	raw, err := gokafka.JSONPayload{V: map[string]string{"msg": "hello"}}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	payload := gokafka.EncodeSchemaWire(id, raw)
	gotID, raw, err := gokafka.DecodeSchemaWire(payload)
	if err != nil {
		t.Fatal(err)
	}
	if gotID != id {
		t.Fatalf("id=%d want %d", gotID, id)
	}
	if string(raw) == "" {
		t.Fatal("empty decoded payload")
	}
}

func TestIntegrationSchemaAvroRoundTrip(t *testing.T) {
	url := os.Getenv("SCHEMA_REGISTRY_URL")
	if url == "" {
		url = "http://127.0.0.1:8081/apis/ccompat/v6"
	}
	_ = integrationBrokers(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	reg, err := schema.New(schema.Config{URL: url})
	if err != nil {
		t.Fatal(err)
	}

	subject := "gokafka-avro-" + time.Now().Format("150405.000")
	schemaText := `{"type":"record","name":"Event","fields":[{"name":"msg","type":"string"}]}`
	serde := schema.NewSerde(reg, schema.SerdeConfig{Subject: subject, Format: schema.FormatAvro})

	encoded, err := serde.EncodeAvro(ctx, schemaText, map[string]any{"msg": "avro-it"})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := serde.DecodeAvro(ctx, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded["msg"] != "avro-it" {
		t.Fatalf("msg=%v", decoded["msg"])
	}
}
