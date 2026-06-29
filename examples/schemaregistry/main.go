// Command schemaregistry demonstrates Schema Registry serde: registering an
// Avro schema and round-tripping a record through the Confluent wire format.
//
// It uses schema.MockRegistry so it runs with no external services. To target a
// real registry instead, swap NewMockRegistry() for schema.New(schema.Config{
// URL: "http://localhost:8081"}) — the Serde API is identical.
package main

import (
	"context"
	"log"

	"github.com/sinamohsenifar/gokafka/schema"
)

func main() {
	ctx := context.Background()

	// In-memory registry — no running Schema Registry required for this example.
	reg := schema.NewMockRegistry()

	// TopicNameStrategy subject; SubjectForRecord / SubjectForTopicRecord are also
	// available for multiple event types per topic.
	subject := schema.SubjectForTopic("users", false)
	serde := schema.NewSerde(reg, schema.SerdeConfig{Subject: subject, Format: schema.FormatAvro})

	avroSchema := `{
		"type": "record",
		"name": "User",
		"namespace": "com.example",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}`

	encoded, err := serde.EncodeAvro(ctx, avroSchema, map[string]any{"id": int32(42), "name": "ada"})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("encoded %d bytes (magic 0x00 + 4-byte schema id %d + Avro payload)", len(encoded), serde.SchemaID())

	decoded, err := serde.DecodeAvro(ctx, encoded)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("decoded record: id=%v name=%v", decoded["id"], decoded["name"])
}
