package gokafka

import "github.com/sinamohsenifar/gokafka/schema"

// SchemaRegistryConfig configures Confluent Schema Registry access.
type SchemaRegistryConfig = schema.Config

// EncodeSchemaWire prepends Confluent schema ID header to a payload.
func EncodeSchemaWire(schemaID int, payload []byte) []byte {
	return schema.EncodeWire(schemaID, payload)
}

// DecodeSchemaWire removes Confluent schema ID header from a payload.
func DecodeSchemaWire(b []byte) (schemaID int, payload []byte, err error) {
	return schema.DecodeWire(b)
}
