package gokafka

import (
	"github.com/sinamohsenifar/gokafka/schema"
)

type (
	SchemaFormat      = schema.Format
	SchemaSerdeConfig = schema.SerdeConfig
	SchemaSerde       = schema.Serde
)

const (
	SchemaFormatJSON       = schema.FormatJSON
	SchemaFormatAvro       = schema.FormatAvro
	SchemaFormatProtobuf   = schema.FormatProtobuf
	SchemaFormatJSONSchema = schema.FormatJSONSchema
)

func NewSchemaSerde(reg *schema.Registry, cfg SchemaSerdeConfig) *SchemaSerde {
	return schema.NewSerde(reg, cfg)
}
