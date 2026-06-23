package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/sinamohsenifar/gokafka/internal/avro"
	srwire "github.com/sinamohsenifar/gokafka/internal/schema/wire"
)

// Format identifies schema serialization format.
type Format int8

const (
	FormatJSON Format = iota
	FormatAvro
	FormatProtobuf
	FormatJSONSchema
)

// SerdeConfig configures a Schema Registry-backed serializer.
type SerdeConfig struct {
	Subject string
	Format  Format
	// ProtobufMessageIndexes for FormatProtobuf (default [0]).
	ProtobufMessageIndexes []int
	// ExpectedSchemaID if > 0, DecodeAvro rejects wire schema IDs that differ.
	ExpectedSchemaID int
	// PinRegisteredSchemaID when true, DecodeAvro requires wire schema ID to match EnsureRegistered ID.
	PinRegisteredSchemaID bool
	// AllowedSchemaIDs if non-empty, DecodeAvro rejects wire schema IDs not in this list.
	AllowedSchemaIDs []int
}

// Serde encodes and decodes values with Confluent wire framing.
type Serde struct {
	reg    *Registry
	cfg    SerdeConfig
	mu     sync.RWMutex
	avro   avro.Schema
	schema string
	id     int
}

// NewSerde creates a Serde for the given subject and format.
func NewSerde(reg *Registry, cfg SerdeConfig) *Serde {
	if len(cfg.ProtobufMessageIndexes) == 0 {
		cfg.ProtobufMessageIndexes = []int{0}
	}
	return &Serde{reg: reg, cfg: cfg}
}

// EnsureRegistered registers the schema text if not already cached.
func (s *Serde) EnsureRegistered(ctx context.Context, schemaText string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.id > 0 && s.schema == schemaText {
		return s.id, nil
	}
	var id int
	var err error
	switch s.cfg.Format {
	case FormatAvro:
		id, err = s.reg.RegisterAvro(ctx, s.cfg.Subject, schemaText)
		if err == nil {
			s.avro, err = avro.ParseRecordSchema(schemaText)
		}
	case FormatProtobuf:
		id, err = s.reg.RegisterProtobuf(ctx, s.cfg.Subject, schemaText)
	case FormatJSONSchema:
		id, err = s.reg.RegisterJSONSchema(ctx, s.cfg.Subject, schemaText)
	default:
		id, err = s.reg.RegisterJSON(ctx, s.cfg.Subject, schemaText)
	}
	if err != nil {
		return 0, err
	}
	s.id = id
	s.schema = schemaText
	return id, nil
}

// EncodeAvro encodes a record map with Avro binary + Confluent wire.
func (s *Serde) EncodeAvro(ctx context.Context, schemaText string, values map[string]any) ([]byte, error) {
	id, err := s.EnsureRegistered(ctx, schemaText)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	schema := s.avro
	s.mu.RUnlock()
	payload, err := avro.EncodeRecord(schema, values)
	if err != nil {
		return nil, err
	}
	return srwire.EncodeConfluent(int32(id), payload), nil
}

// DecodeAvro decodes Confluent-wrapped Avro bytes.
func (s *Serde) DecodeAvro(ctx context.Context, data []byte) (map[string]any, error) {
	h, payload, err := srwire.DecodeConfluent(data)
	if err != nil {
		return nil, err
	}
	wireID := int(h.SchemaID)
	if s.cfg.ExpectedSchemaID > 0 && wireID != s.cfg.ExpectedSchemaID {
		return nil, fmt.Errorf("schema: unexpected schema id %d", wireID)
	}
	if len(s.cfg.AllowedSchemaIDs) > 0 {
		allowed := false
		for _, id := range s.cfg.AllowedSchemaIDs {
			if wireID == id {
				allowed = true
				break
			}
		}
		if !allowed {
			return nil, fmt.Errorf("schema: schema id %d not allowed", wireID)
		}
	}
	s.mu.RLock()
	registeredID := s.id
	s.mu.RUnlock()
	if s.cfg.PinRegisteredSchemaID && registeredID > 0 && wireID != registeredID {
		return nil, fmt.Errorf("schema: wire schema id %d does not match registered id %d", wireID, registeredID)
	}
	text, err := s.reg.SchemaByID(ctx, wireID)
	if err != nil {
		return nil, err
	}
	schema, err := avro.ParseRecordSchema(text)
	if err != nil {
		return nil, err
	}
	return avro.DecodeRecord(schema, payload)
}

// EncodeJSON encodes a JSON value with Confluent wire.
func (s *Serde) EncodeJSON(ctx context.Context, schemaText string, v any) ([]byte, error) {
	id, err := s.EnsureRegistered(ctx, schemaText)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return srwire.EncodeConfluent(int32(id), payload), nil
}

// DecodeJSON decodes Confluent-wrapped JSON.
func (s *Serde) DecodeJSON(ctx context.Context, data []byte, v any) error {
	_, payload, err := srwire.DecodeConfluent(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(payload, v)
}

// EncodeProtobuf wraps pre-encoded Protobuf bytes with Confluent framing.
func (s *Serde) EncodeProtobuf(ctx context.Context, schemaText string, protoPayload []byte) ([]byte, error) {
	id, err := s.EnsureRegistered(ctx, schemaText)
	if err != nil {
		return nil, err
	}
	return srwire.EncodeProtobuf(int32(id), s.cfg.ProtobufMessageIndexes, protoPayload), nil
}

// DecodeProtobuf strips Confluent Protobuf framing.
func (s *Serde) DecodeProtobuf(data []byte) ([]int, []byte, error) {
	_, indexes, payload, err := srwire.DecodeProtobuf(data)
	return indexes, payload, err
}

// SchemaID returns the cached schema ID after registration.
func (s *Serde) SchemaID() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.id
}

// FormatName returns a human-readable format name.
func (c SerdeConfig) FormatName() string {
	switch c.Format {
	case FormatAvro:
		return "AVRO"
	case FormatProtobuf:
		return "PROTOBUF"
	case FormatJSONSchema:
		return "JSON"
	default:
		return fmt.Sprintf("unknown(%d)", c.Format)
	}
}
