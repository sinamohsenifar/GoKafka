package avro

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/sinamohsenifar/gokafka/internal/wire"
)

// Schema is a minimal Avro record schema parsed from JSON.
type Schema struct {
	Name   string
	Fields []Field
}

// Field is one record field.
type Field struct {
	Name string
	Type string
}

// ParseRecordSchema parses a simple Avro JSON record schema.
func ParseRecordSchema(json string) (Schema, error) {
	// Minimal parser for {"type":"record","name":"X","fields":[{"name":"f","type":"string"}]}
	var raw struct {
		Type   string `json:"type"`
		Name   string `json:"name"`
		Fields []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"fields"`
	}
	if err := parseJSON(json, &raw); err != nil {
		return Schema{}, err
	}
	if raw.Type != "record" {
		return Schema{}, fmt.Errorf("avro: expected record schema, got %q", raw.Type)
	}
	s := Schema{Name: raw.Name}
	for _, f := range raw.Fields {
		typ := f.Type
		if typ == "" {
			typ = "string"
		}
		s.Fields = append(s.Fields, Field{Name: f.Name, Type: typ})
	}
	return s, nil
}

func parseJSON(s string, v any) error {
	return jsonUnmarshal([]byte(s), v)
}

// EncodeRecord encodes a map of field values using Avro binary encoding.
func EncodeRecord(s Schema, values map[string]any) ([]byte, error) {
	buf := wire.NewBuffer(64)
	for _, f := range s.Fields {
		v, ok := values[f.Name]
		if !ok {
			return nil, fmt.Errorf("avro: missing field %q", f.Name)
		}
		if err := encodeValue(buf, f.Type, v); err != nil {
			return nil, fmt.Errorf("avro: field %q: %w", f.Name, err)
		}
	}
	return buf.Bytes(), nil
}

// DecodeRecord decodes Avro binary into a map.
func DecodeRecord(s Schema, data []byte) (map[string]any, error) {
	buf := wire.FromBytes(data)
	out := make(map[string]any, len(s.Fields))
	for _, f := range s.Fields {
		v, err := decodeValue(buf, f.Type)
		if err != nil {
			return nil, fmt.Errorf("avro: field %q: %w", f.Name, err)
		}
		out[f.Name] = v
	}
	return out, nil
}

func encodeValue(buf *wire.Buffer, typ string, v any) error {
	switch typ {
	case "null":
		return nil
	case "boolean":
		b, ok := v.(bool)
		if !ok {
			return fmt.Errorf("expected bool")
		}
		if b {
			buf.WriteInt8(1)
		} else {
			buf.WriteInt8(0)
		}
	case "int":
		i, err := toInt32(v)
		if err != nil {
			return err
		}
		buf.WriteVarint(int(i))
	case "long":
		i, err := toInt64(v)
		if err != nil {
			return err
		}
		buf.WriteVarint(int(i))
	case "float":
		f, err := toFloat32(v)
		if err != nil {
			return err
		}
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], math.Float32bits(f))
		buf.B = append(buf.B, b[:]...)
	case "double":
		f, err := toFloat64(v)
		if err != nil {
			return err
		}
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], math.Float64bits(f))
		buf.B = append(buf.B, b[:]...)
	case "string":
		s, ok := v.(string)
		if !ok {
			return fmt.Errorf("expected string")
		}
		buf.WriteVarint(len(s))
		buf.B = append(buf.B, s...)
	case "bytes":
		b, ok := v.([]byte)
		if !ok {
			return fmt.Errorf("expected []byte")
		}
		buf.WriteVarint(len(b))
		buf.B = append(buf.B, b...)
	default:
		return fmt.Errorf("unsupported type %q", typ)
	}
	return nil
}

func decodeValue(buf *wire.Buffer, typ string) (any, error) {
	switch typ {
	case "null":
		return nil, nil
	case "boolean":
		b, err := buf.ReadInt8()
		if err != nil {
			return nil, err
		}
		return b != 0, nil
	case "int":
		return buf.ReadVarint()
	case "long":
		v, err := buf.ReadVarint()
		if err != nil {
			return nil, err
		}
		return int64(v), nil
	case "float":
		b, err := readFixed(buf, 4)
		if err != nil {
			return nil, err
		}
		return math.Float32frombits(binary.LittleEndian.Uint32(b)), nil
	case "double":
		b, err := readFixed(buf, 8)
		if err != nil {
			return nil, err
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(b)), nil
	case "string":
		n, err := buf.ReadVarint()
		if err != nil {
			return nil, err
		}
		if n < 0 || buf.I+n > len(buf.B) {
			return nil, wire.ErrShortBuffer
		}
		s := string(buf.B[buf.I : buf.I+n])
		buf.I += n
		return s, nil
	case "bytes":
		n, err := buf.ReadVarint()
		if err != nil {
			return nil, err
		}
		if n < 0 || buf.I+n > len(buf.B) {
			return nil, wire.ErrShortBuffer
		}
		out := make([]byte, n)
		copy(out, buf.B[buf.I:buf.I+n])
		buf.I += n
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported type %q", typ)
	}
}

func toInt32(v any) (int32, error) {
	switch x := v.(type) {
	case int:
		return int32(x), nil
	case int32:
		return x, nil
	case int64:
		return int32(x), nil
	case float64:
		return int32(x), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int", v)
	}
}

func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int64:
		return x, nil
	case float64:
		return int64(x), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to long", v)
	}
}

func toFloat32(v any) (float32, error) {
	switch x := v.(type) {
	case float32:
		return x, nil
	case float64:
		return float32(x), nil
	case int:
		return float32(x), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float", v)
	}
}

func toFloat64(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case int:
		return float64(x), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to double", v)
	}
}

func readFixed(buf *wire.Buffer, n int) ([]byte, error) {
	if buf.I+n > len(buf.B) {
		return nil, wire.ErrShortBuffer
	}
	out := make([]byte, n)
	copy(out, buf.B[buf.I:buf.I+n])
	buf.I += n
	return out, nil
}
