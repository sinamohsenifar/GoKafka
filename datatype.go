package gokafka

import "encoding/json"

// Encoder serializes a value to Kafka record bytes.
type Encoder interface {
	Encode() ([]byte, error)
}

// BytesPayload is raw opaque bytes.
type BytesPayload []byte

func (b BytesPayload) Encode() ([]byte, error) { return []byte(b), nil }

// StringPayload is UTF-8 text.
type StringPayload string

func (s StringPayload) Encode() ([]byte, error) { return []byte(s), nil }

// JSONPayload marshals a value as JSON.
type JSONPayload struct{ V any }

func (j JSONPayload) Encode() ([]byte, error) { return json.Marshal(j.V) }

// MustEncode encodes or panics (tests/init only).
func MustEncode(e Encoder) []byte {
	b, err := e.Encode()
	if err != nil {
		panic(err)
	}
	return b
}
