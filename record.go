package gokafka

import "time"

// Record is a Kafka record with optional headers.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []Header
	Timestamp time.Time
}

// Header is a record header key-value pair.
type Header struct {
	Key   string
	Value []byte
}
