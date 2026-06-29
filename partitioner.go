package gokafka

import "sync/atomic"

// Partitioner selects a partition for a record key.
type Partitioner interface {
	Partition(key []byte, numPartitions int) int32
}

// HashPartitioner routes records by key using Kafka's murmur2 hash, matching the
// Java client's DefaultPartitioner and librdkafka's consistent partitioner. This
// makes key-based routing interoperable across mixed-client fleets (essential for
// keyed ordering and compacted topics). Records with an empty/nil key go to
// partition 0; use RoundRobinPartitioner to spread keyless records.
type HashPartitioner struct{}

func (HashPartitioner) Partition(key []byte, n int) int32 {
	if n <= 0 {
		return 0
	}
	if len(key) == 0 {
		return 0
	}
	return toPositive(murmur2(key)) % int32(n)
}

// toPositive maps a 32-bit hash to a non-negative int, matching
// org.apache.kafka.common.utils.Utils.toPositive (hash & 0x7fffffff).
func toPositive(v int32) int32 {
	return v & 0x7fffffff
}

// murmur2 is the 32-bit MurmurHash2 variant used by Apache Kafka
// (org.apache.kafka.common.utils.Utils.murmur2), seed 0x9747b28c. The arithmetic
// is performed on uint32 so overflow wraps mod 2^32, matching Java int semantics.
func murmur2(data []byte) int32 {
	const (
		m = uint32(0x5bd1e995)
		r = 24
	)
	length := len(data)
	h := uint32(0x9747b28c) ^ uint32(length)

	length4 := length / 4
	for i := 0; i < length4; i++ {
		i4 := i * 4
		k := uint32(data[i4]) |
			uint32(data[i4+1])<<8 |
			uint32(data[i4+2])<<16 |
			uint32(data[i4+3])<<24
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}

	switch length % 4 {
	case 3:
		h ^= uint32(data[(length & ^3)+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[(length & ^3)+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[length & ^3])
		h *= m
	}

	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return int32(h)
}

// RoundRobinPartitioner ignores keys and cycles partitions.
// Safe for concurrent use by multiple producer goroutines.
type RoundRobinPartitioner struct {
	counter uint32
}

func (r *RoundRobinPartitioner) Partition(_ []byte, n int) int32 {
	if n <= 0 {
		return 0
	}
	v := atomic.AddUint32(&r.counter, 1)
	return int32(v % uint32(n))
}
