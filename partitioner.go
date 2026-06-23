package gokafka

import "hash/fnv"

// Partitioner selects a partition for a record key.
type Partitioner interface {
	Partition(key []byte, numPartitions int) int32
}

// HashPartitioner uses FNV-1a (Kafka default style).
type HashPartitioner struct{}

func (HashPartitioner) Partition(key []byte, n int) int32 {
	if n <= 0 {
		return 0
	}
	if len(key) == 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write(key)
	return int32(int(h.Sum32()) % n)
}

// RoundRobinPartitioner ignores keys and cycles partitions.
type RoundRobinPartitioner struct {
	counter uint32
}

func (r *RoundRobinPartitioner) Partition(_ []byte, n int) int32 {
	if n <= 0 {
		return 0
	}
	r.counter++
	return int32(r.counter % uint32(n))
}
