package gokafka

import (
	"hash/crc32"
	"testing"
)

// TestMurmur2KnownVectors locks in compatibility with the Apache Kafka Java
// client's Utils.murmur2 (org.apache.kafka.common.utils.UtilsTest.testMurmur2),
// so keyed records co-partition with Java/Sarama producers.
func TestMurmur2KnownVectors(t *testing.T) {
	cases := map[string]int32{
		"21":                         -973932308,
		"foobar":                     -790332482,
		"abc":                        479470107,
		"a-little-bit-long-string":   -985981536,
		"a-little-bit-longer-string": -1486304829,
	}
	for in, want := range cases {
		if got := murmur2([]byte(in)); got != want {
			t.Errorf("murmur2(%q) = %d, want %d (Java-client mismatch)", in, got, want)
		}
	}
}

func TestHashPartitionerDeterministicAndBounded(t *testing.T) {
	p := HashPartitioner{}
	const n = 7
	first := p.Partition([]byte("order-42"), n)
	for i := 0; i < 100; i++ {
		if got := p.Partition([]byte("order-42"), n); got != first {
			t.Fatalf("HashPartitioner not deterministic: %d != %d", got, first)
		}
	}
	if first < 0 || first >= n {
		t.Fatalf("partition %d out of range [0,%d)", first, n)
	}
	if got := p.Partition(nil, n); got != 0 {
		t.Fatalf("keyless should map to 0, got %d", got)
	}
}

func TestCRC32PartitionerMatchesLibrdkafkaFormula(t *testing.T) {
	p := CRC32Partitioner{}
	const n = 5
	for _, key := range []string{"a", "user-1", "longer-key-value", "42"} {
		want := int32(crc32.ChecksumIEEE([]byte(key)) % uint32(n))
		if got := p.Partition([]byte(key), n); got != want {
			t.Errorf("CRC32Partitioner(%q) = %d, want %d (crc32 %% n)", key, got, want)
		}
	}
	if got := p.Partition(nil, n); got != 0 {
		t.Fatalf("keyless should map to 0, got %d", got)
	}
}

func TestRoundRobinPartitionerSpreads(t *testing.T) {
	p := &RoundRobinPartitioner{}
	const n = 4
	seen := map[int32]int{}
	for i := 0; i < 4*n; i++ {
		seen[p.Partition(nil, n)]++
	}
	if len(seen) != n {
		t.Fatalf("RoundRobin did not cover all %d partitions: %v", n, seen)
	}
}

func TestPartitionerFromConfigSelection(t *testing.T) {
	if _, ok := partitionerFromConfig(ProducerConfig{PartitionStrategy: ProducerPartitionCRC32}).(CRC32Partitioner); !ok {
		t.Fatal("CRC32 strategy should yield CRC32Partitioner")
	}
	if _, ok := partitionerFromConfig(ProducerConfig{PartitionStrategy: ProducerPartitionRoundRobin}).(*RoundRobinPartitioner); !ok {
		t.Fatal("RoundRobin strategy should yield RoundRobinPartitioner")
	}
	if _, ok := partitionerFromConfig(ProducerConfig{}).(HashPartitioner); !ok {
		t.Fatal("default strategy should yield HashPartitioner")
	}
	// A custom Partitioner overrides the strategy.
	custom := constPartitioner(3)
	if got := partitionerFromConfig(ProducerConfig{Partitioner: custom, PartitionStrategy: ProducerPartitionRoundRobin}); got.Partition(nil, 10) != 3 {
		t.Fatal("custom Partitioner should override PartitionStrategy")
	}
}

type constPartitioner int32

func (c constPartitioner) Partition([]byte, int) int32 { return int32(c) }
