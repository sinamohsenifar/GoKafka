package gokafka

import "testing"

// Canonical murmur2 vectors from Apache Kafka's
// org.apache.kafka.common.utils.UtilsTest#testMurmur2. Matching these proves
// key routing is interoperable with the Java client and librdkafka.
func TestMurmur2KafkaVectors(t *testing.T) {
	cases := map[string]int32{
		"21":                         -973932308,
		"foobar":                     -790332482,
		"a-little-bit-long-string":   -985981536,
		"a-little-bit-longer-string": -1486304829,
		"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8": -58897971,
	}
	for in, want := range cases {
		if got := murmur2([]byte(in)); got != want {
			t.Errorf("murmur2(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestHashPartitionerInRange(t *testing.T) {
	p := HashPartitioner{}
	for _, key := range []string{"", "a", "key-123", "another-key"} {
		got := p.Partition([]byte(key), 7)
		if got < 0 || got >= 7 {
			t.Errorf("Partition(%q) = %d, out of [0,7)", key, got)
		}
	}
}
