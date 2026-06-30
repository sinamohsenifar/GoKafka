package protocol

import (
	"encoding/hex"
	"testing"

	"github.com/sinamohsenifar/gokafka/internal/wire"
)

// These fuzz targets assert the wire decoders never panic, OOM, or hang on
// arbitrary/malformed input — they must return an error instead. Run a target
// with e.g. `go test -fuzz=FuzzDecodeShareFetchResponse -fuzztime=30s ./internal/protocol/`.
// Without -fuzz, `go test` exercises the seed corpus as ordinary tests.

func resolveAnyTopic(wire.UUID) (string, bool) { return "t", true }

func seed(f *testing.F, samples ...[]byte) {
	f.Add([]byte(nil))
	f.Add([]byte{0})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})
	for _, s := range samples {
		f.Add(s)
	}
}

// seedV seeds a (version, data) fuzz target across representative API versions.
func seedV(f *testing.F, samples ...[]byte) {
	bases := [][]byte{nil, {0}, {0, 0, 0, 0}, {0xff, 0xff, 0xff, 0xff}}
	bases = append(bases, samples...)
	for _, v := range []int16{0, 1, 7, 12} {
		for _, b := range bases {
			f.Add(v, b)
		}
	}
}

func mustHex(s string) []byte { b, _ := hex.DecodeString(s); return b }

// A valid legacy (version 0) member assignment: 1 topic "t", 1 partition.
var seedLegacyAssignment = []byte{
	0x00, 0x00, // version 0
	0x00, 0x00, 0x00, 0x01, // nTopics = 1
	0x00, 0x01, 't', // topic "t"
	0x00, 0x00, 0x00, 0x01, // nParts = 1
	0x00, 0x00, 0x00, 0x00, // partition 0
}

func FuzzParseMemberAssignment(f *testing.F) {
	seed(f, seedLegacyAssignment)
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ParseMemberAssignment(data)
	})
}

func FuzzDecodeConsumerSubscription(f *testing.F) {
	seedV(f, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 't'})
	f.Fuzz(func(t *testing.T, ver int16, data []byte) {
		_, _ = DecodeConsumerSubscription(ver, data)
	})
}

func FuzzDecodeRecordBatch(f *testing.F) {
	seed(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = decodeRecordBatch("t", 0, data, nil)
	})
}

func FuzzDecodeFetchResponse(f *testing.F) {
	seedV(f)
	f.Fuzz(func(t *testing.T, ver int16, data []byte) {
		_, _ = DecodeFetchResponse(ver, data, resolveAnyTopic)
	})
}

func FuzzDecodeMetadataResponse(f *testing.F) {
	seedV(f)
	f.Fuzz(func(t *testing.T, ver int16, data []byte) {
		_, _ = DecodeMetadataResponse(ver, data)
	})
}

func FuzzDecodeShareFetchResponse(f *testing.F) {
	seed(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeShareFetchResponse(data, resolveAnyTopic)
	})
}

func FuzzDecodeShareAcknowledgeResponse(f *testing.F) {
	seedV(f, mustHex("0000000000000002c596716554174f099ef18dbb4c2ac9c0020000000000000000000000000000000000000100"))
	f.Fuzz(func(t *testing.T, ver int16, data []byte) {
		_, _ = DecodeShareAcknowledgeResponse(ver, data)
	})
}

func FuzzDecodeShareGroupDescribeResponse(f *testing.F) {
	seed(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeShareGroupDescribeResponse(data)
	})
}

func FuzzDecodeDescribeShareGroupOffsetsResponse(f *testing.F) {
	seed(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = DecodeDescribeShareGroupOffsetsResponse(data)
	})
}

func FuzzDecodeAlterShareGroupOffsetsResponse(f *testing.F) {
	seed(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeAlterShareGroupOffsetsResponse(data)
	})
}

func FuzzDecodeOffsetFetchResponse(f *testing.F) {
	seedV(f)
	f.Fuzz(func(t *testing.T, ver int16, data []byte) {
		_, _ = DecodeOffsetFetchResponse(ver, data)
	})
}

func FuzzDecodeDescribeConfigsResponse(f *testing.F) {
	seedV(f)
	f.Fuzz(func(t *testing.T, ver int16, data []byte) {
		_, _ = DecodeDescribeConfigsResponse(ver, data)
	})
}
