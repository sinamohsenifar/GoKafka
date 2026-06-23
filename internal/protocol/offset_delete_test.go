package protocol

import "testing"

func TestEncodeOffsetDeleteRequestV0(t *testing.T) {
	body := EncodeOffsetDeleteRequest(0, "grp", map[string][]int32{
		"t": {0, 1},
	})
	if len(body) == 0 {
		t.Fatal("empty body")
	}
}

func TestEncodeOffsetDeleteRequestV1Flex(t *testing.T) {
	body := EncodeOffsetDeleteRequest(1, "grp", map[string][]int32{
		"t": {0},
	})
	if len(body) == 0 {
		t.Fatal("empty body")
	}
}
