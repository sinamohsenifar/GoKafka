package auth

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"testing"
)

func TestPBKDF2SCRAMVector(t *testing.T) {
	salt, _ := base64.StdEncoding.DecodeString("eTNrN2kyNHFtTcTZ0ajkxcDJzEDhiMjA1aw==")
	got := pbkdf2(sha256.New, "gokafka-secret", salt, 4096)
	wantHex := "552496c4003fd8622aa0ca1e8d39a825e17fd717d6835a70a36ded29499eb3ae"
	if hex.EncodeToString(got) != wantHex {
		t.Fatalf("pbkdf2 mismatch\ngot  %s\nwant %s", hex.EncodeToString(got), wantHex)
	}
}
