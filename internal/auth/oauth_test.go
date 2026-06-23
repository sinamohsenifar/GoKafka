package auth

import (
	"encoding/binary"
	"strings"
	"testing"
)

func TestBuildOAuthMessage(t *testing.T) {
	msg := buildOAuthMessage("test-token")
	if len(msg) < 4 {
		t.Fatal("short message")
	}
	n := binary.BigEndian.Uint32(msg[:4])
	body := string(msg[4:])
	if int(n) != len(body) {
		t.Fatalf("length prefix=%d body=%d", n, len(body))
	}
	if !strings.HasPrefix(body, "n,,") {
		t.Fatalf("body=%q", body)
	}
	if !strings.Contains(body, "auth=Bearer test-token") {
		t.Fatalf("body=%q", body)
	}
}

func TestBuildPlainMessage(t *testing.T) {
	msg := buildPlainMessage("user", "pass")
	n := binary.BigEndian.Uint32(msg[:4])
	body := msg[4:]
	if int(n) != len(body) {
		t.Fatal("length mismatch")
	}
	if body[0] != 0 || string(body[1:]) != "user\x00pass" {
		t.Fatalf("body=%q", body)
	}
}
