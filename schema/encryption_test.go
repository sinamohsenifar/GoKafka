package schema

import (
	"strings"
	"testing"
)

func TestFieldEncryptRoundTrip(t *testing.T) {
	kms, err := NewLocalKMS([]byte("0123456789abcdef0123456789abcdef")) // 32 bytes
	if err != nil {
		t.Fatal(err)
	}
	enc := NewFieldEncrypter(kms, "ssn", "email")

	rec := map[string]any{
		"id":    float64(7),
		"ssn":   "123-45-6789",
		"email": "ada@example.com",
		"name":  "Ada",
	}
	if err := enc.EncryptFields(rec); err != nil {
		t.Fatal(err)
	}

	// Selected fields are now opaque ciphertext; others untouched.
	for _, f := range []string{"ssn", "email"} {
		s, ok := rec[f].(string)
		if !ok || !strings.HasPrefix(s, "csfle:") {
			t.Fatalf("field %q not encrypted: %v", f, rec[f])
		}
		if strings.Contains(s, "123-45-6789") || strings.Contains(s, "ada@example.com") {
			t.Fatalf("plaintext leaked in %q", f)
		}
	}
	if rec["name"] != "Ada" {
		t.Fatalf("non-selected field changed: %v", rec["name"])
	}

	if err := enc.DecryptFields(rec); err != nil {
		t.Fatal(err)
	}
	if rec["ssn"] != "123-45-6789" || rec["email"] != "ada@example.com" {
		t.Fatalf("round-trip mismatch: %+v", rec)
	}
}

func TestFieldEncryptWrongKeyFails(t *testing.T) {
	k1, _ := NewLocalKMS([]byte("0123456789abcdef")) // 16 bytes
	k2, _ := NewLocalKMS([]byte("ffffffffffffffff"))
	rec := map[string]any{"secret": "top"}
	if err := NewFieldEncrypter(k1, "secret").EncryptFields(rec); err != nil {
		t.Fatal(err)
	}
	// Decrypting with a different master key must fail (GCM auth), not silently
	// return garbage.
	if err := NewFieldEncrypter(k2, "secret").DecryptFields(rec); err == nil {
		t.Fatal("decrypt with wrong KMS key should fail")
	}
}

func TestEncryptFieldsIdempotentSkip(t *testing.T) {
	kms, _ := NewLocalKMS([]byte("0123456789abcdef0123456789abcdef"))
	enc := NewFieldEncrypter(kms, "x")
	rec := map[string]any{"x": "v"}
	if err := enc.EncryptFields(rec); err != nil {
		t.Fatal(err)
	}
	first := rec["x"].(string)
	if err := enc.EncryptFields(rec); err != nil { // second call must not double-encrypt
		t.Fatal(err)
	}
	if rec["x"].(string) != first {
		t.Fatal("already-encrypted field was re-encrypted")
	}
}
