package schema

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// csfleField marks an encrypted field value. The remainder is base64 of the
// envelope: varint(len(wrappedDEK)) | wrappedDEK | 12-byte nonce | ciphertext.
const csfleField = "csfle:"

// KMS performs envelope encryption: it wraps and unwraps a per-record data
// encryption key (DEK) with a key-encryption key (KEK) it holds. This is the
// integration point for client-side field-level encryption (CSFLE): GoKafka
// ships a pure-Go LocalKMS and this interface, and callers can plug AWS/GCP/
// Azure/Vault KMS drivers themselves — keeping GoKafka dependency-free.
type KMS interface {
	WrapDEK(dek []byte) (wrapped []byte, err error)
	UnwrapDEK(wrapped []byte) (dek []byte, err error)
}

// LocalKMS is an in-process KMS that wraps DEKs with AES-256-GCM under a master
// key. Suitable for tests and single-process key custody; for production key
// management, implement KMS against a real key service.
type LocalKMS struct {
	aead cipher.AEAD
}

// NewLocalKMS creates a LocalKMS from a 16-, 24-, or 32-byte master key.
func NewLocalKMS(masterKey []byte) (*LocalKMS, error) {
	block, err := aes.NewCipher(masterKey)
	if err != nil {
		return nil, fmt.Errorf("schema: local KMS key: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &LocalKMS{aead: aead}, nil
}

// WrapDEK encrypts a data encryption key under the master key.
func (k *LocalKMS) WrapDEK(dek []byte) ([]byte, error) {
	nonce := make([]byte, k.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return k.aead.Seal(nonce, nonce, dek, nil), nil
}

// UnwrapDEK decrypts a wrapped data encryption key.
func (k *LocalKMS) UnwrapDEK(wrapped []byte) ([]byte, error) {
	ns := k.aead.NonceSize()
	if len(wrapped) < ns {
		return nil, fmt.Errorf("schema: wrapped DEK too short")
	}
	return k.aead.Open(nil, wrapped[:ns], wrapped[ns:], nil)
}

// FieldEncrypter encrypts and decrypts selected fields of a record map in place,
// using a fresh AES-256-GCM data key per call wrapped by the KMS (envelope
// encryption) — the client-side field-level encryption (CSFLE) pattern. Encrypt
// before serializing with a Serde; Decrypt after deserializing. Field values are
// JSON-encoded before encryption, so string PII fields round-trip exactly;
// numeric fields come back as JSON numbers.
type FieldEncrypter struct {
	kms    KMS
	fields map[string]struct{}
}

// NewFieldEncrypter returns an encrypter for the named fields.
func NewFieldEncrypter(kms KMS, fields ...string) *FieldEncrypter {
	set := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		set[f] = struct{}{}
	}
	return &FieldEncrypter{kms: kms, fields: set}
}

// EncryptFields encrypts the configured fields of record in place. Missing
// fields and already-encrypted values are skipped.
func (e *FieldEncrypter) EncryptFields(record map[string]any) error {
	dek := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		return err
	}
	wrapped, err := e.kms.WrapDEK(dek)
	if err != nil {
		return err
	}
	aead, err := newGCM(dek)
	if err != nil {
		return err
	}
	for name := range e.fields {
		v, ok := record[name]
		if !ok {
			continue
		}
		if s, ok := v.(string); ok && strings.HasPrefix(s, csfleField) {
			continue // already encrypted
		}
		plain, err := json.Marshal(v)
		if err != nil {
			return err
		}
		nonce := make([]byte, aead.NonceSize())
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			return err
		}
		ct := aead.Seal(nil, nonce, plain, nil)
		record[name] = csfleField + base64.StdEncoding.EncodeToString(envelope(wrapped, nonce, ct))
	}
	return nil
}

// DecryptFields decrypts the configured fields of record in place. Fields that
// are absent or not marked encrypted are left unchanged.
func (e *FieldEncrypter) DecryptFields(record map[string]any) error {
	for name := range e.fields {
		s, ok := record[name].(string)
		if !ok || !strings.HasPrefix(s, csfleField) {
			continue
		}
		raw, err := base64.StdEncoding.DecodeString(s[len(csfleField):])
		if err != nil {
			return err
		}
		wrapped, nonce, ct, err := parseEnvelope(raw)
		if err != nil {
			return err
		}
		dek, err := e.kms.UnwrapDEK(wrapped)
		if err != nil {
			return err
		}
		aead, err := newGCM(dek)
		if err != nil {
			return err
		}
		plain, err := aead.Open(nil, nonce, ct, nil)
		if err != nil {
			return err
		}
		var v any
		if err := json.Unmarshal(plain, &v); err != nil {
			return err
		}
		record[name] = v
	}
	return nil
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func envelope(wrapped, nonce, ct []byte) []byte {
	hdr := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(hdr, uint64(len(wrapped)))
	out := make([]byte, 0, n+len(wrapped)+len(nonce)+len(ct))
	out = append(out, hdr[:n]...)
	out = append(out, wrapped...)
	out = append(out, nonce...)
	out = append(out, ct...)
	return out
}

func parseEnvelope(raw []byte) (wrapped, nonce, ct []byte, err error) {
	wlen, n := binary.Uvarint(raw)
	if n <= 0 || int(wlen) > len(raw)-n {
		return nil, nil, nil, fmt.Errorf("schema: corrupt CSFLE envelope")
	}
	raw = raw[n:]
	wrapped = raw[:wlen]
	raw = raw[wlen:]
	const nonceSize = 12 // AES-GCM standard nonce
	if len(raw) < nonceSize {
		return nil, nil, nil, fmt.Errorf("schema: corrupt CSFLE envelope (nonce)")
	}
	nonce = raw[:nonceSize]
	ct = raw[nonceSize:]
	return wrapped, nonce, ct, nil
}
