package auth

import (
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"hash"
)

// SaltedPassword computes the SCRAM SaltedPassword (RFC 5802 Hi / PBKDF2, one
// hLen-sized block) for the given mechanism. mechanism must be "SCRAM-SHA-256"
// or "SCRAM-SHA-512". This is the value sent in an AlterUserScramCredentials
// upsertion; the broker derives the stored/server keys from it.
func SaltedPassword(mechanism, password string, salt []byte, iterations int) ([]byte, error) {
	var hf func() hash.Hash
	switch mechanism {
	case "SCRAM-SHA-256":
		hf = sha256.New
	case "SCRAM-SHA-512":
		hf = sha512.New
	default:
		return nil, fmt.Errorf("auth: unknown SCRAM mechanism %q", mechanism)
	}
	return pbkdf2(hf, password, salt, iterations), nil
}
