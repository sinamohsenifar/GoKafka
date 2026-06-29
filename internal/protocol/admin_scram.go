package protocol

import "github.com/sinamohsenifar/gokafka/internal/wire"

// SCRAM mechanism ids (KIP-554).
const (
	ScramMechanismSHA256 int8 = 1
	ScramMechanismSHA512 int8 = 2
)

// ScramUpsertion creates or updates a user's SCRAM credential.
type ScramUpsertion struct {
	Name           string
	Mechanism      int8
	Iterations     int32
	Salt           []byte
	SaltedPassword []byte
}

// ScramDeletion removes a user's SCRAM credential for one mechanism.
type ScramDeletion struct {
	Name      string
	Mechanism int8
}

// ScramResult is the per-user outcome of AlterUserScramCredentials.
type ScramResult struct {
	User         string
	ErrorCode    int16
	ErrorMessage string
}

// EncodeAlterUserScramCredentialsRequest encodes API 51 (always flexible, v0).
func EncodeAlterUserScramCredentialsRequest(deletions []ScramDeletion, upsertions []ScramUpsertion) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteCompactArrayLen(len(deletions))
	for _, d := range deletions {
		buf.WriteCompactString(d.Name)
		buf.WriteInt8(d.Mechanism)
		buf.WriteEmptyTagSection()
	}
	buf.WriteCompactArrayLen(len(upsertions))
	for _, u := range upsertions {
		buf.WriteCompactString(u.Name)
		buf.WriteInt8(u.Mechanism)
		buf.WriteInt32(u.Iterations)
		buf.WriteCompactBytes(u.Salt)
		buf.WriteCompactBytes(u.SaltedPassword)
		buf.WriteEmptyTagSection()
	}
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

// DecodeAlterUserScramCredentialsResponse decodes API 51 response (flexible v0).
func DecodeAlterUserScramCredentialsResponse(body []byte) ([]ScramResult, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
		return nil, err
	}
	n, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	out := make([]ScramResult, 0, safePrealloc(int(n)-1))
	for i := 1; i < int(n); i++ {
		var r ScramResult
		if r.User, err = buf.ReadCompactString(); err != nil {
			return nil, err
		}
		if r.ErrorCode, err = buf.ReadInt16(); err != nil {
			return nil, err
		}
		if r.ErrorMessage, err = buf.ReadCompactNullableString(); err != nil {
			return nil, err
		}
		if err := buf.SkipTagSection(); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := buf.SkipTagSection(); err != nil {
		return nil, err
	}
	return out, nil
}
