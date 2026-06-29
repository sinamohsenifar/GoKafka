package wire

import (
	"encoding/binary"
	"fmt"
)

// UUID is a 16-byte Kafka topic/member identifier.
type UUID [16]byte

func (u UUID) IsZero() bool {
	return u == UUID{}
}

// String returns the canonical UUID string.
func (u UUID) String() string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		binary.BigEndian.Uint32(u[0:4]),
		binary.BigEndian.Uint16(u[4:6]),
		binary.BigEndian.Uint16(u[6:8]),
		binary.BigEndian.Uint16(u[8:10]),
		u[10:16])
}

// ReadUUID reads a big-endian UUID (two int64).
func (b *Buffer) ReadUUID() (UUID, error) {
	var u UUID
	if b.I+16 > len(b.B) {
		return u, ErrShortBuffer
	}
	copy(u[:], b.B[b.I:b.I+16])
	b.I += 16
	return u, nil
}

// WriteUUID writes a UUID as 16 raw big-endian bytes.
func (b *Buffer) WriteUUID(u UUID) {
	b.B = append(b.B, u[:]...)
}
