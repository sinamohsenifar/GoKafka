package compress

import "fmt"

// ZSTD magic skippable frame prefix (0xFD2FB528 little-endian at frame start).
const zstdMagic uint32 = 0xFD2FB528

// ErrZstdNotSupported indicates ZSTD codec is not yet implemented (see docs/ZSTD.md).
var ErrZstdNotSupported = fmt.Errorf("compress: zstd not supported (stdlib-only); see docs/ZSTD.md")

// IsZstdFrame reports whether data begins with a ZSTD frame magic.
func IsZstdFrame(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	return uint32(data[0])|uint32(data[1])<<8|uint32(data[2])<<16|uint32(data[3])<<24 == zstdMagic
}
