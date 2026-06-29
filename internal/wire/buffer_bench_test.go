package wire

import "testing"

func BenchmarkWriteUUID(b *testing.B) {
	var u UUID
	for i := range u {
		u[i] = byte(i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := NewBuffer(16)
		buf.WriteUUID(u)
		_ = buf.Bytes()
	}
}

func BenchmarkUvarintRoundTrip(b *testing.B) {
	buf := NewBuffer(16)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.B = buf.B[:0]
		buf.I = 0
		buf.WriteUvarint(uint(i))
		if _, err := buf.ReadUvarint(); err != nil {
			b.Fatal(err)
		}
	}
}
