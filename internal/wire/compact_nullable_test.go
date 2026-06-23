package wire

import "testing"

func TestCompactNullableStringNull(t *testing.T) {
	buf := NewBuffer(8)
	buf.WriteCompactNullableString(nil)
	b := FromBytes(buf.Bytes())
	n, err := b.ReadUvarint()
	if err != nil || n != 0 {
		t.Fatalf("null prefix: n=%d err=%v", n, err)
	}
}

func TestCompactNullableStringRoundTrip(t *testing.T) {
	val := "retention.ms"
	buf := NewBuffer(32)
	buf.WriteCompactNullableString(&val)
	got, err := FromBytes(buf.Bytes()).ReadCompactNullableString()
	if err != nil || got != val {
		t.Fatalf("got=%q err=%v", got, err)
	}
}
