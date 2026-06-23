package gokafka

import "testing"

func TestRecordHeaders(t *testing.T) {
	r := Record{Topic: "t"}
	r.SetHeader("a", []byte("1"))
	r.SetHeader("b", []byte("2"))
	r.SetHeader("a", []byte("3"))

	if v, ok := r.GetHeader("a"); !ok || string(v) != "3" {
		t.Fatalf("a=%q ok=%v", v, ok)
	}
	r2 := r.WithHeaders(Header{Key: "c", Value: []byte("4")})
	if _, ok := r2.GetHeader("c"); !ok {
		t.Fatal("missing c")
	}
}
