package gokafka

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestTLSMismatchHint(t *testing.T) {
	eof := fmt.Errorf("transport: read size: %w", io.EOF)

	// Plaintext (no TLS) + EOF -> hint appended, original error still unwrappable.
	got := tlsMismatchHint(eof, false)
	if !strings.Contains(got.Error(), "may require TLS") {
		t.Fatalf("expected TLS hint, got %v", got)
	}
	if !errors.Is(got, io.EOF) {
		t.Fatal("hint must still wrap io.EOF")
	}

	// TLS already configured -> no hint (the EOF means something else).
	if got := tlsMismatchHint(eof, true); strings.Contains(got.Error(), "may require TLS") {
		t.Fatal("must not hint TLS when TLS is already enabled")
	}

	// Non-EOF error -> unchanged.
	other := errors.New("connection refused")
	if got := tlsMismatchHint(other, false); got != other {
		t.Fatalf("non-EOF error must be unchanged, got %v", got)
	}

	// nil -> nil.
	if got := tlsMismatchHint(nil, false); got != nil {
		t.Fatalf("nil must stay nil, got %v", got)
	}
}
