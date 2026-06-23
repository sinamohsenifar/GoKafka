package observe

import "context"

// StatusCode mirrors OpenTelemetry span status codes.
type StatusCode int

const (
	StatusUnset StatusCode = iota
	StatusOK
	StatusError
)

// Span is a single operation span (OpenTelemetry compatible).
type Span interface {
	End()
	SetStatus(code StatusCode, description string)
	RecordError(err error)
	SetAttributes(attrs ...Attr)
}

// Tracer starts spans for distributed tracing.
type Tracer interface {
	Start(ctx context.Context, name string, attrs ...Attr) (context.Context, Span)
}

// NoopTracer discards all traces.
type NoopTracer struct{}

func (NoopTracer) Start(ctx context.Context, _ string, _ ...Attr) (context.Context, Span) {
	return ctx, noopSpan{}
}

type noopSpan struct{}

func (noopSpan) End()                                    {}
func (noopSpan) SetStatus(StatusCode, string)            {}
func (noopSpan) RecordError(error)                         {}
func (noopSpan) SetAttributes(...Attr)                   {}

// RecordingSpan captures span data for tests and debug tracers.
type RecordingSpan struct {
	Name       string
	Attributes []Attr
	Status     StatusCode
	StatusMsg  string
	Err        error
	ended      bool
}

func (s *RecordingSpan) End() { s.ended = true }

func (s *RecordingSpan) SetStatus(code StatusCode, description string) {
	s.Status = code
	s.StatusMsg = description
}

func (s *RecordingSpan) RecordError(err error) { s.Err = err }

func (s *RecordingSpan) SetAttributes(attrs ...Attr) {
	s.Attributes = append(s.Attributes, attrs...)
}

// RecordingTracer creates RecordingSpan instances (useful in tests).
type RecordingTracer struct {
	Spans []*RecordingSpan
}

func (t *RecordingTracer) Start(ctx context.Context, name string, attrs ...Attr) (context.Context, Span) {
	sp := &RecordingSpan{Name: name, Attributes: append([]Attr(nil), attrs...)}
	t.Spans = append(t.Spans, sp)
	// generate simple trace/span ids for context propagation
	traceID := formatHex(len(t.Spans), 32)
	spanID := formatHex(len(t.Spans)*7, 16)
	ctx = ContextWithTrace(ctx, traceID, spanID)
	return ctx, sp
}

func formatHex(n int, width int) string {
	const hex = "0123456789abcdef"
	b := make([]byte, width)
	for i := range b {
		b[i] = hex[(n+i)%16]
	}
	return string(b)
}
