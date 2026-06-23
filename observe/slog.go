package observe

import (
	"context"
	"io"
	"log/slog"
)

// SlogLogger adapts GoKafka logging to log/slog (stdlib, Go 1.21+).
type SlogLogger struct {
	log   *slog.Logger
	level Level
}

// NewSlogLogger returns a Logger backed by slog with optional minimum level filter.
func NewSlogLogger(w io.Writer, level Level) *SlogLogger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slogLevel(level)})
	return &SlogLogger{log: slog.New(h), level: level}
}

// NewSlogTextLogger returns a text-format slog logger.
func NewSlogTextLogger(w io.Writer, level Level) *SlogLogger {
	h := slog.NewTextHandler(w, &slog.HandlerOptions{Level: slogLevel(level)})
	return &SlogLogger{log: slog.New(h), level: level}
}

func (l *SlogLogger) Log(ctx context.Context, level Level, msg string, attrs ...Attr) {
	if !level.enabled(l.level) {
		return
	}
	args := make([]any, 0, len(attrs)*2+4)
	tc := TraceFromContext(ctx)
	if tc.TraceID != "" {
		args = append(args, "trace.id", tc.TraceID)
	}
	if tc.SpanID != "" {
		args = append(args, "span.id", tc.SpanID)
	}
	for _, a := range attrs {
		if a.Key == "error" {
			if err, ok := a.Value.(error); ok {
				args = append(args, "error", ErrorObject(err))
				continue
			}
		}
		args = append(args, a.Key, a.Value)
	}
	l.log.Log(ctx, slogLevel(level), msg, args...)
}

func slogLevel(l Level) slog.Level {
	switch l {
	case LevelDebug:
		return slog.LevelDebug
	case LevelWarn:
		return slog.LevelWarn
	case LevelError:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
