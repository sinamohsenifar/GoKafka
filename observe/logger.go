package observe

import (
	"context"
	"io"
	"os"
	"sync"
	"time"
)

// LogFormat selects structured log encoding.
type LogFormat int

const (
	LogFormatText LogFormat = iota
	LogFormatJSON
	LogFormatECS // Elastic Common Schema for Elasticsearch / Elastic APM
)

// Logger emits structured logs at configurable levels and formats.
type Logger interface {
	Log(ctx context.Context, level Level, msg string, attrs ...Attr)
}

// LoggerConfig configures the native GoKafka logger.
type LoggerConfig struct {
	Level       Level
	Format      LogFormat
	Output      io.Writer
	ServiceName string
	Version     string
}

func defaultLoggerConfig() LoggerConfig {
	return LoggerConfig{
		Level:       LevelInfo,
		Format:      LogFormatJSON,
		Output:      os.Stderr,
		ServiceName: "gokafka",
	}
}

// NativeLogger is the built-in structured logger (text, JSON, or ECS).
type NativeLogger struct {
	cfg LoggerConfig
	mu  sync.Mutex
}

func NewLogger(cfg LoggerConfig) *NativeLogger {
	if cfg.Output == nil {
		cfg.Output = os.Stderr
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "gokafka"
	}
	return &NativeLogger{cfg: cfg}
}

func (l *NativeLogger) Log(ctx context.Context, level Level, msg string, attrs ...Attr) {
	if !level.enabled(l.cfg.Level) {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	switch l.cfg.Format {
	case LogFormatECS:
		_, _ = l.cfg.Output.Write(encodeECS(ctx, l.cfg, level, msg, attrs))
	case LogFormatJSON:
		_, _ = l.cfg.Output.Write(encodeJSON(ctx, l.cfg, level, msg, attrs))
	default:
		_, _ = l.cfg.Output.Write(encodeText(ctx, level, msg, attrs))
	}
}

// NoopLogger discards all log output.
type NoopLogger struct{}

func (NoopLogger) Log(context.Context, Level, string, ...Attr) {}

func encodeText(ctx context.Context, level Level, msg string, attrs []Attr) []byte {
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	b := make([]byte, 0, 256)
	b = append(b, ts...)
	b = append(b, ' ')
	b = append(b, level.String()...)
	b = append(b, ' ')
	b = append(b, msg...)
	if tc := TraceFromContext(ctx); tc.TraceID != "" {
		b = append(b, " trace.id="...)
		b = append(b, tc.TraceID...)
	}
	for _, a := range attrs {
		b = append(b, ' ')
		b = append(b, a.Key...)
		b = append(b, '=')
		b = append(b, formatValue(a.Value)...)
	}
	b = append(b, '\n')
	return b
}
