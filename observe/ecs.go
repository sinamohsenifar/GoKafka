package observe

import (
	"context"
	"encoding/json"
	"time"
)

// TraceContext carries distributed trace identifiers (OpenTelemetry / Elastic APM compatible).
type TraceContext struct {
	TraceID string
	SpanID  string
}

type traceKey struct{}

// ContextWithTrace attaches trace identifiers to ctx.
func ContextWithTrace(ctx context.Context, traceID, spanID string) context.Context {
	return context.WithValue(ctx, traceKey{}, TraceContext{TraceID: traceID, SpanID: spanID})
}

// TraceFromContext reads trace identifiers from ctx.
func TraceFromContext(ctx context.Context) TraceContext {
	if ctx == nil {
		return TraceContext{}
	}
	if tc, ok := ctx.Value(traceKey{}).(TraceContext); ok {
		return tc
	}
	return TraceContext{}
}

func encodeJSON(ctx context.Context, cfg LoggerConfig, level Level, msg string, attrs []Attr) []byte {
	entry := baseLogEntry(ctx, cfg, level, msg, attrs)
	b, _ := json.Marshal(entry)
	b = append(b, '\n')
	return b
}

func encodeECS(ctx context.Context, cfg LoggerConfig, level Level, msg string, attrs []Attr) []byte {
	entry := baseLogEntry(ctx, cfg, level, msg, attrs)
	// ECS field names
	ecs := map[string]any{
		"@timestamp":      entry["timestamp"],
		"message":         entry["message"],
		"log.level":       entry["level"],
		"service.name":    cfg.ServiceName,
		"service.version": cfg.Version,
		"labels":          entry["labels"],
	}
	if tc := TraceFromContext(ctx); tc.TraceID != "" {
		ecs["trace.id"] = tc.TraceID
	}
	if tc := TraceFromContext(ctx); tc.SpanID != "" {
		ecs["span.id"] = tc.SpanID
	}
	if errObj, ok := entry["error"].(map[string]any); ok {
		ecs["error.type"] = errObj["type"]
		ecs["error.message"] = errObj["message"]
		for k, v := range errObj {
			if k != "type" && k != "message" {
				ecs[k] = v
			}
		}
	}
	for k, v := range entry {
		switch k {
		case "timestamp", "message", "level", "labels", "error":
			continue
		default:
			ecs[k] = v
		}
	}
	b, _ := json.Marshal(ecs)
	b = append(b, '\n')
	return b
}

func baseLogEntry(ctx context.Context, cfg LoggerConfig, level Level, msg string, attrs []Attr) map[string]any {
	entry := map[string]any{
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"level":     level.String(),
		"message":   msg,
		"service":   cfg.ServiceName,
	}
	if cfg.Version != "" {
		entry["service.version"] = cfg.Version
	}
	tc := TraceFromContext(ctx)
	if tc.TraceID != "" {
		entry["trace.id"] = tc.TraceID
	}
	if tc.SpanID != "" {
		entry["span.id"] = tc.SpanID
	}
	labels := map[string]string{}
	for _, a := range attrs {
		switch a.Key {
		case "error":
			if err, ok := a.Value.(error); ok {
				entry["error"] = ErrorObject(err)
			}
		default:
			labels[a.Key] = formatValue(a.Value)
			entry[a.Key] = a.Value
		}
	}
	if len(labels) > 0 {
		entry["labels"] = labels
	}
	return entry
}
