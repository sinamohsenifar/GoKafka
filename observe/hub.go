package observe

import (
	"context"
)

// Hub bundles logging, metrics, and tracing for the Kafka client.
type Hub struct {
	Logger  Logger
	Metrics *Collector
	Tracer  Tracer
}

// Config configures observability for GoKafka.
type Config struct {
	ServiceName string
	Version     string
	LogLevel    Level
	LogFormat   LogFormat
	Metrics     MetricsConfig
	Logger      Logger // optional override
	Tracer      Tracer // optional override
}

func NewHub(cfg Config) *Hub {
	logger := cfg.Logger
	if logger == nil {
		logger = NewLogger(LoggerConfig{
			Level:       cfg.LogLevel,
			Format:      cfg.LogFormat,
			ServiceName: cfg.ServiceName,
			Version:     cfg.Version,
		})
	}
	tracer := cfg.Tracer
	if tracer == nil {
		tracer = NoopTracer{}
	}
	metrics := NewCollector(cfg.Metrics)
	return &Hub{Logger: logger, Metrics: metrics, Tracer: tracer}
}

func (h *Hub) Log(ctx context.Context, level Level, msg string, attrs ...Attr) {
	if h == nil || h.Logger == nil {
		return
	}
	h.Logger.Log(ctx, level, msg, attrs...)
}

func (h *Hub) StartSpan(ctx context.Context, name string, attrs ...Attr) (context.Context, Span) {
	if h == nil || h.Tracer == nil {
		return ctx, noopSpan{}
	}
	return h.Tracer.Start(ctx, name, attrs...)
}

func (h *Hub) RegisterMetricsHook(rec MetricsRecorder) {
	if h != nil && h.Metrics != nil {
		h.Metrics.RegisterHook(rec)
	}
}
