package gokafka

import (
	"context"
	"io"
	"net/http"

	"github.com/sinamohsenifar/gokafka/observe"
)

type (
	ObservabilityConfig = observe.Config
	LogLevel            = observe.Level
	LogFormat           = observe.LogFormat
	Logger              = observe.Logger
	MetricsRecorder     = observe.MetricsRecorder
	Tracer              = observe.Tracer
	Span                = observe.Span
	Attr                = observe.Attr
	PrometheusRecorder  = observe.PrometheusRecorder
	OTelBridge          = observe.OTelBridge
	ElasticAPMLogger    = observe.ElasticAPMLogger
)

const (
	LogLevelDebug = observe.LevelDebug
	LogLevelInfo  = observe.LevelInfo
	LogLevelWarn  = observe.LevelWarn
	LogLevelError = observe.LevelError

	LogFormatText = observe.LogFormatText
	LogFormatJSON = observe.LogFormatJSON
	LogFormatECS  = observe.LogFormatECS
)

// Attr helpers.
var (
	StringAttr  = observe.String
	Int64Attr   = observe.Int64
	IntAttr     = observe.Int
	BoolAttr    = observe.Bool
	Float64Attr = observe.Float64
	ErrorAttr   = observe.Error
)

// WithObservability configures logging, metrics, and tracing.
func WithObservability(cfg ObservabilityConfig) Option {
	return func(c *Config) { c.Observability.Config = cfg }
}

// WithLogLevel sets the built-in logger minimum level.
func WithLogLevel(level LogLevel) Option {
	return func(c *Config) { c.Observability.LogLevel = level }
}

// WithLogFormat sets text, JSON, or ECS (Elastic APM) log encoding.
func WithLogFormat(format LogFormat) Option {
	return func(c *Config) { c.Observability.LogFormat = format }
}

// WithLogger replaces the built-in logger (e.g. custom OTel log bridge).
func WithLogger(l Logger) Option {
	return func(c *Config) { c.Observability.Logger = l }
}

// WithTracer replaces the built-in tracer (wire OTel Tracer via adapter).
func WithTracer(t Tracer) Option {
	return func(c *Config) { c.Observability.Tracer = t }
}

// WithSlogLogger uses log/slog JSON output as the client logger.
func WithSlogLogger(w io.Writer, level LogLevel) Option {
	return func(c *Config) {
		c.Observability.Logger = observe.NewSlogLogger(w, level)
	}
}

// WithMetricsHook registers an external metrics recorder (Prometheus, OTel, etc.).
func WithMetricsHook(h MetricsRecorder) Option {
	return func(c *Config) {
		c.Observability.extraHooks = append(c.Observability.extraHooks, h)
	}
}

// PrometheusHandler returns an http.Handler exposing metrics for Prometheus scraping.
func (c *Client) PrometheusHandler() http.Handler {
	ns := c.cfg.Metrics.Namespace
	if ns == "" {
		ns = "gokafka"
	}
	return observe.PrometheusHTTPHandler(c.observe.Metrics, ns)
}

// WritePrometheus writes current metrics in Prometheus text exposition format.
func (c *Client) WritePrometheus(w io.Writer) error {
	ns := c.cfg.Metrics.Namespace
	if ns == "" {
		ns = "gokafka"
	}
	return observe.WritePrometheus(w, c.observe.Metrics.Snapshot(), ns)
}

// RegisterPrometheusBridge wires Prometheus client_golang callbacks without a gokafka dependency on it.
func (c *Client) RegisterPrometheusBridge(rec PrometheusRecorder) {
	c.observe.RegisterMetricsHook(rec)
}

// RegisterOTelBridge wires OpenTelemetry SDK callbacks without a gokafka dependency on it.
func (c *Client) RegisterOTelBridge(b OTelBridge) {
	c.observe.RegisterMetricsHook(observe.OTelMetricsRecorder(b))
}

// Log emits a structured log through the client logger.
func (c *Client) Log(ctx context.Context, level LogLevel, msg string, attrs ...Attr) {
	c.observe.Log(ctx, level, msg, attrs...)
}

// ErrorObject returns structured error fields for JSON/ECS logging.
func ErrorObject(err error) map[string]any { return observe.ErrorObject(err) }

// ErrorJSON marshals an error for log aggregation pipelines.
func ErrorJSON(err error) []byte { return observe.ErrorJSON(err) }
