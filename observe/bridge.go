package observe

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
)

// WritePrometheus writes metrics in Prometheus text exposition format (no external deps).
func WritePrometheus(w io.Writer, snap Snapshot, namespace string) error {
	if namespace == "" {
		namespace = "gokafka"
	}
	prefix := strings.ReplaceAll(namespace, "-", "_")
	metrics := []struct {
		name  string
		help  string
		value uint64
	}{
		{prefix + "_produce_records_total", "Total records produced", snap.Produced},
		{prefix + "_consume_records_total", "Total records consumed", snap.Consumed},
		{prefix + "_produce_errors_total", "Total produce errors", snap.ProduceErrors},
		{prefix + "_consume_errors_total", "Total consume errors", snap.ConsumeErrors},
		{prefix + "_produce_bytes_total", "Total bytes produced", snap.BytesProduced},
		{prefix + "_consume_bytes_total", "Total bytes consumed", snap.BytesConsumed},
		{prefix + "_broker_requests_total", "Total broker requests", snap.BrokerRequests},
		{prefix + "_broker_request_errors_total", "Total broker request errors", snap.BrokerReqErrors},
	}
	for _, m := range metrics {
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n",
			m.name, m.help, m.name, m.name, m.value); err != nil {
			return err
		}
	}
	gaugeName := prefix + "_broker_request_duration_avg_seconds"
	if _, err := fmt.Fprintf(w, "# HELP %s Average broker request duration in seconds\n# TYPE %s gauge\n%s %.6f\n",
		gaugeName, gaugeName, gaugeName, snap.AvgRequestMillis/1000.0); err != nil {
		return err
	}
	return nil
}

// PrometheusRecorder adapts MetricsRecorder calls to a Prometheus registry callback.
// Wire to github.com/prometheus/client_golang in application code without gokafka importing it.
type PrometheusRecorder struct {
	OnCounter   func(name string, value int64, labels map[string]string)
	OnHistogram func(name string, value float64, labels map[string]string)
	OnGauge     func(name string, value float64, labels map[string]string)
}

func (p PrometheusRecorder) IncCounter(name string, value int64, labels map[string]string) {
	if p.OnCounter != nil {
		p.OnCounter(sanitizeMetric(name), value, labels)
	}
}

func (p PrometheusRecorder) RecordHistogram(name string, value float64, labels map[string]string) {
	if p.OnHistogram != nil {
		p.OnHistogram(sanitizeMetric(name), value, labels)
	}
}

func (p PrometheusRecorder) SetGauge(name string, value float64, labels map[string]string) {
	if p.OnGauge != nil {
		p.OnGauge(sanitizeMetric(name), value, labels)
	}
}

func sanitizeMetric(name string) string {
	return strings.ReplaceAll(name, "-", "_")
}

// OTelBridge adapts GoKafka observability to OpenTelemetry SDK types in application code.
type OTelBridge struct {
	EmitLog       func(level Level, msg string, attrs map[string]any)
	RecordMetric  func(name string, kind string, value float64, attrs map[string]string)
	StartSpan     func(ctx context.Context, name string, attrs map[string]string) (context.Context, func())
}

// OTelMetricsRecorder wraps OTelBridge as a MetricsRecorder.
func OTelMetricsRecorder(b OTelBridge) MetricsRecorder {
	return otelMetricsAdapter{b}
}

type otelMetricsAdapter struct{ OTelBridge }

func (a otelMetricsAdapter) IncCounter(name string, value int64, labels map[string]string) {
	if a.RecordMetric != nil {
		a.RecordMetric(name, "counter", float64(value), labels)
	}
}

func (a otelMetricsAdapter) RecordHistogram(name string, value float64, labels map[string]string) {
	if a.RecordMetric != nil {
		a.RecordMetric(name, "histogram", value, labels)
	}
}

func (a otelMetricsAdapter) SetGauge(name string, value float64, labels map[string]string) {
	if a.RecordMetric != nil {
		a.RecordMetric(name, "gauge", value, labels)
	}
}

// ElasticAPMLogger adapts logs to Elastic APM / ECS ingestion pipelines.
type ElasticAPMLogger struct {
	ServiceName string
	Version     string
	Emit        func(ecsJSON []byte)
}

func (e ElasticAPMLogger) Log(ctx context.Context, level Level, msg string, attrs ...Attr) {
	l := NewLogger(LoggerConfig{
		Level:       LevelDebug,
		Format:      LogFormatECS,
		ServiceName: e.ServiceName,
		Version:     e.Version,
		Output:      writerFunc(func(b []byte) (int, error) {
			if e.Emit != nil {
				e.Emit(b)
			}
			return len(b), nil
		}),
	})
	l.Log(ctx, level, msg, attrs...)
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(b []byte) (int, error) { return f(b) }

// LabelString returns sorted label key=value pairs for debugging.
func LabelString(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "=" + labels[k]
	}
	return strings.Join(parts, ",")
}
