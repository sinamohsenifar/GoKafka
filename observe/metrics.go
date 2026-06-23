package observe

import (
	"sync"
	"sync/atomic"
	"time"
)

// MetricsRecorder records counters, gauges, and histograms.
// Implementations should use OpenTelemetry/Prometheus-compatible naming.
type MetricsRecorder interface {
	IncCounter(name string, value int64, labels map[string]string)
	RecordHistogram(name string, value float64, labels map[string]string)
	SetGauge(name string, value float64, labels map[string]string)
}

// MetricsConfig configures the built-in metrics collector.
type MetricsConfig struct {
	Namespace string
	Enabled   bool
}

// Collector is the native in-process metrics implementation.
type Collector struct {
	namespace string
	enabled   bool

	produced      atomic.Uint64
	consumed      atomic.Uint64
	produceErrors atomic.Uint64
	consumeErrors atomic.Uint64
	bytesProduced atomic.Uint64
	bytesConsumed atomic.Uint64
	requests      atomic.Uint64
	requestErrors atomic.Uint64
	requestNanos  atomic.Uint64

	hooks []MetricsRecorder
	mu    sync.RWMutex
}

func NewCollector(cfg MetricsConfig) *Collector {
	ns := cfg.Namespace
	if ns == "" {
		ns = "gokafka"
	}
	return &Collector{namespace: ns, enabled: cfg.Enabled}
}

// RegisterHook adds an external metrics recorder (Prometheus, OTel, etc.).
func (c *Collector) RegisterHook(h MetricsRecorder) {
	c.mu.Lock()
	c.hooks = append(c.hooks, h)
	c.mu.Unlock()
}

func (c *Collector) labels(extra map[string]string) map[string]string {
	out := map[string]string{"client": "gokafka"}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

func (c *Collector) emit(fn func(MetricsRecorder)) {
	c.mu.RLock()
	hooks := append([]MetricsRecorder(nil), c.hooks...)
	c.mu.RUnlock()
	for _, h := range hooks {
		fn(h)
	}
}

func (c *Collector) OnProduce(nBytes int, err error) {
	if !c.enabled {
		return
	}
	if err != nil {
		c.produceErrors.Add(1)
		c.emit(func(h MetricsRecorder) {
			h.IncCounter(c.name("produce_errors_total"), 1, c.labels(map[string]string{"operation": "produce"}))
		})
		return
	}
	c.produced.Add(1)
	c.bytesProduced.Add(uint64(nBytes))
	c.emit(func(h MetricsRecorder) {
		h.IncCounter(c.name("produce_records_total"), 1, c.labels(map[string]string{"operation": "produce"}))
		h.IncCounter(c.name("produce_bytes_total"), int64(nBytes), c.labels(map[string]string{"operation": "produce"}))
	})
}

func (c *Collector) OnConsume(nBytes int, err error) {
	if !c.enabled {
		return
	}
	if err != nil {
		c.consumeErrors.Add(1)
		c.emit(func(h MetricsRecorder) {
			h.IncCounter(c.name("consume_errors_total"), 1, c.labels(map[string]string{"operation": "consume"}))
		})
		return
	}
	c.consumed.Add(1)
	c.bytesConsumed.Add(uint64(nBytes))
	c.emit(func(h MetricsRecorder) {
		h.IncCounter(c.name("consume_records_total"), 1, c.labels(map[string]string{"operation": "consume"}))
		h.IncCounter(c.name("consume_bytes_total"), int64(nBytes), c.labels(map[string]string{"operation": "consume"}))
	})
}

// OnRequest records broker request latency (seconds).
func (c *Collector) OnRequest(apiKey int16, d time.Duration, err error) {
	if !c.enabled {
		return
	}
	c.requests.Add(1)
	c.requestNanos.Add(uint64(d.Nanoseconds()))
	labels := map[string]string{"api_key": formatValue(apiKey)}
	if err != nil {
		c.requestErrors.Add(1)
		labels["result"] = "error"
	} else {
		labels["result"] = "success"
	}
	sec := d.Seconds()
	c.emit(func(h MetricsRecorder) {
		h.IncCounter(c.name("broker_requests_total"), 1, c.labels(labels))
		h.RecordHistogram(c.name("broker_request_duration_seconds"), sec, c.labels(labels))
	})
}

func (c *Collector) name(metric string) string { return c.namespace + "_" + metric }

// Snapshot is a point-in-time metrics summary.
type Snapshot struct {
	Produced         uint64
	Consumed         uint64
	ProduceErrors    uint64
	ConsumeErrors    uint64
	BytesProduced    uint64
	BytesConsumed    uint64
	BrokerRequests   uint64
	BrokerReqErrors  uint64
	AvgRequestMillis float64
}

func (c *Collector) Snapshot() Snapshot {
	reqs := c.requests.Load()
	var avg float64
	if reqs > 0 {
		avg = float64(c.requestNanos.Load()) / float64(reqs) / 1e6
	}
	return Snapshot{
		Produced:         c.produced.Load(),
		Consumed:         c.consumed.Load(),
		ProduceErrors:    c.produceErrors.Load(),
		ConsumeErrors:    c.consumeErrors.Load(),
		BytesProduced:    c.bytesProduced.Load(),
		BytesConsumed:    c.bytesConsumed.Load(),
		BrokerRequests:   reqs,
		BrokerReqErrors:  c.requestErrors.Load(),
		AvgRequestMillis: avg,
	}
}

func (c *Collector) IncCounter(name string, value int64, labels map[string]string) {
	if !c.enabled {
		return
	}
	c.emit(func(h MetricsRecorder) { h.IncCounter(name, value, labels) })
}

func (c *Collector) RecordHistogram(name string, value float64, labels map[string]string) {
	if !c.enabled {
		return
	}
	c.emit(func(h MetricsRecorder) { h.RecordHistogram(name, value, labels) })
}

func (c *Collector) SetGauge(name string, value float64, labels map[string]string) {
	if !c.enabled {
		return
	}
	c.emit(func(h MetricsRecorder) { h.SetGauge(name, value, labels) })
}

// NoopMetrics discards all metrics.
type NoopMetrics struct{}

func (NoopMetrics) IncCounter(string, int64, map[string]string)          {}
func (NoopMetrics) RecordHistogram(string, float64, map[string]string)   {}
func (NoopMetrics) SetGauge(string, float64, map[string]string)        {}
