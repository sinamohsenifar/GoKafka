package observe

import (
	"net/http"
	"sync"
)

// PrometheusHTTPHandler serves metrics in Prometheus text format.
func PrometheusHTTPHandler(collector *Collector, namespace string) http.Handler {
	var mu sync.Mutex
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if collector == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		mu.Lock()
		snap := collector.Snapshot()
		mu.Unlock()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_ = WritePrometheus(w, snap, namespace)
	})
}
