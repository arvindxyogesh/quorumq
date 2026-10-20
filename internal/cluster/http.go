package cluster

import (
	"encoding/json"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/arvindxyogesh/quorumq/internal/metrics"
)

// AdminHandler serves the node's operational HTTP endpoints:
//
//	GET /healthz  200 while the process is running (liveness)
//	GET /readyz   200 once the node knows a leader, 503 otherwise (readiness)
//	GET /status   JSON Raft state: role, term, leader, log indexes
//	GET /metrics  Prometheus metrics (when m is not nil)
func AdminHandler(n *Node, m *metrics.Metrics) http.Handler {
	mux := http.NewServeMux()
	if m != nil {
		m.Registry.MustRegister(metrics.NewCollector(n))
		mux.Handle("GET /metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{Registry: m.Registry}))
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if n.LeaderClientAddr() == "" {
			http.Error(w, "no leader", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(n.Status())
	})
	return mux
}
