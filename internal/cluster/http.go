package cluster

import (
	"encoding/json"
	"net/http"
)

// AdminHandler serves the node's operational HTTP endpoints:
//
//	GET /healthz  200 while the process is running (liveness)
//	GET /readyz   200 once the node knows a leader, 503 otherwise (readiness)
//	GET /status   JSON Raft state: role, term, leader, log indexes
func AdminHandler(n *Node) http.Handler {
	mux := http.NewServeMux()
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
