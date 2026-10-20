package cluster

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/arvindxyogesh/quorumq/internal/broker"
	"github.com/arvindxyogesh/quorumq/internal/metrics"
)

// A real cluster must export job counters, queue depth and Raft state on
// /metrics in the Prometheus text format.
func TestMetricsEndpoint(t *testing.T) {
	c := newTestCluster(t, 1) // created without metrics; restart node 0 with them
	c.stop(0)
	m := metrics.New()
	c.cfgs[0].Metrics = m
	c.start(0)
	c.waitForLeader()

	n := c.nodes[0]
	b := NewBroker(n, broker.DefaultRetryPolicy)
	ctx := context.Background()
	id, _ := b.Submit(ctx, "emails", []byte("x"), 3)
	_, _ = b.Submit(ctx, "emails", []byte("y"), 3)
	j, err := b.Claim(ctx, "emails", "w", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Ack(ctx, id, j.LeaseID); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(AdminHandler(n, m))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	text := string(body)

	for _, want := range []string{
		`quorumq_jobs_submitted_total{queue="emails"} 2`,
		`quorumq_jobs_claimed_total{queue="emails"} 1`,
		`quorumq_jobs_acked_total{queue="emails"} 1`,
		`quorumq_queue_jobs{queue="emails",state="visible"} 1`,
		`quorumq_queue_jobs{queue="emails",state="in_flight"} 0`,
		`quorumq_queue_acked{queue="emails"} 1`,
		`quorumq_raft_is_leader 1`,
		`quorumq_raft_has_leader 1`,
		`quorumq_raft_apply_duration_seconds_count{op="submit"} 2`,
		`quorumq_fsm_apply_batch_entries_count`,
		`quorumq_raft_leader_changes_total`,
		`quorumq_raft_index{kind="commit"}`,
		`quorumq_raft_index{kind="fsm"}`,
		`quorumq_collector_errors 0`,
		`go_goroutines`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("/metrics is missing %q", want)
		}
	}
	if t.Failed() {
		t.Logf("metrics output:\n%s", text)
	}
}
