// Package metrics defines quorumq's Prometheus metrics.
//
// Metrics fall into three groups:
//
//   - API traffic, recorded by a gRPC interceptor on every node:
//     quorumq_grpc_requests_total and quorumq_grpc_request_duration_seconds.
//   - Job flow, recorded by the node that serves a request (the leader):
//     quorumq_jobs_*_total counters per queue.
//   - Replication, recorded on every node: Raft state, leader changes, how
//     long proposals take to commit, and how the state machine applies
//     batches.
//
// Queue depth and Raft indexes are read when Prometheus scrapes, from the
// node's own replica, by the collector in collector.go.
package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

const namespace = "quorumq"

// Metrics holds every metric a node exports. A nil *Metrics is valid and
// records nothing, which keeps tests that do not care about metrics simple.
type Metrics struct {
	Registry *prometheus.Registry

	requests        *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec

	submitted    *prometheus.CounterVec
	claimed      *prometheus.CounterVec
	emptyClaims  *prometheus.CounterVec
	acked        *prometheus.CounterVec
	nacked       *prometheus.CounterVec
	deadLettered *prometheus.CounterVec
	redriven     *prometheus.CounterVec

	leaderChanges prometheus.Counter
	applyDuration *prometheus.HistogramVec
	fsmBatchTime  prometheus.Histogram
	fsmBatchSize  prometheus.Histogram
}

// latencyBuckets span 0.5ms to ~8s: a local Raft commit takes a few
// milliseconds, a failover a few seconds.
var latencyBuckets = prometheus.ExponentialBuckets(0.0005, 2, 15)

// New creates the metrics and registers them, plus Go runtime and process
// metrics, on a fresh registry.
func New() *Metrics {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "grpc_requests_total",
			Help: "gRPC requests handled by this node, by method and status code.",
		}, []string{"method", "code"}),
		requestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "grpc_request_duration_seconds",
			Help:    "Time to handle a gRPC request on this node, by method.",
			Buckets: latencyBuckets,
		}, []string{"method"}),
		submitted:    jobCounter("jobs_submitted_total", "Jobs enqueued."),
		claimed:      jobCounter("jobs_claimed_total", "Jobs leased to a worker (including redeliveries)."),
		emptyClaims:  jobCounter("claims_empty_total", "Claims that found nothing to lease."),
		acked:        jobCounter("jobs_acked_total", "Jobs acknowledged."),
		nacked:       jobCounter("jobs_nacked_total", "Failed attempts reported with a nack."),
		deadLettered: jobCounter("jobs_dead_lettered_total", "Jobs moved to the dead-letter queue by a nack."),
		redriven:     jobCounter("jobs_redriven_total", "Jobs moved from the dead-letter queue back to their queue."),
		leaderChanges: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "raft_leader_changes_total",
			Help: "Leader changes observed by this node (including losing a leader during an election).",
		}),
		applyDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "raft_apply_duration_seconds",
			Help:    "On the leader, time from proposing a command until it is committed by a quorum and applied, by operation.",
			Buckets: latencyBuckets,
		}, []string{"op"}),
		fsmBatchTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Name: "fsm_apply_batch_duration_seconds",
			Help:    "Time for the state machine to apply one batch of committed log entries (one BoltDB transaction).",
			Buckets: latencyBuckets,
		}),
		fsmBatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Name: "fsm_apply_batch_entries",
			Help:    "Log entries per state-machine batch.",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128},
		}),
	}
	m.Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.requests, m.requestDuration,
		m.submitted, m.claimed, m.emptyClaims, m.acked, m.nacked, m.deadLettered, m.redriven,
		m.leaderChanges, m.applyDuration, m.fsmBatchTime, m.fsmBatchSize,
	)
	return m
}

func jobCounter(name, help string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: name, Help: help + " Counted by the node that served the request.",
	}, []string{"queue"})
}

// JobSubmitted and the methods below record job-flow events.
func (m *Metrics) JobSubmitted(queue string) {
	if m != nil {
		m.submitted.WithLabelValues(queue).Inc()
	}
}

// JobClaimed records a claim; empty means the queue had nothing to lease.
func (m *Metrics) JobClaimed(queue string, empty bool) {
	switch {
	case m == nil:
	case empty:
		m.emptyClaims.WithLabelValues(queue).Inc()
	default:
		m.claimed.WithLabelValues(queue).Inc()
	}
}

// JobAcked records an ack.
func (m *Metrics) JobAcked(queue string) {
	if m != nil {
		m.acked.WithLabelValues(queue).Inc()
	}
}

// JobNacked records a nack and whether it dead-lettered the job.
func (m *Metrics) JobNacked(queue string, deadLettered bool) {
	if m == nil {
		return
	}
	m.nacked.WithLabelValues(queue).Inc()
	if deadLettered {
		m.deadLettered.WithLabelValues(queue).Inc()
	}
}

// JobRedriven records a redrive.
func (m *Metrics) JobRedriven(queue string) {
	if m != nil {
		m.redriven.WithLabelValues(queue).Inc()
	}
}

// LeaderChanged records a leader change observed by this node.
func (m *Metrics) LeaderChanged() {
	if m != nil {
		m.leaderChanges.Inc()
	}
}

// ObserveApply records how long a proposal took to commit and apply.
func (m *Metrics) ObserveApply(op string, d time.Duration) {
	if m != nil {
		m.applyDuration.WithLabelValues(op).Observe(d.Seconds())
	}
}

// ObserveFSMBatch records one state-machine batch.
func (m *Metrics) ObserveFSMBatch(entries int, d time.Duration) {
	if m != nil {
		m.fsmBatchSize.Observe(float64(entries))
		m.fsmBatchTime.Observe(d.Seconds())
	}
}

// UnaryServerInterceptor records request counts and latency per method.
func (m *Metrics) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		if m != nil {
			method := methodName(info.FullMethod)
			m.requests.WithLabelValues(method, status.Code(err).String()).Inc()
			m.requestDuration.WithLabelValues(method).Observe(time.Since(start).Seconds())
		}
		return resp, err
	}
}

// methodName trims "/quorumq.v1.QueueService/SubmitJob" to "SubmitJob".
func methodName(full string) string {
	for i := len(full) - 1; i >= 0; i-- {
		if full[i] == '/' {
			return full[i+1:]
		}
	}
	return full
}
