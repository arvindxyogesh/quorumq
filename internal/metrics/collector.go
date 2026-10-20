package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/arvindxyogesh/quorumq/internal/queue"
)

// RaftStatus is the subset of a node's Raft state exported as gauges.
type RaftStatus struct {
	IsLeader          bool
	HasLeader         bool
	Term              uint64
	LastLogIndex      uint64
	CommitIndex       uint64
	AppliedIndex      uint64
	FSMAppliedIndex   uint64
	LastSnapshotIndex uint64
}

// StateSource is what the collector reads at scrape time. *cluster.Node
// implements it.
type StateSource interface {
	Store() *queue.Store
	RaftStatus() RaftStatus
}

var (
	queueJobsDesc = prometheus.NewDesc(namespace+"_queue_jobs",
		"Jobs in a queue by state, read from this node's replica. visible = claimable now; "+
			"in_flight = leased; delayed = waiting out a retry backoff; dead = in the dead-letter queue.",
		[]string{"queue", "state"}, nil)
	queueAckedDesc = prometheus.NewDesc(namespace+"_queue_acked",
		"Jobs acknowledged since the queue was created, read from this node's replica.",
		[]string{"queue"}, nil)
	isLeaderDesc = prometheus.NewDesc(namespace+"_raft_is_leader",
		"1 if this node is the Raft leader.", nil, nil)
	hasLeaderDesc = prometheus.NewDesc(namespace+"_raft_has_leader",
		"1 if this node knows of a leader.", nil, nil)
	termDesc = prometheus.NewDesc(namespace+"_raft_term",
		"Current Raft term.", nil, nil)
	indexDesc = prometheus.NewDesc(namespace+"_raft_index",
		"Raft log positions on this node: last (in the log), commit (known committed), "+
			"applied (handed to the state machine), fsm (last queue command the state machine "+
			"applied; lags applied by any trailing no-op or barrier entries), snapshot (covered by the newest snapshot).",
		[]string{"kind"}, nil)
	scrapeErrorsDesc = prometheus.NewDesc(namespace+"_collector_errors",
		"1 if reading queue state for this scrape failed.", nil, nil)
)

// Collector exports queue depth and Raft state when Prometheus scrapes.
// Queue depth comes from scanning each queue's index, so a scrape costs
// O(jobs waiting); that is fine at scrape intervals of several seconds.
type Collector struct {
	src StateSource
	now func() time.Time
}

// NewCollector returns a collector reading from src.
func NewCollector(src StateSource) *Collector {
	return &Collector{src: src, now: time.Now}
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{queueJobsDesc, queueAckedDesc, isLeaderDesc, hasLeaderDesc, termDesc, indexDesc, scrapeErrorsDesc} {
		ch <- d
	}
}

// Collect implements prometheus.Collector.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	rs := c.src.RaftStatus()
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	gauge(isLeaderDesc, boolToFloat(rs.IsLeader))
	gauge(hasLeaderDesc, boolToFloat(rs.HasLeader))
	gauge(termDesc, float64(rs.Term))
	gauge(indexDesc, float64(rs.LastLogIndex), "last")
	gauge(indexDesc, float64(rs.CommitIndex), "commit")
	gauge(indexDesc, float64(rs.AppliedIndex), "applied")
	gauge(indexDesc, float64(rs.FSMAppliedIndex), "fsm")
	gauge(indexDesc, float64(rs.LastSnapshotIndex), "snapshot")

	failed := 0.0
	if err := c.collectQueues(ch); err != nil {
		failed = 1
	}
	gauge(scrapeErrorsDesc, failed)
}

func (c *Collector) collectQueues(ch chan<- prometheus.Metric) error {
	store := c.src.Store()
	names, err := store.Queues()
	if err != nil {
		return err
	}
	now := c.now().UnixNano()
	for _, q := range names {
		st, err := store.Stats(q, now)
		if err != nil {
			return err
		}
		for state, v := range map[string]uint64{
			"visible": st.Visible, "in_flight": st.InFlight, "delayed": st.Delayed, "dead": st.Dead,
		} {
			ch <- prometheus.MustNewConstMetric(queueJobsDesc, prometheus.GaugeValue, float64(v), q, state)
		}
		ch <- prometheus.MustNewConstMetric(queueAckedDesc, prometheus.GaugeValue, float64(st.Acked), q)
	}
	return nil
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
