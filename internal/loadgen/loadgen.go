// Package loadgen drives a quorumq cluster with concurrent producers and
// workers, measures end-to-end latency, and reconciles every acknowledged
// submit against the jobs workers completed.
//
// Each payload carries a unique token and the time its Submit call started.
// A job counts as completed when a worker's Ack for it succeeds; its
// submit-to-ack latency runs from the start of Submit to the end of that
// Ack, so it includes the submit commit, time queued, the claim commit,
// worker polling delay and the ack commit. Producers and workers run in the
// same process, so both ends use one clock.
package loadgen

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/arvindxyogesh/quorumq/client"
)

// Config describes one load run.
type Config struct {
	Queue     string
	Producers int
	Workers   int
	// Rate is the target total submits per second across all producers;
	// zero means each producer submits as fast as its requests complete
	// (closed loop).
	Rate        float64
	Duration    time.Duration
	PayloadSize int
	// Visibility is the lease length workers claim with.
	Visibility time.Duration
	MaxRetries uint32
	// DrainTimeout bounds how long workers keep going after producers stop,
	// waiting for every acknowledged submit to be completed.
	DrainTimeout time.Duration
	// IdleSleep is how long a worker waits after an empty claim.
	IdleSleep time.Duration
	// RequestTimeout bounds each client call, including retries across a
	// failover.
	RequestTimeout time.Duration
	RunID          string
}

// Result is everything a run measured.
type Result struct {
	Config      Config    `json:"config"`
	StartedAt   time.Time `json:"started_at"`
	GoMaxProcs  int       `json:"gomaxprocs"`
	NumCPU      int       `json:"num_cpu"`
	WindowSec   float64   `json:"window_seconds"`
	DrainSec    float64   `json:"drain_seconds"`
	Accepted    int       `json:"submits_acknowledged"`
	SubmitErrs  int64     `json:"submit_errors"`
	Completed   int       `json:"jobs_completed"`
	Lost        int       `json:"jobs_lost"`
	Duplicates  int64     `json:"duplicate_completions"`
	StaleAcks   int64     `json:"stale_acks_rejected"`
	AckErrs     int64     `json:"ack_errors"`
	ClaimErrs   int64     `json:"claim_errors"`
	EmptyClaims int64     `json:"empty_claims"`

	// SubmitRate is acknowledged submits per second during the window.
	SubmitRate float64 `json:"submit_rate_per_sec"`
	// AckRate is jobs completed per second during the window (end-to-end
	// throughput).
	AckRate float64 `json:"ack_rate_per_sec"`
	// BacklogAtWindowEnd is acknowledged submits not yet completed when the
	// producers stopped.
	BacklogAtWindowEnd int `json:"backlog_at_window_end"`

	Latency LatencySummary `json:"submit_to_ack_latency_ms"`
	// SubmitLatency is the time for Submit calls alone.
	SubmitLatency LatencySummary `json:"submit_latency_ms"`
	// PerSecond counts submits acknowledged and jobs completed in each
	// second of the run (window and drain), for spotting failover gaps.
	PerSecond []SecondCount `json:"per_second"`

	submitted map[string]string // token -> job ID
	completed map[string]Completion
}

// SecondCount is one second of the per-second timeline.
type SecondCount struct {
	Second    int `json:"t"`
	Submitted int `json:"submitted"`
	Completed int `json:"completed"`
}

// Completion records the first successful ack of a token's job.
type Completion struct {
	JobID     string
	LatencyMS float64
}

// LatencySummary holds percentiles in milliseconds.
type LatencySummary struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
	Max   float64 `json:"max"`
	Mean  float64 `json:"mean"`
}

// SubmittedIDs returns token -> job ID for every acknowledged submit.
func (r *Result) SubmittedIDs() map[string]string { return r.submitted }

// CompletedIDs returns token -> completion for every completed job.
func (r *Result) CompletedIDs() map[string]Completion { return r.completed }

type event struct {
	at   time.Time
	done bool // false: submit acknowledged; true: job completed
}

type run struct {
	cfg   Config
	c     *client.Client
	start time.Time

	mu            sync.Mutex
	submitted     map[string]string
	completed     map[string]Completion
	latencies     []float64
	submitLatency []float64
	events        []event

	submitErrs, dups, staleAcks, ackErrs, claimErrs, empty atomic.Int64
}

// Run executes cfg against the cluster behind c.
func Run(ctx context.Context, c *client.Client, cfg Config) (*Result, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	r := &run{cfg: cfg, c: c, submitted: make(map[string]string), completed: make(map[string]Completion)}
	r.start = time.Now()
	windowEnd := r.start.Add(cfg.Duration)

	producersCtx, stopProducers := context.WithDeadline(ctx, windowEnd)
	defer stopProducers()
	workersCtx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()

	var prodWG, workWG sync.WaitGroup
	for p := 0; p < cfg.Producers; p++ {
		prodWG.Add(1)
		go func(p int) { defer prodWG.Done(); r.produce(producersCtx, p) }(p)
	}
	for w := 0; w < cfg.Workers; w++ {
		workWG.Add(1)
		go func(w int) { defer workWG.Done(); r.work(workersCtx, w) }(w)
	}

	prodWG.Wait()
	backlog := r.outstanding()

	// Drain: keep workers running until every acknowledged submit is done.
	drainStart := time.Now()
	drainDeadline := drainStart.Add(cfg.DrainTimeout)
	for r.outstanding() > 0 && time.Now().Before(drainDeadline) && ctx.Err() == nil {
		time.Sleep(20 * time.Millisecond)
	}
	stopWorkers()
	workWG.Wait()

	return r.result(windowEnd, backlog, time.Since(drainStart)), nil
}

func (cfg *Config) validate() error {
	switch {
	case cfg.Queue == "":
		return errors.New("loadgen: queue is required")
	case cfg.Producers < 1 || cfg.Workers < 1:
		return errors.New("loadgen: need at least one producer and one worker")
	case cfg.Duration <= 0:
		return errors.New("loadgen: duration must be positive")
	case cfg.PayloadSize < minPayload:
		return fmt.Errorf("loadgen: payload size must be at least %d bytes", minPayload)
	}
	if cfg.Visibility == 0 {
		cfg.Visibility = 30 * time.Second
	}
	if cfg.IdleSleep == 0 {
		cfg.IdleSleep = 2 * time.Millisecond
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	if cfg.RunID == "" {
		cfg.RunID = strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return nil
}

// Payload layout: "<token>|<submit start unix nanos>|" then padding.
const minPayload = 64

func (r *run) payload(token string, at time.Time) []byte {
	head := token + "|" + strconv.FormatInt(at.UnixNano(), 10) + "|"
	if pad := r.cfg.PayloadSize - len(head); pad > 0 {
		return append([]byte(head), bytes.Repeat([]byte{'x'}, pad)...)
	}
	return []byte(head)
}

func parsePayload(p []byte) (token string, submitted time.Time, ok bool) {
	parts := bytes.SplitN(p, []byte("|"), 3)
	if len(parts) < 3 {
		return "", time.Time{}, false
	}
	ns, err := strconv.ParseInt(string(parts[1]), 10, 64)
	if err != nil {
		return "", time.Time{}, false
	}
	return string(parts[0]), time.Unix(0, ns), true
}

func (r *run) produce(ctx context.Context, p int) {
	var interval time.Duration
	if r.cfg.Rate > 0 {
		interval = time.Duration(float64(time.Second) * float64(r.cfg.Producers) / r.cfg.Rate)
	}
	next := time.Now()
	for seq := 0; ; seq++ {
		if interval > 0 {
			next = next.Add(interval)
			if d := time.Until(next); d > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(d):
				}
			} else if -d > 10*interval {
				next = time.Now() // fell far behind (e.g. a failover): do not burst to catch up
			}
		}
		if ctx.Err() != nil {
			return
		}
		token := fmt.Sprintf("%s-%d-%d", r.cfg.RunID, p, seq)
		start := time.Now()
		// Submits started inside the window may finish after it; they get
		// their own timeout rather than being cut off by the window.
		reqCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.cfg.RequestTimeout)
		id, err := r.c.Submit(reqCtx, r.cfg.Queue, r.payload(token, start), r.cfg.MaxRetries)
		cancel()
		if err != nil {
			r.submitErrs.Add(1)
			continue
		}
		done := time.Now()
		r.mu.Lock()
		r.submitted[token] = id
		r.submitLatency = append(r.submitLatency, ms(done.Sub(start)))
		r.events = append(r.events, event{at: done})
		r.mu.Unlock()
	}
}

func (r *run) work(ctx context.Context, w int) {
	worker := fmt.Sprintf("load-%s-w%d", r.cfg.RunID, w)
	for ctx.Err() == nil {
		reqCtx, cancel := context.WithTimeout(ctx, r.cfg.RequestTimeout)
		lease, err := r.c.Claim(reqCtx, r.cfg.Queue, worker, r.cfg.Visibility)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				r.claimErrs.Add(1)
			}
			continue
		}
		if lease == nil {
			r.empty.Add(1)
			select {
			case <-ctx.Done():
			case <-time.After(r.cfg.IdleSleep):
			}
			continue
		}

		reqCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), r.cfg.RequestTimeout)
		err = r.c.Ack(reqCtx, lease.Job.GetId(), lease.ID)
		cancel()
		done := time.Now()
		switch {
		case status.Code(err) == codes.FailedPrecondition:
			r.staleAcks.Add(1) // lease expired and was re-claimed; the other holder will finish it
			continue
		case err != nil:
			r.ackErrs.Add(1) // the lease will expire and the job will be redelivered
			continue
		}

		token, submittedAt, ok := parsePayload(lease.Job.GetPayload())
		if !ok {
			continue // not one of ours (another run sharing the queue)
		}
		r.mu.Lock()
		if _, seen := r.completed[token]; seen {
			r.dups.Add(1)
		} else {
			lat := ms(done.Sub(submittedAt))
			r.completed[token] = Completion{JobID: lease.Job.GetId(), LatencyMS: lat}
			r.latencies = append(r.latencies, lat)
			r.events = append(r.events, event{at: done, done: true})
		}
		r.mu.Unlock()
	}
}

// outstanding counts acknowledged submits that no worker has completed.
func (r *run) outstanding() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for token := range r.submitted {
		if _, ok := r.completed[token]; !ok {
			n++
		}
	}
	return n
}

func (r *run) result(windowEnd time.Time, backlog int, drain time.Duration) *Result {
	r.mu.Lock()
	defer r.mu.Unlock()
	res := &Result{
		Config: r.cfg, StartedAt: r.start, GoMaxProcs: runtime.GOMAXPROCS(0), NumCPU: runtime.NumCPU(),
		WindowSec: r.cfg.Duration.Seconds(), DrainSec: drain.Seconds(),
		Accepted: len(r.submitted), SubmitErrs: r.submitErrs.Load(), Completed: len(r.completed),
		Duplicates: r.dups.Load(), StaleAcks: r.staleAcks.Load(), AckErrs: r.ackErrs.Load(),
		ClaimErrs: r.claimErrs.Load(), EmptyClaims: r.empty.Load(),
		BacklogAtWindowEnd: backlog,
		Latency:            summarize(r.latencies), SubmitLatency: summarize(r.submitLatency),
		submitted: r.submitted, completed: r.completed,
	}
	for token := range r.submitted {
		if _, ok := r.completed[token]; !ok {
			res.Lost++
		}
	}

	var inWindowSubmits, inWindowAcks int
	seconds := int(math.Ceil(time.Since(r.start).Seconds()))
	res.PerSecond = make([]SecondCount, seconds)
	for i := range res.PerSecond {
		res.PerSecond[i].Second = i
	}
	for _, e := range r.events {
		sec := int(e.at.Sub(r.start).Seconds())
		if sec >= seconds {
			sec = seconds - 1
		}
		inWindow := !e.at.After(windowEnd)
		if e.done {
			res.PerSecond[sec].Completed++
			if inWindow {
				inWindowAcks++
			}
		} else {
			res.PerSecond[sec].Submitted++
			if inWindow {
				inWindowSubmits++
			}
		}
	}
	res.SubmitRate = float64(inWindowSubmits) / r.cfg.Duration.Seconds()
	res.AckRate = float64(inWindowAcks) / r.cfg.Duration.Seconds()
	return res
}

func summarize(xs []float64) LatencySummary {
	if len(xs) == 0 {
		return LatencySummary{}
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	var sum float64
	for _, x := range s {
		sum += x
	}
	return LatencySummary{
		Count: len(s), P50: percentile(s, 50), P95: percentile(s, 95), P99: percentile(s, 99),
		Max: s[len(s)-1], Mean: sum / float64(len(s)),
	}
}

// percentile uses the nearest-rank method on sorted data.
func percentile(sorted []float64, p float64) float64 {
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	return sorted[rank-1]
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
