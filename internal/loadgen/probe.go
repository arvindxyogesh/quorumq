package loadgen

import (
	"context"
	"time"

	"github.com/arvindxyogesh/quorumq/client"
)

// ProbeResult is the outcome of a failover probe.
type ProbeResult struct {
	// FailoverMS is the time from since (when the leader was killed) until
	// a Submit was acknowledged by the cluster again.
	FailoverMS float64 `json:"failover_ms"`
	Attempts   int     `json:"attempts"`
	JobID      string  `json:"job_id"`
}

// Probe submits to queue until a submit is acknowledged, and reports how
// long after since that happened. Pass a client built with a short attempt
// timeout and small backoff: its retry interval bounds the resolution of
// the measurement.
func Probe(ctx context.Context, c *client.Client, queue string, since time.Time) (ProbeResult, error) {
	attempts := 0
	for {
		attempts++
		attemptCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		id, err := c.Submit(attemptCtx, queue, []byte("failover-probe"), 0)
		cancel()
		if err == nil {
			return ProbeResult{FailoverMS: ms(time.Since(since)), Attempts: attempts, JobID: id}, nil
		}
		if ctx.Err() != nil {
			return ProbeResult{Attempts: attempts}, ctx.Err()
		}
	}
}
