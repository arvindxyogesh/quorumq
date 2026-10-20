// Package broker turns API calls into state-machine commands. It owns the
// clock and lease-ID generation, so the state machine stays deterministic.
//
// Local applies commands directly to a single store. Milestone 2 adds a
// Raft-backed broker that proposes the same commands through a replicated log.
package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/arvindxyogesh/quorumq/internal/queue"
)

// RetryPolicy controls the backoff before a nacked job is retried.
type RetryPolicy struct {
	// BackoffBase is the delay after the first failure; it doubles with each
	// further failure.
	BackoffBase time.Duration
	// BackoffMax caps the delay.
	BackoffMax time.Duration
}

// DefaultRetryPolicy retries after 1s, 2s, 4s, ... up to 5 minutes.
var DefaultRetryPolicy = RetryPolicy{BackoffBase: time.Second, BackoffMax: 5 * time.Minute}

// NackCmd builds the command for a nack under this policy.
func (p RetryPolicy) NackCmd(jobID uint64, leaseID, reason string, now time.Time) *queue.NackCmd {
	return &queue.NackCmd{
		JobID: jobID, LeaseID: leaseID, Reason: reason,
		BackoffBase: p.BackoffBase, BackoffMax: p.BackoffMax, Now: now.UnixNano(),
	}
}

// Local is a single-node broker backed by one queue.Store.
type Local struct {
	store *queue.Store
	now   func() time.Time
	retry RetryPolicy
}

// Option configures a Local broker.
type Option func(*Local)

// WithClock overrides the clock, for tests.
func WithClock(now func() time.Time) Option {
	return func(l *Local) { l.now = now }
}

// WithRetryPolicy overrides DefaultRetryPolicy.
func WithRetryPolicy(p RetryPolicy) Option {
	return func(l *Local) { l.retry = p }
}

// NewLocal returns a broker that applies commands to store.
func NewLocal(store *queue.Store, opts ...Option) *Local {
	l := &Local{store: store, now: time.Now, retry: DefaultRetryPolicy}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

func (l *Local) Submit(_ context.Context, queueName string, payload []byte, maxRetries uint32) (uint64, error) {
	return l.store.Submit(queue.SubmitCmd{
		Queue:      queueName,
		Payload:    payload,
		MaxRetries: maxRetries,
		Now:        l.now().UnixNano(),
	})
}

func (l *Local) Claim(_ context.Context, queueName, workerID string, visibility time.Duration) (*queue.Job, error) {
	return l.store.Claim(queue.ClaimCmd{
		Queue:             queueName,
		WorkerID:          workerID,
		LeaseID:           NewLeaseID(),
		VisibilityTimeout: visibility,
		Now:               l.now().UnixNano(),
	})
}

func (l *Local) Ack(_ context.Context, jobID uint64, leaseID string) error {
	return l.store.Ack(queue.AckCmd{JobID: jobID, LeaseID: leaseID, Now: l.now().UnixNano()})
}

func (l *Local) Nack(_ context.Context, jobID uint64, leaseID, reason string) error {
	return l.store.Nack(*l.retry.NackCmd(jobID, leaseID, reason, l.now()))
}

func (l *Local) Redrive(_ context.Context, jobID uint64) error {
	return l.store.Redrive(queue.RedriveCmd{JobID: jobID, Now: l.now().UnixNano()})
}

func (l *Local) ListDead(_ context.Context, queueName string, limit int) ([]*queue.Job, error) {
	return l.store.ListDead(queueName, limit)
}

func (l *Local) Get(_ context.Context, jobID uint64) (*queue.Job, error) {
	return l.store.Get(jobID)
}

func (l *Local) Stats(_ context.Context, queueName string) (queue.Stats, error) {
	return l.store.Stats(queueName, l.now().UnixNano())
}

// NewLeaseID returns a random 128-bit lease token. Leases are unguessable so
// a worker cannot ack a job it was never handed.
func NewLeaseID() string {
	var b [16]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never returns an error
	return hex.EncodeToString(b[:])
}
