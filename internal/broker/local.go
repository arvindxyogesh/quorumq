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

// Local is a single-node broker backed by one queue.Store.
type Local struct {
	store *queue.Store
	now   func() time.Time
}

// Option configures a Local broker.
type Option func(*Local)

// WithClock overrides the clock, for tests.
func WithClock(now func() time.Time) Option {
	return func(l *Local) { l.now = now }
}

// NewLocal returns a broker that applies commands to store.
func NewLocal(store *queue.Store, opts ...Option) *Local {
	l := &Local{store: store, now: time.Now}
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

func (l *Local) Nack(_ context.Context, jobID uint64, leaseID string) error {
	return l.store.Nack(queue.NackCmd{JobID: jobID, LeaseID: leaseID, Now: l.now().UnixNano()})
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
