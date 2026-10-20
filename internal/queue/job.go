// Package queue implements the broker's replicated state machine: a set of
// named FIFO queues stored in BoltDB.
//
// Every mutating operation takes a command that carries all of its inputs,
// including the current time and any generated IDs. The store never reads
// the wall clock or a random source itself, so applying the same commands in
// the same order always produces the same state. That property is what lets
// the store be replicated with Raft later: each node applies the same log and
// ends up identical.
package queue

import (
	"encoding/json"
	"errors"
	"fmt"
)

// State is the lifecycle state of a job.
type State uint8

const (
	// StatePending means the job is waiting to be claimed.
	StatePending State = iota + 1
	// StateLeased means a worker holds a lease on the job. The lease may have
	// expired, in which case the job is claimable again.
	StateLeased
	// StateAcked means a worker acknowledged the job. Terminal.
	StateAcked
)

func (s State) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateLeased:
		return "leased"
	case StateAcked:
		return "acked"
	default:
		return fmt.Sprintf("State(%d)", s)
	}
}

// Job is the stored record for one job. Times are Unix nanoseconds.
type Job struct {
	ID         uint64 `json:"id"`
	Queue      string `json:"queue"`
	Payload    []byte `json:"payload"`
	State      State  `json:"state"`
	Attempts   uint32 `json:"attempts"`
	MaxRetries uint32 `json:"max_retries"`
	CreatedAt  int64  `json:"created_at"`
	// VisibleAt is when the job next becomes claimable. Zero once acked.
	VisibleAt int64 `json:"visible_at"`
	// LeaseID is the most recently issued lease. It is kept after an ack or
	// nack so that a retried ack or nack with the same lease is a no-op.
	LeaseID  string `json:"lease_id,omitempty"`
	WorkerID string `json:"worker_id,omitempty"`
}

func encodeJob(j *Job) ([]byte, error) { return json.Marshal(j) }

func decodeJob(b []byte) (*Job, error) {
	var j Job
	if err := json.Unmarshal(b, &j); err != nil {
		return nil, fmt.Errorf("decode job: %w", err)
	}
	return &j, nil
}

var (
	// ErrNotFound is returned when a job ID does not exist.
	ErrNotFound = errors.New("job not found")
	// ErrLeaseMismatch is returned when an ack or nack presents a lease that
	// is not the job's current lease, for example because the lease expired
	// and the job was re-claimed by another worker.
	ErrLeaseMismatch = errors.New("lease is not the job's current lease")
)
