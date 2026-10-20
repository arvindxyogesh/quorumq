package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// The command types below carry every input an operation needs, including
// the time it happens at. They are produced by the broker (which owns the
// clock and the ID generator) and applied by the Store, either directly
// (single node) or after being committed to the Raft log.

// SubmitCmd enqueues a new job.
type SubmitCmd struct {
	Queue      string `json:"queue"`
	Payload    []byte `json:"payload"`
	MaxRetries uint32 `json:"max_retries"`
	Now        int64  `json:"now"`
}

// ClaimCmd leases the oldest visible job in a queue.
type ClaimCmd struct {
	Queue             string        `json:"queue"`
	WorkerID          string        `json:"worker_id"`
	LeaseID           string        `json:"lease_id"`
	VisibilityTimeout time.Duration `json:"visibility_timeout"`
	Now               int64         `json:"now"`
}

// AckCmd acknowledges a leased job.
type AckCmd struct {
	JobID   uint64 `json:"job_id"`
	LeaseID string `json:"lease_id"`
	Now     int64  `json:"now"`
}

// NackCmd reports that a leased job failed. The job is retried after an
// exponential backoff (BackoffBase doubled per previous attempt, capped at
// BackoffMax), or dead-lettered if it has no retries left. The backoff
// settings travel in the command, so every replica computes the same delay.
type NackCmd struct {
	JobID       uint64        `json:"job_id"`
	LeaseID     string        `json:"lease_id"`
	Reason      string        `json:"reason,omitempty"`
	BackoffBase time.Duration `json:"backoff_base,omitempty"`
	BackoffMax  time.Duration `json:"backoff_max,omitempty"`
	Now         int64         `json:"now"`
}

// RedriveCmd moves a dead-lettered job back to its queue with a fresh retry
// budget.
type RedriveCmd struct {
	JobID uint64 `json:"job_id"`
	Now   int64  `json:"now"`
}

// Op identifies a command type.
type Op uint8

// Command operations.
const (
	OpSubmit Op = iota + 1
	OpClaim
	OpAck
	OpNack
	OpRedrive
)

// Command is the envelope written to the Raft log: exactly one of the
// pointer fields is set, matching Op.
type Command struct {
	Op      Op          `json:"op"`
	Submit  *SubmitCmd  `json:"submit,omitempty"`
	Claim   *ClaimCmd   `json:"claim,omitempty"`
	Ack     *AckCmd     `json:"ack,omitempty"`
	Nack    *NackCmd    `json:"nack,omitempty"`
	Redrive *RedriveCmd `json:"redrive,omitempty"`
}

// Encode serialises the command for the Raft log.
func (c Command) Encode() ([]byte, error) { return json.Marshal(c) }

// DecodeCommand parses a command written by Encode.
func DecodeCommand(b []byte) (Command, error) {
	var c Command
	if err := json.Unmarshal(b, &c); err != nil {
		return Command{}, fmt.Errorf("decode command: %w", err)
	}
	return c, nil
}

// Result is the outcome of applying one command. Err holds a domain error
// (ErrNotFound, ErrLeaseMismatch, ErrNotDead, ErrBadCommand); storage failures are
// reported separately, because they mean the state machine cannot continue.
type Result struct {
	JobID uint64
	Job   *Job
	Err   error
}

// ErrBadCommand is returned for a log entry that is not a valid command.
var ErrBadCommand = errors.New("invalid command")

func isDomainError(err error) bool {
	return errors.Is(err, ErrNotFound) || errors.Is(err, ErrLeaseMismatch) ||
		errors.Is(err, ErrNotDead) || errors.Is(err, ErrBadCommand)
}
