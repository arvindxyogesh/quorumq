package queue

import "time"

// The command types below carry every input an operation needs, including
// the time it happens at. They are produced by the broker (which owns the
// clock and the ID generator) and applied by the Store.

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

// NackCmd returns a leased job to its queue.
type NackCmd struct {
	JobID   uint64 `json:"job_id"`
	LeaseID string `json:"lease_id"`
	Now     int64  `json:"now"`
}
