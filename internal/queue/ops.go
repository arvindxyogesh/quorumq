package queue

import (
	"math"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Each operation below runs inside a caller-provided write transaction, so a
// whole batch of Raft entries can be applied in one BoltDB commit. Domain
// errors (ErrNotFound, ErrLeaseMismatch) are always returned before anything
// is written, so a rejected command never leaves a partial change behind.

// submitTx enqueues a job. IDs are a per-store sequence, so replicas applying
// the same commands assign the same IDs.
func submitTx(tx *bolt.Tx, cmd SubmitCmd) (uint64, error) {
	id, err := incr(tx.Bucket(bucketMeta), keyNextID)
	if err != nil {
		return 0, err
	}
	j := &Job{
		ID:         id,
		Queue:      cmd.Queue,
		Payload:    cmd.Payload,
		State:      StatePending,
		MaxRetries: cmd.MaxRetries,
		CreatedAt:  cmd.Now,
		VisibleAt:  cmd.Now,
	}
	if err := putJob(tx, j); err != nil {
		return 0, err
	}
	idx, err := queueIndex(tx, cmd.Queue)
	if err != nil {
		return 0, err
	}
	return id, idx.Put(indexKey(j.VisibleAt, j.ID), []byte{byte(j.State)})
}

// claimTx leases the job in cmd.Queue that became visible earliest. It
// returns (nil, nil) when no job is visible at cmd.Now.
//
// A job at the front whose lease expired after its final attempt is not
// redelivered: it is dead-lettered here, and the claim moves on to the next
// job. Expired leases are discovered lazily like this rather than by a
// background reaper, so the state machine only changes in response to log
// entries.
func claimTx(tx *bolt.Tx, cmd ClaimCmd) (*Job, error) {
	idx, err := queueIndex(tx, cmd.Queue)
	if err != nil {
		return nil, err
	}
	for {
		k, _ := idx.Cursor().First()
		if k == nil {
			return nil, nil
		}
		visibleAt, id := parseIndexKey(k)
		if visibleAt > cmd.Now {
			return nil, nil // the earliest job is still hidden, so all of them are
		}
		j, err := loadJob(tx, id)
		if err != nil {
			return nil, err
		}
		if j.State == StateLeased && j.Exhausted() {
			j.LastError = "visibility timeout expired on the final attempt"
			if err := deadLetterTx(tx, idx, j, cmd.Now); err != nil {
				return nil, err
			}
			continue
		}
		if j.State == StateLeased {
			j.LastError = "visibility timeout expired"
		}
		j.State = StateLeased
		j.Attempts++
		j.LeaseID = cmd.LeaseID
		j.WorkerID = cmd.WorkerID
		j.VisibleAt = cmd.Now + int64(cmd.VisibilityTimeout)
		if err := reindex(idx, j, visibleAt); err != nil {
			return nil, err
		}
		return j, putJob(tx, j)
	}
}

// checkLease loads a job and verifies that leaseID is its current lease.
// done reports that the job is already in one of doneStates (a retried ack
// or nack), in which case the caller should do nothing.
func checkLease(tx *bolt.Tx, jobID uint64, leaseID string, doneStates ...State) (j *Job, done bool, err error) {
	j, err = loadJob(tx, jobID)
	if err != nil {
		return nil, false, err
	}
	if leaseID == "" || j.LeaseID != leaseID {
		return nil, false, ErrLeaseMismatch
	}
	if j.State == StateLeased {
		return j, false, nil
	}
	for _, s := range doneStates {
		if j.State == s {
			return j, true, nil
		}
	}
	return nil, false, ErrLeaseMismatch
}

// ackTx marks a leased job as done. A lease stays valid until the job is
// re-claimed, even if its visibility timeout has passed: acking late is
// harmless as long as nobody else holds the job. Retrying a successful ack
// with the same lease is a no-op.
func ackTx(tx *bolt.Tx, cmd AckCmd) error {
	j, done, err := checkLease(tx, cmd.JobID, cmd.LeaseID, StateAcked)
	if err != nil || done {
		return err
	}
	idx, err := queueIndex(tx, j.Queue)
	if err != nil {
		return err
	}
	if err := idx.Delete(indexKey(j.VisibleAt, j.ID)); err != nil {
		return err
	}
	j.State = StateAcked
	j.VisibleAt = 0
	if err := putJob(tx, j); err != nil {
		return err
	}
	counters, err := queueCounters(tx, j.Queue)
	if err != nil {
		return err
	}
	_, err = incr(counters, keyAcked)
	return err
}

// nackTx records a failed attempt. If the job has retries left it becomes
// claimable again after an exponential backoff; otherwise it is
// dead-lettered. Retrying a successful nack with the same lease is a no-op.
func nackTx(tx *bolt.Tx, cmd NackCmd) error {
	j, done, err := checkLease(tx, cmd.JobID, cmd.LeaseID, StatePending, StateDead)
	if err != nil || done {
		return err
	}
	idx, err := queueIndex(tx, j.Queue)
	if err != nil {
		return err
	}
	j.LastError = cmd.Reason
	if j.LastError == "" {
		j.LastError = "nacked"
	}
	if j.Exhausted() {
		return deadLetterTx(tx, idx, j, cmd.Now)
	}
	old := j.VisibleAt
	j.State = StatePending
	j.VisibleAt = cmd.Now + int64(RetryDelay(j.Attempts, cmd.BackoffBase, cmd.BackoffMax))
	if err := reindex(idx, j, old); err != nil {
		return err
	}
	return putJob(tx, j)
}

// RetryDelay is the backoff before retrying a job that has failed attempt
// times: base, 2*base, 4*base, ... capped at maxDelay (uncapped if zero).
// A zero base means retry immediately.
func RetryDelay(attempt uint32, base, maxDelay time.Duration) time.Duration {
	if base <= 0 || attempt == 0 {
		return 0
	}
	if maxDelay <= 0 {
		maxDelay = math.MaxInt64
	}
	d := base
	for i := uint32(1); i < attempt; i++ {
		if d >= maxDelay/2 {
			return maxDelay // doubling again would pass the cap (or overflow)
		}
		d *= 2
	}
	return min(d, maxDelay)
}

// deadLetterTx moves a job from its queue's index to the queue's
// dead-letter index. The caller sets LastError.
func deadLetterTx(tx *bolt.Tx, idx *bolt.Bucket, j *Job, now int64) error {
	if err := idx.Delete(indexKey(j.VisibleAt, j.ID)); err != nil {
		return err
	}
	dead, err := deadIndex(tx, j.Queue)
	if err != nil {
		return err
	}
	j.State = StateDead
	j.VisibleAt = 0
	j.DeadAt = now
	if err := dead.Put(indexKey(j.DeadAt, j.ID), []byte{}); err != nil {
		return err
	}
	return putJob(tx, j)
}

// redriveTx moves a dead-lettered job back to its queue, claimable now and
// with its attempt count reset. LastError is kept for the record.
func redriveTx(tx *bolt.Tx, cmd RedriveCmd) error {
	j, err := loadJob(tx, cmd.JobID)
	if err != nil {
		return err
	}
	if j.State != StateDead {
		return ErrNotDead
	}
	dead, err := deadIndex(tx, j.Queue)
	if err != nil {
		return err
	}
	if err := dead.Delete(indexKey(j.DeadAt, j.ID)); err != nil {
		return err
	}
	idx, err := queueIndex(tx, j.Queue)
	if err != nil {
		return err
	}
	j.State = StatePending
	j.Attempts = 0
	j.DeadAt = 0
	j.LeaseID = ""
	j.VisibleAt = cmd.Now
	if err := idx.Put(indexKey(j.VisibleAt, j.ID), []byte{byte(j.State)}); err != nil {
		return err
	}
	return putJob(tx, j)
}
