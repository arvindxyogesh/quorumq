package queue

import (
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
func claimTx(tx *bolt.Tx, cmd ClaimCmd) (*Job, error) {
	idx, err := queueIndex(tx, cmd.Queue)
	if err != nil {
		return nil, err
	}
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

// checkLease loads a job and verifies that leaseID is its current lease.
// done reports that the job is already in the state the caller wants (a
// retried ack or nack), in which case the caller should do nothing.
func checkLease(tx *bolt.Tx, jobID uint64, leaseID string, doneState State) (j *Job, done bool, err error) {
	j, err = loadJob(tx, jobID)
	if err != nil {
		return nil, false, err
	}
	if leaseID == "" || j.LeaseID != leaseID {
		return nil, false, ErrLeaseMismatch
	}
	switch j.State {
	case doneState:
		return j, true, nil
	case StateLeased:
		return j, false, nil
	default:
		return nil, false, ErrLeaseMismatch
	}
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

// nackTx releases a leased job so it is immediately claimable again.
// Retrying a successful nack with the same lease is a no-op.
func nackTx(tx *bolt.Tx, cmd NackCmd) error {
	j, done, err := checkLease(tx, cmd.JobID, cmd.LeaseID, StatePending)
	if err != nil || done {
		return err
	}
	idx, err := queueIndex(tx, j.Queue)
	if err != nil {
		return err
	}
	old := j.VisibleAt
	j.State = StatePending
	j.VisibleAt = cmd.Now
	if err := reindex(idx, j, old); err != nil {
		return err
	}
	return putJob(tx, j)
}
