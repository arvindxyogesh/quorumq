package queue

import (
	bolt "go.etcd.io/bbolt"
)

// Submit enqueues a job and returns its ID. IDs are a per-store sequence, so
// replicas applying the same commands assign the same IDs.
func (s *Store) Submit(cmd SubmitCmd) (uint64, error) {
	var id uint64
	err := s.db.Update(func(tx *bolt.Tx) error {
		var err error
		if id, err = incr(tx.Bucket(bucketMeta), keyNextID); err != nil {
			return err
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
			return err
		}
		idx, err := queueIndex(tx, cmd.Queue)
		if err != nil {
			return err
		}
		return idx.Put(indexKey(j.VisibleAt, j.ID), []byte{byte(j.State)})
	})
	return id, err
}

// Claim leases the job in cmd.Queue that became visible earliest. It returns
// (nil, nil) when no job is visible at cmd.Now.
func (s *Store) Claim(cmd ClaimCmd) (*Job, error) {
	var claimed *Job
	err := s.db.Update(func(tx *bolt.Tx) error {
		idx, err := queueIndex(tx, cmd.Queue)
		if err != nil {
			return err
		}
		k, _ := idx.Cursor().First()
		if k == nil {
			return nil
		}
		visibleAt, id := parseIndexKey(k)
		if visibleAt > cmd.Now {
			return nil // the earliest job is still hidden, so all of them are
		}
		j, err := loadJob(tx, id)
		if err != nil {
			return err
		}
		j.State = StateLeased
		j.Attempts++
		j.LeaseID = cmd.LeaseID
		j.WorkerID = cmd.WorkerID
		j.VisibleAt = cmd.Now + int64(cmd.VisibilityTimeout)
		if err := reindex(idx, j, visibleAt); err != nil {
			return err
		}
		if err := putJob(tx, j); err != nil {
			return err
		}
		claimed = j
		return nil
	})
	if err != nil {
		return nil, err
	}
	return claimed, nil
}

// Ack marks a leased job as done. A lease stays valid until the job is
// re-claimed, even if its visibility timeout has passed: acking late is
// harmless as long as nobody else holds the job. Retrying a successful ack
// with the same lease is a no-op.
func (s *Store) Ack(cmd AckCmd) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		j, err := loadJob(tx, cmd.JobID)
		if err != nil {
			return err
		}
		if j.LeaseID != cmd.LeaseID || cmd.LeaseID == "" {
			return ErrLeaseMismatch
		}
		switch j.State {
		case StateAcked:
			return nil // duplicate ack
		case StateLeased:
		default:
			return ErrLeaseMismatch
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
	})
}

// Nack releases a leased job so it is immediately claimable again. Retrying
// a successful nack with the same lease is a no-op.
func (s *Store) Nack(cmd NackCmd) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		j, err := loadJob(tx, cmd.JobID)
		if err != nil {
			return err
		}
		if j.LeaseID != cmd.LeaseID || cmd.LeaseID == "" {
			return ErrLeaseMismatch
		}
		switch j.State {
		case StatePending:
			return nil // duplicate nack
		case StateLeased:
		default:
			return ErrLeaseMismatch
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
	})
}
