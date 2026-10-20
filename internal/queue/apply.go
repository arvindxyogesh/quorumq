package queue

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// applyCommand applies one command inside tx. The returned error is a storage
// failure; domain errors are reported in Result.Err.
//
// Time inside the state machine never goes backwards: each command's
// timestamp is raised to the latest timestamp already applied. A new leader
// whose clock is behind the old leader's therefore cannot make an existing
// lease look younger or a delayed job look further away than it was.
func applyCommand(tx *bolt.Tx, cmd Command) (Result, error) {
	var r Result
	var err error
	switch {
	case cmd.Op == OpSubmit && cmd.Submit != nil:
		c := *cmd.Submit
		if c.Now, err = advanceClock(tx, c.Now); err != nil {
			return r, err
		}
		r.JobID, err = submitTx(tx, c)
	case cmd.Op == OpClaim && cmd.Claim != nil:
		c := *cmd.Claim
		if c.Now, err = advanceClock(tx, c.Now); err != nil {
			return r, err
		}
		r.Job, err = claimTx(tx, c)
	case cmd.Op == OpAck && cmd.Ack != nil:
		c := *cmd.Ack
		if c.Now, err = advanceClock(tx, c.Now); err != nil {
			return r, err
		}
		r.Job, err = ackTx(tx, c)
	case cmd.Op == OpNack && cmd.Nack != nil:
		c := *cmd.Nack
		if c.Now, err = advanceClock(tx, c.Now); err != nil {
			return r, err
		}
		r.Job, err = nackTx(tx, c)
	case cmd.Op == OpRedrive && cmd.Redrive != nil:
		c := *cmd.Redrive
		if c.Now, err = advanceClock(tx, c.Now); err != nil {
			return r, err
		}
		err = redriveTx(tx, c)
	default:
		err = fmt.Errorf("%w: op %d", ErrBadCommand, cmd.Op)
	}
	if err != nil && isDomainError(err) {
		r.Err, err = err, nil
	}
	return r, err
}

func advanceClock(tx *bolt.Tx, now int64) (int64, error) {
	meta := tx.Bucket(bucketMeta)
	if last := int64(getUint64(meta.Get(keyClock))); last > now {
		return last, nil
	}
	return now, meta.Put(keyClock, idKey(uint64(now)))
}

// apply runs a single command in its own transaction.
func (s *Store) apply(cmd Command) (Result, error) {
	var r Result
	err := s.update(func(tx *bolt.Tx) error {
		var err error
		r, err = applyCommand(tx, cmd)
		return err
	})
	return r, err
}

// Submit enqueues a job and returns its ID.
func (s *Store) Submit(cmd SubmitCmd) (uint64, error) {
	r, err := s.apply(Command{Op: OpSubmit, Submit: &cmd})
	if err != nil {
		return 0, err
	}
	return r.JobID, r.Err
}

// Claim leases the job in cmd.Queue that became visible earliest. It returns
// (nil, nil) when no job is visible at cmd.Now.
func (s *Store) Claim(cmd ClaimCmd) (*Job, error) {
	r, err := s.apply(Command{Op: OpClaim, Claim: &cmd})
	if err != nil {
		return nil, err
	}
	return r.Job, r.Err
}

// Ack marks a leased job as done.
func (s *Store) Ack(cmd AckCmd) error {
	r, err := s.apply(Command{Op: OpAck, Ack: &cmd})
	if err != nil {
		return err
	}
	return r.Err
}

// Nack records a failed attempt: the job is retried after a backoff or
// dead-lettered.
func (s *Store) Nack(cmd NackCmd) error {
	r, err := s.apply(Command{Op: OpNack, Nack: &cmd})
	if err != nil {
		return err
	}
	return r.Err
}

// Redrive moves a dead-lettered job back to its queue.
func (s *Store) Redrive(cmd RedriveCmd) error {
	r, err := s.apply(Command{Op: OpRedrive, Redrive: &cmd})
	if err != nil {
		return err
	}
	return r.Err
}

// Entry is one Raft log entry to apply. Data is nil for entries that only
// advance the log (for example cluster configuration changes).
type Entry struct {
	Index uint64
	Data  []byte
}

// ApplyBatch applies log entries in a single transaction and records the
// index of the last one. Entries at or below the recorded index were applied
// before (the node restarted and Raft is replaying its log) and are skipped,
// which makes replay idempotent. A non-nil error is a storage failure.
func (s *Store) ApplyBatch(entries []Entry) ([]Result, error) {
	results := make([]Result, len(entries))
	err := s.update(func(tx *bolt.Tx) error {
		meta := tx.Bucket(bucketMeta)
		applied := getUint64(meta.Get(keyApplied))
		last := applied
		for i, e := range entries {
			if e.Index <= applied {
				continue
			}
			last = e.Index
			if e.Data == nil {
				continue
			}
			cmd, err := DecodeCommand(e.Data)
			if err != nil {
				results[i] = Result{Err: fmt.Errorf("%w: %w", ErrBadCommand, err)}
				continue
			}
			if results[i], err = applyCommand(tx, cmd); err != nil {
				return err
			}
		}
		if last == applied {
			return nil
		}
		return meta.Put(keyApplied, idKey(last))
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

// AppliedIndex returns the index of the last Raft entry applied to the store.
func (s *Store) AppliedIndex() (uint64, error) {
	var idx uint64
	err := s.view(func(tx *bolt.Tx) error {
		idx = getUint64(tx.Bucket(bucketMeta).Get(keyApplied))
		return nil
	})
	return idx, err
}
