package queue

import (
	bolt "go.etcd.io/bbolt"
)

// Get returns the job with the given ID.
func (s *Store) Get(id uint64) (*Job, error) {
	var j *Job
	err := s.view(func(tx *bolt.Tx) error {
		var err error
		j, err = loadJob(tx, id)
		return err
	})
	return j, err
}

// Stats are counts for one queue at a point in time.
type Stats struct {
	// Visible jobs can be claimed now, including jobs whose lease expired.
	Visible uint64
	// InFlight jobs are leased and their lease has not expired.
	InFlight uint64
	// Delayed jobs are pending but waiting out a retry backoff.
	Delayed uint64
	// Acked is the number of jobs acknowledged since the queue was created.
	Acked uint64
	// Dead is the number of jobs currently in the dead-letter queue.
	Dead uint64
}

// Stats counts the jobs in a queue as of now. It scans the queue's index and
// dead-letter index, so it costs O(jobs in the queue); acked jobs are in
// neither.
func (s *Store) Stats(queue string, now int64) (Stats, error) {
	var st Stats
	err := s.view(func(tx *bolt.Tx) error {
		if c := tx.Bucket(bucketCounters).Bucket([]byte(queue)); c != nil {
			st.Acked = getUint64(c.Get(keyAcked))
		}
		if dead := tx.Bucket(bucketDead).Bucket([]byte(queue)); dead != nil {
			st.Dead = uint64(dead.Stats().KeyN)
		}
		idx := tx.Bucket(bucketQueues).Bucket([]byte(queue))
		if idx == nil {
			return nil
		}
		return idx.ForEach(func(k, v []byte) error {
			visibleAt, _ := parseIndexKey(k)
			switch {
			case visibleAt <= now:
				st.Visible++
			case len(v) == 1 && State(v[0]) == StateLeased:
				st.InFlight++
			default:
				st.Delayed++
			}
			return nil
		})
	})
	return st, err
}

// ListDead returns up to limit of a queue's dead-lettered jobs, in the
// order they were dead-lettered.
func (s *Store) ListDead(queue string, limit int) ([]*Job, error) {
	var jobs []*Job
	err := s.view(func(tx *bolt.Tx) error {
		dead := tx.Bucket(bucketDead).Bucket([]byte(queue))
		if dead == nil {
			return nil
		}
		c := dead.Cursor()
		for k, _ := c.First(); k != nil && len(jobs) < limit; k, _ = c.Next() {
			_, id := parseIndexKey(k)
			j, err := loadJob(tx, id)
			if err != nil {
				return err
			}
			jobs = append(jobs, j)
		}
		return nil
	})
	return jobs, err
}
