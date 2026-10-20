package queue

import (
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Store is a BoltDB-backed set of job queues. It is safe for concurrent use:
// BoltDB serialises write transactions, and each operation is a single
// transaction, so every operation is atomic.
type Store struct {
	db *bolt.DB
}

// Open opens (or creates) the store at path.
func Open(path string) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open bolt db %q: %w", path, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{bucketJobs, bucketQueues, bucketCounters, bucketMeta} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create buckets: %w", err)
	}
	return &Store{db: db}, nil
}

// Close closes the underlying database.
func (s *Store) Close() error { return s.db.Close() }

func loadJob(tx *bolt.Tx, id uint64) (*Job, error) {
	b := tx.Bucket(bucketJobs).Get(idKey(id))
	if b == nil {
		return nil, ErrNotFound
	}
	return decodeJob(b)
}

func putJob(tx *bolt.Tx, j *Job) error {
	b, err := encodeJob(j)
	if err != nil {
		return err
	}
	return tx.Bucket(bucketJobs).Put(idKey(j.ID), b)
}

// queueIndex returns the index bucket for a queue, creating it if needed.
func queueIndex(tx *bolt.Tx, queue string) (*bolt.Bucket, error) {
	return tx.Bucket(bucketQueues).CreateBucketIfNotExists([]byte(queue))
}

func queueCounters(tx *bolt.Tx, queue string) (*bolt.Bucket, error) {
	return tx.Bucket(bucketCounters).CreateBucketIfNotExists([]byte(queue))
}

func incr(b *bolt.Bucket, key []byte) (uint64, error) {
	n := getUint64(b.Get(key)) + 1
	return n, b.Put(key, idKey(n))
}

// reindex moves a job's index entry from oldVisibleAt to the job's current
// VisibleAt and records its current state.
func reindex(idx *bolt.Bucket, j *Job, oldVisibleAt int64) error {
	if err := idx.Delete(indexKey(oldVisibleAt, j.ID)); err != nil {
		return err
	}
	return idx.Put(indexKey(j.VisibleAt, j.ID), []byte{byte(j.State)})
}
