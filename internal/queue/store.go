package queue

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Store is a BoltDB-backed set of job queues. It is safe for concurrent use:
// BoltDB serialises write transactions, and each operation (or batch of Raft
// entries) is a single transaction, so every operation is atomic.
type Store struct {
	path string

	// mu guards db. Operations hold it for reading; Restore holds it for
	// writing while it swaps in a new database file.
	mu sync.RWMutex
	db *bolt.DB
}

// Open opens (or creates) the store at path.
func Open(path string) (*Store, error) {
	db, err := openDB(path)
	if err != nil {
		return nil, err
	}
	return &Store{path: path, db: db}, nil
}

func openDB(path string) (*bolt.DB, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open bolt db %q: %w", path, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{bucketJobs, bucketQueues, bucketDead, bucketCounters, bucketMeta} {
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
	return db, nil
}

// Close closes the underlying database.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Close()
}

func (s *Store) update(fn func(*bolt.Tx) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.Update(fn)
}

func (s *Store) view(fn func(*bolt.Tx) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.View(fn)
}

// Snapshot is a consistent, point-in-time view of the store. It holds a
// BoltDB read transaction open until Release, so writes can continue while
// the snapshot is being written out.
type Snapshot struct {
	tx *bolt.Tx
}

// Snapshot starts a snapshot of the current state.
func (s *Store) Snapshot() (*Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tx, err := s.db.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("begin snapshot: %w", err)
	}
	return &Snapshot{tx: tx}, nil
}

// WriteTo writes the snapshot as a complete BoltDB file.
func (sn *Snapshot) WriteTo(w io.Writer) (int64, error) { return sn.tx.WriteTo(w) }

// Release ends the snapshot's read transaction.
func (sn *Snapshot) Release() { _ = sn.tx.Rollback() }

// Restore replaces the store's entire state with a snapshot produced by
// Snapshot.WriteTo. The snapshot is written to a temporary file first, so a
// failed restore leaves the current state untouched.
func (s *Store) Restore(r io.Reader) error {
	tmp := s.path + ".restore"
	if err := writeFileSync(tmp, r); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("write snapshot: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("close db for restore: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return errors.Join(fmt.Errorf("install snapshot: %w", err), s.reopenLocked())
	}
	return s.reopenLocked()
}

func (s *Store) reopenLocked() error {
	db, err := openDB(s.path)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

func writeFileSync(path string, r io.Reader) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

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

// deadIndex returns the dead-letter index for a queue, creating it if needed.
func deadIndex(tx *bolt.Tx, queue string) (*bolt.Bucket, error) {
	return tx.Bucket(bucketDead).CreateBucketIfNotExists([]byte(queue))
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
