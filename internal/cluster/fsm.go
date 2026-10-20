// Package cluster replicates the queue state machine with Raft
// (hashicorp/raft). It contains the raft.FSM adapter, node setup, and a
// Broker that proposes commands through the replicated log.
package cluster

import (
	"fmt"
	"io"

	"github.com/hashicorp/raft"

	"github.com/arvindxyogesh/quorumq/internal/queue"
)

// FSM adapts queue.Store to raft.BatchingFSM. Raft calls ApplyBatch with up
// to MaxAppendEntries committed entries at a time; the store applies each
// batch in one BoltDB transaction, which amortises the fsync.
type FSM struct {
	store *queue.Store
}

var _ raft.BatchingFSM = (*FSM)(nil)

// NewFSM returns an FSM backed by store.
func NewFSM(store *queue.Store) *FSM { return &FSM{store: store} }

// Apply applies a single committed entry.
func (f *FSM) Apply(l *raft.Log) any {
	return f.ApplyBatch([]*raft.Log{l})[0]
}

// ApplyBatch applies committed entries in order. Each response is a
// queue.Result, delivered to the ApplyFuture on the node that proposed it.
func (f *FSM) ApplyBatch(logs []*raft.Log) []any {
	entries := make([]queue.Entry, len(logs))
	for i, l := range logs {
		entries[i].Index = l.Index
		if l.Type == raft.LogCommand {
			entries[i].Data = l.Data
		}
	}
	results, err := f.store.ApplyBatch(entries)
	if err != nil {
		// A storage failure means this replica can no longer apply the log
		// faithfully. Continuing would let it diverge from the others, so
		// crash and let it recover from disk and the log on restart.
		panic(fmt.Sprintf("quorumq: applying raft log failed: %v", err))
	}
	out := make([]any, len(results))
	for i, r := range results {
		out[i] = r
	}
	return out
}

// Snapshot captures the state machine at the current applied index. It only
// opens a read transaction; the data is written later by Persist, which can
// run concurrently with Apply.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	sn, err := f.store.Snapshot()
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{sn: sn}, nil
}

// Restore replaces the state machine with a snapshot, for example one sent
// by the leader to a follower that fell too far behind.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer func() { _ = rc.Close() }()
	return f.store.Restore(rc)
}

type fsmSnapshot struct {
	sn *queue.Snapshot
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := s.sn.WriteTo(sink); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("write snapshot: %w", err)
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() { s.sn.Release() }
