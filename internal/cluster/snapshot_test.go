package cluster

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arvindxyogesh/quorumq/internal/broker"
	"github.com/arvindxyogesh/quorumq/internal/queue"
)

// aggressiveSnapshots makes Raft snapshot and compact its log after a
// handful of entries, so tests exercise snapshot paths without thousands of
// writes.
var aggressiveSnapshots = SnapshotPolicy{Threshold: 32, Interval: 50 * time.Millisecond, TrailingLogs: 8}

// waitForSnapshot waits until n has taken a snapshot covering minIndex. If
// b is not nil it keeps writing while it waits: a snapshot is only taken
// once Threshold new entries have accumulated, so a fixed number of writes
// can stop just short of the next one.
func waitForSnapshot(t *testing.T, n *Node, b *Broker, minIndex uint64) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if n.Status().LastSnapshotIndex >= minIndex {
			return
		}
		if b != nil {
			submitN(t, b, 5)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s never snapshotted past index %d (status %+v)", n.cfg.NodeID, minIndex, n.Status())
}

func visibleJobs(t *testing.T, n *Node) uint64 {
	t.Helper()
	st, err := n.store.Stats("q", time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	return st.Visible
}

// A follower that is down while the leader snapshots and truncates its log
// cannot be caught up from the log any more; the leader must send it the
// snapshot (InstallSnapshot), and the follower must restore it.
func TestLaggingFollowerCatchesUpViaSnapshot(t *testing.T) {
	c := newTestClusterWith(t, 3, aggressiveSnapshots)
	leader := c.waitForLeader()
	b := NewBroker(c.nodes[leader], broker.DefaultRetryPolicy)
	submitN(t, b, 10)
	c.waitForConvergence()

	follower := (leader + 1) % 3
	behind, _ := c.nodes[follower].store.AppliedIndex()
	c.stop(follower)

	submitN(t, b, 200)
	ln := c.nodes[leader]
	waitForSnapshot(t, ln, b, behind+100)
	first, err := ln.logStore.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if first <= behind+1 {
		t.Fatalf("leader log still starts at %d; the follower (at %d) could catch up without a snapshot", first, behind)
	}

	c.start(follower)
	c.waitForConvergence()
	fn := c.nodes[follower]
	if got, want := visibleJobs(t, fn), visibleJobs(t, c.nodes[leader]); got != want || got < 210 {
		t.Fatalf("follower has %d jobs after catching up; leader has %d", got, want)
	}
	snaps, err := fn.snaps.List()
	if err != nil || len(snaps) == 0 {
		t.Fatalf("follower has no installed snapshot (err %v)", err)
	}
}

// If a node loses its state-machine file but keeps the Raft log and
// snapshots, it must rebuild its state from the newest snapshot plus the
// log entries after it, with nothing missing and nothing applied twice.
func TestNodeRebuildsStateFromSnapshotAndLog(t *testing.T) {
	c := newTestClusterWith(t, 3, aggressiveSnapshots)
	leader := c.waitForLeader()
	b := NewBroker(c.nodes[leader], broker.DefaultRetryPolicy)
	submitN(t, b, 150)
	c.waitForConvergence()

	victim := (leader + 1) % 3
	waitForSnapshot(t, c.nodes[victim], b, 64)
	submitN(t, b, 7) // some entries newer than the snapshot
	c.waitForConvergence()
	c.stop(victim)

	if err := os.Remove(filepath.Join(c.cfgs[victim].DataDir, fsmFile)); err != nil {
		t.Fatal(err)
	}
	c.start(victim)
	c.waitForConvergence()
	if got, want := visibleJobs(t, c.nodes[victim]), visibleJobs(t, c.nodes[leader]); got != want || got < 157 {
		t.Fatalf("rebuilt node has %d jobs; leader has %d", got, want)
	}
}

// Snapshots must carry leases, retry state and the dead-letter queue, not
// just pending jobs.
func TestSnapshotPreservesLeasesAndDeadLetters(t *testing.T) {
	c := newTestClusterWith(t, 3, aggressiveSnapshots)
	leader := c.waitForLeader()
	b := NewBroker(c.nodes[leader], broker.RetryPolicy{})
	ctx := context.Background()

	leased, _ := b.Submit(ctx, "q", []byte("leased"), 3)
	poison, _ := b.Submit(ctx, "q", []byte("poison"), 0)
	l1, _ := b.Claim(ctx, "q", "w", time.Hour)
	l2, _ := b.Claim(ctx, "q", "w", time.Hour)
	if l1.ID != leased || l2.ID != poison {
		t.Fatalf("unexpected claim order %d, %d", l1.ID, l2.ID)
	}
	if err := b.Nack(ctx, poison, l2.LeaseID, "bad input"); err != nil {
		t.Fatal(err)
	}

	follower := (leader + 1) % 3
	c.waitForConvergence()
	c.stop(follower)
	submitN(t, b, 120) // force snapshots and compaction while it is down
	waitForSnapshot(t, c.nodes[leader], b, 100)
	c.start(follower)
	c.waitForConvergence()

	fs := c.nodes[follower].store
	if j, _ := fs.Get(leased); j.State != queue.StateLeased || j.LeaseID != l1.LeaseID {
		t.Fatalf("lease lost through snapshot: %+v", j)
	}
	if j, _ := fs.Get(poison); j.State != queue.StateDead || j.LastError != "bad input" {
		t.Fatalf("dead letter lost through snapshot: %+v", j)
	}
}

// The stale-worker scenario through Raft: an ack carrying a superseded lease
// is rejected by the replicated state machine, and every replica agrees.
func TestStaleAckRejectedThroughRaft(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitForLeader()
	b := NewBroker(c.nodes[leader], broker.DefaultRetryPolicy)
	ctx := context.Background()

	id, _ := b.Submit(ctx, "q", []byte("x"), 3)
	stale, _ := b.Claim(ctx, "q", "slow-worker", 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	current, err := b.Claim(ctx, "q", "fast-worker", time.Hour)
	if err != nil || current == nil || current.ID != id {
		t.Fatalf("re-claim after expiry: %+v, %v", current, err)
	}

	if err := b.Ack(ctx, id, stale.LeaseID); !errors.Is(err, queue.ErrLeaseMismatch) {
		t.Fatalf("stale ack: got %v, want ErrLeaseMismatch", err)
	}
	if err := b.Ack(ctx, id, current.LeaseID); err != nil {
		t.Fatalf("current ack: %v", err)
	}
	c.waitForConvergence()
	for _, n := range c.nodes {
		if j, _ := n.store.Get(id); j.State != queue.StateAcked || j.WorkerID != "fast-worker" {
			t.Fatalf("%s disagrees: %+v", n.cfg.NodeID, j)
		}
	}
}
