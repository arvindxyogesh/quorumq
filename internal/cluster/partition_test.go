package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/arvindxyogesh/quorumq/internal/broker"
	"github.com/arvindxyogesh/quorumq/internal/queue"
)

func (c *testCluster) raftAddrs(idx ...int) []string {
	out := make([]string, len(idx))
	for i, j := range idx {
		out[i] = c.cfgs[j].RaftBind
	}
	return out
}

// leaderAmong waits for one of the given nodes to become leader.
func (c *testCluster) leaderAmong(idx []int, timeout time.Duration) int {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, i := range idx {
			if c.nodes[i] != nil && c.nodes[i].IsLeader() {
				return i
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("none of nodes %v became leader", idx)
	return -1
}

// TestPartitionMinorityLeader splits a 5-node cluster 2|3 with the current
// leader on the 2-node side, and checks the guarantees the README claims:
//
//   - the minority side never acknowledges a write;
//   - the old leader steps down and refuses reads (no stale reads);
//   - the majority side elects a new leader and keeps serving;
//   - after healing, every replica converges to the majority's history:
//     every acknowledged job is present and no write attempted on the
//     minority side survives.
func TestPartitionMinorityLeader(t *testing.T) {
	fnet := newFaultNet()
	c := newTestClusterOn(t, 5, SnapshotPolicy{}, fnet)
	old := c.waitForLeader()
	policy := broker.RetryPolicy{}
	acked := make(map[uint64]bool)
	for _, id := range submitN(t, NewBroker(c.nodes[old], policy), 20) {
		acked[id] = true
	}
	c.waitForConvergence()
	oldTerm := c.nodes[old].Status().Term

	minority := []int{old, (old + 1) % 5}
	var majority []int
	for i := 0; i < 5; i++ {
		if i != minority[0] && i != minority[1] {
			majority = append(majority, i)
		}
	}
	fnet.partition(c.raftAddrs(minority...), c.raftAddrs(majority...))
	cut := time.Now()
	t.Logf("partitioned: minority %v (old leader n%d, term %d) | majority %v", minority, old+1, oldTerm, majority)

	// 1. The minority keeps receiving writes for 3 seconds; none may succeed.
	ctx := context.Background()
	attempts, minorityAcked := 0, 0
	for time.Since(cut) < 3*time.Second {
		for _, i := range minority {
			reqCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			_, err := NewBroker(c.nodes[i], policy).Submit(reqCtx, "q", []byte("minority"), 0)
			cancel()
			attempts++
			if err == nil {
				minorityAcked++
			}
		}
		time.Sleep(5 * time.Millisecond) // redirects return instantly; do not spin
	}
	if minorityAcked != 0 {
		t.Fatalf("the minority side acknowledged %d of %d writes", minorityAcked, attempts)
	}
	t.Logf("minority side: %d write attempts, 0 acknowledged", attempts)

	// 2. The old leader has stepped down and cannot serve a read.
	if c.nodes[old].IsLeader() {
		t.Fatal("old leader still believes it leads 3s into the partition")
	}
	readCtx, cancel := context.WithTimeout(ctx, time.Second)
	_, err := NewBroker(c.nodes[old], policy).Stats(readCtx, "q")
	cancel()
	if err == nil {
		t.Fatal("old leader served a read while partitioned from the majority")
	}

	// 3. The majority has a leader in a newer term and accepts writes.
	newLeader := c.leaderAmong(majority, 10*time.Second)
	newTerm := c.nodes[newLeader].Status().Term
	if newTerm <= oldTerm {
		t.Fatalf("new leader's term %d is not newer than %d", newTerm, oldTerm)
	}
	t.Logf("majority elected n%d in term %d", newLeader+1, newTerm)
	nb := NewBroker(c.nodes[newLeader], policy)
	for _, id := range submitN(t, nb, 20) {
		acked[id] = true
	}
	if st, err := nb.Stats(ctx, "q"); err != nil || st.Visible != 40 {
		t.Fatalf("majority stats = %+v, %v; want 40 visible", st, err)
	}

	// 4. Heal. Every node converges to exactly the majority's history.
	fnet.heal()
	c.waitForConvergence()
	now := time.Now().UnixNano()
	for i, n := range c.nodes {
		st, err := n.store.Stats("q", now)
		if err != nil || st.Visible != 40 {
			t.Fatalf("n%d after heal: stats %+v, %v; want exactly the 40 acknowledged jobs", i+1, st, err)
		}
		for id := range acked {
			if j, err := n.store.Get(id); err != nil || j.State != queue.StatePending || string(j.Payload) == "minority" {
				t.Fatalf("n%d after heal: acknowledged job %d = %+v, %v", i+1, id, j, err)
			}
		}
	}
	t.Logf("healed: all 5 replicas hold exactly the %d acknowledged jobs", len(acked))
}

// TestPartitionIsolatedFollower cuts one follower off. The rest of the
// cluster must keep committing; the isolated node must catch up on heal.
func TestPartitionIsolatedFollower(t *testing.T) {
	fnet := newFaultNet()
	c := newTestClusterOn(t, 3, SnapshotPolicy{}, fnet)
	leader := c.waitForLeader()
	follower := (leader + 1) % 3
	var rest []int
	for i := 0; i < 3; i++ {
		if i != follower {
			rest = append(rest, i)
		}
	}
	fnet.partition(c.raftAddrs(follower), c.raftAddrs(rest...))

	b := NewBroker(c.nodes[leader], broker.RetryPolicy{})
	submitN(t, b, 30) // a 2-of-3 majority still commits
	if c.nodes[follower].IsLeader() {
		t.Fatal("isolated follower made itself leader")
	}
	if got := visibleJobs(t, c.nodes[follower]); got != 0 {
		t.Fatalf("isolated follower received %d jobs while cut off", got)
	}

	fnet.heal()
	c.waitForConvergence()
	if got := visibleJobs(t, c.nodes[follower]); got != 30 {
		t.Fatalf("follower after heal has %d jobs, want 30", got)
	}
}
