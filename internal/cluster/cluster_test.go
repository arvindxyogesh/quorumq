package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/arvindxyogesh/quorumq/internal/broker"
	"github.com/arvindxyogesh/quorumq/internal/queue"
)

// testCluster runs real Raft nodes in one process, talking over localhost TCP.
type testCluster struct {
	t     *testing.T
	cfgs  []Config
	nodes []*Node // nil while a node is stopped
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func newTestCluster(t *testing.T, size int) *testCluster {
	t.Helper()
	return newTestClusterWith(t, size, SnapshotPolicy{})
}

func newTestClusterWith(t *testing.T, size int, snap SnapshotPolicy) *testCluster {
	t.Helper()
	peers := make([]Peer, size)
	for i := range peers {
		peers[i] = Peer{ID: fmt.Sprintf("n%d", i+1), RaftAddr: freeAddr(t), ClientAddr: fmt.Sprintf("client-n%d:7070", i+1)}
	}
	c := &testCluster{t: t, nodes: make([]*Node, size)}
	for i, p := range peers {
		c.cfgs = append(c.cfgs, Config{
			NodeID: p.ID, RaftBind: p.RaftAddr, DataDir: t.TempDir(), Peers: peers, Bootstrap: true, RaftLogLevel: "off", Snapshot: snap,
			Timing: Timing{HeartbeatTimeout: 200 * time.Millisecond, ElectionTimeout: 200 * time.Millisecond, LeaderLeaseTimeout: 100 * time.Millisecond},
		})
		c.start(i)
	}
	t.Cleanup(func() {
		for i := range c.nodes {
			c.stop(i)
		}
	})
	return c
}

func (c *testCluster) start(i int) {
	c.t.Helper()
	n, err := NewNode(c.cfgs[i], slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		c.t.Fatalf("start %s: %v", c.cfgs[i].NodeID, err)
	}
	c.nodes[i] = n
}

func (c *testCluster) stop(i int) {
	if c.nodes[i] != nil {
		_ = c.nodes[i].Shutdown()
		c.nodes[i] = nil
	}
}

func (c *testCluster) waitForLeader() int {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for i, n := range c.nodes {
			if n != nil && n.IsLeader() {
				return i
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatal("no leader elected")
	return -1
}

// waitForConvergence waits until every running node has applied the same
// log index to its state machine.
func (c *testCluster) waitForConvergence() {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var want uint64
		same := true
		for _, n := range c.nodes {
			if n == nil {
				continue
			}
			got, _ := n.store.AppliedIndex()
			if want == 0 {
				want = got
			}
			same = same && got == want && got > 0
		}
		if same {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatal("replicas did not converge")
}

func submitN(t *testing.T, b *Broker, n int) []uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ids := make([]uint64, n)
	for i := range ids {
		id, err := b.Submit(ctx, "q", []byte(fmt.Sprint(i)), 3)
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		ids[i] = id
	}
	return ids
}

func TestReplicatesToEveryNode(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitForLeader()
	submitN(t, NewBroker(c.nodes[leader], broker.DefaultRetryPolicy), 20)
	c.waitForConvergence()

	for _, n := range c.nodes {
		st, err := n.store.Stats("q", time.Now().UnixNano())
		if err != nil || st.Visible != 20 {
			t.Fatalf("%s: stats=%+v err=%v, want 20 visible", n.cfg.NodeID, st, err)
		}
	}
}

func TestFollowerRedirectsToLeader(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitForLeader()
	follower := (leader + 1) % 3

	// Wait for the follower to learn who the leader is.
	deadline := time.Now().Add(5 * time.Second)
	for c.nodes[follower].LeaderClientAddr() == "" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	ctx := context.Background()
	b := NewBroker(c.nodes[follower], broker.DefaultRetryPolicy)
	_, err := b.Submit(ctx, "q", []byte("x"), 0)
	var nl *broker.NotLeaderError
	if !errors.As(err, &nl) {
		t.Fatalf("follower submit: got %v, want NotLeaderError", err)
	}
	if want := c.cfgs[leader].Peers[leader].ClientAddr; nl.Leader != want {
		t.Fatalf("leader hint = %q, want %q", nl.Leader, want)
	}
	// Reads are redirected too, so a follower never serves stale data.
	if _, err := b.Stats(ctx, "q"); !errors.As(err, &nl) {
		t.Fatalf("follower stats: got %v, want NotLeaderError", err)
	}
}

func TestCommittedJobsSurviveLeaderLoss(t *testing.T) {
	c := newTestCluster(t, 3)
	old := c.waitForLeader()
	ids := submitN(t, NewBroker(c.nodes[old], broker.DefaultRetryPolicy), 10)

	c.stop(old)
	leader := c.waitForLeader()
	if leader == old {
		t.Fatal("stopped node is still leader")
	}
	b := NewBroker(c.nodes[leader], broker.DefaultRetryPolicy)
	ctx := context.Background()
	for _, id := range ids {
		j, err := b.Get(ctx, id)
		if err != nil || j.State != queue.StatePending {
			t.Fatalf("job %d after failover: job=%+v err=%v", id, j, err)
		}
	}
	// The new leader keeps the ID sequence going.
	if more := submitN(t, b, 1); more[0] != 11 {
		t.Fatalf("next ID on new leader = %d, want 11", more[0])
	}
}

// A restarted node replays its Raft log on top of a state machine that has
// already applied most of it. Nothing may be applied twice.
func TestRestartedNodeDoesNotReapplyLog(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitForLeader()
	submitN(t, NewBroker(c.nodes[leader], broker.DefaultRetryPolicy), 15)
	c.waitForConvergence()

	follower := (leader + 1) % 3
	c.stop(follower)
	submitN(t, NewBroker(c.nodes[leader], broker.DefaultRetryPolicy), 5) // committed while it is down
	c.start(follower)
	c.waitForConvergence()

	st, err := c.nodes[follower].store.Stats("q", time.Now().UnixNano())
	if err != nil || st.Visible != 20 {
		t.Fatalf("restarted node: stats=%+v err=%v, want exactly 20 jobs", st, err)
	}
}

func TestParsePeers(t *testing.T) {
	peers, err := ParsePeers("n1=a:7000/a:7070, n2=b:7000/b:7070")
	if err != nil || len(peers) != 2 || peers[1] != (Peer{ID: "n2", RaftAddr: "b:7000", ClientAddr: "b:7070"}) {
		t.Fatalf("ParsePeers: %+v, %v", peers, err)
	}
	for _, bad := range []string{"", "n1", "n1=a:7000", "n1=a/b,n1=c/d"} {
		if _, err := ParsePeers(bad); err == nil {
			t.Errorf("ParsePeers(%q) succeeded", bad)
		}
	}
}
