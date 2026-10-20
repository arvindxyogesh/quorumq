package client_test

import (
	"context"
	"net"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/arvindxyogesh/quorumq/api/quorumq/v1"
	"github.com/arvindxyogesh/quorumq/client"
	"github.com/arvindxyogesh/quorumq/internal/broker"
	"github.com/arvindxyogesh/quorumq/internal/queue"
	"github.com/arvindxyogesh/quorumq/internal/server"
)

// newLocalBroker returns a single-process broker that retries nacked jobs
// immediately, so tests do not wait out a backoff.
func newLocalBroker(t *testing.T) *broker.Local {
	t.Helper()
	store, err := queue.Open(filepath.Join(t.TempDir(), "queue.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return broker.NewLocal(store, broker.WithRetryPolicy(broker.RetryPolicy{}))
}

// startBufconn runs a real gRPC server over an in-memory listener.
func startBufconn(t *testing.T, b server.Broker) *client.Client {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	pb.RegisterQueueServiceServer(gs, server.New(b))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	c, err := client.New([]string{"passthrough:///bufnet"}, client.WithDialOptions(
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) })))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// startTCP runs a gRPC server on a real localhost port and returns its address.
func startTCP(t *testing.T, b server.Broker) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	pb.RegisterQueueServiceServer(gs, server.New(b))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return lis.Addr().String()
}

func TestSubmitClaimAckRoundTrip(t *testing.T) {
	c := startBufconn(t, newLocalBroker(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	id, err := c.Submit(ctx, "emails", []byte(`{"to":"a@example.com"}`), 3)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	lease, err := c.Claim(ctx, "emails", "worker-1", 10*time.Second)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if lease == nil || lease.Job.GetId() != id || string(lease.Job.GetPayload()) != `{"to":"a@example.com"}` {
		t.Fatalf("unexpected lease: %+v", lease)
	}
	if lease.ID == "" || time.Until(lease.ExpiresAt) <= 0 {
		t.Fatalf("lease is missing an ID or expiry: %+v", lease)
	}

	if err := c.Ack(ctx, id, lease.ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	job, err := c.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if job.GetState() != pb.JobState_JOB_STATE_ACKED || job.GetWorkerId() != "worker-1" {
		t.Fatalf("job after ack: %v", job)
	}

	stats, err := c.Stats(ctx, "emails")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.GetAcked() != 1 || stats.GetVisible() != 0 || stats.GetInFlight() != 0 {
		t.Fatalf("stats: %v", stats)
	}

	if again, err := c.Claim(ctx, "emails", "worker-1", 0); err != nil || again != nil {
		t.Fatalf("claim on drained queue: lease=%v err=%v", again, err)
	}
}

func TestNackRedelivers(t *testing.T) {
	c := startBufconn(t, newLocalBroker(t))
	ctx := context.Background()
	id, _ := c.Submit(ctx, "q", []byte("x"), 3)

	first, _ := c.Claim(ctx, "q", "w1", time.Hour)
	if err := c.Nack(ctx, id, first.ID, "transient"); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	second, err := c.Claim(ctx, "q", "w2", time.Hour)
	if err != nil || second == nil || second.Job.GetId() != id || second.Job.GetAttempts() != 2 {
		t.Fatalf("expected redelivery, got lease=%+v err=%v", second, err)
	}
	err = c.Ack(ctx, id, first.ID)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ack with superseded lease: got %v, want FailedPrecondition", err)
	}
}

// follower is a broker that always redirects to a leader and counts calls.
type follower struct {
	server.Broker
	leader string
	calls  atomic.Int32
}

func (f *follower) Submit(context.Context, string, []byte, uint32) (uint64, error) {
	f.calls.Add(1)
	return 0, &broker.NotLeaderError{Leader: f.leader}
}

func TestClientFollowsLeaderHint(t *testing.T) {
	leaderAddr := startTCP(t, newLocalBroker(t))
	f := &follower{leader: leaderAddr}
	followerAddr := startTCP(t, f)

	c, err := client.New([]string{followerAddr})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 3; i++ {
		if _, err := c.Submit(ctx, "q", []byte("x"), 0); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	// Only the first call reached the follower; the rest went to the leader.
	if n := f.calls.Load(); n != 1 {
		t.Fatalf("follower received %d calls, want 1", n)
	}
}

func TestNoRetryAsksOnlyTheFirstNode(t *testing.T) {
	leaderAddr := startTCP(t, newLocalBroker(t))
	f := &follower{leader: leaderAddr}
	c, err := client.New([]string{startTCP(t, f), leaderAddr}, client.WithNoRetry())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	_, err = c.Submit(context.Background(), "q", []byte("x"), 0)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("got %v, want the follower's UNAVAILABLE without a redirect", err)
	}
	if n := f.calls.Load(); n != 1 {
		t.Fatalf("follower received %d calls, want exactly 1", n)
	}
}

func TestClientSkipsUnreachableNodes(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := lis.Addr().String()
	_ = lis.Close() // nothing listens here any more

	live := startTCP(t, newLocalBroker(t))
	c, err := client.New([]string{dead, live}, client.WithBackoff(time.Millisecond, 10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := c.Submit(ctx, "q", []byte("x"), 0); err != nil {
		t.Fatalf("Submit with one dead node: %v", err)
	}
}

func TestClientGivesUpWhenContextExpires(t *testing.T) {
	f := &follower{} // no leader known: every call is Unavailable
	c, err := client.New([]string{startTCP(t, f)}, client.WithBackoff(time.Millisecond, 5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err = c.Submit(ctx, "q", []byte("x"), 0)
	if status.Code(err) != codes.DeadlineExceeded || !strings.Contains(err.Error(), "not the leader") {
		t.Fatalf("got %v, want DeadlineExceeded carrying the last cluster error", err)
	}
	if f.calls.Load() < 2 {
		t.Fatalf("expected retries before giving up, got %d calls", f.calls.Load())
	}
}

func TestDeadLetterAndRedrive(t *testing.T) {
	c := startBufconn(t, newLocalBroker(t))
	ctx := context.Background()
	id, _ := c.Submit(ctx, "q", []byte("poison"), 0) // no retries

	lease, _ := c.Claim(ctx, "q", "w1", time.Minute)
	if err := c.Nack(ctx, id, lease.ID, "cannot parse payload"); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	dead, err := c.ListDead(ctx, "q", 0)
	if err != nil || len(dead) != 1 || dead[0].GetId() != id {
		t.Fatalf("ListDead = %v, %v", dead, err)
	}
	if dead[0].GetState() != pb.JobState_JOB_STATE_DEAD || dead[0].GetLastError() != "cannot parse payload" || dead[0].GetDeadAt() == nil {
		t.Fatalf("dead job = %v", dead[0])
	}
	stats, _ := c.Stats(ctx, "q")
	if stats.GetDead() != 1 || stats.GetVisible() != 0 {
		t.Fatalf("stats = %v", stats)
	}

	if err := c.Redrive(ctx, id); err != nil {
		t.Fatalf("Redrive: %v", err)
	}
	again, err := c.Claim(ctx, "q", "w2", time.Minute)
	if err != nil || again == nil || again.Job.GetId() != id || again.Job.GetAttempts() != 1 {
		t.Fatalf("claim after redrive: %+v, %v", again, err)
	}
	if err := c.Redrive(ctx, id); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("redrive of a live job: got %v, want FailedPrecondition", err)
	}
}

func TestClientDoesNotRetryApplicationErrors(t *testing.T) {
	c := startBufconn(t, newLocalBroker(t))
	_, err := c.Get(context.Background(), "12345")
	if status.Code(err) != codes.NotFound {
		t.Fatalf("got %v, want NotFound without retries", err)
	}
}
