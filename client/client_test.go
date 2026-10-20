package client_test

import (
	"context"
	"net"
	"path/filepath"
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

// startServer runs a real gRPC server over an in-memory listener.
func startServer(t *testing.T) *client.Client {
	t.Helper()
	store, err := queue.Open(filepath.Join(t.TempDir(), "queue.db"))
	if err != nil {
		t.Fatal(err)
	}
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	pb.RegisterQueueServiceServer(gs, server.New(broker.NewLocal(store)))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(func() {
		gs.Stop()
		_ = store.Close()
	})

	c, err := client.Dial("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestSubmitClaimAckRoundTrip(t *testing.T) {
	c := startServer(t)
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
	c := startServer(t)
	ctx := context.Background()
	id, _ := c.Submit(ctx, "q", []byte("x"), 3)

	first, _ := c.Claim(ctx, "q", "w1", time.Hour)
	if err := c.Nack(ctx, id, first.ID); err != nil {
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
