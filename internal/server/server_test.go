package server

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/arvindxyogesh/quorumq/api/quorumq/v1"
	"github.com/arvindxyogesh/quorumq/internal/broker"
	"github.com/arvindxyogesh/quorumq/internal/queue"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestServer(t *testing.T) (*Server, *fakeClock) {
	t.Helper()
	store, err := queue.Open(filepath.Join(t.TempDir(), "queue.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	clock := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	return New(broker.NewLocal(store, broker.WithClock(clock.Now))), clock
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("got code %s (%v), want %s", got, err, want)
	}
}

func TestSubmitValidation(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()
	cases := map[string]*pb.SubmitJobRequest{
		"empty queue":      {Queue: ""},
		"bad queue chars":  {Queue: "has spaces"},
		"long queue":       {Queue: strings.Repeat("q", 81)},
		"payload too big":  {Queue: "q", Payload: make([]byte, MaxPayloadBytes+1)},
		"too many retries": {Queue: "q", MaxRetries: MaxRetriesLimit + 1},
	}
	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := s.SubmitJob(ctx, req)
			wantCode(t, err, codes.InvalidArgument)
		})
	}
}

func TestClaimValidation(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()
	cases := map[string]*pb.ClaimJobRequest{
		"missing worker":  {Queue: "q"},
		"timeout too low": {Queue: "q", WorkerId: "w", VisibilityTimeout: durationpb.New(time.Millisecond)},
		"timeout too big": {Queue: "q", WorkerId: "w", VisibilityTimeout: durationpb.New(13 * time.Hour)},
		"negative":        {Queue: "q", WorkerId: "w", VisibilityTimeout: durationpb.New(-time.Second)},
	}
	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := s.ClaimJob(ctx, req)
			wantCode(t, err, codes.InvalidArgument)
		})
	}
}

func TestJobIDValidation(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()
	for _, id := range []string{"", "0", "-1", "abc"} {
		_, err := s.GetJob(ctx, &pb.GetJobRequest{JobId: id})
		wantCode(t, err, codes.InvalidArgument)
	}
	_, err := s.AckJob(ctx, &pb.AckJobRequest{JobId: "1"})
	wantCode(t, err, codes.InvalidArgument) // missing lease
}

func TestErrorMapping(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()

	_, err := s.GetJob(ctx, &pb.GetJobRequest{JobId: "999"})
	wantCode(t, err, codes.NotFound)

	sub, err := s.SubmitJob(ctx, &pb.SubmitJobRequest{Queue: "q"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.AckJob(ctx, &pb.AckJobRequest{JobId: sub.GetJobId(), LeaseId: "not-the-lease"})
	wantCode(t, err, codes.FailedPrecondition)
}

func TestClaimDefaultsVisibilityTimeout(t *testing.T) {
	s, clock := newTestServer(t)
	ctx := context.Background()
	if _, err := s.SubmitJob(ctx, &pb.SubmitJobRequest{Queue: "q"}); err != nil {
		t.Fatal(err)
	}
	resp, err := s.ClaimJob(ctx, &pb.ClaimJobRequest{Queue: "q", WorkerId: "w"})
	if err != nil {
		t.Fatal(err)
	}
	got := resp.GetLeaseExpiresAt().AsTime().Sub(clock.Now())
	if got != DefaultVisibilityTimeout {
		t.Fatalf("lease length = %s, want %s", got, DefaultVisibilityTimeout)
	}
}

func TestClaimEmptyQueueHasNoJob(t *testing.T) {
	s, _ := newTestServer(t)
	resp, err := s.ClaimJob(context.Background(), &pb.ClaimJobRequest{Queue: "q", WorkerId: "w"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetJob() != nil || resp.GetLeaseId() != "" {
		t.Fatalf("expected empty response, got %v", resp)
	}
}

func TestVisibilityTimeoutThroughAPI(t *testing.T) {
	s, clock := newTestServer(t)
	ctx := context.Background()
	sub, _ := s.SubmitJob(ctx, &pb.SubmitJobRequest{Queue: "q", Payload: []byte("x"), MaxRetries: 3})
	claimReq := &pb.ClaimJobRequest{Queue: "q", WorkerId: "w", VisibilityTimeout: durationpb.New(5 * time.Second)}

	first, _ := s.ClaimJob(ctx, claimReq)
	clock.Advance(5 * time.Second)
	second, err := s.ClaimJob(ctx, claimReq)
	if err != nil {
		t.Fatal(err)
	}
	if second.GetJob().GetId() != sub.GetJobId() || second.GetJob().GetAttempts() != 2 {
		t.Fatalf("job not redelivered after timeout: %v", second)
	}
	_, err = s.AckJob(ctx, &pb.AckJobRequest{JobId: sub.GetJobId(), LeaseId: first.GetLeaseId()})
	wantCode(t, err, codes.FailedPrecondition)

	stats, _ := s.QueueStats(ctx, &pb.QueueStatsRequest{Queue: "q"})
	if stats.GetInFlight() != 1 || stats.GetVisible() != 0 {
		t.Fatalf("stats = %v", stats)
	}
}

func TestListDeadJobsValidation(t *testing.T) {
	s, _ := newTestServer(t)
	_, err := s.ListDeadJobs(context.Background(), &pb.ListDeadJobsRequest{Queue: "q", Limit: MaxListLimit + 1})
	wantCode(t, err, codes.InvalidArgument)
	_, err = s.NackJob(context.Background(), &pb.NackJobRequest{JobId: "1", LeaseId: "l", Reason: strings.Repeat("x", 1025)})
	wantCode(t, err, codes.InvalidArgument)
}

func TestExpiredLeaseWithoutRetriesIsDeadLettered(t *testing.T) {
	s, clock := newTestServer(t)
	ctx := context.Background()
	sub, _ := s.SubmitJob(ctx, &pb.SubmitJobRequest{Queue: "q", MaxRetries: 0})
	claimReq := &pb.ClaimJobRequest{Queue: "q", WorkerId: "w", VisibilityTimeout: durationpb.New(time.Second)}
	if _, err := s.ClaimJob(ctx, claimReq); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Second)

	resp, err := s.ClaimJob(ctx, claimReq)
	if err != nil || resp.GetJob() != nil {
		t.Fatalf("expected no redelivery, got %v, %v", resp, err)
	}
	got, _ := s.GetJob(ctx, &pb.GetJobRequest{JobId: sub.GetJobId()})
	if got.GetJob().GetState() != pb.JobState_JOB_STATE_DEAD {
		t.Fatalf("job = %v, want DEAD", got.GetJob())
	}
	_, err = s.RedriveJob(ctx, &pb.RedriveJobRequest{JobId: "999"})
	wantCode(t, err, codes.NotFound)
}
