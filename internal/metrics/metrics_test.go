package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNilMetricsAreNoOps(t *testing.T) {
	var m *Metrics
	m.JobSubmitted("q")
	m.JobClaimed("q", true)
	m.JobAcked("q")
	m.JobNacked("q", true)
	m.JobRedriven("q")
	m.LeaderChanged()
	m.ObserveApply("submit", time.Millisecond)
	m.ObserveFSMBatch(3, time.Millisecond)
	_, err := m.UnaryServerInterceptor()(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/x/Y"},
		func(context.Context, any) (any, error) { return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
}

func TestJobCounters(t *testing.T) {
	m := New()
	m.JobSubmitted("emails")
	m.JobSubmitted("emails")
	m.JobClaimed("emails", false)
	m.JobClaimed("emails", true)
	m.JobNacked("emails", true)

	for name, c := range map[string]struct {
		got, want float64
	}{
		"submitted":     {testutil.ToFloat64(m.submitted.WithLabelValues("emails")), 2},
		"claimed":       {testutil.ToFloat64(m.claimed.WithLabelValues("emails")), 1},
		"empty claims":  {testutil.ToFloat64(m.emptyClaims.WithLabelValues("emails")), 1},
		"nacked":        {testutil.ToFloat64(m.nacked.WithLabelValues("emails")), 1},
		"dead-lettered": {testutil.ToFloat64(m.deadLettered.WithLabelValues("emails")), 1},
	} {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", name, c.got, c.want)
		}
	}
}

func TestInterceptorRecordsMethodAndCode(t *testing.T) {
	m := New()
	icpt := m.UnaryServerInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/quorumq.v1.QueueService/AckJob"}
	ok := func(context.Context, any) (any, error) { return "ok", nil }
	stale := func(context.Context, any) (any, error) {
		return nil, status.Error(codes.FailedPrecondition, "stale lease")
	}

	_, _ = icpt(context.Background(), nil, info, ok)
	_, err := icpt(context.Background(), nil, info, stale)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("interceptor changed the handler's error: %v", err)
	}
	if got := testutil.ToFloat64(m.requests.WithLabelValues("AckJob", "OK")); got != 1 {
		t.Errorf("OK requests = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.requests.WithLabelValues("AckJob", "FailedPrecondition")); got != 1 {
		t.Errorf("FailedPrecondition requests = %v, want 1", got)
	}
	if n := testutil.CollectAndCount(m.requestDuration); n != 1 {
		t.Errorf("duration series = %d, want 1 (one method)", n)
	}
}

func TestMethodName(t *testing.T) {
	for in, want := range map[string]string{
		"/quorumq.v1.QueueService/SubmitJob": "SubmitJob",
		"NoSlash":                            "NoSlash",
	} {
		if got := methodName(in); got != want {
			t.Errorf("methodName(%q) = %q, want %q", in, got, want)
		}
	}
}
