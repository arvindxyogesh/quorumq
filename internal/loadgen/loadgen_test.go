package loadgen

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"

	pb "github.com/arvindxyogesh/quorumq/api/quorumq/v1"
	"github.com/arvindxyogesh/quorumq/client"
	"github.com/arvindxyogesh/quorumq/internal/broker"
	"github.com/arvindxyogesh/quorumq/internal/queue"
	"github.com/arvindxyogesh/quorumq/internal/server"
)

func startServer(t *testing.T) string {
	t.Helper()
	store, err := queue.Open(filepath.Join(t.TempDir(), "q.db"))
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	pb.RegisterQueueServiceServer(gs, server.New(broker.NewLocal(store)))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(func() { gs.Stop(); _ = store.Close() })
	return lis.Addr().String()
}

func TestRunReconcilesEverySubmit(t *testing.T) {
	c, err := client.New([]string{startServer(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	res, err := Run(context.Background(), c, Config{
		Queue: "load", Producers: 2, Workers: 2, Rate: 200, Duration: time.Second,
		PayloadSize: 128, MaxRetries: 3, DrainTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Accepted < 150 || res.Accepted > 220 {
		t.Fatalf("accepted %d submits at a 200/s target over 1s", res.Accepted)
	}
	if res.Lost != 0 || res.Completed != res.Accepted {
		t.Fatalf("lost %d; completed %d of %d", res.Lost, res.Completed, res.Accepted)
	}
	if res.Latency.Count != res.Completed || res.Latency.P50 <= 0 || res.Latency.P99 < res.Latency.P50 {
		t.Fatalf("latency summary looks wrong: %+v", res.Latency)
	}
	var timeline int
	for _, s := range res.PerSecond {
		timeline += s.Completed
	}
	if timeline != res.Completed {
		t.Fatalf("timeline has %d completions, want %d", timeline, res.Completed)
	}
	if len(res.SubmittedIDs()) != res.Accepted || len(res.CompletedIDs()) != res.Completed {
		t.Fatal("ID maps do not match the counts")
	}
}

func TestClosedLoopRun(t *testing.T) {
	c, err := client.New([]string{startServer(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	res, err := Run(context.Background(), c, Config{
		Queue: "load", Producers: 4, Workers: 4, Duration: 500 * time.Millisecond,
		PayloadSize: 64, MaxRetries: 3, DrainTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Accepted == 0 || res.Lost != 0 {
		t.Fatalf("closed loop: accepted %d, lost %d", res.Accepted, res.Lost)
	}
}

func TestPayloadRoundTrip(t *testing.T) {
	r := &run{cfg: Config{PayloadSize: 100}}
	at := time.Unix(0, 1_700_000_000_123_456_789)
	p := r.payload("run-1-42", at)
	if len(p) != 100 {
		t.Fatalf("payload is %d bytes, want 100", len(p))
	}
	token, got, ok := parsePayload(p)
	if !ok || token != "run-1-42" || !got.Equal(at) {
		t.Fatalf("parsePayload = %q, %v, %v", token, got, ok)
	}
	if _, _, ok := parsePayload([]byte("not ours")); ok {
		t.Fatal("parsed a foreign payload")
	}
}

func TestPercentiles(t *testing.T) {
	var xs []float64
	for i := 1; i <= 100; i++ {
		xs = append(xs, float64(i))
	}
	s := summarize(xs)
	if s.P50 != 50 || s.P95 != 95 || s.P99 != 99 || s.Max != 100 || s.Mean != 50.5 {
		t.Fatalf("summary = %+v", s)
	}
	if (summarize(nil) != LatencySummary{}) {
		t.Fatal("empty summary not zero")
	}
}

func TestConfigValidation(t *testing.T) {
	for _, cfg := range []Config{
		{Producers: 1, Workers: 1, Duration: time.Second, PayloadSize: 64},
		{Queue: "q", Workers: 1, Duration: time.Second, PayloadSize: 64},
		{Queue: "q", Producers: 1, Workers: 1, PayloadSize: 64},
		{Queue: "q", Producers: 1, Workers: 1, Duration: time.Second, PayloadSize: 10},
	} {
		if _, err := Run(context.Background(), nil, cfg); err == nil {
			t.Errorf("config %+v accepted", cfg)
		}
	}
}
