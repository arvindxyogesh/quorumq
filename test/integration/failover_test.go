//go:build integration

package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/arvindxyogesh/quorumq/client"
)

// TestNoJobLostWhenLeaderIsKilled runs producers and workers against a
// 3-node cluster and SIGKILLs the leader twice mid-stream (restarting the
// first victim in between, so a quorum always exists). Every job whose
// Submit returned successfully must end up processed by a worker.
func TestNoJobLostWhenLeaderIsKilled(t *testing.T) {
	const (
		producers      = 2
		jobsPerProd    = 1000
		workers        = 4
		visibility     = 3 * time.Second
		submitInterval = 4 * time.Millisecond
	)
	total := producers * jobsPerProd
	c := startCluster(t, 3)

	newClient := func() *client.Client {
		cl, err := client.New(c.grpcAddrs())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cl.Close() })
		return cl
	}
	producerClient, workerClient := newClient(), newClient()

	var (
		mu        sync.Mutex
		submitted = make(map[string]string) // payload -> job ID returned by Submit
		processed = make(map[string]int)    // payload -> times a worker handled it
		submits   atomic.Int64
		staleAcks atomic.Int64
	)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Producers.
	var prodWG sync.WaitGroup
	for p := 0; p < producers; p++ {
		prodWG.Add(1)
		go func(p int) {
			defer prodWG.Done()
			for i := 0; i < jobsPerProd; i++ {
				payload := fmt.Sprintf("p%d-job%d", p, i)
				reqCtx, reqCancel := context.WithTimeout(ctx, 30*time.Second)
				id, err := producerClient.Submit(reqCtx, "failover", []byte(payload), 3)
				reqCancel()
				if err != nil {
					t.Errorf("submit %s was not acknowledged within 30s: %v", payload, err)
					return
				}
				mu.Lock()
				submitted[payload] = id
				mu.Unlock()
				submits.Add(1)
				time.Sleep(submitInterval)
			}
		}(p)
	}

	// Workers run until every acknowledged submit has been processed.
	allDone := make(chan struct{})
	var workWG sync.WaitGroup
	for w := 0; w < workers; w++ {
		workWG.Add(1)
		go func(w int) {
			defer workWG.Done()
			worker := fmt.Sprintf("worker-%d", w)
			for {
				select {
				case <-allDone:
					return
				case <-ctx.Done():
					return
				default:
				}
				reqCtx, reqCancel := context.WithTimeout(ctx, 30*time.Second)
				lease, err := workerClient.Claim(reqCtx, "failover", worker, visibility)
				reqCancel()
				if err != nil {
					t.Errorf("%s claim: %v", worker, err)
					return
				}
				if lease == nil {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				payload := string(lease.Job.GetPayload())
				mu.Lock()
				processed[payload]++
				mu.Unlock()

				reqCtx, reqCancel = context.WithTimeout(ctx, 30*time.Second)
				err = workerClient.Ack(reqCtx, lease.Job.GetId(), lease.ID)
				reqCancel()
				switch {
				case err == nil:
				case status.Code(err) == codes.FailedPrecondition:
					staleAcks.Add(1) // lease expired during failover; someone else re-ran it
				default:
					t.Errorf("%s ack: %v", worker, err)
					return
				}
			}
		}(w)
	}

	waitForSubmits := func(n int64) {
		for submits.Load() < n {
			if ctx.Err() != nil || t.Failed() {
				t.FailNow()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Kill #1: the leader, a third of the way through.
	waitForSubmits(int64(total / 3))
	first := c.leader()
	t.Logf("kill #1: SIGKILL leader %s after %d submits", first.id, submits.Load())
	c.kill(first)
	c.leader() // a new leader is elected among the survivors

	time.Sleep(time.Second)
	t.Logf("restarting %s", first.id)
	c.start(first)

	// Kill #2: whoever leads now, two thirds of the way through.
	waitForSubmits(int64(2 * total / 3))
	second := c.leader()
	t.Logf("kill #2: SIGKILL leader %s after %d submits", second.id, submits.Load())
	c.kill(second)
	c.leader()

	prodWG.Wait()
	if t.Failed() {
		t.FailNow()
	}

	// Wait until every acknowledged submit has been processed at least once.
	deadline := time.Now().Add(90 * time.Second)
	for {
		mu.Lock()
		missing := 0
		for payload := range submitted {
			if processed[payload] == 0 {
				missing++
			}
		}
		mu.Unlock()
		if missing == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d acknowledged jobs were never processed (lost)", missing, len(submitted))
		}
		time.Sleep(50 * time.Millisecond)
	}
	close(allDone)
	workWG.Wait()

	// Bring the second victim back and check all three replicas converge.
	c.start(second)
	idx := c.waitConverged()

	mu.Lock()
	defer mu.Unlock()
	var duplicates int
	for _, n := range processed {
		duplicates += n - 1
	}
	stats, err := producerClient.Stats(ctx, "failover")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	t.Logf("acknowledged submits: %d, distinct jobs processed: %d, lost: 0", len(submitted), len(processed))
	t.Logf("redeliveries (at-least-once duplicates): %d, stale acks rejected: %d", duplicates, staleAcks.Load())
	t.Logf("final queue stats: %v; all 3 replicas converged at log index %d", stats, idx)

	if len(submitted) != total {
		t.Fatalf("only %d of %d submits were acknowledged", len(submitted), total)
	}
	if stats.GetVisible() != 0 || stats.GetInFlight() != 0 {
		t.Fatalf("queue not drained: %v", stats)
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("test context expired: %v", err)
	}
}
