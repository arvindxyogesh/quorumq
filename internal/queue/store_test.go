package queue

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// t0 is an arbitrary fixed start time; tests advance it explicitly instead of
// sleeping.
var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()

func at(d time.Duration) int64 { return t0 + int64(d) }

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "queue.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func mustSubmit(t *testing.T, s *Store, queue, payload string, now int64) uint64 {
	t.Helper()
	id, err := s.Submit(SubmitCmd{Queue: queue, Payload: []byte(payload), MaxRetries: 3, Now: now})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	return id
}

func claim(t *testing.T, s *Store, queue, lease string, vt time.Duration, now int64) *Job {
	t.Helper()
	j, err := s.Claim(ClaimCmd{Queue: queue, WorkerID: "w-" + lease, LeaseID: lease, VisibilityTimeout: vt, Now: now})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	return j
}

func TestSubmitAndGet(t *testing.T) {
	s := openTestStore(t)
	id := mustSubmit(t, s, "emails", "hello", t0)

	j, err := s.Get(id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if j.Queue != "emails" || string(j.Payload) != "hello" || j.State != StatePending {
		t.Fatalf("unexpected job: %+v", j)
	}
	if j.CreatedAt != t0 || j.VisibleAt != t0 || j.MaxRetries != 3 {
		t.Fatalf("unexpected times or retries: %+v", j)
	}
	if _, err := s.Get(id + 100); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get missing job: got %v, want ErrNotFound", err)
	}
}

func TestIDsAreSequential(t *testing.T) {
	s := openTestStore(t)
	for want := uint64(1); want <= 3; want++ {
		if got := mustSubmit(t, s, "q", "x", t0); got != want {
			t.Fatalf("id = %d, want %d", got, want)
		}
	}
}

func TestClaimIsFIFO(t *testing.T) {
	s := openTestStore(t)
	first := mustSubmit(t, s, "q", "a", at(1*time.Second))
	second := mustSubmit(t, s, "q", "b", at(2*time.Second))

	if j := claim(t, s, "q", "l1", time.Minute, at(3*time.Second)); j.ID != first {
		t.Fatalf("first claim got job %d, want %d", j.ID, first)
	}
	if j := claim(t, s, "q", "l2", time.Minute, at(3*time.Second)); j.ID != second {
		t.Fatalf("second claim got job %d, want %d", j.ID, second)
	}
}

func TestClaimEmptyQueueReturnsNil(t *testing.T) {
	s := openTestStore(t)
	if j := claim(t, s, "nothing-here", "l1", time.Minute, t0); j != nil {
		t.Fatalf("claim on empty queue returned %+v", j)
	}
}

func TestQueuesAreIsolated(t *testing.T) {
	s := openTestStore(t)
	mustSubmit(t, s, "a", "x", t0)
	if j := claim(t, s, "b", "l1", time.Minute, t0); j != nil {
		t.Fatalf("claimed job %d from the wrong queue", j.ID)
	}
}

func TestClaimSetsLease(t *testing.T) {
	s := openTestStore(t)
	id := mustSubmit(t, s, "q", "x", t0)
	j := claim(t, s, "q", "lease-1", 30*time.Second, at(time.Second))

	if j.ID != id || j.State != StateLeased || j.Attempts != 1 {
		t.Fatalf("unexpected claimed job: %+v", j)
	}
	if j.LeaseID != "lease-1" || j.WorkerID != "w-lease-1" {
		t.Fatalf("lease not recorded: %+v", j)
	}
	if want := at(31 * time.Second); j.VisibleAt != want {
		t.Fatalf("VisibleAt = %d, want %d", j.VisibleAt, want)
	}
}

func TestVisibilityTimeoutRedelivers(t *testing.T) {
	s := openTestStore(t)
	id := mustSubmit(t, s, "q", "x", t0)
	claim(t, s, "q", "lease-1", 10*time.Second, t0)

	// Still hidden before the timeout.
	if j := claim(t, s, "q", "lease-2", 10*time.Second, at(9*time.Second)); j != nil {
		t.Fatalf("job %d was redelivered before its lease expired", j.ID)
	}
	// Claimable again once the lease expires.
	j := claim(t, s, "q", "lease-2", 10*time.Second, at(10*time.Second))
	if j == nil || j.ID != id {
		t.Fatalf("expected job %d to be redelivered, got %+v", id, j)
	}
	if j.Attempts != 2 || j.LeaseID != "lease-2" {
		t.Fatalf("redelivery did not issue a new lease: %+v", j)
	}
}

func TestExpiredLeaseIsRedeliveredBeforeNewerJobs(t *testing.T) {
	s := openTestStore(t)
	old := mustSubmit(t, s, "q", "old", t0)
	claim(t, s, "q", "lease-1", 5*time.Second, t0)
	mustSubmit(t, s, "q", "new", at(6*time.Second))

	// The expired job became visible at t0+5s, before the new job at t0+6s.
	if j := claim(t, s, "q", "lease-2", time.Minute, at(7*time.Second)); j.ID != old {
		t.Fatalf("got job %d, want the expired job %d", j.ID, old)
	}
}

func TestAck(t *testing.T) {
	s := openTestStore(t)
	id := mustSubmit(t, s, "q", "x", t0)
	claim(t, s, "q", "lease-1", time.Minute, t0)

	if err := s.Ack(AckCmd{JobID: id, LeaseID: "lease-1", Now: at(time.Second)}); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	j, _ := s.Get(id)
	if j.State != StateAcked || j.VisibleAt != 0 {
		t.Fatalf("job not acked: %+v", j)
	}
	// An acked job is never redelivered, even after its lease would have expired.
	if j := claim(t, s, "q", "lease-2", time.Minute, at(time.Hour)); j != nil {
		t.Fatalf("acked job %d was redelivered", j.ID)
	}
}

func TestAckIsIdempotentForTheSameLease(t *testing.T) {
	s := openTestStore(t)
	id := mustSubmit(t, s, "q", "x", t0)
	claim(t, s, "q", "lease-1", time.Minute, t0)

	for i := 0; i < 2; i++ {
		if err := s.Ack(AckCmd{JobID: id, LeaseID: "lease-1", Now: t0}); err != nil {
			t.Fatalf("ack #%d: %v", i+1, err)
		}
	}
	st, _ := s.Stats("q", t0)
	if st.Acked != 1 {
		t.Fatalf("Acked = %d after a duplicate ack, want 1", st.Acked)
	}
}

func TestAckRejectsWrongLease(t *testing.T) {
	s := openTestStore(t)
	id := mustSubmit(t, s, "q", "x", t0)

	// Not yet claimed: no lease can be valid, including an empty one.
	for _, lease := range []string{"", "made-up"} {
		if err := s.Ack(AckCmd{JobID: id, LeaseID: lease, Now: t0}); !errors.Is(err, ErrLeaseMismatch) {
			t.Fatalf("ack of unclaimed job with lease %q: got %v, want ErrLeaseMismatch", lease, err)
		}
	}
	claim(t, s, "q", "lease-1", time.Minute, t0)
	if err := s.Ack(AckCmd{JobID: id, LeaseID: "lease-2", Now: t0}); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("ack with wrong lease: got %v, want ErrLeaseMismatch", err)
	}
}

func TestStaleWorkerCannotAckAfterRelease(t *testing.T) {
	s := openTestStore(t)
	id := mustSubmit(t, s, "q", "x", t0)
	claim(t, s, "q", "slow-worker", 10*time.Second, t0)
	claim(t, s, "q", "fast-worker", 10*time.Second, at(11*time.Second)) // lease expired, re-claimed

	err := s.Ack(AckCmd{JobID: id, LeaseID: "slow-worker", Now: at(12 * time.Second)})
	if !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("stale ack: got %v, want ErrLeaseMismatch", err)
	}
	if err := s.Ack(AckCmd{JobID: id, LeaseID: "fast-worker", Now: at(12 * time.Second)}); err != nil {
		t.Fatalf("current lease ack: %v", err)
	}
}

func TestAckAfterExpiryWhenNotReclaimed(t *testing.T) {
	s := openTestStore(t)
	id := mustSubmit(t, s, "q", "x", t0)
	claim(t, s, "q", "lease-1", 10*time.Second, t0)

	// The lease has expired but nobody else holds the job, so the late ack wins.
	if err := s.Ack(AckCmd{JobID: id, LeaseID: "lease-1", Now: at(time.Minute)}); err != nil {
		t.Fatalf("late ack: %v", err)
	}
	if j := claim(t, s, "q", "lease-2", time.Minute, at(time.Minute)); j != nil {
		t.Fatalf("job %d was redelivered after a late ack", j.ID)
	}
}

func TestAckUnknownJob(t *testing.T) {
	s := openTestStore(t)
	if err := s.Ack(AckCmd{JobID: 42, LeaseID: "x", Now: t0}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestNackMakesJobVisibleImmediately(t *testing.T) {
	s := openTestStore(t)
	id := mustSubmit(t, s, "q", "x", t0)
	claim(t, s, "q", "lease-1", time.Hour, t0)

	if err := s.Nack(NackCmd{JobID: id, LeaseID: "lease-1", Now: at(time.Second)}); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	// A duplicate nack is a no-op.
	if err := s.Nack(NackCmd{JobID: id, LeaseID: "lease-1", Now: at(time.Second)}); err != nil {
		t.Fatalf("duplicate Nack: %v", err)
	}
	j := claim(t, s, "q", "lease-2", time.Hour, at(time.Second))
	if j == nil || j.ID != id || j.Attempts != 2 {
		t.Fatalf("nacked job not redelivered: %+v", j)
	}
	// The old lease can no longer nack the job now that it is re-leased.
	if err := s.Nack(NackCmd{JobID: id, LeaseID: "lease-1", Now: at(time.Second)}); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("stale nack: got %v, want ErrLeaseMismatch", err)
	}
}

func TestStats(t *testing.T) {
	s := openTestStore(t)
	for i := 0; i < 4; i++ {
		mustSubmit(t, s, "q", "x", t0)
	}
	mustSubmit(t, s, "other", "x", t0)

	acked := claim(t, s, "q", "l1", time.Minute, t0)
	if err := s.Ack(AckCmd{JobID: acked.ID, LeaseID: "l1", Now: t0}); err != nil {
		t.Fatal(err)
	}
	claim(t, s, "q", "l2", time.Minute, t0) // in flight
	claim(t, s, "q", "l3", time.Second, t0) // expires at t0+1s
	now := at(2 * time.Second)              // one job never claimed, one expired

	st, err := s.Stats("q", now)
	if err != nil {
		t.Fatal(err)
	}
	want := Stats{Visible: 2, InFlight: 1, Acked: 1}
	if st != want {
		t.Fatalf("Stats = %+v, want %+v", st, want)
	}
	if st, _ := s.Stats("missing", now); st != (Stats{}) {
		t.Fatalf("Stats for unknown queue = %+v, want zero", st)
	}
}

func TestStateSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id := mustSubmit(t, s, "q", "durable", t0)
	claim(t, s, "q", "lease-1", time.Minute, t0)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	j, err := s.Get(id)
	if err != nil || j.State != StateLeased || j.LeaseID != "lease-1" {
		t.Fatalf("after reopen: job=%+v err=%v", j, err)
	}
	if err := s.Ack(AckCmd{JobID: id, LeaseID: "lease-1", Now: t0}); err != nil {
		t.Fatalf("ack after reopen: %v", err)
	}
	if next := mustSubmit(t, s, "q", "x", t0); next != id+1 {
		t.Fatalf("ID sequence restarted: got %d, want %d", next, id+1)
	}
}

// Concurrent workers must never be handed the same job while its lease is
// valid. Run with -race.
func TestConcurrentClaimsDeliverEachJobOnce(t *testing.T) {
	s := openTestStore(t)
	const jobs, workers = 200, 8
	for i := 0; i < jobs; i++ {
		mustSubmit(t, s, "q", fmt.Sprint(i), t0)
	}

	var (
		mu   sync.Mutex
		seen = make(map[uint64]int)
		wg   sync.WaitGroup
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for n := 0; ; n++ {
				lease := fmt.Sprintf("w%d-%d", w, n)
				j, err := s.Claim(ClaimCmd{Queue: "q", WorkerID: fmt.Sprint(w), LeaseID: lease, VisibilityTimeout: time.Hour, Now: t0})
				if err != nil {
					t.Errorf("Claim: %v", err)
					return
				}
				if j == nil {
					return
				}
				mu.Lock()
				seen[j.ID]++
				mu.Unlock()
				if err := s.Ack(AckCmd{JobID: j.ID, LeaseID: lease, Now: t0}); err != nil {
					t.Errorf("Ack: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	if len(seen) != jobs {
		t.Fatalf("delivered %d distinct jobs, want %d", len(seen), jobs)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("job %d delivered %d times", id, n)
		}
	}
	if st, _ := s.Stats("q", t0); st.Acked != jobs || st.Visible != 0 || st.InFlight != 0 {
		t.Fatalf("final stats %+v", st)
	}
}

func TestQueuesListsEveryQueue(t *testing.T) {
	s := openTestStore(t)
	mustSubmit(t, s, "b", "x", t0)
	mustSubmit(t, s, "a", "x", t0)
	mustSubmit(t, s, "a", "x", t0)
	got, err := s.Queues()
	if err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("Queues() = %v, %v", got, err)
	}
}

func TestHasVisible(t *testing.T) {
	s := openTestStore(t)
	if ok, _ := s.HasVisible("q", t0); ok {
		t.Fatal("empty queue reported a visible job")
	}
	mustSubmit(t, s, "q", "x", t0)
	if ok, _ := s.HasVisible("q", t0); !ok {
		t.Fatal("submitted job not visible")
	}
	claim(t, s, "q", "l1", 10*time.Second, t0)
	if ok, _ := s.HasVisible("q", at(9*time.Second)); ok {
		t.Fatal("leased job reported visible before its lease expired")
	}
	if ok, _ := s.HasVisible("q", at(10*time.Second)); !ok {
		t.Fatal("expired lease not reported visible")
	}
}
