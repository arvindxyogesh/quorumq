package queue

import (
	"errors"
	"testing"
	"time"
)

func submitWithRetries(t *testing.T, s *Store, maxRetries uint32) uint64 {
	t.Helper()
	id, err := s.Submit(SubmitCmd{Queue: "q", Payload: []byte("x"), MaxRetries: maxRetries, Now: t0})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func nack(t *testing.T, s *Store, id uint64, lease string, now int64) {
	t.Helper()
	err := s.Nack(NackCmd{JobID: id, LeaseID: lease, Reason: "boom", BackoffBase: time.Second, BackoffMax: 10 * time.Second, Now: now})
	if err != nil {
		t.Fatalf("Nack: %v", err)
	}
}

func TestRetryDelay(t *testing.T) {
	cases := []struct {
		attempt   uint32
		base, max time.Duration
		want      time.Duration
	}{
		{1, time.Second, time.Minute, time.Second},
		{2, time.Second, time.Minute, 2 * time.Second},
		{3, time.Second, time.Minute, 4 * time.Second},
		{7, time.Second, time.Minute, time.Minute}, // 64s capped
		{200, time.Second, time.Minute, time.Minute},
		{5, 0, time.Minute, 0}, // no base: retry immediately
		{0, time.Second, time.Minute, 0},
		{70, time.Second, 0, 1 << 62 * time.Nanosecond}, // uncapped stays positive
	}
	for _, c := range cases {
		got := RetryDelay(c.attempt, c.base, c.max)
		if c.max == 0 {
			if got <= 0 {
				t.Errorf("RetryDelay(%d, %s, uncapped) = %s, overflowed", c.attempt, c.base, got)
			}
			continue
		}
		if got != c.want {
			t.Errorf("RetryDelay(%d, %s, %s) = %s, want %s", c.attempt, c.base, c.max, got, c.want)
		}
	}
}

func TestNackBacksOffExponentially(t *testing.T) {
	s := openTestStore(t)
	id := submitWithRetries(t, s, 5)
	now := t0
	for attempt, wantDelay := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second} {
		lease := string(rune('a' + attempt))
		if j := claim(t, s, "q", lease, time.Minute, now); j == nil || j.ID != id {
			t.Fatalf("attempt %d: job not claimable at %d", attempt+1, now)
		}
		nack(t, s, id, lease, now)

		// Hidden for exactly the backoff, then visible again.
		if j := claim(t, s, "q", "early", time.Minute, now+int64(wantDelay)-1); j != nil {
			t.Fatalf("attempt %d: job visible before its %s backoff", attempt+1, wantDelay)
		}
		st, _ := s.Stats("q", now+1)
		if st.Delayed != 1 || st.Visible != 0 {
			t.Fatalf("attempt %d: stats during backoff = %+v, want 1 delayed", attempt+1, st)
		}
		now += int64(wantDelay)
	}
	j, _ := s.Get(id)
	if j.State != StatePending || j.Attempts != 5 || j.LastError != "boom" {
		t.Fatalf("after 5 nacks: %+v", j)
	}
}

func TestNackOnLastAttemptDeadLetters(t *testing.T) {
	s := openTestStore(t)
	id := submitWithRetries(t, s, 1) // one retry: two attempts in total

	claim(t, s, "q", "l1", time.Minute, t0)
	nack(t, s, id, "l1", t0)
	claim(t, s, "q", "l2", time.Minute, at(time.Second))
	nack(t, s, id, "l2", at(time.Second))

	j, _ := s.Get(id)
	if j.State != StateDead || j.DeadAt != at(time.Second) || j.VisibleAt != 0 || j.LastError != "boom" {
		t.Fatalf("job not dead-lettered: %+v", j)
	}
	if j := claim(t, s, "q", "l3", time.Minute, at(time.Hour)); j != nil {
		t.Fatalf("dead job %d was delivered", j.ID)
	}
	st, _ := s.Stats("q", at(time.Hour))
	if st != (Stats{Dead: 1}) {
		t.Fatalf("stats = %+v, want only 1 dead", st)
	}
	// A retried nack of the dead-lettering call is a no-op; an ack is refused.
	nack(t, s, id, "l2", at(time.Second))
	if err := s.Ack(AckCmd{JobID: id, LeaseID: "l2", Now: at(time.Second)}); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("ack of a dead job: got %v, want ErrLeaseMismatch", err)
	}
}

func TestZeroRetriesDeadLettersOnFirstFailure(t *testing.T) {
	s := openTestStore(t)
	id := submitWithRetries(t, s, 0)
	claim(t, s, "q", "l1", time.Minute, t0)
	nack(t, s, id, "l1", t0)
	if j, _ := s.Get(id); j.State != StateDead {
		t.Fatalf("state = %s, want dead", j.State)
	}
}

// A worker that crashes on the final attempt never nacks; the job must
// still reach the dead-letter queue once its lease expires, and the claim
// that discovers this must move on to the next job.
func TestExpiredFinalLeaseDeadLettersOnNextClaim(t *testing.T) {
	s := openTestStore(t)
	poison := submitWithRetries(t, s, 1)
	claim(t, s, "q", "l1", 10*time.Second, t0)                      // attempt 1, lease expires
	claim(t, s, "q", "l2", 10*time.Second, at(10*time.Second))      // attempt 2 (last), expires too
	healthy := mustSubmit(t, s, "q", "healthy", at(25*time.Second)) // queued behind the poison job

	j := claim(t, s, "q", "l3", time.Minute, at(30*time.Second))
	if j == nil || j.ID != healthy {
		t.Fatalf("claim got %+v, want the healthy job %d", j, healthy)
	}
	dead, _ := s.Get(poison)
	if dead.State != StateDead || dead.LastError == "" {
		t.Fatalf("poison job not dead-lettered: %+v", dead)
	}
}

func TestExpiredLeaseRecordsLastError(t *testing.T) {
	s := openTestStore(t)
	submitWithRetries(t, s, 3)
	claim(t, s, "q", "l1", time.Second, t0)
	j := claim(t, s, "q", "l2", time.Second, at(time.Second))
	if j.LastError != "visibility timeout expired" {
		t.Fatalf("LastError = %q", j.LastError)
	}
}

func TestListDeadAndRedrive(t *testing.T) {
	s := openTestStore(t)
	var ids []uint64
	for i := 0; i < 3; i++ {
		id := submitWithRetries(t, s, 0)
		ids = append(ids, id)
		lease := string(rune('a' + i))
		claim(t, s, "q", lease, time.Minute, at(time.Duration(i)*time.Second))
		nack(t, s, id, lease, at(time.Duration(i)*time.Second))
	}

	dead, err := s.ListDead("q", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 2 || dead[0].ID != ids[0] || dead[1].ID != ids[1] {
		t.Fatalf("ListDead = %+v, want the two oldest", dead)
	}
	if none, _ := s.ListDead("other", 10); len(none) != 0 {
		t.Fatalf("ListDead for an unknown queue = %+v", none)
	}

	if err := s.Redrive(RedriveCmd{JobID: ids[0], Now: at(time.Minute)}); err != nil {
		t.Fatalf("Redrive: %v", err)
	}
	j := claim(t, s, "q", "fresh", time.Minute, at(time.Minute))
	if j == nil || j.ID != ids[0] || j.Attempts != 1 || j.LastError != "boom" {
		t.Fatalf("redriven job: %+v", j)
	}
	if err := s.Redrive(RedriveCmd{JobID: ids[0], Now: at(time.Minute)}); !errors.Is(err, ErrNotDead) {
		t.Fatalf("redrive of a live job: got %v, want ErrNotDead", err)
	}
	if st, _ := s.Stats("q", at(time.Minute)); st.Dead != 2 || st.InFlight != 1 {
		t.Fatalf("stats after redrive = %+v", st)
	}
}

// The stale-worker scenario end to end: worker A's lease expires, worker B
// gets the job, and A's late ack and nack must both be rejected without
// disturbing B.
func TestStaleWorkerCannotAckOrNack(t *testing.T) {
	s := openTestStore(t)
	id := submitWithRetries(t, s, 3)
	claim(t, s, "q", "worker-a", 5*time.Second, t0)
	claim(t, s, "q", "worker-b", time.Minute, at(6*time.Second))

	if err := s.Ack(AckCmd{JobID: id, LeaseID: "worker-a", Now: at(7 * time.Second)}); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("stale ack: %v", err)
	}
	if err := s.Nack(NackCmd{JobID: id, LeaseID: "worker-a", Now: at(7 * time.Second)}); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("stale nack: %v", err)
	}
	if j, _ := s.Get(id); j.State != StateLeased || j.LeaseID != "worker-b" {
		t.Fatalf("stale calls disturbed the current lease: %+v", j)
	}
	if err := s.Ack(AckCmd{JobID: id, LeaseID: "worker-b", Now: at(8 * time.Second)}); err != nil {
		t.Fatalf("current ack: %v", err)
	}
}
