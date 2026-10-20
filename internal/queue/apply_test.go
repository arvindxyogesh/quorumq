package queue

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func encode(t *testing.T, c Command) []byte {
	t.Helper()
	b, err := c.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func submitEntry(t *testing.T, index uint64, payload string, now int64) Entry {
	return Entry{Index: index, Data: encode(t, Command{Op: OpSubmit, Submit: &SubmitCmd{Queue: "q", Payload: []byte(payload), Now: now}})}
}

func TestCommandRoundTrip(t *testing.T) {
	in := Command{Op: OpClaim, Claim: &ClaimCmd{Queue: "q", WorkerID: "w", LeaseID: "l", VisibilityTimeout: 5 * time.Second, Now: t0}}
	out, err := DecodeCommand(encode(t, in))
	if err != nil {
		t.Fatal(err)
	}
	if out.Op != in.Op || *out.Claim != *in.Claim {
		t.Fatalf("round trip: got %+v, want %+v", out.Claim, in.Claim)
	}
}

func TestApplyBatchAppliesInOrderAndRecordsIndex(t *testing.T) {
	s := openTestStore(t)
	results, err := s.ApplyBatch([]Entry{
		submitEntry(t, 1, "a", t0),
		{Index: 2}, // a configuration change: advances the index only
		submitEntry(t, 3, "b", t0),
	})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].JobID != 1 || results[2].JobID != 2 {
		t.Fatalf("unexpected results: %+v", results)
	}
	if idx, _ := s.AppliedIndex(); idx != 3 {
		t.Fatalf("AppliedIndex = %d, want 3", idx)
	}
}

// After a restart Raft replays log entries the store has already applied;
// they must not be applied twice.
func TestApplyBatchSkipsAlreadyAppliedEntries(t *testing.T) {
	s := openTestStore(t)
	entries := []Entry{submitEntry(t, 1, "a", t0), submitEntry(t, 2, "b", t0)}
	if _, err := s.ApplyBatch(entries); err != nil {
		t.Fatal(err)
	}
	replay := []Entry{entries[0], entries[1], submitEntry(t, 3, "c", t0)}
	results, err := s.ApplyBatch(replay)
	if err != nil {
		t.Fatal(err)
	}
	if results[0].JobID != 0 || results[1].JobID != 0 || results[2].JobID != 3 {
		t.Fatalf("replayed entries were re-applied: %+v", results)
	}
	if st, _ := s.Stats("q", t0); st.Visible != 3 {
		t.Fatalf("Visible = %d after replay, want 3", st.Visible)
	}
}

func TestApplyBatchReportsDomainErrorsPerEntry(t *testing.T) {
	s := openTestStore(t)
	results, err := s.ApplyBatch([]Entry{
		{Index: 1, Data: []byte("not json")},
		{Index: 2, Data: encode(t, Command{Op: OpAck, Ack: &AckCmd{JobID: 99, LeaseID: "x", Now: t0}})},
		submitEntry(t, 3, "still applied", t0),
	})
	if err != nil {
		t.Fatalf("domain errors must not fail the batch: %v", err)
	}
	if !errors.Is(results[0].Err, ErrBadCommand) || !errors.Is(results[1].Err, ErrNotFound) {
		t.Fatalf("unexpected errors: %v, %v", results[0].Err, results[1].Err)
	}
	if results[2].JobID != 1 {
		t.Fatalf("entry after a failed command was not applied: %+v", results[2])
	}
}

// A new leader whose clock is behind must not shorten or lengthen leases
// that were already granted: state-machine time never goes backwards.
func TestClockNeverGoesBackwards(t *testing.T) {
	s := openTestStore(t)
	id := mustSubmit(t, s, "q", "x", at(10*time.Second))
	claim(t, s, "q", "lease-1", 5*time.Second, at(10*time.Second)) // expires at +15s

	// A command stamped with an older time is applied at +10s, not +1s.
	if j := claim(t, s, "q", "lease-2", time.Second, at(time.Second)); j != nil {
		t.Fatalf("job %d redelivered by a command with a stale clock", j.ID)
	}
	if err := s.Nack(NackCmd{JobID: id, LeaseID: "lease-1", Now: at(2 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	if j, _ := s.Get(id); j.VisibleAt != at(10*time.Second) {
		t.Fatalf("nack used a time from the past: VisibleAt=%d", j.VisibleAt)
	}
}

func TestSnapshotRestore(t *testing.T) {
	src := openTestStore(t)
	if _, err := src.ApplyBatch([]Entry{submitEntry(t, 1, "a", t0), submitEntry(t, 2, "b", t0)}); err != nil {
		t.Fatal(err)
	}
	claim(t, src, "q", "lease-1", time.Minute, t0)

	sn, err := src.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	// Writes after the snapshot started must not leak into it.
	mustSubmit(t, src, "q", "after", t0)
	var buf bytes.Buffer
	if _, err := sn.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	sn.Release()

	dst := openTestStore(t)
	mustSubmit(t, dst, "other", "overwritten", t0)
	if err := dst.Restore(&buf); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if idx, _ := dst.AppliedIndex(); idx != 2 {
		t.Fatalf("AppliedIndex after restore = %d, want 2", idx)
	}
	if st, _ := dst.Stats("q", t0); st.Visible != 1 || st.InFlight != 1 {
		t.Fatalf("restored stats = %+v", st)
	}
	if st, _ := dst.Stats("other", t0); st != (Stats{}) {
		t.Fatalf("pre-restore state survived: %+v", st)
	}
	if err := dst.Ack(AckCmd{JobID: 1, LeaseID: "lease-1", Now: t0}); err != nil {
		t.Fatalf("lease lost in restore: %v", err)
	}
	if id := mustSubmit(t, dst, "q", "next", t0); id != 3 {
		t.Fatalf("ID sequence after restore = %d, want 3", id)
	}
}
