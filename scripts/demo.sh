#!/usr/bin/env bash
# Demo of a single quorumq node: submit, claim, ack, nack with retry backoff,
# a visibility timeout expiring, and the dead-letter queue. Requires Go and jq.
set -euo pipefail

cd "$(dirname "$0")/.."
ADDR="${QUORUMQ_ADDR:-127.0.0.1:7070}"
export QUORUMQ_ADDR="$ADDR"
DATA_DIR="$(mktemp -d)"
LOG="$DATA_DIR/server.log"

echo "==> building"
go build -o bin/quorumq-server ./cmd/quorumq-server
go build -o bin/quorumq ./cmd/quorumq
q() { ./bin/quorumq "$@"; }

echo "==> starting server on $ADDR (data in $DATA_DIR)"
./bin/quorumq-server -addr "$ADDR" -data-dir "$DATA_DIR" -retry-backoff 1s 2>"$LOG" &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null || true; wait $SERVER_PID 2>/dev/null || true; rm -rf "$DATA_DIR"' EXIT

for _ in $(seq 1 100); do
  q stats -queue demo >/dev/null 2>&1 && break
  sleep 0.1
done

echo
echo "==> submitting 3 jobs to queue 'demo'"
for n in 1 2 3; do
  q submit -queue demo -payload "{\"task\":\"resize-image\",\"n\":$n}" | jq -c .
done
q stats -queue demo | jq -c .

echo
echo "==> claim + ack (happy path)"
CLAIM=$(q claim -queue demo -worker worker-a -visibility 30s)
JOB=$(jq -r .job.id <<<"$CLAIM")
LEASE=$(jq -r .lease_id <<<"$CLAIM")
echo "claimed job $JOB payload=$(jq -r .job.payload <<<"$CLAIM" | base64 -d) lease=$LEASE"
q ack -job "$JOB" -lease "$LEASE" | jq -c .

echo
echo "==> claim + nack: the job is retried after a backoff (1s, then 2s, 4s, ...)"
CLAIM=$(q claim -queue demo -worker worker-a)
JOB=$(jq -r .job.id <<<"$CLAIM")
q nack -job "$JOB" -lease "$(jq -r .lease_id <<<"$CLAIM")" -reason "thumbnail service timed out" | jq -c .
q get -job "$JOB" | jq -c '{id, state, attempts, last_error}'
q stats -queue demo | jq -c '{visible, delayed}'
echo "while job $JOB waits out its backoff, worker-b gets the next job:"
CLAIM=$(q claim -queue demo -worker worker-b)
echo "worker-b claimed job $(jq -r .job.id <<<"$CLAIM")"
q ack -job "$(jq -r .job.id <<<"$CLAIM")" -lease "$(jq -r .lease_id <<<"$CLAIM")" | jq -c .
sleep 1.1

echo
echo "==> visibility timeout: worker-a claims with a 2s lease and never acks"
CLAIM=$(q claim -queue demo -worker worker-a -visibility 2s)
JOB=$(jq -r .job.id <<<"$CLAIM")
STALE_LEASE=$(jq -r .lease_id <<<"$CLAIM")
echo "worker-a holds job $JOB"
echo "claim before expiry returns: $(q claim -queue demo -worker worker-b | jq -c .)"
sleep 2.5
CLAIM=$(q claim -queue demo -worker worker-b)
echo "after 2.5s worker-b got job $(jq -r .job.id <<<"$CLAIM"), attempts=$(jq -r .job.attempts <<<"$CLAIM")"

echo "worker-a wakes up and tries to ack with its stale lease:"
if q ack -job "$JOB" -lease "$STALE_LEASE" 2>&1; then
  echo "ERROR: stale ack was accepted" >&2
  exit 1
fi
q ack -job "$JOB" -lease "$(jq -r .lease_id <<<"$CLAIM")" | jq -c .

echo
echo "==> dead-letter queue: a job with no retries left fails once more"
POISON=$(q submit -queue demo -payload '{"task":"resize-image","n":"not-a-number"}' -max-retries 0 | jq -r .job_id)
CLAIM=$(q claim -queue demo -worker worker-a)
q nack -job "$POISON" -lease "$(jq -r .lease_id <<<"$CLAIM")" -reason "invalid payload" | jq -c .
q dead -queue demo | jq -c '.jobs[] | {id, state, attempts, last_error}'
q stats -queue demo | jq -c '{visible, dead}'
echo "after fixing the bug, redrive it:"
q redrive -job "$POISON" | jq -c .
CLAIM=$(q claim -queue demo -worker worker-a)
q ack -job "$(jq -r .job.id <<<"$CLAIM")" -lease "$(jq -r .lease_id <<<"$CLAIM")" | jq -c .

echo
echo "==> final state"
q stats -queue demo | jq -c .
q get -job "$JOB" | jq -c '{id, state, attempts, worker_id, last_error}'
