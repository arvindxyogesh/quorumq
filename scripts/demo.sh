#!/usr/bin/env bash
# Demo of a single quorumq node: submit, claim, ack, nack, and a visibility
# timeout expiring. Requires Go and jq.
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
./bin/quorumq-server -addr "$ADDR" -data-dir "$DATA_DIR" 2>"$LOG" &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null || true; wait $SERVER_PID 2>/dev/null || true; rm -rf "$DATA_DIR"' EXIT

for _ in $(seq 1 50); do
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
echo "==> claim + nack: the job goes back to the queue"
CLAIM=$(q claim -queue demo -worker worker-a)
JOB=$(jq -r .job.id <<<"$CLAIM")
q nack -job "$JOB" -lease "$(jq -r .lease_id <<<"$CLAIM")" | jq -c .
q get -job "$JOB" | jq -c '{id, state, attempts}'
echo "jobs are claimed in the order they became visible, so job 3 (visible since"
echo "it was submitted) comes before the job that was just nacked:"
CLAIM=$(q claim -queue demo -worker worker-b)
echo "worker-b claimed job $(jq -r .job.id <<<"$CLAIM")"
q ack -job "$(jq -r .job.id <<<"$CLAIM")" -lease "$(jq -r .lease_id <<<"$CLAIM")" | jq -c .

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
echo "==> final state"
q stats -queue demo | jq -c .
q get -job "$JOB" | jq -c '{id, state, attempts, worker_id}'
