#!/usr/bin/env bash
# Leader failover demo on the 3-node Docker Compose cluster: submit jobs,
# `docker kill` the leader, show a new leader take over with every job
# intact, then bring the old leader back and show it catch up.
# Requires Docker (with Compose), Go, curl and jq.
#
#   ./scripts/compose-failover.sh            # build the image and run
#   NO_BUILD=1 ./scripts/compose-failover.sh # reuse an existing quorumq:dev image
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE=(docker compose -f deploy/docker-compose.yml)
ADDRS="localhost:7071,localhost:7072,localhost:7073"
JOBS="${JOBS:-50}"

go build -o bin/quorumq ./cmd/quorumq
q() { ./bin/quorumq -addr "$ADDRS" "$@"; }

status() { curl -sf "localhost:808$1/status" 2>/dev/null || echo '{}'; }
leader() {
  for i in 1 2 3; do
    if [[ "$(status "$i" | jq -r .state)" == "Leader" ]]; then echo "n$i"; return; fi
  done
}
wait_leader() {
  for _ in $(seq 1 100); do
    L=$(leader); [[ -n "$L" ]] && { echo "$L"; return; }
    sleep 0.1
  done
  echo "no leader elected" >&2; exit 1
}

echo "==> starting cluster"
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
if [[ -n "${NO_BUILD:-}" ]]; then "${COMPOSE[@]}" up -d --no-build; else "${COMPOSE[@]}" up -d --build; fi
trap '"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true' EXIT

LEADER=$(wait_leader)
echo "leader is $LEADER"
for i in 1 2 3; do status "$i" | jq -c '{node_id, state, term, leader_id}'; done

echo
echo "==> submitting $JOBS jobs through the cluster"
for n in $(seq 1 "$JOBS"); do q submit -queue demo -payload "job-$n" >/dev/null; done
q stats -queue demo | jq -c .

echo
echo "==> docker kill $LEADER (SIGKILL, no graceful shutdown)"
docker kill "quorumq-$LEADER-1" >/dev/null
START=$(date +%s%N)
NEW=$(wait_leader)
echo "new leader: $NEW"
echo "first successful write after the kill:"
q submit -queue demo -payload "after-failover" | jq -c .
echo "  ($(( ($(date +%s%N) - START) / 1000000 )) ms after the kill, measured by this script, single run)"
q stats -queue demo | jq -c .

echo
echo "==> draining every job through the new leader"
DONE=0
while :; do
  CLAIM=$(q claim -queue demo -worker drain)
  [[ "$(jq -r .job <<<"$CLAIM")" == "null" ]] && break
  q ack -job "$(jq -r .job.id <<<"$CLAIM")" -lease "$(jq -r .lease_id <<<"$CLAIM")" >/dev/null
  DONE=$((DONE + 1))
done
echo "processed $DONE jobs (submitted $((JOBS + 1)))"
[[ "$DONE" -eq $((JOBS + 1)) ]] || { echo "ERROR: jobs were lost" >&2; exit 1; }

echo
echo "==> restarting $LEADER; it rejoins as a follower and catches up"
"${COMPOSE[@]}" start "$LEADER" >/dev/null
for _ in $(seq 1 100); do
  IDX=$(for i in 1 2 3; do status "$i" | jq -r .fsm_applied_index; done | sort -u)
  [[ $(wc -l <<<"$IDX") -eq 1 && "$IDX" != "null" ]] && break
  sleep 0.1
done
for i in 1 2 3; do status "$i" | jq -c '{node_id, state, term, fsm_applied_index}'; done
