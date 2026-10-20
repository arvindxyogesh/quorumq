#!/usr/bin/env bash
# Kills the leader container at random intervals while load runs.
#
#   scripts/chaos.sh KILLS MIN_GAP MAX_GAP OUT.jsonl
#
# For each kill: wait a random MIN_GAP..MAX_GAP seconds, find the leader,
# `docker kill` it (SIGKILL), and measure the failover time with
# `quorumq-load probe`: from the kill until a write is acknowledged again.
# Then restart the killed container and wait for it to catch up, so a
# quorum survives the next kill. One JSON line per kill goes to OUT.jsonl.
#
# Expects a cluster started with scripts/cluster.sh and bin/quorumq-load.
set -euo pipefail

cd "$(dirname "$0")/.."
KILLS=${1:?kills}; MIN_GAP=${2:?min gap seconds}; MAX_GAP=${3:?max gap seconds}; OUT=${4:?output file}
ADDRS=$(scripts/cluster.sh addrs)
: > "$OUT"

node_status() { curl -sf --max-time 1 "localhost:808${1#n}/status" 2>/dev/null || echo '{}'; }

for k in $(seq 1 "$KILLS"); do
  gap=$(awk -v lo="$MIN_GAP" -v hi="$MAX_GAP" -v s="$RANDOM" 'BEGIN { srand(s); printf "%.1f", lo + rand() * (hi - lo) }')
  sleep "$gap"

  leader=$(scripts/cluster.sh leader) || { echo "kill $k: no leader to kill" >&2; exit 1; }
  term_before=$(node_status "$leader" | jq -r .term)
  t0=$(date +%s%N)
  docker kill "quorumq-bench-$leader-1" >/dev/null
  probe=$(bin/quorumq-load probe -addr "$ADDRS" -since "$t0" -backoff 10ms)
  new_leader=$(scripts/cluster.sh leader || echo "?")
  echo "kill $k: killed leader $leader (term $term_before) after ${gap}s; new leader $new_leader; failover $(jq -r .failover_ms <<<"$probe") ms" >&2
  jq -c --arg killed "$leader" --arg next "$new_leader" --argjson k "$k" --argjson t0 "$t0" --argjson gap "$gap" \
    '. + {kill: $k, killed: $killed, new_leader: $next, killed_at_ns: $t0, gap_s: $gap}' <<<"$probe" >> "$OUT"

  # Bring the old leader back as a follower and let it catch up.
  sleep 2
  docker start "quorumq-bench-$leader-1" >/dev/null
  for _ in $(seq 1 300); do
    lead=$(scripts/cluster.sh leader 2>/dev/null) || { sleep 0.2; continue; }
    commit=$(node_status "$lead" | jq -r '.commit_index // 0')
    applied=$(node_status "$leader" | jq -r '.fsm_applied_index // 0')
    if [[ "$(node_status "$leader" | jq -r .state)" == "Follower" ]] && (( commit - applied < 500 )); then break; fi
    sleep 0.2
  done
  echo "kill $k: $leader rejoined as follower" >&2
done
