#!/usr/bin/env bash
# Network-partition experiment on a real 5-node Docker cluster under load.
#
#   scripts/partition.sh OUT_DIR [PARTITION_SECONDS]
#
# 1. Starts 5 nodes and a fixed-rate load (producers and workers using all
#    five addresses).
# 2. Partitions the network 2|3 with the current leader on the 2-node side,
#    by dropping packets between the two sides with iptables inside each
#    container's network namespace (clients on the host still reach every
#    node, so we can watch both sides).
# 3. While partitioned, sends writes and reads to each minority node with
#    `quorumq -no-retry` (no redirects), and records how many succeed (the
#    claim is: none). Records when the majority elects a leader and when it
#    first acknowledges a write.
# 4. Heals, times how long the minority takes to catch up, lets the load
#    finish, checks all five replicas agree and that no write sent to the
#    minority survived, and reconciles every acknowledged submit against
#    completed jobs.
#
# Needs root (nsenter + iptables), Docker, the quorumq:dev image, jq, curl.
set -euo pipefail

cd "$(dirname "$0")/.."
OUT=${1:?usage: partition.sh OUT_DIR [PARTITION_SECONDS]}
HOLD=${2:-20}
RATE=${RATE:-200}
mkdir -p "$OUT"
go build -o bin/ ./cmd/...
trap 'scripts/cluster.sh down' EXIT

container() { echo "quorumq-bench-$1-1"; }
pid() { docker inspect -f '{{.State.Pid}}' "$(container "$1")"; }
ip() { docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$(container "$1")"; }
status() { curl -sf --max-time 1 "localhost:808${1#n}/status" 2>/dev/null || echo '{}'; }
now_ns() { date +%s%N; }

# drop_between A B: no packets between nodes A and B, in either direction.
drop_between() {
  local pa pb ia ib
  pa=$(pid "$1"); pb=$(pid "$2"); ia=$(ip "$1"); ib=$(ip "$2")
  nsenter -t "$pa" -n iptables -A INPUT -s "$ib" -j DROP
  nsenter -t "$pa" -n iptables -A OUTPUT -d "$ib" -j DROP
  nsenter -t "$pb" -n iptables -A INPUT -s "$ia" -j DROP
  nsenter -t "$pb" -n iptables -A OUTPUT -d "$ia" -j DROP
}

scripts/cluster.sh up 5
ALL=$(scripts/cluster.sh addrs)

bin/quorumq-load run -addr "$ALL" -queue partition -producers 16 -workers 32 -rate "$RATE" \
  -duration "$((HOLD + 40))s" -visibility 5s -drain-timeout 3m \
  -label "partition 2|3, ${HOLD}s, rate=$RATE" -out "$OUT" > "$OUT/load.txt" 2>&1 &
LOAD_PID=$!
sleep 15

OLD=$(scripts/cluster.sh leader)
OLD_TERM=$(status "$OLD" | jq -r .term)
OTHERS=()
for i in 1 2 3 4 5; do [[ "n$i" != "$OLD" ]] && OTHERS+=("n$i"); done
MINORITY=("$OLD" "${OTHERS[0]}")
MAJORITY=("${OTHERS[@]:1}")
MAJ_ADDRS=$(for n in "${MAJORITY[@]}"; do printf 'localhost:707%s,' "${n#n}"; done | sed 's/,$//')
echo "partitioning: minority ${MINORITY[*]} (old leader $OLD, term $OLD_TERM) | majority ${MAJORITY[*]}"

T0=$(now_ns)
for a in "${MINORITY[@]}"; do for b in "${MAJORITY[@]}"; do drop_between "$a" "$b"; done; done

# Time until the majority acknowledges a write (election + first commit).
bin/quorumq-load probe -addr "$MAJ_ADDRS" -since "$T0" -backoff 10ms > "$OUT/majority-probe.json"
NEW=""
for n in "${MAJORITY[@]}"; do [[ "$(status "$n" | jq -r .state)" == "Leader" ]] && NEW=$n; done
NEW_TERM=$(status "$NEW" | jq -r .term)

# Hammer the minority directly until the partition ends.
attempts=0; acked=0; read_attempts=0; reads_served=0
end=$(( $(date +%s) + HOLD ))
while (( $(date +%s) < end )); do
  for n in "${MINORITY[@]}"; do
    attempts=$((attempts + 1))
    if bin/quorumq -addr "localhost:707${n#n}" -no-retry -timeout 300ms submit -queue partition-minority -payload minority >/dev/null 2>&1; then
      acked=$((acked + 1))
    fi
    read_attempts=$((read_attempts + 1))
    if bin/quorumq -addr "localhost:707${n#n}" -no-retry -timeout 300ms stats -queue partition >/dev/null 2>&1; then
      reads_served=$((reads_served + 1))
    fi
  done
done
OLD_STATE=$(status "$OLD" | jq -r .state)
MINORITY_COMMIT=$(status "$OLD" | jq -r .commit_index)

# Heal, then time how long the minority nodes take to apply everything the
# majority had committed at the moment of healing.
TARGET=$(status "$NEW" | jq -r .commit_index)
T_HEAL=$(now_ns)
for n in "${MINORITY[@]}" "${MAJORITY[@]}"; do
  nsenter -t "$(pid "$n")" -n iptables -F INPUT
  nsenter -t "$(pid "$n")" -n iptables -F OUTPUT
done
CAUGHT_UP_MS=null
for _ in $(seq 1 1200); do
  behind=0
  for n in "${MINORITY[@]}"; do
    (( $(status "$n" | jq -r '.fsm_applied_index // 0') >= TARGET )) || behind=1
  done
  if (( behind == 0 )); then CAUGHT_UP_MS=$(( ($(now_ns) - T_HEAL) / 1000000 )); break; fi
  sleep 0.05
done

wait "$LOAD_PID"
cat "$OUT/load.txt"

# Every replica must agree, and no write sent to the minority may survive:
# its queue must be empty everywhere.
for _ in $(seq 1 600); do
  idx=$(for i in 1 2 3 4 5; do status "$i" | jq -r '.fsm_applied_index // "x"'; done | sort -u)
  [[ $(wc -l <<<"$idx") -eq 1 && "$idx" != "x" ]] && break
  sleep 0.1
done
CONVERGED_INDEX=$(status 1 | jq -r .fsm_applied_index)
SURVIVORS=$(bin/quorumq -addr "$ALL" stats -queue partition-minority |
  jq '[.visible, .in_flight, .delayed, .acked, .dead] | map(tonumber) | add')

jq -n \
  --arg old "$OLD" --argjson old_term "$OLD_TERM" --arg new "$NEW" --argjson new_term "$NEW_TERM" \
  --arg minority "${MINORITY[*]}" --arg majority "${MAJORITY[*]}" --argjson hold "$HOLD" \
  --argjson attempts "$attempts" --argjson acked "$acked" \
  --argjson read_attempts "$read_attempts" --argjson reads_served "$reads_served" \
  --arg old_state "$OLD_STATE" --argjson minority_commit "$MINORITY_COMMIT" \
  --argjson caught_up_ms "$CAUGHT_UP_MS" --argjson converged_index "$CONVERGED_INDEX" \
  --argjson survivors "$SURVIVORS" \
  --slurpfile probe "$OUT/majority-probe.json" --slurpfile load "$OUT/result.json" \
  '{old_leader: $old, old_term: $old_term, minority: $minority, majority: $majority,
    partition_seconds: $hold,
    majority_new_leader: $new, majority_new_term: $new_term,
    majority_first_ack_ms: $probe[0].failover_ms,
    minority_write_attempts: $attempts, minority_writes_acknowledged: $acked,
    minority_read_attempts: $read_attempts, minority_reads_served: $reads_served,
    old_leader_state_during_partition: $old_state,
    minority_writes_surviving_after_heal: $survivors,
    minority_caught_up_ms_after_heal: $caught_up_ms,
    all_replicas_converged_at_index: $converged_index,
    load: {acknowledged: $load[0].submits_acknowledged, completed: $load[0].jobs_completed,
           lost: $load[0].jobs_lost, duplicates: $load[0].duplicate_completions}}' \
  > "$OUT/partition.json"
cat "$OUT/partition.json"
