#!/usr/bin/env bash
# Runs the benchmark matrix and writes everything under bench/results/<run>/:
#
#   1. Throughput at 1, 3 and 5 nodes: closed-loop producers and workers.
#   2. Latency at a fixed submit rate below capacity, 3 nodes.
#   3. Chaos: fixed-rate load on 3 nodes while scripts/chaos.sh kills the
#      leader container KILLS times; every acknowledged submit is reconciled
#      against completed jobs, and each failover is timed.
#
# Then scripts/bench-report.sh renders summary.md from the raw results.
# Every number in summary.md comes from files this script wrote.
#
# Tunables (environment): REPS DURATION PRODUCERS WORKERS PAYLOAD NODES
# LAT_RATE LAT_DURATION CHAOS_RATE CHAOS_DURATION KILLS GAP_MIN GAP_MAX
# NO_BUILD=1 reuses an existing quorumq:dev image; IMAGE_NOTE describes it.
set -euo pipefail

cd "$(dirname "$0")/.."
RUN=${RUN:-$(date -u +%Y%m%d-%H%M%S)}
OUT=bench/results/$RUN
REPS=${REPS:-3}
DURATION=${DURATION:-30s}
PRODUCERS=${PRODUCERS:-32}
WORKERS=${WORKERS:-64}
PAYLOAD=${PAYLOAD:-256}
NODES=${NODES:-"1 3 5"}
LAT_RATE=${LAT_RATE:-500}
LAT_DURATION=${LAT_DURATION:-60s}
CHAOS_RATE=${CHAOS_RATE:-300}
CHAOS_DURATION=${CHAOS_DURATION:-200s}
KILLS=${KILLS:-6}
GAP_MIN=${GAP_MIN:-15}
GAP_MAX=${GAP_MAX:-25}
IMAGE_NOTE=${IMAGE_NOTE:-"built from ./Dockerfile"}

mkdir -p "$OUT"
go build -o bin/ ./cmd/...
if [[ -z "${NO_BUILD:-}" ]]; then docker build -q -t quorumq:dev . >/dev/null; fi
trap 'scripts/cluster.sh down' EXIT

# Record the environment next to the results.
jq -n \
  --arg run "$RUN" \
  --arg commit "$(git rev-parse --short HEAD)$(git diff --quiet || echo -dirty)" \
  --arg cpu "$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ //')" \
  --arg cpus "$(nproc)" \
  --arg mem "$(free -g | awk '/^Mem:/ {print $2 " GiB"}')" \
  --arg kernel "$(uname -r)" \
  --arg docker "$(docker version --format '{{.Server.Version}}')" \
  --arg go "$(go version | awk '{print $3}')" \
  --arg image "$IMAGE_NOTE" \
  --arg reps "$REPS" --arg duration "$DURATION" --arg producers "$PRODUCERS" --arg workers "$WORKERS" \
  --arg payload "$PAYLOAD" --arg lat_rate "$LAT_RATE" --arg lat_duration "$LAT_DURATION" \
  --arg chaos_rate "$CHAOS_RATE" --arg chaos_duration "$CHAOS_DURATION" --arg kills "$KILLS" \
  --arg gaps "${GAP_MIN}-${GAP_MAX}s" \
  '{run: $run, commit: $commit, cpu: $cpu, cpus: $cpus, memory: $mem, kernel: $kernel,
    docker: $docker, go: $go, image: $image,
    setup: "all nodes and the load generator on this one machine; nodes in Docker containers on a bridge network with host port mapping; data on Docker named volumes",
    params: {reps: $reps, duration: $duration, producers: $producers, workers: $workers, payload_bytes: $payload,
             latency_rate: $lat_rate, latency_duration: $lat_duration,
             chaos_rate: $chaos_rate, chaos_duration: $chaos_duration, kills: $kills, kill_gap: $gaps}}' \
  > "$OUT/env.json"

load() { bin/quorumq-load run -addr "$(scripts/cluster.sh addrs)" -payload-size "$PAYLOAD" "$@"; }

echo "==> throughput (closed loop, $PRODUCERS producers / $WORKERS workers, $DURATION x $REPS)"
for n in $NODES; do
  scripts/cluster.sh up "$n"
  for r in $(seq 1 "$REPS"); do
    echo -n "  $n nodes, run $r: "
    load -queue "tput-$r" -producers "$PRODUCERS" -workers "$WORKERS" -duration "$DURATION" \
      -label "throughput nodes=$n run=$r" -out "$OUT/throughput/n$n-r$r"
  done
  scripts/cluster.sh down
done

echo "==> latency (3 nodes, $LAT_RATE submits/s for $LAT_DURATION)"
scripts/cluster.sh up 3
load -queue latency -producers "$PRODUCERS" -workers "$WORKERS" -rate "$LAT_RATE" -duration "$LAT_DURATION" \
  -label "latency nodes=3 rate=$LAT_RATE" -out "$OUT/latency"
scripts/cluster.sh down

echo "==> chaos (3 nodes, $CHAOS_RATE submits/s for $CHAOS_DURATION, $KILLS leader kills)"
scripts/cluster.sh up 3
load -queue chaos -producers "$PRODUCERS" -workers "$WORKERS" -rate "$CHAOS_RATE" -duration "$CHAOS_DURATION" \
  -visibility 5s -drain-timeout 3m -label "chaos nodes=3 rate=$CHAOS_RATE kills=$KILLS" -out "$OUT/chaos" \
  > "$OUT/chaos-load.txt" 2>&1 &
LOAD_PID=$!
mkdir -p "$OUT/chaos"
sleep 5
scripts/chaos.sh "$KILLS" "$GAP_MIN" "$GAP_MAX" "$OUT/chaos/kills.jsonl"
wait "$LOAD_PID"
cat "$OUT/chaos-load.txt"
scripts/cluster.sh down

scripts/bench-report.sh "$OUT" > "$OUT/summary.md"
echo "==> wrote $OUT/summary.md"
cat "$OUT/summary.md"
