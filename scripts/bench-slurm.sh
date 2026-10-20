#!/usr/bin/env bash
#SBATCH --job-name=quorumq-bench
#SBATCH --partition=standard
#SBATCH --nodes=6
#SBATCH --ntasks-per-node=1
#SBATCH --output=bench-slurm-%j.log
#
# Runs the scripts/bench.sh experiments on a Slurm cluster with bare
# quorumq-server processes, one broker per node, and writes the same layout
# under bench/results/<run>/ so scripts/reconcile.sh and scripts/bench-report.sh
# work on it:
#
#   throughput/nN-rR/       closed loop at 1, 3 and 5 brokers, REPS runs each
#   saturation/nN-pP-wW/    more producers/workers on 3 and 5 brokers, 1 run each
#   latency/rRATE/          fixed submit rates on 3 brokers (rates above the
#                           measured 3-broker capacity are skipped; see skipped.txt)
#   chaos/                  fixed rate on 3 brokers while the leader is SIGKILLed
#
# The last node of the allocation runs quorumq-load; the others run one broker
# each. Broker data directories go on node-local disk and are wiped between
# experiments. Build the binaries first and submit from the repository root,
# with the Slurm account and time limit on the command line:
#
#   CGO_ENABLED=0 go build -o bin/ ./cmd/...
#   ALLOC=a sbatch -A ACCOUNT -t 01:00:00 --exclusive --mem=0 scripts/bench-slurm.sh
#   ALLOC=b sbatch -A ACCOUNT -t 01:00:00 --cpus-per-task=8 --mem=24G scripts/bench-slurm.sh
#
# Option a gives each broker a whole node; option b shares nodes with other jobs
# and gives each step --cpus-per-task CPUs (the committed results used 6).
#
# Tunables (environment), defaults as scripts/bench.sh where it has them:
# RUN REPS DURATION PRODUCERS WORKERS PAYLOAD NODES SAT_NODES SAT_CONFIGS
# LAT_RATES LAT_DURATION CHAOS_RATE CHAOS_DURATION KILLS GAP_MIN GAP_MAX
# DATA_ROOT RAFT_PORT GRPC_PORT HTTP_PORT ALLOC (a or b, recorded in env.json).
set -euo pipefail

cd "${SLURM_SUBMIT_DIR:-$(dirname "$0")/..}"
[[ -n "${SLURM_JOB_ID:-}" ]] || { echo "run this inside a Slurm allocation (sbatch)" >&2; exit 1; }
[[ -x bin/quorumq-server && -x bin/quorumq-load ]] || { echo "build bin/ first: CGO_ENABLED=0 go build -o bin/ ./cmd/..." >&2; exit 1; }

RUN=${RUN:-greatlakes-$(date -u +%Y%m%d)}
OUT=$PWD/bench/results/$RUN
REPS=${REPS:-3}
DURATION=${DURATION:-30s}
PRODUCERS=${PRODUCERS:-32}
WORKERS=${WORKERS:-64}
PAYLOAD=${PAYLOAD:-256}
NODES=${NODES:-"1 3 5"}
SAT_NODES=${SAT_NODES:-"3 5"}
SAT_CONFIGS=${SAT_CONFIGS:-"64/128 128/256"}
LAT_RATES=${LAT_RATES:-"500 1000 2000"}
LAT_DURATION=${LAT_DURATION:-60s}
CHAOS_RATE=${CHAOS_RATE:-300}
CHAOS_DURATION=${CHAOS_DURATION:-320s}
KILLS=${KILLS:-10}
GAP_MIN=${GAP_MIN:-15}
GAP_MAX=${GAP_MAX:-25}
RAFT_PORT=${RAFT_PORT:-7000}
GRPC_PORT=${GRPC_PORT:-7070}
HTTP_PORT=${HTTP_PORT:-8080}
ALLOC=${ALLOC:-unspecified}
DATA_ROOT=${DATA_ROOT:-/tmp/quorumq-$SLURM_JOB_ID}
BIN=$PWD/bin
# Broker logs can name node addresses, so they stay out of the results directory.
LOGS=$PWD/.bench/slurm-$SLURM_JOB_ID

[[ ! -e "$OUT" ]] || { echo "$OUT already exists; set RUN to a new name" >&2; exit 1; }
mkdir -p "$OUT" "$LOGS"

mapfile -t HOSTS < <(scontrol show hostnames "$SLURM_JOB_NODELIST")
LOAD_HOST=${HOSTS[-1]}
BROKERS=("${HOSTS[@]:0:${#HOSTS[@]}-1}")
for n in $NODES $SAT_NODES 3; do
  (( n <= ${#BROKERS[@]} )) || { echo "need $n broker nodes plus 1 load node; got ${#HOSTS[@]} nodes" >&2; exit 1; }
done
echo "brokers: ${BROKERS[*]}; load generator: $LOAD_HOST"

# Steps get every CPU the job holds on a node: all of them with --exclusive,
# --cpus-per-task otherwise. srun does not inherit --cpus-per-task from sbatch.
STEP_CPUS=${SLURM_CPUS_PER_TASK:-$SLURM_CPUS_ON_NODE}

# on HOST CMD...: runs CMD as a one-task step on HOST, sharing the job's
# resources. --cpu-bind=none keeps a CPU mask inherited from an enclosing step
# (which holds different CPUs on another node) from being applied; the step is
# still confined to the job's CPUs on HOST. Steps run without the site's HTTP
# proxy settings: gRPC honours them and would send broker traffic to the proxy.
on() {
  local host=$1; shift
  srun --overlap --nodes=1 --ntasks=1 --cpus-per-task="$STEP_CPUS" --cpu-bind=none -w "$host" \
    env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY -u all_proxy -u ALL_PROXY \
    NO_PROXY='*' no_proxy='*' "$@" </dev/null
}
# stop SIGNAL HOST: signals this user's quorumq-server on HOST, if any.
stop() { on "$2" bash -c 'pkill -"$1" -u "$USER" -x quorumq-server; true' _ "$1"; }

join() { local IFS=,; echo "$*"; }
peers() { local i out=(); for ((i = 1; i <= $1; i++)); do out+=("n$i=${BROKERS[i-1]}:$RAFT_PORT/${BROKERS[i-1]}:$GRPC_PORT"); done; join "${out[@]}"; }
addrs() { local i out=(); for ((i = 1; i <= $1; i++)); do out+=("${BROKERS[i-1]}:$GRPC_PORT"); done; join "${out[@]}"; }
status() { curl -sf --noproxy '*' --max-time 1 "http://${BROKERS[$1-1]}:$HTTP_PORT/status" 2>/dev/null || echo '{}'; }

leader() {
  local i
  for ((i = 1; i <= $1; i++)); do
    if [[ "$(status "$i" | jq -r .state)" == "Leader" ]]; then echo "n$i"; return 0; fi
  done
  return 1
}

declare -A PIDS=()
SIZE=0

start_broker() {
  local i=$1 n=$2 host=${BROKERS[$1-1]}
  on "$host" "$BIN/quorumq-server" -node-id "n$i" \
    -raft-addr "0.0.0.0:$RAFT_PORT" -raft-advertise "$host:$RAFT_PORT" \
    -addr "0.0.0.0:$GRPC_PORT" -http-addr "0.0.0.0:$HTTP_PORT" \
    -peers "$(peers "$n")" -data-dir "$DATA_ROOT/n$i" >>"$LOGS/n$i.log" 2>&1 &
  PIDS[$i]=$!
}

up() {
  local n=$1 i ready
  SIZE=$n
  for ((i = 1; i <= n; i++)); do start_broker "$i" "$n"; done
  for _ in $(seq 1 300); do
    ready=0
    for ((i = 1; i <= n; i++)); do
      curl -sf --noproxy '*' --max-time 1 "http://${BROKERS[i-1]}:$HTTP_PORT/readyz" >/dev/null 2>&1 && ready=$((ready + 1))
    done
    if (( ready == n )) && leader "$n" >/dev/null; then echo "cluster of $n ready; leader $(leader "$n")"; return 0; fi
    sleep 0.2
  done
  echo "cluster of $n did not become ready" >&2
  tail -n 20 "$LOGS"/n*.log >&2
  return 1
}

# down stops every broker (SIGTERM, then SIGKILL after 15 s) and wipes the
# data directories on every broker node.
down() {
  local host pid
  (( SIZE > 0 )) || return 0
  for host in "${BROKERS[@]:0:SIZE}"; do stop TERM "$host"; done
  for pid in "${PIDS[@]}"; do
    for _ in $(seq 1 75); do kill -0 "$pid" 2>/dev/null || break; sleep 0.2; done
  done
  for host in "${BROKERS[@]:0:SIZE}"; do
    stop KILL "$host"
    on "$host" rm -rf "$DATA_ROOT"
  done
  for pid in "${PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done
  PIDS=()
  SIZE=0
}

cleanup() {
  local rc=$?
  set +e
  [[ -n "${LOAD_PID:-}" ]] && kill "$LOAD_PID" 2>/dev/null
  SIZE=${#BROKERS[@]} down
  exit "$rc"
}
trap cleanup EXIT

load() {
  local n=$1; shift
  on "$LOAD_HOST" "$BIN/quorumq-load" run -addr "$(addrs "$n")" -payload-size "$PAYLOAD" "$@"
}

# --- environment -------------------------------------------------------------

# Data directories must be on node-local disk: a network filesystem makes every
# fsync a network round trip and would distort the results.
fs_types=()
for host in "${BROKERS[@]}"; do
  t=$(on "$host" bash -c 'mkdir -p "$1" && df -PT "$1" | awk "NR == 2 { print \$2 }"; rmdir "$1"' _ "$DATA_ROOT")
  case "$t" in
    nfs* | gpfs | lustre | beegfs | fuse* | autofs | cifs | smb*) echo "$host: $DATA_ROOT is on $t, not local disk; set DATA_ROOT" >&2; exit 1 ;;
  esac
  fs_types+=("$t")
done
DATA_FS=$(printf '%s\n' "${fs_types[@]}" | sort -u | paste -sd, -)
first=${BROKERS[0]}
data_disk=$(on "$first" bash -c 'mkdir -p "$1"; src=$(df -P "$1" | awk "NR == 2 { print \$1 }"); rmdir "$1"
  rota=$(lsblk -dno ROTA "$src" 2>/dev/null | tr -d " ")
  case $rota in 0) echo "non-rotational (SSD/NVMe)";; 1) echo rotational;; *) echo unknown;; esac' _ "$DATA_ROOT")

# Which interface the broker hostnames resolve to, and its link type. Only the
# interface name and type are recorded, not addresses.
net_info=$(on "$first" bash -c 'ip=$(getent ahostsv4 "$1" | awk "NR == 1 { print \$1 }")
  dev=$(ip -o -4 addr show | awk -v ip="$ip" "{ split(\$4, a, \"/\"); if (a[1] == ip) print \$2 }" | head -1)
  case $(cat /sys/class/net/$dev/type 2>/dev/null) in 1) type=Ethernet;; 32) type=InfiniBand;; *) type=unknown;; esac
  speed=$(cat /sys/class/net/$dev/speed 2>/dev/null || echo unknown)
  ib=$(ls /sys/class/infiniband 2>/dev/null | paste -sd, -)
  printf "%s\t%s\t%s\t%s\n" "$dev" "$type" "$speed" "${ib:-none}"' _ "$first")
IFS=$'\t' read -r NET_IF NET_TYPE NET_SPEED IB_DEVS <<<"$net_info"

rtt() { on "$1" bash -c 'ping -c 20 -i 0.2 -q "$1" 2>&1 | awk -F" = " "/^(rtt|round-trip)/ { print \$2 }"' _ "$2"; }
RTT_BROKER=$(rtt "$first" "${BROKERS[1]}")
RTT_LOAD=$(rtt "$LOAD_HOST" "$first")

cpu_models=$(for host in "${HOSTS[@]}"; do on "$host" bash -c 'lscpu | sed -n "s/^Model name: *//p"'; done | sort | uniq -c | awk '{ $1 = $1 " x"; print }' | paste -sd';' -)
lscpu_field() { lscpu | sed -n "s/^$1: *//p" | head -1; }
node_mem=$(free -g | awk '/^Mem:/ { print $2 " GiB" }')

slurm=$(jq -n \
  --arg partition "${SLURM_JOB_PARTITION:-}" --arg alloc "$ALLOC" --arg nodes "${#HOSTS[@]}" \
  --arg brokers "$(join "${BROKERS[@]}")" --arg load_host "$LOAD_HOST" \
  --arg exclusive "$( [[ "$ALLOC" == a ]] && echo yes || echo no)" --arg step_cpus "$STEP_CPUS" \
  --arg cpu_models "$cpu_models" --arg sockets "$(lscpu_field 'Socket(s)')" \
  --arg cores_per_socket "$(lscpu_field 'Core(s) per socket')" --arg threads_per_core "$(lscpu_field 'Thread(s) per core')" \
  --arg node_cpus "$(lscpu_field 'CPU(s)')" --arg node_mem "$node_mem" \
  --arg net_if "$NET_IF" --arg net_type "$NET_TYPE" --arg net_speed "$NET_SPEED" --arg ib "$IB_DEVS" \
  --arg rtt_broker "$RTT_BROKER" --arg rtt_load "$RTT_LOAD" \
  --arg data_fs "$DATA_FS" --arg data_disk "$data_disk" --arg data_root "${DATA_ROOT%"-$SLURM_JOB_ID"}-<jobid>" \
  '{partition: $partition, allocation: $alloc, exclusive: $exclusive, nodes: ($nodes | tonumber),
    brokers: $brokers, load_host: $load_host, cpus_per_step: $step_cpus,
    node: {cpu_models: $cpu_models, sockets: $sockets, cores_per_socket: $cores_per_socket,
           threads_per_core: $threads_per_core, cpus: $node_cpus, memory: $node_mem},
    network: {interface: $net_if, type: $net_type, speed_mbps: $net_speed, infiniband_devices: $ib,
              ping_rtt_broker_to_broker_ms: $rtt_broker, ping_rtt_load_to_broker_ms: $rtt_load},
    data_dir: {path: $data_root, filesystem: $data_fs, disk: $data_disk}}')

SETUP="one quorumq-server process per Slurm node, bare processes (no containers); the load generator on its own node; data on node-local disk" \
EXTRA="{\"slurm\": $slurm}" \
  scripts/env-json.sh "$RUN" "bare static binaries (CGO_ENABLED=0 go build), no container image" \
  reps="$REPS" duration="$DURATION" producers="$PRODUCERS" workers="$WORKERS" payload_bytes="$PAYLOAD" \
  saturation_nodes="$SAT_NODES" saturation_configs="$SAT_CONFIGS" \
  latency_rates="$LAT_RATES" latency_duration="$LAT_DURATION" \
  chaos_rate="$CHAOS_RATE" chaos_duration="$CHAOS_DURATION" kills="$KILLS" kill_gap="${GAP_MIN}-${GAP_MAX}s" \
  > "$OUT/env.json"
cat "$OUT/env.json"

# --- experiments -------------------------------------------------------------

echo "==> throughput (closed loop, $PRODUCERS producers / $WORKERS workers, $DURATION x $REPS)"
for n in $NODES; do
  up "$n"
  for r in $(seq 1 "$REPS"); do
    echo -n "  $n nodes, run $r: "
    load "$n" -queue "tput-$r" -producers "$PRODUCERS" -workers "$WORKERS" -duration "$DURATION" \
      -label "throughput nodes=$n run=$r" -out "$OUT/throughput/n$n-r$r"
  done
  down
done

echo "==> saturation (closed loop, $DURATION, producers/workers: $SAT_CONFIGS)"
for n in $SAT_NODES; do
  for pw in $SAT_CONFIGS; do
    p=${pw%/*}; w=${pw#*/}
    up "$n"
    echo -n "  $n nodes, $p producers / $w workers: "
    load "$n" -queue sat -producers "$p" -workers "$w" -duration "$DURATION" \
      -label "saturation nodes=$n producers=$p workers=$w" -out "$OUT/saturation/n$n-p$p-w$w"
    down
  done
done

# A fixed rate above what 3 brokers can complete in closed loop only measures
# queueing, so such rates are skipped.
capacity=$(jq -s '[.[].ack_rate_per_sec] | max // 0' "$OUT"/throughput/n3-r*/result.json "$OUT"/saturation/n3-*/result.json 2>/dev/null || echo 0)
echo "==> latency (3 nodes, rates $LAT_RATES submits/s for $LAT_DURATION; 3-node capacity $capacity jobs/s)"
for rate in $LAT_RATES; do
  if awk -v r="$rate" -v c="$capacity" 'BEGIN { exit !(r > c) }'; then
    echo "  $rate submits/s skipped: above the highest 3-node closed-loop rate, $capacity jobs/s"
    mkdir -p "$OUT/latency"
    echo "$rate $capacity" >> "$OUT/latency/skipped.txt"
    continue
  fi
  up 3
  echo -n "  $rate submits/s: "
  load 3 -queue latency -producers "$PRODUCERS" -workers "$WORKERS" -rate "$rate" -duration "$LAT_DURATION" \
    -label "latency nodes=3 rate=$rate" -out "$OUT/latency/r$rate"
  down
done

echo "==> chaos (3 nodes, $CHAOS_RATE submits/s for $CHAOS_DURATION, $KILLS leader kills)"
up 3
ADDRS3=$(addrs 3)
K=$OUT/chaos/kills.jsonl
mkdir -p "$OUT/chaos"
: > "$K"
load 3 -queue chaos -producers "$PRODUCERS" -workers "$WORKERS" -rate "$CHAOS_RATE" -duration "$CHAOS_DURATION" \
  -visibility 5s -drain-timeout 3m -label "chaos nodes=3 rate=$CHAOS_RATE kills=$KILLS" -out "$OUT/chaos" \
  > "$OUT/chaos-load.txt" 2>&1 &
LOAD_PID=$!
sleep 5
for k in $(seq 1 "$KILLS"); do
  gap=$(awk -v lo="$GAP_MIN" -v hi="$GAP_MAX" -v s="$RANDOM" 'BEGIN { srand(s); printf "%.1f", lo + rand() * (hi - lo) }')
  sleep "$gap"

  leader=$(leader 3) || { echo "kill $k: no leader to kill" >&2; exit 1; }
  li=${leader#n}
  host=${BROKERS[li-1]}
  term_before=$(status "$li" | jq -r .term)
  # One step on the leader's node takes the kill time, SIGKILLs the broker
  # and starts the failover probe at once, so srun's launch delay is not part
  # of the measurement and both timestamps come from the same clock. -x
  # matches the process name only, never the srun or shell command lines.
  out=$(on "$host" bash -c 't0=$(date +%s%N); pkill -KILL -u "$USER" -x quorumq-server; echo "$t0"
    exec "$1" probe -addr "$2" -since "$t0" -backoff 10ms' _ "$BIN/quorumq-load" "$ADDRS3")
  t0=$(head -1 <<<"$out")
  probe=$(tail -1 <<<"$out")
  new_leader=$(leader 3 || echo "?")
  echo "kill $k: killed leader $leader (term $term_before) on $host after ${gap}s; new leader $new_leader; failover $(jq -r .failover_ms <<<"$probe") ms"
  jq -c --arg killed "$leader" --arg next "$new_leader" --argjson k "$k" --argjson t0 "$t0" --argjson gap "$gap" \
    '. + {kill: $k, killed: $killed, new_leader: $next, killed_at_ns: $t0, gap_s: $gap}' <<<"$probe" >> "$K"

  # Bring the old leader back on the same node with the same data directory
  # and wait until it is a follower that has caught up, so a quorum survives
  # the next kill.
  sleep 2
  wait "${PIDS[$li]}" 2>/dev/null || true
  start_broker "$li" 3
  rejoined=
  for _ in $(seq 1 300); do
    lead=$(leader 3 2>/dev/null) || { sleep 0.2; continue; }
    commit=$(status "${lead#n}" | jq -r '.commit_index // 0')
    st=$(status "$li")
    if [[ "$(jq -r .state <<<"$st")" == "Follower" ]] && (( commit - $(jq -r '.fsm_applied_index // 0' <<<"$st") < 500 )); then
      rejoined=1; break
    fi
    sleep 0.2
  done
  [[ -n "$rejoined" ]] || { echo "kill $k: $leader did not rejoin as a caught-up follower within 60 s" >&2; tail -n 20 "$LOGS/$leader.log" >&2; exit 1; }
  echo "kill $k: $leader rejoined as follower"
done
wait "$LOAD_PID"
LOAD_PID=
cat "$OUT/chaos-load.txt"
down

# Keep the per-job ID files, compressed, for auditing with scripts/reconcile.sh.
find "$OUT" -name '*.csv' -exec gzip -9 {} +
rc=0
scripts/reconcile.sh "$OUT" > "$OUT/reconcile.txt" || rc=$?
scripts/bench-report.sh "$OUT" > "$OUT/summary.md"
echo "==> wrote $OUT/summary.md"
cat "$OUT/summary.md"
exit "$rc"
