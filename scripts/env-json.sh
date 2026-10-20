#!/usr/bin/env bash
# Prints a JSON description of the machine and repo state for a results
# directory. Extra key=value arguments are added under "params".
#
#   scripts/env-json.sh RUN_NAME IMAGE_NOTE [key=value ...] > env.json
set -euo pipefail
cd "$(dirname "$0")/.."
RUN=${1:?run name}; IMAGE=${2:?image note}; shift 2

params='{}'
for kv in "$@"; do params=$(jq --arg k "${kv%%=*}" --arg v "${kv#*=}" '. + {($k): $v}' <<<"$params"); done

jq -n \
  --arg run "$RUN" \
  --arg commit "$(git rev-parse --short HEAD)$(git diff --quiet || echo -dirty)" \
  --arg cpu "$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ //')" \
  --arg cpus "$(nproc)" \
  --arg mem "$(free -g | awk '/^Mem:/ {print $2 " GiB"}')" \
  --arg kernel "$(uname -r)" \
  --arg docker "$(docker version --format '{{.Server.Version}}' 2>/dev/null || echo none)" \
  --arg go "$(go version | awk '{print $3}')" \
  --arg image "$IMAGE" \
  --argjson params "$params" \
  '{run: $run, commit: $commit, cpu: $cpu, cpus: $cpus, memory: $mem, kernel: $kernel,
    docker: $docker, go: $go, image: $image,
    setup: "all nodes and the load generator on this one machine; nodes in Docker containers on a bridge network with host port mapping; data on Docker named volumes",
    params: $params}'
