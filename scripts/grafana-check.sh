#!/usr/bin/env bash
# Checks the Grafana dashboard end to end against the Compose cluster with
# the observability profile: Prometheus scrapes all nodes, Grafana loads the
# provisioned dashboard, and every panel query returns data when run through
# Grafana's own Prometheus data source.
#
#   docker compose -f deploy/docker-compose.yml --profile observability up -d --build
#   scripts/grafana-check.sh [screenshot.png]
#
# With a file argument it also screenshots the dashboard (needs npx/Playwright).
set -euo pipefail
cd "$(dirname "$0")/.."
GRAFANA=http://localhost:3000
PROM=http://localhost:9090
ADDRS=localhost:7071,localhost:7072,localhost:7073

go build -o bin/ ./cmd/...
wait_for() { for _ in $(seq 1 120); do curl -sf "$1" >/dev/null && return 0; sleep 1; done; echo "timeout: $1" >&2; return 1; }
wait_for "$GRAFANA/api/health"
wait_for "$PROM/-/ready"
for i in 1 2 3; do wait_for "localhost:808$i/readyz"; done

echo "==> traffic: load, nacks, dead letters"
bin/quorumq-load run -addr "$ADDRS" -queue dashboard -producers 8 -workers 16 -rate 200 -duration 45s &
LOAD=$!
for n in $(seq 1 5); do
  ID=$(bin/quorumq -addr "$ADDRS" submit -queue flaky -payload "bad-$n" -max-retries 0 | jq -r .job_id)
  C=$(bin/quorumq -addr "$ADDRS" claim -queue flaky -worker w)
  bin/quorumq -addr "$ADDRS" nack -job "$ID" -lease "$(jq -r .lease_id <<<"$C")" -reason "invalid input" >/dev/null
done
wait "$LOAD"
sleep 15 # at least two scrapes after the load, so rate() has data

echo "==> Prometheus targets"
curl -sf "$PROM/api/v1/targets" | jq -r '.data.activeTargets[] | "\(.labels.instance) \(.health)"'
up=$(curl -sf "$PROM/api/v1/targets" | jq '[.data.activeTargets[] | select(.health == "up")] | length')
[[ "$up" -eq 3 ]] || { echo "FAIL: $up of 3 targets up" >&2; exit 1; }

echo "==> dashboard"
DASH=$(curl -sf "$GRAFANA/api/dashboards/uid/quorumq")
jq -r '.dashboard.title' <<<"$DASH"

fail=0
while IFS= read -r line; do
  title=$(jq -r .title <<<"$line"); expr=$(jq -r .expr <<<"$line")
  body=$(jq -n --arg e "${expr//\$__rate_interval/1m}" \
    '{from: "now-5m", to: "now", queries: [{refId: "A", datasource: {type: "prometheus", uid: "prometheus"}, expr: $e, instant: true, range: false}]}')
  resp=$(curl -sf -H 'Content-Type: application/json' -d "$body" "$GRAFANA/api/ds/query" || echo '{}')
  n=$(jq '[.results.A.frames[]? | select((.data.values // []) | length > 0 and (.[0] | length) > 0)] | length' <<<"$resp")
  if [[ "$n" -gt 0 ]]; then
    printf 'OK    %-38s %2d series  %s\n' "$title" "$n" "$expr"
  else
    printf 'EMPTY %-38s           %s\n' "$title" "$expr"; fail=1
    jq -c '.results.A.error? // .message? // .' <<<"$resp" | head -c 300; echo
  fi
done < <(jq -c '.dashboard.panels[] | select(.targets) | .title as $t | .targets[] | {title: $t, expr}' <<<"$DASH")

if [[ -n "${1:-}" ]]; then
  echo "==> screenshot"
  npx -y playwright@1.48.2 install --with-deps chromium >/dev/null
  npx -y playwright@1.48.2 screenshot --viewport-size=1600,1500 --wait-for-timeout=10000 --full-page \
    "$GRAFANA/d/quorumq/quorumq?orgId=1&from=now-5m&to=now&kiosk&theme=dark" "$1"
fi

(( fail == 0 )) || { echo "FAIL: some dashboard queries returned no data" >&2; exit 1; }
echo "OK: every dashboard query returned data through Grafana"
