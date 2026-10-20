#!/usr/bin/env bash
# Deploys quorumq to a Kubernetes cluster and checks it survives losing its
# leader pod under load. Used by CI on a kind cluster.
#
#   scripts/k8s-smoke.sh            # needs kubectl pointed at a cluster with quorumq:dev available
#
# 1. kubectl apply -k deploy/k8s and wait for 3 ready pods.
# 2. Run an in-cluster load Job (test/k8s/load-job.yaml).
# 3. 30 s in, find the leader through the API server's pod proxy and
#    force-delete it (no graceful shutdown). The StatefulSet recreates it
#    with the same name and volume.
# 4. Wait for the Job and require that it lost no acknowledged job.
set -euo pipefail
cd "$(dirname "$0")/.."

status() { kubectl get --raw "/api/v1/namespaces/default/pods/$1:8080/proxy/status" 2>/dev/null || echo '{}'; }
leader() {
  for p in quorumq-0 quorumq-1 quorumq-2; do
    [[ "$(status "$p" | jq -r .state)" == "Leader" ]] && { echo "$p"; return 0; }
  done
  return 1
}

echo "==> deploying"
kubectl apply -k deploy/k8s
kubectl rollout status statefulset/quorumq --timeout=300s
kubectl get pods -l app=quorumq -o wide
for _ in $(seq 1 60); do L=$(leader) && break; sleep 1; done
echo "leader: $L"
for p in quorumq-0 quorumq-1 quorumq-2; do status "$p" | jq -c '{node_id, state, term, leader_id}'; done

echo "==> CLI round trip inside the cluster"
ADDRS=quorumq-0.quorumq-peers:7070,quorumq-1.quorumq-peers:7070,quorumq-2.quorumq-peers:7070
q() { kubectl exec quorumq-0 -- /usr/local/bin/quorumq -addr "$ADDRS" "$@"; }
JOB=$(q submit -queue smoke -payload hello | jq -r .job_id)
CLAIM=$(q claim -queue smoke -worker smoke)
q ack -job "$JOB" -lease "$(jq -r .lease_id <<<"$CLAIM")"
q stats -queue smoke

echo "==> load + leader pod kill"
kubectl delete job quorumq-load --ignore-not-found
kubectl apply -f test/k8s/load-job.yaml
sleep 30
L=$(leader)
echo "force-deleting leader pod $L"
T0=$(date +%s%N)
kubectl delete pod "$L" --grace-period=0 --force
for _ in $(seq 1 300); do
  NEW=$(leader 2>/dev/null) && [[ "$NEW" != "$L" ]] && break
  sleep 0.1
done
echo "new leader $NEW after $(( ($(date +%s%N) - T0) / 1000000 )) ms (observed via the API proxy, 100 ms polling)"
kubectl rollout status statefulset/quorumq --timeout=300s

kubectl wait --for=condition=complete job/quorumq-load --timeout=360s || {
  kubectl logs job/quorumq-load; kubectl describe job quorumq-load; exit 1; }
OUT=$(kubectl logs job/quorumq-load)
echo "$OUT"
grep -q "lost 0 " <<<"$OUT" || { echo "FAIL: jobs were lost" >&2; exit 1; }
for p in quorumq-0 quorumq-1 quorumq-2; do status "$p" | jq -c '{node_id, state, term, fsm_applied_index}'; done
echo "OK: no acknowledged job lost after force-deleting the leader pod"
