# quorumq

[![CI](https://github.com/arvindxyogesh/quorumq/actions/workflows/ci.yml/badge.svg)](https://github.com/arvindxyogesh/quorumq/actions/workflows/ci.yml)

A fault-tolerant distributed job queue in Go (a simplified SQS / Celery broker).
Producers submit jobs, workers claim and acknowledge them, and delivery is
at-least-once with visibility timeouts. Brokers replicate their state with Raft,
so an acknowledged submit survives the crash of any minority of nodes,
including the leader.

> Work in progress. Milestones 1-4 (single-node queue, Raft replication,
> retries / dead-letter queue / snapshots, observability and ops) are done;
> load/chaos results follow. The full design write-up lands with milestone 6.

## Quick start

Requires Go 1.24+ and, for the scripts, `jq` and `curl`.

```sh
make test          # unit tests, including in-process 3-node Raft clusters, with -race
make integration   # 3 real server processes; SIGKILLs the leader twice mid-stream
make demo          # single node: submit / claim / ack / nack + backoff / timeout / dead-letter / redrive
```

### A 3-node cluster

With Docker:

```sh
make cluster-up                      # docker compose, ports 7071-7073 (gRPC) and 8081-8083 (HTTP)
go run ./cmd/quorumq -addr localhost:7071,localhost:7072,localhost:7073 submit -queue emails -payload hi
curl -s localhost:8081/status        # role, term, leader, log indexes
make failover-demo                   # docker kill the leader and watch a new one take over
```

Add `--profile observability` to `docker compose ... up` for Prometheus
(localhost:9090) and Grafana with the quorumq dashboard (localhost:3000).

On Kubernetes (3-pod StatefulSet, headless Service for Raft, client Service,
PodDisruptionBudget):

```sh
docker build -t quorumq:dev . && kind load docker-image quorumq:dev   # or push to your registry
kubectl apply -k deploy/k8s
kubectl port-forward svc/quorumq 7070 &
go run ./cmd/quorumq -addr localhost:7070 stats -queue demo
```

Without Docker, start three processes (see the comment at the top of
`cmd/quorumq-server/main.go`). The CLI takes any subset of the node
addresses and finds the leader itself.

## How it fits together

| Path | What it is |
| --- | --- |
| `api/quorumq/v1` | Protobuf API and generated gRPC code (`make proto` regenerates with buf) |
| `internal/queue` | Deterministic BoltDB state machine: queues, leases, batched log apply, snapshots |
| `internal/cluster` | hashicorp/raft node, `raft.FSM` adapter, Raft-backed broker, admin HTTP |
| `internal/broker` | Single-process broker (used in tests) and shared broker errors |
| `internal/server` | gRPC `QueueService`: validation, error-code mapping, leader redirects |
| `client` | Go client: finds the leader, follows redirects, retries across failover |
| `cmd/quorumq-server`, `cmd/quorumq` | Broker binary and CLI |
| `test/integration` | Process-level failover test |
| `internal/metrics` | Prometheus metrics and the scrape-time collector |
| `deploy/docker-compose.yml`, `Dockerfile` | 3-node local cluster (+ Prometheus and Grafana profile) |
| `deploy/k8s` | Kubernetes manifests (kustomize) |
| `deploy/grafana`, `deploy/prometheus` | Dashboard and scrape config |

## Job lifecycle

```
submit -> PENDING --claim--> LEASED --ack--> ACKED
             ^                  |
             |   nack, retries left: back to PENDING after 1s, 2s, 4s ... (capped)
             +------------------+   lease expired: claimable again immediately
                                |
                                +-- nack or lease expiry after the last attempt --> DEAD
                                                                  (RedriveJob -> PENDING)
```

A job may be delivered `max_retries + 1` times. Acks and nacks must present
the lease from the claim that delivered the job; once a lease expires and the
job is re-claimed, the old lease is rejected, so a slow worker cannot ack a
job someone else now holds. Retrying an ack or nack with the same lease is a
no-op.

## Replication

Every write (submit, claim, ack, nack, redrive) goes through the Raft leader and is
acknowledged only after a quorum has it in its log. Reads use a Raft barrier on
the leader, so they are linearizable. Followers answer with gRPC `UNAVAILABLE`
plus a `google.rpc.ErrorInfo` naming the leader, and the client retries there.

Raft snapshots the state machine every `-snapshot-threshold` entries and
compacts its log, keeping `-trailing-logs` entries. A follower that falls
further behind than that is sent the snapshot. A restarted node replays only
the log entries after the last one its state machine applied.

## Operations

Each node serves `/healthz` (liveness), `/readyz` (ready once it knows a
leader), `/status` (Raft role, term, log indexes) and `/metrics` on its admin
port. The gRPC health service reports `quorumq.v1.QueueService` as SERVING on
the same condition as `/readyz`.

| Metric | What it tells you |
| --- | --- |
| `quorumq_jobs_{submitted,claimed,acked,nacked,dead_lettered,redriven}_total{queue}` | Job flow, counted by the node that served it (the leader) |
| `quorumq_queue_jobs{queue,state}` | Depth by state (`visible`, `in_flight`, `delayed`, `dead`) from each replica |
| `quorumq_grpc_request_duration_seconds{method}`, `quorumq_grpc_requests_total{method,code}` | API latency and errors |
| `quorumq_raft_apply_duration_seconds{op}` | Leader-side time to commit a command to a quorum and apply it |
| `quorumq_fsm_apply_batch_{entries,duration_seconds}` | How many log entries share each BoltDB transaction |
| `quorumq_raft_is_leader`, `quorumq_raft_leader_changes_total`, `quorumq_raft_term`, `quorumq_raft_index{kind}` | Leadership and replication progress |

CI (GitHub Actions) runs vet, gofmt, golangci-lint, `go mod tidy` and
generated-code drift checks, buf lint, race-enabled unit tests on the go.mod
Go version and latest stable, the leader-kill integration test, a Docker
build plus the Compose failover demo on the real image, and kubeconform /
promtool validation of the deploy files.
