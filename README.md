# quorumq

A fault-tolerant distributed job queue in Go (a simplified SQS / Celery broker).
Producers submit jobs, workers claim and acknowledge them, and delivery is
at-least-once with visibility timeouts. Brokers replicate their state with Raft,
so an acknowledged submit survives the crash of any minority of nodes,
including the leader.

> Work in progress. Milestones 1 (single-node queue) and 2 (Raft replication)
> are done; retries and a dead-letter queue, observability, and load/chaos
> results follow. The full design write-up lands with milestone 6.

## Quick start

Requires Go 1.24+ and, for the scripts, `jq` and `curl`.

```sh
make test          # unit tests, including in-process 3-node Raft clusters, with -race
make integration   # 3 real server processes; SIGKILLs the leader twice mid-stream
make demo          # single node: submit / claim / ack / nack / visibility timeout
```

### A 3-node cluster

With Docker:

```sh
make cluster-up                      # docker compose, ports 7071-7073 (gRPC) and 8081-8083 (HTTP)
go run ./cmd/quorumq -addr localhost:7071,localhost:7072,localhost:7073 submit -queue emails -payload hi
curl -s localhost:8081/status        # role, term, leader, log indexes
make failover-demo                   # docker kill the leader and watch a new one take over
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
| `deploy/docker-compose.yml`, `Dockerfile` | 3-node local cluster |

Every write (submit, claim, ack, nack) goes through the Raft leader and is
acknowledged only after a quorum has it in its log. Reads use a Raft barrier on
the leader, so they are linearizable. Followers answer with gRPC `UNAVAILABLE`
plus a `google.rpc.ErrorInfo` naming the leader, and the client retries there.
