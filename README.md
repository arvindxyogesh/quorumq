# quorumq

A fault-tolerant distributed job queue in Go (a simplified SQS / Celery broker).
Producers submit jobs, workers claim and acknowledge them, and delivery is
at-least-once with visibility timeouts.

> Work in progress. Milestone 1 (single-node queue) is done; Raft replication,
> retries and a dead-letter queue, observability, and load/chaos results follow.
> The full design write-up lands with milestone 6.

## Quick start

Requires Go 1.24+ and, for the demo, `jq`.

```sh
make test        # unit tests with the race detector
make demo        # starts a node and walks through submit / claim / ack / nack / timeout
```

Run a node and use the CLI:

```sh
go run ./cmd/quorumq-server -addr :7070 -data-dir ./data
go run ./cmd/quorumq submit -queue emails -payload '{"to":"a@example.com"}'
go run ./cmd/quorumq claim  -queue emails -worker w1 -visibility 30s
go run ./cmd/quorumq ack    -job 1 -lease <lease_id from claim>
go run ./cmd/quorumq stats  -queue emails
```

## Layout

| Path | What it is |
| --- | --- |
| `api/quorumq/v1` | Protobuf API and generated gRPC code (`make proto` regenerates with buf) |
| `internal/queue` | Deterministic BoltDB-backed state machine: queues, leases, acks |
| `internal/broker` | Turns API calls into state-machine commands (owns the clock and lease IDs) |
| `internal/server` | gRPC `QueueService`: validation and error-code mapping |
| `client` | Go client library |
| `cmd/quorumq-server`, `cmd/quorumq` | Broker binary and CLI |
| `scripts/demo.sh` | End-to-end demo against a real node |
