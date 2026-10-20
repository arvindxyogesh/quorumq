package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/raft"

	"github.com/arvindxyogesh/quorumq/internal/broker"
	"github.com/arvindxyogesh/quorumq/internal/metrics"
	"github.com/arvindxyogesh/quorumq/internal/queue"
)

// Broker serves the queue API from a Raft node. Every operation, including
// reads, is handled by the leader:
//
//   - Writes (submit, claim, ack, nack, redrive) are proposed to the Raft log and
//     return once a quorum has committed them and the leader has applied
//     them, so an acknowledged submit survives the loss of any minority.
//   - Reads (get, stats, list dead) first issue a Raft barrier, which only succeeds if
//     this node is still the leader and has applied everything committed
//     before it. That makes reads linearizable: a deposed leader on the
//     wrong side of a partition cannot return stale data.
//
// On a follower every call fails with *broker.NotLeaderError naming the
// leader, and the client retries there.
type Broker struct {
	node    *Node
	now     func() time.Time
	retry   broker.RetryPolicy
	metrics *metrics.Metrics
}

// NewBroker returns a Broker for node that retries nacked jobs under retry.
// Job-flow metrics go to the node's configured Metrics.
func NewBroker(node *Node, retry broker.RetryPolicy) *Broker {
	return &Broker{node: node, now: time.Now, retry: retry, metrics: node.cfg.Metrics}
}

func (b *Broker) Submit(ctx context.Context, queueName string, payload []byte, maxRetries uint32) (uint64, error) {
	r, err := b.propose(ctx, "submit", queue.Command{Op: queue.OpSubmit, Submit: &queue.SubmitCmd{
		Queue: queueName, Payload: payload, MaxRetries: maxRetries, Now: b.now().UnixNano(),
	}})
	if err != nil {
		return 0, err
	}
	if r.Err == nil {
		b.metrics.JobSubmitted(queueName)
	}
	return r.JobID, r.Err
}

// Claim leases the next visible job through the log. If the leader's own
// replica shows nothing claimable it answers "empty" without writing to the
// log: workers polling idle queues would otherwise cost a quorum fsync per
// poll. The shortcut can only err towards a spurious empty answer (the
// leader's state machine may trail the commit index by a few entries, or the
// node may have just been deposed), which a polling worker simply retries;
// a job is only ever handed out by a committed log entry.
func (b *Broker) Claim(ctx context.Context, queueName, workerID string, visibility time.Duration) (*queue.Job, error) {
	now := b.now()
	if !b.node.IsLeader() {
		return nil, b.notLeader()
	}
	if visible, err := b.node.store.HasVisible(queueName, now.UnixNano()); err == nil && !visible {
		b.metrics.JobClaimed(queueName, true)
		return nil, nil
	}
	r, err := b.propose(ctx, "claim", queue.Command{Op: queue.OpClaim, Claim: &queue.ClaimCmd{
		Queue: queueName, WorkerID: workerID, LeaseID: broker.NewLeaseID(),
		VisibilityTimeout: visibility, Now: now.UnixNano(),
	}})
	if err != nil {
		return nil, err
	}
	if r.Err == nil {
		b.metrics.JobClaimed(queueName, r.Job == nil)
	}
	return r.Job, r.Err
}

func (b *Broker) Ack(ctx context.Context, jobID uint64, leaseID string) error {
	r, err := b.propose(ctx, "ack", queue.Command{Op: queue.OpAck, Ack: &queue.AckCmd{
		JobID: jobID, LeaseID: leaseID, Now: b.now().UnixNano(),
	}})
	if err != nil {
		return err
	}
	if r.Err == nil && r.Job != nil {
		b.metrics.JobAcked(r.Job.Queue)
	}
	return r.Err
}

func (b *Broker) Nack(ctx context.Context, jobID uint64, leaseID, reason string) error {
	r, err := b.propose(ctx, "nack", queue.Command{Op: queue.OpNack, Nack: b.retry.NackCmd(jobID, leaseID, reason, b.now())})
	if err != nil {
		return err
	}
	if r.Err == nil && r.Job != nil {
		b.metrics.JobNacked(r.Job.Queue, r.Job.State == queue.StateDead)
	}
	return r.Err
}

func (b *Broker) Redrive(ctx context.Context, jobID uint64) error {
	r, err := b.propose(ctx, "redrive", queue.Command{Op: queue.OpRedrive, Redrive: &queue.RedriveCmd{
		JobID: jobID, Now: b.now().UnixNano(),
	}})
	if err != nil {
		return err
	}
	if r.Err == nil {
		if j, err := b.node.store.Get(jobID); err == nil {
			b.metrics.JobRedriven(j.Queue)
		}
	}
	return r.Err
}

func (b *Broker) ListDead(ctx context.Context, queueName string, limit int) ([]*queue.Job, error) {
	if err := b.barrier(ctx); err != nil {
		return nil, err
	}
	return b.node.store.ListDead(queueName, limit)
}

func (b *Broker) Get(ctx context.Context, jobID uint64) (*queue.Job, error) {
	if err := b.barrier(ctx); err != nil {
		return nil, err
	}
	return b.node.store.Get(jobID)
}

func (b *Broker) Stats(ctx context.Context, queueName string) (queue.Stats, error) {
	if err := b.barrier(ctx); err != nil {
		return queue.Stats{}, err
	}
	return b.node.store.Stats(queueName, b.now().UnixNano())
}

// propose commits cmd to the Raft log and returns the state machine's result.
func (b *Broker) propose(ctx context.Context, op string, cmd queue.Command) (queue.Result, error) {
	if !b.node.IsLeader() {
		return queue.Result{}, b.notLeader()
	}
	data, err := cmd.Encode()
	if err != nil {
		return queue.Result{}, err
	}
	start := time.Now()
	f := b.node.raft.Apply(data, b.timeout(ctx))
	if err := wait(ctx, f); err != nil {
		return queue.Result{}, b.mapRaftError(err)
	}
	b.metrics.ObserveApply(op, time.Since(start))
	r, ok := f.Response().(queue.Result)
	if !ok {
		return queue.Result{}, fmt.Errorf("unexpected FSM response %T", f.Response())
	}
	return r, nil
}

func (b *Broker) barrier(ctx context.Context) error {
	if !b.node.IsLeader() {
		return b.notLeader()
	}
	if err := wait(ctx, b.node.raft.Barrier(b.timeout(ctx))); err != nil {
		return b.mapRaftError(err)
	}
	return nil
}

func (b *Broker) timeout(ctx context.Context) time.Duration {
	if dl, ok := ctx.Deadline(); ok {
		if d := time.Until(dl); d > 0 && d < b.node.cfg.ApplyTimeout {
			return d
		}
	}
	return b.node.cfg.ApplyTimeout
}

// wait blocks until f completes or ctx is done. If ctx ends first the
// operation may still commit later; at-least-once clients handle that by
// retrying.
func wait(ctx context.Context, f raft.Future) error {
	done := make(chan error, 1)
	go func() { done <- f.Error() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// mapRaftError turns Raft's leadership errors into errors the client knows
// to retry. ErrLeadershipLost is the subtle one: the entry may or may not
// have been committed, so a retried submit can create a duplicate job.
func (b *Broker) mapRaftError(err error) error {
	switch {
	case errors.Is(err, raft.ErrNotLeader),
		errors.Is(err, raft.ErrLeadershipLost),
		errors.Is(err, raft.ErrLeadershipTransferInProgress):
		return b.notLeader()
	case errors.Is(err, raft.ErrRaftShutdown),
		errors.Is(err, raft.ErrEnqueueTimeout):
		return &broker.UnavailableError{Err: err}
	default:
		return err
	}
}

func (b *Broker) notLeader() error {
	leader := b.node.LeaderClientAddr()
	if leader == b.node.self.ClientAddr {
		leader = "" // deposed mid-request; pointing the client back here would loop
	}
	return &broker.NotLeaderError{Leader: leader}
}
