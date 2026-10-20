// Package client is a Go client for a quorumq cluster.
//
//	c, err := client.New([]string{"node1:7070", "node2:7070", "node3:7070"})
//	id, err := c.Submit(ctx, "emails", payload, 3)
//	lease, err := c.Claim(ctx, "emails", "worker-1", 30*time.Second)
//	if lease != nil {
//		// do the work, then
//		err = c.Ack(ctx, lease.Job.GetId(), lease.ID)
//	}
//
// Only the Raft leader serves requests. The client sends each call to the
// node it believes is the leader; when a node answers that it is not the
// leader, the client follows the leader hint in the error, and when a node is
// unreachable it moves on to the next address with exponential backoff. It
// keeps retrying until the call's context expires, so a leader crash shows
// up as added latency rather than an error.
package client

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/arvindxyogesh/quorumq/api/quorumq/v1"
)

// Kept in sync with internal/server.
const (
	reasonNotLeader   = "NOT_LEADER"
	metadataLeaderKey = "leader"
)

// Client talks to a quorumq cluster. It is safe for concurrent use.
type Client struct {
	opts options

	mu     sync.Mutex
	addrs  []string // seed addresses plus any leaders learned from hints
	target int      // index into addrs of the node believed to be leader
	conns  map[string]*grpc.ClientConn
	closed bool
}

// Lease is a claimed job together with the token needed to ack or nack it.
type Lease struct {
	Job       *pb.Job
	ID        string
	ExpiresAt time.Time
}

type options struct {
	dialOpts       []grpc.DialOption
	attemptTimeout time.Duration
	baseBackoff    time.Duration
	maxBackoff     time.Duration
}

// Option configures a Client.
type Option func(*options)

// WithDialOptions adds gRPC dial options, applied after the default
// (plaintext) transport credentials so they can override them.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) { o.dialOpts = append(o.dialOpts, opts...) }
}

// WithAttemptTimeout bounds each individual attempt. It stops a call from
// spending its whole deadline on a node that has hung rather than crashed.
// The default is 3s.
func WithAttemptTimeout(d time.Duration) Option {
	return func(o *options) { o.attemptTimeout = d }
}

// WithBackoff sets the retry backoff range. The defaults are 50ms and 1s.
func WithBackoff(base, maxDelay time.Duration) Option {
	return func(o *options) { o.baseBackoff, o.maxBackoff = base, maxDelay }
}

// New returns a client for the cluster reachable at addrs. Connections are
// established lazily, so New succeeds even if some nodes are down.
func New(addrs []string, opts ...Option) (*Client, error) {
	if len(addrs) == 0 {
		return nil, errors.New("client: no addresses")
	}
	o := options{attemptTimeout: 3 * time.Second, baseBackoff: 50 * time.Millisecond, maxBackoff: time.Second}
	for _, opt := range opts {
		opt(&o)
	}
	o.dialOpts = append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, o.dialOpts...)
	return &Client{opts: o, addrs: append([]string(nil), addrs...), conns: make(map[string]*grpc.ClientConn)}, nil
}

// Close closes all connections.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	var errs []error
	for _, conn := range c.conns {
		errs = append(errs, conn.Close())
	}
	c.conns = nil
	return errors.Join(errs...)
}

// Submit enqueues a job and returns its ID.
//
// Submit is retried across leader changes. If a leader commits the job and
// then crashes before replying, the retry creates a second copy, so a job
// may be enqueued more than once but is never lost once Submit returns.
func (c *Client) Submit(ctx context.Context, queue string, payload []byte, maxRetries uint32) (string, error) {
	var resp *pb.SubmitJobResponse
	err := c.call(ctx, func(ctx context.Context, api pb.QueueServiceClient) (err error) {
		resp, err = api.SubmitJob(ctx, &pb.SubmitJobRequest{Queue: queue, Payload: payload, MaxRetries: maxRetries})
		return err
	})
	if err != nil {
		return "", err
	}
	return resp.GetJobId(), nil
}

// Claim leases the next visible job. It returns (nil, nil) when the queue has
// nothing to claim. A zero visibility timeout uses the server default.
func (c *Client) Claim(ctx context.Context, queue, workerID string, visibility time.Duration) (*Lease, error) {
	req := &pb.ClaimJobRequest{Queue: queue, WorkerId: workerID}
	if visibility > 0 {
		req.VisibilityTimeout = durationpb.New(visibility)
	}
	var resp *pb.ClaimJobResponse
	err := c.call(ctx, func(ctx context.Context, api pb.QueueServiceClient) (err error) {
		resp, err = api.ClaimJob(ctx, req)
		return err
	})
	if err != nil {
		return nil, err
	}
	if resp.GetJob() == nil {
		return nil, nil
	}
	return &Lease{Job: resp.GetJob(), ID: resp.GetLeaseId(), ExpiresAt: resp.GetLeaseExpiresAt().AsTime()}, nil
}

// Ack marks a leased job as done. Retries are safe: acking twice with the
// same lease is a no-op on the server.
func (c *Client) Ack(ctx context.Context, jobID, leaseID string) error {
	return c.call(ctx, func(ctx context.Context, api pb.QueueServiceClient) error {
		_, err := api.AckJob(ctx, &pb.AckJobRequest{JobId: jobID, LeaseId: leaseID})
		return err
	})
}

// Nack returns a leased job to the queue.
func (c *Client) Nack(ctx context.Context, jobID, leaseID string) error {
	return c.call(ctx, func(ctx context.Context, api pb.QueueServiceClient) error {
		_, err := api.NackJob(ctx, &pb.NackJobRequest{JobId: jobID, LeaseId: leaseID})
		return err
	})
}

// Get returns a job by ID.
func (c *Client) Get(ctx context.Context, jobID string) (*pb.Job, error) {
	var resp *pb.GetJobResponse
	err := c.call(ctx, func(ctx context.Context, api pb.QueueServiceClient) (err error) {
		resp, err = api.GetJob(ctx, &pb.GetJobRequest{JobId: jobID})
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp.GetJob(), nil
}

// Stats returns counts for a queue.
func (c *Client) Stats(ctx context.Context, queue string) (*pb.QueueStatsResponse, error) {
	var resp *pb.QueueStatsResponse
	err := c.call(ctx, func(ctx context.Context, api pb.QueueServiceClient) (err error) {
		resp, err = api.QueueStats(ctx, &pb.QueueStatsRequest{Queue: queue})
		return err
	})
	return resp, err
}

// call runs fn against the believed leader, retrying on errors that mean
// "try another node" until ctx is done.
func (c *Client) call(ctx context.Context, fn func(context.Context, pb.QueueServiceClient) error) error {
	backoff := c.opts.baseBackoff
	for {
		addr, api, err := c.current()
		if err != nil {
			return err
		}
		attemptCtx, cancel := context.WithTimeout(ctx, c.opts.attemptTimeout)
		err = fn(attemptCtx, api)
		cancel()
		if err == nil || !retryable(ctx, err) {
			return err
		}
		if ctx.Err() != nil {
			return err // the caller's deadline, not the attempt's, expired
		}

		if leader := leaderHint(err); leader != "" && leader != addr {
			c.redirect(addr, leader) // follow the hint straight away
			continue
		}
		c.rotate(addr)
		select {
		case <-ctx.Done():
			return err
		case <-time.After(jitter(backoff)):
		}
		backoff = min(backoff*2, c.opts.maxBackoff)
	}
}

// retryable reports whether err means the request may succeed on another
// node or after a short wait.
func retryable(ctx context.Context, err error) bool {
	switch status.Code(err) {
	case codes.Unavailable:
		return true // node down, shutting down, or not the leader
	case codes.DeadlineExceeded, codes.Canceled:
		return ctx.Err() == nil // only the per-attempt timeout fired
	default:
		return false
	}
}

func leaderHint(err error) string {
	st, ok := status.FromError(err)
	if !ok {
		return ""
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetReason() == reasonNotLeader {
			return info.GetMetadata()[metadataLeaderKey]
		}
	}
	return ""
}

func (c *Client) current() (string, pb.QueueServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return "", nil, errors.New("client: closed")
	}
	addr := c.addrs[c.target]
	conn, ok := c.conns[addr]
	if !ok {
		var err error
		if conn, err = grpc.NewClient(addr, c.opts.dialOpts...); err != nil {
			return "", nil, fmt.Errorf("client: dial %s: %w", addr, err)
		}
		c.conns[addr] = conn
	}
	return addr, pb.NewQueueServiceClient(conn), nil
}

// redirect points the client at leader, adding it to the address list if it
// was not a seed address.
func (c *Client) redirect(from, leader string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.addrs[c.target] != from {
		return // another goroutine already moved on
	}
	for i, a := range c.addrs {
		if a == leader {
			c.target = i
			return
		}
	}
	c.addrs = append(c.addrs, leader)
	c.target = len(c.addrs) - 1
}

// rotate moves to the next address after a failure against from.
func (c *Client) rotate(from string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.addrs[c.target] == from {
		c.target = (c.target + 1) % len(c.addrs)
	}
}

// jitter spreads retries over [d/2, d) so clients do not retry in lockstep.
func jitter(d time.Duration) time.Duration {
	if d <= 1 {
		return d
	}
	return d/2 + rand.N(d/2)
}
