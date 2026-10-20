// Package client is a small Go client for quorumq.
//
//	c, err := client.Dial("localhost:7070")
//	id, err := c.Submit(ctx, "emails", payload, 3)
//	lease, err := c.Claim(ctx, "emails", "worker-1", 30*time.Second)
//	if lease != nil {
//		// do the work, then
//		err = c.Ack(ctx, lease.Job.GetId(), lease.ID)
//	}
package client

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/arvindxyogesh/quorumq/api/quorumq/v1"
)

// Client talks to a quorumq broker.
type Client struct {
	conn *grpc.ClientConn
	api  pb.QueueServiceClient
}

// Lease is a claimed job together with the token needed to ack or nack it.
type Lease struct {
	Job       *pb.Job
	ID        string
	ExpiresAt time.Time
}

// Dial connects to the broker at addr. Extra gRPC options are appended after
// the default (plaintext) transport credentials, so callers can override them.
func Dial(addr string, opts ...grpc.DialOption) (*Client, error) {
	opts = append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, opts...)
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return &Client{conn: conn, api: pb.NewQueueServiceClient(conn)}, nil
}

// Close closes the connection.
func (c *Client) Close() error { return c.conn.Close() }

// Submit enqueues a job and returns its ID.
func (c *Client) Submit(ctx context.Context, queue string, payload []byte, maxRetries uint32) (string, error) {
	resp, err := c.api.SubmitJob(ctx, &pb.SubmitJobRequest{Queue: queue, Payload: payload, MaxRetries: maxRetries})
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
	resp, err := c.api.ClaimJob(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.GetJob() == nil {
		return nil, nil
	}
	return &Lease{Job: resp.GetJob(), ID: resp.GetLeaseId(), ExpiresAt: resp.GetLeaseExpiresAt().AsTime()}, nil
}

// Ack marks a leased job as done.
func (c *Client) Ack(ctx context.Context, jobID, leaseID string) error {
	_, err := c.api.AckJob(ctx, &pb.AckJobRequest{JobId: jobID, LeaseId: leaseID})
	return err
}

// Nack returns a leased job to the queue.
func (c *Client) Nack(ctx context.Context, jobID, leaseID string) error {
	_, err := c.api.NackJob(ctx, &pb.NackJobRequest{JobId: jobID, LeaseId: leaseID})
	return err
}

// Get returns a job by ID.
func (c *Client) Get(ctx context.Context, jobID string) (*pb.Job, error) {
	resp, err := c.api.GetJob(ctx, &pb.GetJobRequest{JobId: jobID})
	if err != nil {
		return nil, err
	}
	return resp.GetJob(), nil
}

// Stats returns counts for a queue.
func (c *Client) Stats(ctx context.Context, queue string) (*pb.QueueStatsResponse, error) {
	return c.api.QueueStats(ctx, &pb.QueueStatsRequest{Queue: queue})
}
