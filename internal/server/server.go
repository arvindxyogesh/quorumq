// Package server implements the QueueService gRPC API on top of a Broker.
package server

import (
	"context"
	"errors"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/arvindxyogesh/quorumq/api/quorumq/v1"
	"github.com/arvindxyogesh/quorumq/internal/broker"
	"github.com/arvindxyogesh/quorumq/internal/queue"
)

// Broker is what the server needs from the layer that applies commands. It
// is satisfied by broker.Local (single process, used in tests) and by
// cluster.Broker (Raft-replicated).
type Broker interface {
	Submit(ctx context.Context, queue string, payload []byte, maxRetries uint32) (uint64, error)
	Claim(ctx context.Context, queue, workerID string, visibility time.Duration) (*queue.Job, error)
	Ack(ctx context.Context, jobID uint64, leaseID string) error
	Nack(ctx context.Context, jobID uint64, leaseID, reason string) error
	Redrive(ctx context.Context, jobID uint64) error
	Get(ctx context.Context, jobID uint64) (*queue.Job, error)
	Stats(ctx context.Context, queue string) (queue.Stats, error)
	ListDead(ctx context.Context, queue string, limit int) ([]*queue.Job, error)
}

// Server is the gRPC QueueService implementation.
type Server struct {
	pb.UnimplementedQueueServiceServer
	broker Broker
}

// New returns a Server backed by b.
func New(b Broker) *Server { return &Server{broker: b} }

func (s *Server) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	if err := firstErr(
		validateQueue(req.GetQueue()),
		validatePayload(req.GetPayload()),
		validateMaxRetries(req.GetMaxRetries()),
	); err != nil {
		return nil, err
	}
	id, err := s.broker.Submit(ctx, req.GetQueue(), req.GetPayload(), req.GetMaxRetries())
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.SubmitJobResponse{JobId: formatJobID(id)}, nil
}

func (s *Server) ClaimJob(ctx context.Context, req *pb.ClaimJobRequest) (*pb.ClaimJobResponse, error) {
	vt, err := visibilityTimeout(req.GetVisibilityTimeout())
	if err != nil {
		return nil, err
	}
	if err := firstErr(validateQueue(req.GetQueue()), validateWorkerID(req.GetWorkerId())); err != nil {
		return nil, err
	}
	j, err := s.broker.Claim(ctx, req.GetQueue(), req.GetWorkerId(), vt)
	if err != nil {
		return nil, toStatus(err)
	}
	if j == nil {
		return &pb.ClaimJobResponse{}, nil
	}
	return &pb.ClaimJobResponse{
		Job:            toProto(j),
		LeaseId:        j.LeaseID,
		LeaseExpiresAt: timestamppb.New(time.Unix(0, j.VisibleAt)),
	}, nil
}

func (s *Server) AckJob(ctx context.Context, req *pb.AckJobRequest) (*pb.AckJobResponse, error) {
	id, err := parseJobID(req.GetJobId())
	if err != nil {
		return nil, err
	}
	if err := validateLeaseID(req.GetLeaseId()); err != nil {
		return nil, err
	}
	if err := s.broker.Ack(ctx, id, req.GetLeaseId()); err != nil {
		return nil, toStatus(err)
	}
	return &pb.AckJobResponse{}, nil
}

func (s *Server) NackJob(ctx context.Context, req *pb.NackJobRequest) (*pb.NackJobResponse, error) {
	id, err := parseJobID(req.GetJobId())
	if err != nil {
		return nil, err
	}
	if err := firstErr(validateLeaseID(req.GetLeaseId()), validateReason(req.GetReason())); err != nil {
		return nil, err
	}
	if err := s.broker.Nack(ctx, id, req.GetLeaseId(), req.GetReason()); err != nil {
		return nil, toStatus(err)
	}
	return &pb.NackJobResponse{}, nil
}

func (s *Server) RedriveJob(ctx context.Context, req *pb.RedriveJobRequest) (*pb.RedriveJobResponse, error) {
	id, err := parseJobID(req.GetJobId())
	if err != nil {
		return nil, err
	}
	if err := s.broker.Redrive(ctx, id); err != nil {
		return nil, toStatus(err)
	}
	return &pb.RedriveJobResponse{}, nil
}

func (s *Server) ListDeadJobs(ctx context.Context, req *pb.ListDeadJobsRequest) (*pb.ListDeadJobsResponse, error) {
	limit, err := listLimit(req.GetLimit())
	if err != nil {
		return nil, err
	}
	if err := validateQueue(req.GetQueue()); err != nil {
		return nil, err
	}
	jobs, err := s.broker.ListDead(ctx, req.GetQueue(), limit)
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &pb.ListDeadJobsResponse{Jobs: make([]*pb.Job, len(jobs))}
	for i, j := range jobs {
		resp.Jobs[i] = toProto(j)
	}
	return resp, nil
}

func (s *Server) GetJob(ctx context.Context, req *pb.GetJobRequest) (*pb.GetJobResponse, error) {
	id, err := parseJobID(req.GetJobId())
	if err != nil {
		return nil, err
	}
	j, err := s.broker.Get(ctx, id)
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.GetJobResponse{Job: toProto(j)}, nil
}

func (s *Server) QueueStats(ctx context.Context, req *pb.QueueStatsRequest) (*pb.QueueStatsResponse, error) {
	if err := validateQueue(req.GetQueue()); err != nil {
		return nil, err
	}
	st, err := s.broker.Stats(ctx, req.GetQueue())
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.QueueStatsResponse{
		Queue:    req.GetQueue(),
		Visible:  st.Visible,
		InFlight: st.InFlight,
		Acked:    st.Acked,
		Delayed:  st.Delayed,
		Dead:     st.Dead,
	}, nil
}

// toStatus maps broker errors onto gRPC status codes. Errors that already
// carry a status pass through unchanged.
func toStatus(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	var notLeader *broker.NotLeaderError
	var unavailable *broker.UnavailableError
	switch {
	case errors.As(err, &notLeader):
		return notLeaderStatus(notLeader)
	case errors.As(err, &unavailable):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, queue.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, queue.ErrLeaseMismatch), errors.Is(err, queue.ErrNotDead):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// Leader redirects use the standard google.rpc.ErrorInfo error detail, so any
// gRPC client can read the hint without a custom header convention.
const (
	ErrorDomain       = "quorumq.dev"
	ReasonNotLeader   = "NOT_LEADER"
	MetadataLeaderKey = "leader"
)

func notLeaderStatus(e *broker.NotLeaderError) error {
	st := status.New(codes.Unavailable, e.Error())
	info := &errdetails.ErrorInfo{Reason: ReasonNotLeader, Domain: ErrorDomain}
	if e.Leader != "" {
		info.Metadata = map[string]string{MetadataLeaderKey: e.Leader}
	}
	if withInfo, err := st.WithDetails(info); err == nil {
		st = withInfo
	}
	return st.Err()
}

func toProto(j *queue.Job) *pb.Job {
	out := &pb.Job{
		Id:         formatJobID(j.ID),
		Queue:      j.Queue,
		Payload:    j.Payload,
		State:      stateToProto(j.State),
		Attempts:   j.Attempts,
		MaxRetries: j.MaxRetries,
		CreatedAt:  timestamppb.New(time.Unix(0, j.CreatedAt)),
		WorkerId:   j.WorkerID,
		LastError:  j.LastError,
	}
	if j.VisibleAt != 0 {
		out.VisibleAt = timestamppb.New(time.Unix(0, j.VisibleAt))
	}
	if j.DeadAt != 0 {
		out.DeadAt = timestamppb.New(time.Unix(0, j.DeadAt))
	}
	return out
}

func stateToProto(s queue.State) pb.JobState {
	switch s {
	case queue.StatePending:
		return pb.JobState_JOB_STATE_PENDING
	case queue.StateLeased:
		return pb.JobState_JOB_STATE_LEASED
	case queue.StateAcked:
		return pb.JobState_JOB_STATE_ACKED
	case queue.StateDead:
		return pb.JobState_JOB_STATE_DEAD
	default:
		return pb.JobState_JOB_STATE_UNSPECIFIED
	}
}
