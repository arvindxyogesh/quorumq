package server

import (
	"regexp"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Limits are modelled on Amazon SQS.
const (
	MaxPayloadBytes          = 256 << 10
	MaxRetriesLimit          = 100
	DefaultVisibilityTimeout = 30 * time.Second
	MinVisibilityTimeout     = time.Second
	MaxVisibilityTimeout     = 12 * time.Hour
	maxWorkerIDLen           = 128
	maxReasonLen             = 1024
	DefaultListLimit         = 20
	MaxListLimit             = 100
)

var queueNameRE = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,80}$`)

func invalid(format string, args ...any) error {
	return status.Errorf(codes.InvalidArgument, format, args...)
}

func validateQueue(name string) error {
	if !queueNameRE.MatchString(name) {
		return invalid("queue name must be 1-80 characters of letters, digits, '.', '_' or '-'; got %q", name)
	}
	return nil
}

func validatePayload(p []byte) error {
	if len(p) > MaxPayloadBytes {
		return invalid("payload is %d bytes; the limit is %d", len(p), MaxPayloadBytes)
	}
	return nil
}

func validateMaxRetries(n uint32) error {
	if n > MaxRetriesLimit {
		return invalid("max_retries is %d; the limit is %d", n, MaxRetriesLimit)
	}
	return nil
}

func validateWorkerID(id string) error {
	if id == "" || len(id) > maxWorkerIDLen {
		return invalid("worker_id must be 1-%d bytes", maxWorkerIDLen)
	}
	return nil
}

func visibilityTimeout(d *durationpb.Duration) (time.Duration, error) {
	if d == nil {
		return DefaultVisibilityTimeout, nil
	}
	if err := d.CheckValid(); err != nil {
		return 0, invalid("visibility_timeout: %v", err)
	}
	vt := d.AsDuration()
	if vt == 0 {
		return DefaultVisibilityTimeout, nil
	}
	if vt < MinVisibilityTimeout || vt > MaxVisibilityTimeout {
		return 0, invalid("visibility_timeout must be between %s and %s; got %s",
			MinVisibilityTimeout, MaxVisibilityTimeout, vt)
	}
	return vt, nil
}

func parseJobID(s string) (uint64, error) {
	id, err := strconv.ParseUint(s, 10, 64)
	if err != nil || id == 0 {
		return 0, invalid("job_id must be a positive integer; got %q", s)
	}
	return id, nil
}

func formatJobID(id uint64) string { return strconv.FormatUint(id, 10) }

func validateReason(s string) error {
	if len(s) > maxReasonLen {
		return invalid("reason is %d bytes; the limit is %d", len(s), maxReasonLen)
	}
	return nil
}

func listLimit(n uint32) (int, error) {
	switch {
	case n == 0:
		return DefaultListLimit, nil
	case n > MaxListLimit:
		return 0, invalid("limit is %d; the maximum is %d", n, MaxListLimit)
	default:
		return int(n), nil
	}
}

func validateLeaseID(s string) error {
	if s == "" {
		return invalid("lease_id is required")
	}
	return nil
}

// firstErr returns the first non-nil error.
func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
