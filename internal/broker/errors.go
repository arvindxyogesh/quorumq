package broker

import "fmt"

// NotLeaderError is returned when a request reaches a node that cannot serve
// it because it is not the Raft leader. Leader is the leader's client
// address, or "" while an election is in progress.
type NotLeaderError struct {
	Leader string
}

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "not the leader, and no leader is currently known"
	}
	return fmt.Sprintf("not the leader; the leader is %s", e.Leader)
}

// UnavailableError wraps a failure that a client should retry, possibly on
// another node: the node is shutting down, or a proposal timed out before
// it entered the log.
type UnavailableError struct {
	Err error
}

func (e *UnavailableError) Error() string { return "node unavailable: " + e.Err.Error() }
func (e *UnavailableError) Unwrap() error { return e.Err }
