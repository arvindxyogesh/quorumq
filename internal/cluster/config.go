package cluster

import (
	"fmt"
	"strings"
	"time"
)

// Peer is one member of the cluster.
type Peer struct {
	ID string
	// RaftAddr is where other nodes reach this node's Raft transport.
	RaftAddr string
	// ClientAddr is where clients reach this node's gRPC API. Followers hand
	// it out as the leader hint, so it must be reachable by clients.
	ClientAddr string
}

// ParsePeers parses a comma-separated list of id=raftAddr/clientAddr, for
// example "n1=n1:7000/localhost:7071,n2=n2:7000/localhost:7072".
func ParsePeers(s string) ([]Peer, error) {
	var peers []Peer
	seen := make(map[string]bool)
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		id, addrs, ok := strings.Cut(item, "=")
		raftAddr, clientAddr, ok2 := strings.Cut(addrs, "/")
		if !ok || !ok2 || id == "" || raftAddr == "" || clientAddr == "" {
			return nil, fmt.Errorf("peer %q: want id=raftAddr/clientAddr", item)
		}
		if seen[id] {
			return nil, fmt.Errorf("peer %q listed twice", id)
		}
		seen[id] = true
		peers = append(peers, Peer{ID: id, RaftAddr: raftAddr, ClientAddr: clientAddr})
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("no peers in %q", s)
	}
	return peers, nil
}

// Timing holds Raft's failure-detection timeouts.
type Timing struct {
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
}

// LANTiming is tuned for nodes on one network (a laptop, a data centre or a
// Kubernetes cluster). hashicorp/raft's defaults (1s/1s/500ms) target WANs;
// halving them roughly halves the time to detect a dead leader, at the cost
// of more spurious elections on a congested network.
var LANTiming = Timing{
	HeartbeatTimeout:   500 * time.Millisecond,
	ElectionTimeout:    500 * time.Millisecond,
	LeaderLeaseTimeout: 250 * time.Millisecond,
}

// SnapshotPolicy controls when Raft snapshots the state machine and how much
// log it keeps afterwards. Snapshots bound both disk usage and the time a
// restarted or new node needs to catch up.
type SnapshotPolicy struct {
	// Threshold is how many log entries must accumulate since the last
	// snapshot before a new one is taken.
	Threshold uint64
	// Interval is how often Raft checks whether Threshold has been reached.
	Interval time.Duration
	// TrailingLogs is how many entries are kept after a snapshot, so a
	// follower that is only slightly behind can catch up from the log
	// instead of being sent the whole snapshot.
	TrailingLogs uint64
}

// DefaultSnapshotPolicy matches hashicorp/raft's defaults.
var DefaultSnapshotPolicy = SnapshotPolicy{Threshold: 8192, Interval: 2 * time.Minute, TrailingLogs: 10240}

// Config configures a Node.
type Config struct {
	NodeID string
	// RaftBind is the local address the Raft transport listens on.
	RaftBind string
	// RaftAdvertise is the address other nodes use to reach this node. It
	// defaults to this node's RaftAddr in Peers.
	RaftAdvertise string
	DataDir       string
	// Peers is the full initial membership, including this node.
	Peers []Peer
	// Bootstrap makes the node bootstrap the cluster from Peers if it has
	// no Raft state yet. Every node may be started with Bootstrap and the
	// same Peers: hashicorp/raft treats identical bootstraps as one.
	Bootstrap bool
	Timing    Timing
	Snapshot  SnapshotPolicy
	// ApplyTimeout bounds how long a proposal may wait to enter the log.
	ApplyTimeout time.Duration
	// RaftLogLevel is the hashicorp/raft log level ("info" by default).
	RaftLogLevel string
}

func (c *Config) self() (Peer, error) {
	for _, p := range c.Peers {
		if p.ID == c.NodeID {
			return p, nil
		}
	}
	return Peer{}, fmt.Errorf("node %q is not in the peer list", c.NodeID)
}
