package cluster

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/arvindxyogesh/quorumq/internal/metrics"
	"github.com/arvindxyogesh/quorumq/internal/queue"
)

// Node is one quorumq broker: a Raft replica plus the queue state machine
// it drives.
type Node struct {
	cfg       Config
	self      Peer
	raft      *raft.Raft
	store     *queue.Store
	logStore  *raftboltdb.BoltStore
	snaps     *raft.FileSnapshotStore
	transport *raft.NetworkTransport
	peers     map[raft.ServerID]Peer
	logger    *slog.Logger

	observer     *raft.Observer
	observations chan raft.Observation
	stopObserver chan struct{}
	observerDone chan struct{}
}

// On-disk layout of a node's data directory.
const (
	fsmFile          = "fsm.db"  // queue state machine (BoltDB)
	raftFile         = "raft.db" // Raft log and stable store (BoltDB)
	snapshotsRetain  = 2
	transportMaxPool = 3
	transportTimeout = 10 * time.Second
)

// NewNode opens a node's storage and starts its Raft replica.
func NewNode(cfg Config, logger *slog.Logger) (*Node, error) {
	self, err := cfg.self()
	if err != nil {
		return nil, err
	}
	if cfg.RaftAdvertise == "" {
		cfg.RaftAdvertise = self.RaftAddr
	}
	if cfg.Timing == (Timing{}) {
		cfg.Timing = LANTiming
	}
	if cfg.Snapshot == (SnapshotPolicy{}) {
		cfg.Snapshot = DefaultSnapshotPolicy
	}
	if cfg.RaftLogLevel == "" {
		cfg.RaftLogLevel = "info"
	}
	if cfg.ApplyTimeout == 0 {
		cfg.ApplyTimeout = 5 * time.Second
	}
	if err := os.MkdirAll(cfg.DataDir, 0o750); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	n := &Node{cfg: cfg, self: self, logger: logger, peers: make(map[raft.ServerID]Peer)}
	for _, p := range cfg.Peers {
		n.peers[raft.ServerID(p.ID)] = p
	}
	if err := n.start(); err != nil {
		_ = n.Shutdown()
		return nil, err
	}
	return n, nil
}

func (n *Node) start() error {
	cfg := n.cfg
	hlog := hclog.New(&hclog.LoggerOptions{
		Name:       "raft",
		Level:      hclog.LevelFromString(cfg.RaftLogLevel),
		JSONFormat: true,
		Output:     os.Stderr,
	}).With("node", cfg.NodeID)

	var err error
	if n.store, err = queue.Open(filepath.Join(cfg.DataDir, fsmFile)); err != nil {
		return err
	}
	if n.logStore, err = raftboltdb.New(raftboltdb.Options{Path: filepath.Join(cfg.DataDir, raftFile)}); err != nil {
		return fmt.Errorf("open raft log: %w", err)
	}
	logs, err := raft.NewLogCache(512, n.logStore)
	if err != nil {
		return err
	}
	if n.snaps, err = raft.NewFileSnapshotStoreWithLogger(cfg.DataDir, snapshotsRetain, hlog); err != nil {
		return fmt.Errorf("open snapshot store: %w", err)
	}
	snaps := n.snaps
	if err := n.restoreIfBehind(snaps); err != nil {
		return err
	}

	advertise, err := resolveWithRetry(cfg.RaftAdvertise, 30*time.Second)
	if err != nil {
		return fmt.Errorf("resolve raft advertise address %q: %w", cfg.RaftAdvertise, err)
	}
	if n.transport, err = raft.NewTCPTransportWithLogger(cfg.RaftBind, advertise, transportMaxPool, transportTimeout, hlog); err != nil {
		return fmt.Errorf("start raft transport on %s: %w", cfg.RaftBind, err)
	}

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.NodeID)
	rc.Logger = hlog
	rc.HeartbeatTimeout = cfg.Timing.HeartbeatTimeout
	rc.ElectionTimeout = cfg.Timing.ElectionTimeout
	rc.LeaderLeaseTimeout = cfg.Timing.LeaderLeaseTimeout
	rc.SnapshotThreshold = cfg.Snapshot.Threshold
	rc.SnapshotInterval = cfg.Snapshot.Interval
	rc.TrailingLogs = cfg.Snapshot.TrailingLogs
	// The FSM is persistent and remembers the last index it applied, so on
	// restart Raft only needs to replay entries after that point; restoring
	// the latest snapshot first would throw away newer on-disk state.
	rc.NoSnapshotRestoreOnStart = true

	if n.raft, err = raft.NewRaft(rc, NewFSM(n.store, cfg.Metrics), logs, n.logStore, snaps, n.transport); err != nil {
		return fmt.Errorf("start raft: %w", err)
	}
	n.watchLeader()

	if cfg.Bootstrap {
		if err := n.bootstrap(logs, snaps); err != nil {
			return err
		}
	}
	return nil
}

// resolveWithRetry resolves addr, retrying for up to timeout. In Kubernetes
// a pod's own DNS record can appear a few seconds after the pod starts.
func resolveWithRetry(addr string, timeout time.Duration) (*net.TCPAddr, error) {
	deadline := time.Now().Add(timeout)
	for {
		a, err := net.ResolveTCPAddr("tcp", addr)
		if err == nil || time.Now().After(deadline) {
			return a, err
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// restoreIfBehind restores the newest snapshot if the FSM's own state is
// older than it, for example because fsm.db was lost. Normally the FSM is
// ahead of every snapshot and this is a no-op.
func (n *Node) restoreIfBehind(snaps *raft.FileSnapshotStore) error {
	list, err := snaps.List()
	if err != nil || len(list) == 0 {
		return err
	}
	applied, err := n.store.AppliedIndex()
	if err != nil {
		return err
	}
	latest := list[0]
	if applied >= latest.Index {
		return nil
	}
	n.logger.Info("fsm is behind the latest snapshot; restoring",
		"fsm_applied", applied, "snapshot_index", latest.Index)
	_, rc, err := snaps.Open(latest.ID)
	if err != nil {
		return fmt.Errorf("open snapshot %s: %w", latest.ID, err)
	}
	defer func() { _ = rc.Close() }()
	return n.store.Restore(rc)
}

func (n *Node) bootstrap(logs raft.LogStore, snaps raft.SnapshotStore) error {
	has, err := raft.HasExistingState(logs, n.logStore, snaps)
	if err != nil {
		return err
	}
	if has {
		return nil
	}
	servers := make([]raft.Server, 0, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		servers = append(servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(p.ID),
			Address:  raft.ServerAddress(p.RaftAddr),
		})
	}
	err = n.raft.BootstrapCluster(raft.Configuration{Servers: servers}).Error()
	if err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
		return fmt.Errorf("bootstrap cluster: %w", err)
	}
	n.logger.Info("bootstrapped cluster", "peers", len(servers))
	return nil
}

// watchLeader counts leader changes. Raft delivers a LeaderObservation each
// time this node's view of the leader changes, including to "no leader"
// when an election starts.
func (n *Node) watchLeader() {
	n.observations = make(chan raft.Observation, 16)
	n.stopObserver = make(chan struct{})
	n.observerDone = make(chan struct{})
	n.observer = raft.NewObserver(n.observations, false, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	})
	n.raft.RegisterObserver(n.observer)
	go func() {
		defer close(n.observerDone)
		for {
			select {
			case o := <-n.observations:
				lo := o.Data.(raft.LeaderObservation)
				n.cfg.Metrics.LeaderChanged()
				n.logger.Info("leader changed", "leader_id", string(lo.LeaderID))
			case <-n.stopObserver:
				return
			}
		}
	}()
}

// Shutdown stops Raft and closes storage.
func (n *Node) Shutdown() error {
	var errs []error
	if n.observer != nil {
		n.raft.DeregisterObserver(n.observer)
		close(n.stopObserver)
		<-n.observerDone
		n.observer = nil
	}
	if n.raft != nil {
		errs = append(errs, n.raft.Shutdown().Error())
	}
	if n.transport != nil {
		errs = append(errs, n.transport.Close())
	}
	errs = append(errs, n.closeStorage())
	return errors.Join(errs...)
}

func (n *Node) closeStorage() error {
	var errs []error
	if n.logStore != nil {
		errs = append(errs, n.logStore.Close())
	}
	if n.store != nil {
		errs = append(errs, n.store.Close())
	}
	return errors.Join(errs...)
}

// IsLeader reports whether this node is currently the Raft leader.
func (n *Node) IsLeader() bool { return n.raft.State() == raft.Leader }

// LeaderClientAddr returns the leader's client address, or "" if there is no
// known leader (for example during an election).
func (n *Node) LeaderClientAddr() string {
	_, id := n.raft.LeaderWithID()
	if id == "" {
		return ""
	}
	return n.peers[id].ClientAddr
}

// Store returns the node's state machine store (for the metrics collector).
func (n *Node) Store() *queue.Store { return n.store }

// RaftStatus returns the Raft state exported as metrics.
func (n *Node) RaftStatus() metrics.RaftStatus {
	st := n.Status()
	return metrics.RaftStatus{
		IsLeader:          st.State == raft.Leader.String(),
		HasLeader:         st.LeaderID != "",
		Term:              st.Term,
		LastLogIndex:      st.LastLogIndex,
		CommitIndex:       st.CommitIndex,
		AppliedIndex:      st.AppliedIndex,
		FSMAppliedIndex:   st.FSMAppliedIndex,
		LastSnapshotIndex: st.LastSnapshotIndex,
	}
}

// Status is a snapshot of a node's Raft state, for the admin endpoint.
type Status struct {
	NodeID          string `json:"node_id"`
	State           string `json:"state"`
	Term            uint64 `json:"term"`
	LeaderID        string `json:"leader_id"`
	LeaderAddr      string `json:"leader_addr"`
	LastLogIndex    uint64 `json:"last_log_index"`
	CommitIndex     uint64 `json:"commit_index"`
	AppliedIndex    uint64 `json:"applied_index"`
	FSMAppliedIndex uint64 `json:"fsm_applied_index"`
	// LastSnapshotIndex is the log index covered by the newest snapshot;
	// entries up to roughly this point may have been compacted away.
	LastSnapshotIndex uint64 `json:"last_snapshot_index"`
	Peers             int    `json:"peers"`
}

// Status returns the node's current Raft state.
func (n *Node) Status() Status {
	_, leaderID := n.raft.LeaderWithID()
	stats := n.raft.Stats()
	term, _ := strconv.ParseUint(stats["term"], 10, 64)
	lastSnap, _ := strconv.ParseUint(stats["last_snapshot_index"], 10, 64)
	fsmApplied, _ := n.store.AppliedIndex()
	return Status{
		NodeID:            n.cfg.NodeID,
		State:             n.raft.State().String(),
		Term:              term,
		LeaderID:          string(leaderID),
		LeaderAddr:        n.LeaderClientAddr(),
		LastLogIndex:      n.raft.LastIndex(),
		CommitIndex:       n.raft.CommitIndex(),
		AppliedIndex:      n.raft.AppliedIndex(),
		FSMAppliedIndex:   fsmApplied,
		LastSnapshotIndex: lastSnap,
		Peers:             len(n.peers),
	}
}
