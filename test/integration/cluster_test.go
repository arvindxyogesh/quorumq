//go:build integration

// Package integration runs real quorumq-server processes and crashes them.
//
//	go test -tags integration -race -v ./test/integration
package integration

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

var serverBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "quorumq-it")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	serverBin = filepath.Join(dir, "quorumq-server")
	build := exec.Command("go", "build", "-o", serverBin, "../../cmd/quorumq-server")
	build.Stdout, build.Stderr = os.Stdout, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "building server:", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

type node struct {
	id, raftAddr, grpcAddr, httpAddr, dataDir, logPath string
	cmd                                                *exec.Cmd
}

type cluster struct {
	t     *testing.T
	nodes []*node
	peers string
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func startCluster(t *testing.T, size int) *cluster {
	t.Helper()
	c := &cluster{t: t}
	var peers []string
	for i := 1; i <= size; i++ {
		dir := t.TempDir()
		n := &node{
			id: fmt.Sprintf("n%d", i), raftAddr: freeAddr(t), grpcAddr: freeAddr(t), httpAddr: freeAddr(t),
			dataDir: filepath.Join(dir, "data"), logPath: filepath.Join(dir, "server.log"),
		}
		c.nodes = append(c.nodes, n)
		peers = append(peers, fmt.Sprintf("%s=%s/%s", n.id, n.raftAddr, n.grpcAddr))
	}
	c.peers = strings.Join(peers, ",")
	for _, n := range c.nodes {
		c.start(n)
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			c.kill(n)
		}
		if t.Failed() {
			for _, n := range c.nodes {
				logs, _ := os.ReadFile(n.logPath)
				t.Logf("---- %s log (last 4KB) ----\n%s", n.id, tail(logs, 4096))
			}
		}
	})
	c.waitReady()
	return c
}

func (c *cluster) start(n *node) {
	c.t.Helper()
	logFile, err := os.OpenFile(n.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		c.t.Fatal(err)
	}
	n.cmd = exec.Command(serverBin,
		"-node-id", n.id, "-raft-addr", n.raftAddr, "-addr", n.grpcAddr, "-http-addr", n.httpAddr,
		"-peers", c.peers, "-data-dir", n.dataDir)
	n.cmd.Stdout, n.cmd.Stderr = logFile, logFile
	if err := n.cmd.Start(); err != nil {
		c.t.Fatalf("start %s: %v", n.id, err)
	}
	go func() { _ = n.cmd.Wait(); _ = logFile.Close() }()
}

// kill crashes a node with SIGKILL: no graceful shutdown, no final fsync.
func (c *cluster) kill(n *node) {
	if n.cmd != nil && n.cmd.Process != nil {
		_ = n.cmd.Process.Signal(syscall.SIGKILL)
		n.cmd = nil
	}
}

func (c *cluster) grpcAddrs() []string {
	addrs := make([]string, len(c.nodes))
	for i, n := range c.nodes {
		addrs[i] = n.grpcAddr
	}
	return addrs
}

type nodeStatus struct {
	NodeID          string `json:"node_id"`
	State           string `json:"state"`
	LeaderID        string `json:"leader_id"`
	FSMAppliedIndex uint64 `json:"fsm_applied_index"`
}

var httpClient = &http.Client{Timeout: 500 * time.Millisecond}

func (c *cluster) status(n *node) (nodeStatus, bool) {
	var st nodeStatus
	if n.cmd == nil {
		return st, false
	}
	resp, err := httpClient.Get("http://" + n.httpAddr + "/status")
	if err != nil {
		return st, false
	}
	defer func() { _ = resp.Body.Close() }()
	return st, json.NewDecoder(resp.Body).Decode(&st) == nil
}

// leader returns the running node that reports itself as leader.
func (c *cluster) leader() *node {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range c.nodes {
			if st, ok := c.status(n); ok && st.State == "Leader" {
				return n
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	c.t.Fatal("no leader elected")
	return nil
}

func (c *cluster) waitReady() {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for _, n := range c.nodes {
		for {
			resp, err := httpClient.Get("http://" + n.httpAddr + "/readyz")
			if err == nil {
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					break
				}
			}
			if time.Now().After(deadline) {
				c.t.Fatalf("%s never became ready", n.id)
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
}

// waitConverged waits until every running node has applied the same log
// index, i.e. the replicas hold identical state.
func (c *cluster) waitConverged() uint64 {
	c.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var idx uint64
		same := true
		for _, n := range c.nodes {
			st, ok := c.status(n)
			if !ok {
				same = false
				break
			}
			if idx == 0 {
				idx = st.FSMAppliedIndex
			}
			same = same && st.FSMAppliedIndex == idx
		}
		if same && idx > 0 {
			return idx
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatal("replicas did not converge")
	return 0
}

func tail(b []byte, n int) []byte {
	if len(b) > n {
		return b[len(b)-n:]
	}
	return b
}
