package cluster

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// faultNet simulates a network that can be partitioned. Each node gets a
// stream layer: a real TCP listener plus a dialer that refuses, and a set of
// open connections that are cut, whenever the two ends are on different
// sides of a partition.
//
// Raft sends every RPC on a connection the sender dialed, so refusing dials
// and closing dialed connections in both directions stops all traffic
// between the two sides, as a real partition would.
type faultNet struct {
	mu     sync.Mutex
	groups map[string]int // address -> partition group; empty means fully connected
	conns  map[*faultConn]struct{}
}

func newFaultNet() *faultNet {
	return &faultNet{groups: map[string]int{}, conns: map[*faultConn]struct{}{}}
}

// partition splits the network: addresses in different groups cannot talk.
func (n *faultNet) partition(groups ...[]string) {
	n.mu.Lock()
	n.groups = map[string]int{}
	for g, addrs := range groups {
		for _, a := range addrs {
			n.groups[a] = g
		}
	}
	var cut []*faultConn
	for c := range n.conns {
		if n.blockedLocked(c.from, c.to) {
			cut = append(cut, c)
		}
	}
	n.mu.Unlock()
	for _, c := range cut {
		_ = c.Close()
	}
}

// heal reconnects everything.
func (n *faultNet) heal() {
	n.mu.Lock()
	n.groups = map[string]int{}
	n.mu.Unlock()
}

func (n *faultNet) blockedLocked(a, b string) bool {
	ga, okA := n.groups[a]
	gb, okB := n.groups[b]
	return okA && okB && ga != gb
}

// stream returns the stream layer for the node listening on a real TCP
// address.
func (n *faultNet) stream(addr string) (*faultStream, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &faultStream{Listener: l, net: n, self: addr}, nil
}

type faultStream struct {
	net.Listener
	net  *faultNet
	self string
}

var errPartitioned = errors.New("faultnet: partitioned")

func (s *faultStream) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	to := string(address)
	s.net.mu.Lock()
	blocked := s.net.blockedLocked(s.self, to)
	s.net.mu.Unlock()
	if blocked {
		time.Sleep(10 * time.Millisecond) // a refused dial should not spin
		return nil, errPartitioned
	}
	c, err := net.DialTimeout("tcp", to, timeout)
	if err != nil {
		return nil, err
	}
	fc := &faultConn{Conn: c, net: s.net, from: s.self, to: to}
	s.net.mu.Lock()
	s.net.conns[fc] = struct{}{}
	s.net.mu.Unlock()
	return fc, nil
}

func (s *faultStream) Addr() net.Addr { return s.Listener.Addr() }

type faultConn struct {
	net.Conn
	net      *faultNet
	from, to string
	once     sync.Once
}

func (c *faultConn) Close() error {
	c.once.Do(func() {
		c.net.mu.Lock()
		delete(c.net.conns, c)
		c.net.mu.Unlock()
	})
	return c.Conn.Close()
}
