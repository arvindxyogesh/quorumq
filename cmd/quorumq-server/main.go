// Command quorumq-server runs one quorumq broker node.
//
// A three-node cluster on one machine:
//
//	PEERS=n1=127.0.0.1:7001/127.0.0.1:7071,n2=127.0.0.1:7002/127.0.0.1:7072,n3=127.0.0.1:7003/127.0.0.1:7073
//	quorumq-server -node-id n1 -raft-addr 127.0.0.1:7001 -addr 127.0.0.1:7071 -http-addr 127.0.0.1:8081 -peers $PEERS -data-dir data/n1
//	(and likewise for n2 and n3)
//
// With no -peers the node forms a single-node cluster on its own.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	pb "github.com/arvindxyogesh/quorumq/api/quorumq/v1"
	"github.com/arvindxyogesh/quorumq/internal/cluster"
	"github.com/arvindxyogesh/quorumq/internal/server"
)

type flags struct {
	nodeID        string
	addr          string
	advertiseAddr string
	raftAddr      string
	raftAdvertise string
	httpAddr      string
	peers         string
	dataDir       string
	bootstrap     bool
	logLevel      string
}

func main() {
	if err := run(); err != nil {
		slog.Error("server exited", "err", err)
		os.Exit(1)
	}
}

func parseFlags() flags {
	var f flags
	flag.StringVar(&f.nodeID, "node-id", "n1", "unique ID of this node")
	flag.StringVar(&f.addr, "addr", "127.0.0.1:7070", "gRPC listen address")
	flag.StringVar(&f.advertiseAddr, "advertise-addr", "", "gRPC address clients use to reach this node (single-node mode only; defaults to -addr)")
	flag.StringVar(&f.raftAddr, "raft-addr", "127.0.0.1:7000", "Raft transport listen address")
	flag.StringVar(&f.raftAdvertise, "raft-advertise", "", "Raft address other nodes use to reach this node (defaults to this node's entry in -peers)")
	flag.StringVar(&f.httpAddr, "http-addr", "127.0.0.1:8080", "admin HTTP address (/healthz, /readyz, /status); empty disables it")
	flag.StringVar(&f.peers, "peers", "", "cluster members as id=raftAddr/clientAddr,...; empty runs a single-node cluster")
	flag.StringVar(&f.dataDir, "data-dir", "./data", "directory for the Raft log, snapshots and queue state")
	flag.BoolVar(&f.bootstrap, "bootstrap", true, "bootstrap the cluster from -peers if this node has no Raft state")
	flag.StringVar(&f.logLevel, "log-level", "info", "log level: debug, info, warn or error")
	flag.Parse()
	return f
}

func run() error {
	f := parseFlags()

	var level slog.Level
	if err := level.UnmarshalText([]byte(f.logLevel)); err != nil {
		return fmt.Errorf("invalid -log-level: %w", err)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})).With("node", f.nodeID)
	slog.SetDefault(logger)

	peers, err := peersFromFlags(f)
	if err != nil {
		return err
	}
	node, err := cluster.NewNode(cluster.Config{
		NodeID:        f.nodeID,
		RaftBind:      f.raftAddr,
		RaftAdvertise: f.raftAdvertise,
		DataDir:       f.dataDir,
		Peers:         peers,
		Bootstrap:     f.bootstrap,
	}, logger)
	if err != nil {
		return err
	}
	defer func() {
		if err := node.Shutdown(); err != nil {
			logger.Error("raft shutdown", "err", err)
		}
	}()

	lis, err := net.Listen("tcp", f.addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", f.addr, err)
	}
	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(logRequests(logger)))
	pb.RegisterQueueServiceServer(gs, server.New(cluster.NewBroker(node)))
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(gs, healthSrv)
	reflection.Register(gs)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 2)
	go func() { errCh <- gs.Serve(lis) }()

	var admin *http.Server
	if f.httpAddr != "" {
		admin = &http.Server{Addr: f.httpAddr, Handler: cluster.AdminHandler(node), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := admin.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- fmt.Errorf("admin http: %w", err)
			}
		}()
	}
	logger.Info("quorumq node started", "grpc", lis.Addr().String(), "raft", f.raftAddr,
		"http", f.httpAddr, "peers", len(peers), "data_dir", f.dataDir)

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	logger.Info("shutting down")
	healthSrv.Shutdown()
	if admin != nil {
		_ = admin.Close()
	}
	stopped := make(chan struct{})
	go func() { gs.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		gs.Stop()
	}
	return nil
}

func peersFromFlags(f flags) ([]cluster.Peer, error) {
	if f.peers != "" {
		return cluster.ParsePeers(f.peers)
	}
	client := f.advertiseAddr
	if client == "" {
		client = f.addr
	}
	raftAddr := f.raftAdvertise
	if raftAddr == "" {
		raftAddr = f.raftAddr
	}
	return []cluster.Peer{{ID: f.nodeID, RaftAddr: raftAddr, ClientAddr: client}}, nil
}

// logRequests logs every unary call at debug level, and server-side failures
// at error level. Client mistakes (bad arguments, stale leases) and leader
// redirects stay at debug.
func logRequests(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		code := status.Code(err)
		level := slog.LevelDebug
		if code == codes.Internal || code == codes.Unknown || code == codes.DataLoss {
			level = slog.LevelError
		}
		logger.Log(ctx, level, "rpc",
			"method", info.FullMethod,
			"code", code.String(),
			"duration", time.Since(start),
			"err", err)
		return resp, err
	}
}
