// Command quorumq-server runs a quorumq broker node.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	pb "github.com/arvindxyogesh/quorumq/api/quorumq/v1"
	"github.com/arvindxyogesh/quorumq/internal/broker"
	"github.com/arvindxyogesh/quorumq/internal/queue"
	"github.com/arvindxyogesh/quorumq/internal/server"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server exited", "err", err)
		os.Exit(1)
	}
}

func run() error {
	addr := flag.String("addr", ":7070", "gRPC listen address")
	dataDir := flag.String("data-dir", "./data", "directory for persistent state")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn or error")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return fmt.Errorf("invalid -log-level: %w", err)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	if err := os.MkdirAll(*dataDir, 0o750); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	store, err := queue.Open(filepath.Join(*dataDir, "queue.db"))
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *addr, err)
	}

	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(logRequests(logger)))
	pb.RegisterQueueServiceServer(gs, server.New(broker.NewLocal(store)))
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(gs, healthSrv)
	reflection.Register(gs)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() { errCh <- gs.Serve(lis) }()
	logger.Info("quorumq server started", "addr", lis.Addr().String(), "data_dir", *dataDir)

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	logger.Info("shutting down")
	healthSrv.Shutdown()
	stopped := make(chan struct{})
	go func() { gs.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		gs.Stop()
	}
	if err := <-errCh; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	return nil
}

// logRequests logs every unary call at debug level, and server-side failures
// at error level. Client mistakes (bad arguments, stale leases) stay at debug.
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
