// Command quorumq-load generates load against a quorumq cluster and measures
// it.
//
//	quorumq-load run   -addr host:port,... [flags]   producers + workers; writes results
//	quorumq-load probe -addr host:port,... -since NS  time until writes succeed again
//
// run prints a one-line summary and, with -out DIR, writes DIR/result.json
// plus DIR/submitted.csv and DIR/completed.csv (one row per job) so the
// reconciliation of submitted against completed jobs can be audited.
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/arvindxyogesh/quorumq/client"
	"github.com/arvindxyogesh/quorumq/internal/loadgen"
)

func main() {
	if err := dispatch(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "quorumq-load:", err)
		os.Exit(1)
	}
}

func dispatch(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: quorumq-load run|probe [flags]")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	switch args[0] {
	case "run":
		return runCmd(ctx, args[1:])
	case "probe":
		return probeCmd(ctx, args[1:])
	default:
		return fmt.Errorf("unknown command %q (want run or probe)", args[0])
	}
}

func runCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	addr := fs.String("addr", "localhost:7071,localhost:7072,localhost:7073", "comma-separated broker addresses")
	var cfg loadgen.Config
	fs.StringVar(&cfg.Queue, "queue", "load", "queue to use")
	fs.IntVar(&cfg.Producers, "producers", 8, "concurrent producers")
	fs.IntVar(&cfg.Workers, "workers", 8, "concurrent workers")
	fs.Float64Var(&cfg.Rate, "rate", 0, "target total submits per second; 0 = closed loop (as fast as possible)")
	fs.DurationVar(&cfg.Duration, "duration", 30*time.Second, "how long producers submit")
	fs.IntVar(&cfg.PayloadSize, "payload-size", 256, "payload bytes per job")
	fs.DurationVar(&cfg.Visibility, "visibility", 30*time.Second, "lease length for workers")
	maxRetries := fs.Uint("max-retries", 5, "max_retries for submitted jobs")
	fs.DurationVar(&cfg.DrainTimeout, "drain-timeout", 2*time.Minute, "how long to wait for outstanding jobs after producers stop")
	fs.DurationVar(&cfg.IdleSleep, "idle-sleep", 2*time.Millisecond, "worker pause after an empty claim")
	fs.StringVar(&cfg.RunID, "run-id", "", "unique ID for this run's job tokens (default: derived from the time)")
	out := fs.String("out", "", "directory for result.json and the ID files")
	label := fs.String("label", "", "free-form label stored in result.json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg.MaxRetries = uint32(*maxRetries)

	c, err := client.New(strings.Split(*addr, ","))
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()

	res, err := loadgen.Run(ctx, c, cfg)
	if err != nil {
		return err
	}
	fmt.Printf("submitted %d (%.0f/s)  completed %d (%.0f/s in window)  lost %d  dup %d  "+
		"submit-to-ack p50 %.1fms p95 %.1fms p99 %.1fms  backlog@end %d  drain %.1fs\n",
		res.Accepted, res.SubmitRate, res.Completed, res.AckRate, res.Lost, res.Duplicates,
		res.Latency.P50, res.Latency.P95, res.Latency.P99, res.BacklogAtWindowEnd, res.DrainSec)
	if *out == "" {
		return nil
	}
	return writeResults(*out, *label, *addr, res)
}

func writeResults(dir, label, addrs string, res *loadgen.Result) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	doc := struct {
		Label string `json:"label,omitempty"`
		Addrs string `json:"addrs"`
		*loadgen.Result
	}{label, addrs, res}
	b, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "result.json"), append(b, '\n'), 0o644); err != nil {
		return err
	}

	submitted := res.SubmittedIDs()
	rows := make([][]string, 0, len(submitted))
	for token, id := range submitted {
		rows = append(rows, []string{token, id})
	}
	if err := writeCSV(filepath.Join(dir, "submitted.csv"), []string{"token", "job_id"}, rows); err != nil {
		return err
	}
	completed := res.CompletedIDs()
	rows = rows[:0]
	for token, c := range completed {
		rows = append(rows, []string{token, c.JobID, strconv.FormatFloat(c.LatencyMS, 'f', 3, 64)})
	}
	return writeCSV(filepath.Join(dir, "completed.csv"), []string{"token", "job_id", "submit_to_ack_ms"}, rows)
}

func writeCSV(path string, header []string, rows [][]string) error {
	sort.Slice(rows, func(i, j int) bool { return rows[i][0] < rows[j][0] })
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	_ = w.Write(header)
	_ = w.WriteAll(rows)
	if err := w.Error(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func probeCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("probe", flag.ContinueOnError)
	addr := fs.String("addr", "localhost:7071,localhost:7072,localhost:7073", "comma-separated broker addresses")
	queue := fs.String("queue", "failover-probe", "queue the probe submits to")
	since := fs.Int64("since", 0, "Unix time in nanoseconds the failure started (default: now)")
	timeout := fs.Duration("timeout", time.Minute, "give up after this long")
	backoff := fs.Duration("backoff", 10*time.Millisecond, "retry interval; bounds the measurement resolution")
	if err := fs.Parse(args); err != nil {
		return err
	}
	start := time.Now()
	if *since != 0 {
		start = time.Unix(0, *since)
	}
	c, err := client.New(strings.Split(*addr, ","),
		client.WithAttemptTimeout(250*time.Millisecond), client.WithBackoff(*backoff, *backoff))
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	res, err := loadgen.Probe(ctx, c, *queue, start)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(res)
}
