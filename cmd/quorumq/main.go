// Command quorumq is a CLI for a quorumq broker.
//
//	quorumq [-addr host:port] <command> [flags]
//
// Every command prints its result as one JSON object, so it is easy to use
// from scripts (for example with jq).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/arvindxyogesh/quorumq/client"
)

const usage = `usage: quorumq [-addr host:port[,host:port...]] [-timeout D] [-no-retry] <command> [flags]

commands:
  submit  -queue Q -payload P [-max-retries N]   enqueue a job
  claim   -queue Q -worker W [-visibility D]     lease the next visible job
  ack     -job ID -lease L                       acknowledge a leased job
  nack    -job ID -lease L [-reason R]           report a failure (retry with backoff, or dead-letter)
  get     -job ID                                show a job
  stats   -queue Q                               show queue counts
  dead    -queue Q [-limit N]                    list dead-lettered jobs, oldest first
  redrive -job ID                                move a dead-lettered job back to its queue

-addr defaults to $QUORUMQ_ADDR, or localhost:7070. Give several nodes and
the CLI finds the leader itself.
`

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "quorumq:", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	global := flag.NewFlagSet("quorumq", flag.ContinueOnError)
	global.Usage = func() { _, _ = fmt.Fprint(global.Output(), usage) }
	addr := global.String("addr", envOr("QUORUMQ_ADDR", "localhost:7070"), "comma-separated broker addresses")
	timeout := global.Duration("timeout", 10*time.Second, "per-request timeout")
	noRetry := global.Bool("no-retry", false, "send one attempt to the first address only: no leader redirects or retries")
	if err := global.Parse(args); err != nil {
		return err
	}
	if global.NArg() == 0 {
		global.Usage()
		return errors.New("missing command")
	}

	var opts []client.Option
	if *noRetry {
		opts = append(opts, client.WithNoRetry())
	}
	c, err := client.New(strings.Split(*addr, ","), opts...)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	cmd, cmdArgs := global.Arg(0), global.Args()[1:]
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	switch cmd {
	case "submit":
		queue := fs.String("queue", "", "queue name")
		payload := fs.String("payload", "", "job payload")
		retries := fs.Uint("max-retries", 3, "redeliveries allowed after a failure")
		if err := fs.Parse(cmdArgs); err != nil {
			return err
		}
		id, err := c.Submit(ctx, *queue, []byte(*payload), uint32(*retries))
		if err != nil {
			return err
		}
		return printJSON(out, map[string]string{"job_id": id})

	case "claim":
		queue := fs.String("queue", "", "queue name")
		worker := fs.String("worker", hostname(), "worker ID")
		visibility := fs.Duration("visibility", 30*time.Second, "visibility timeout")
		if err := fs.Parse(cmdArgs); err != nil {
			return err
		}
		lease, err := c.Claim(ctx, *queue, *worker, *visibility)
		if err != nil {
			return err
		}
		if lease == nil {
			return printJSON(out, map[string]any{"job": nil})
		}
		job, err := protoToMap(lease.Job)
		if err != nil {
			return err
		}
		return printJSON(out, map[string]any{
			"job":              job,
			"lease_id":         lease.ID,
			"lease_expires_at": lease.ExpiresAt.Format(time.RFC3339Nano),
		})

	case "ack", "nack":
		job := fs.String("job", "", "job ID")
		lease := fs.String("lease", "", "lease ID from claim")
		reason := fs.String("reason", "", "failure reason (nack only)")
		if err := fs.Parse(cmdArgs); err != nil {
			return err
		}
		if cmd == "ack" {
			err = c.Ack(ctx, *job, *lease)
		} else {
			err = c.Nack(ctx, *job, *lease, *reason)
		}
		if err != nil {
			return err
		}
		return printJSON(out, map[string]string{"job_id": *job, "result": cmd + "ed"})

	case "dead":
		queue := fs.String("queue", "", "queue name")
		limit := fs.Uint("limit", 20, "maximum jobs to list (1-100)")
		if err := fs.Parse(cmdArgs); err != nil {
			return err
		}
		jobs, err := c.ListDead(ctx, *queue, uint32(*limit))
		if err != nil {
			return err
		}
		list := make([]map[string]any, 0, len(jobs))
		for _, j := range jobs {
			m, err := protoToMap(j)
			if err != nil {
				return err
			}
			list = append(list, m)
		}
		return printJSON(out, map[string]any{"jobs": list})

	case "redrive":
		job := fs.String("job", "", "job ID")
		if err := fs.Parse(cmdArgs); err != nil {
			return err
		}
		if err := c.Redrive(ctx, *job); err != nil {
			return err
		}
		return printJSON(out, map[string]string{"job_id": *job, "result": "redriven"})

	case "get":
		job := fs.String("job", "", "job ID")
		if err := fs.Parse(cmdArgs); err != nil {
			return err
		}
		j, err := c.Get(ctx, *job)
		if err != nil {
			return err
		}
		return printProto(out, j)

	case "stats":
		queue := fs.String("queue", "", "queue name")
		if err := fs.Parse(cmdArgs); err != nil {
			return err
		}
		st, err := c.Stats(ctx, *queue)
		if err != nil {
			return err
		}
		return printProto(out, st)

	default:
		global.Usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
}

// protoMarshal emits zero values too, so counts of 0 are printed rather than
// omitted.
var protoMarshal = protojson.MarshalOptions{EmitUnpopulated: true, UseProtoNames: true}

func protoToMap(m proto.Message) (map[string]any, error) {
	b, err := protoMarshal.Marshal(m)
	if err != nil {
		return nil, err
	}
	var v map[string]any
	return v, json.Unmarshal(b, &v)
}

func printProto(out io.Writer, m proto.Message) error {
	v, err := protoToMap(m)
	if err != nil {
		return err
	}
	return printJSON(out, v)
}

func printJSON(out io.Writer, v any) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "worker"
	}
	return h
}
