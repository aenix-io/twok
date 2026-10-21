package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/aenix-io/twok/internal/arbiter"
	"github.com/aenix-io/twok/internal/arbiter/httparb"
	"github.com/aenix-io/twok/internal/engine"
	"github.com/aenix-io/twok/internal/version"
)

// commandTimeout bounds the arbiter calls of a one-shot command.
const commandTimeout = 30 * time.Second

// recordOutput is what the arbiter commands print.
type recordOutput struct {
	Found   bool            `json:"found"`
	Version arbiter.Version `json:"version,omitempty"`
	Record  *arbiter.Record `json:"record,omitempty"`
}

func runArbiterGet(ctx context.Context, args []string, e env) int {
	fs := flag.NewFlagSet("arbiter get", flag.ContinueOnError)
	arbiterURL := fs.String("arbiter", "", "the arbiter URL")
	if code, ok := parse(fs, args, e); !ok {
		return code
	}
	if !required(e, fs.Name(), [2]string{"arbiter", *arbiterURL}) {
		return exitInvalid
	}
	arb, err := arbiter.Open(*arbiterURL)
	if err != nil {
		fmt.Fprintf(e.stderr, "twok arbiter get: %v\n", err)
		return exitFailed
	}
	ctx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()
	rec, v, err := arb.Read(ctx)
	out := recordOutput{Found: err == nil, Version: v}
	switch {
	case errors.Is(err, arbiter.ErrNotFound):
	case err != nil:
		fmt.Fprintf(e.stderr, "twok arbiter get: %s: %v\n", arb, err)
		return exitFailed
	default:
		out.Record = &rec
	}
	if err := printJSON(e.stdout, out); err != nil {
		return exitFailed
	}
	return exitOK
}

// runArbiterClaim moves the generation forward by hand. It is the way out
// of a double failure: the survivor died before the other member rejoined,
// and the operator accepts that the writes the survivor took alone are
// lost. twok on the named member finishes the takeover at its next poll.
func runArbiterClaim(ctx context.Context, args []string, e env) int {
	fs := flag.NewFlagSet("arbiter claim", flag.ContinueOnError)
	arbiterURL := fs.String("arbiter", "", "the arbiter URL")
	survivor := fs.String("survivor", "", "etcd member name of the new survivor")
	accept := fs.Bool("accept-data-loss", false, "confirm that writes the current survivor took after the last common point are lost")
	if code, ok := parse(fs, args, e); !ok {
		return code
	}
	if !required(e, fs.Name(), [2]string{"arbiter", *arbiterURL}, [2]string{"survivor", *survivor}) {
		return exitInvalid
	}
	if !*accept {
		fmt.Fprintln(e.stderr, "twok arbiter claim: a manual claim discards every write the current survivor took alone;"+
			" pass --accept-data-loss to confirm")
		return exitInvalid
	}
	arb, err := arbiter.Open(*arbiterURL)
	if err != nil {
		fmt.Fprintf(e.stderr, "twok arbiter claim: %v\n", err)
		return exitFailed
	}
	ctx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()
	cur, v, err := arb.Read(ctx)
	if err != nil && !errors.Is(err, arbiter.ErrNotFound) {
		fmt.Fprintf(e.stderr, "twok arbiter claim: %s: %v\n", arb, err)
		return exitFailed
	}
	rec := arbiter.Record{
		SchemaVersion: arbiter.SchemaVersion,
		ClusterID:     cur.ClusterID,
		Generation:    cur.Generation + 1,
		Survivor:      *survivor,
		Since:         time.Now().UTC(),
		TwokVersion:   version.Version,
		Reason:        engine.ReasonManual,
	}
	next, err := arb.CompareAndSwap(ctx, v, rec)
	if err != nil {
		if errors.Is(err, arbiter.ErrConflict) {
			err = fmt.Errorf("the record changed while the claim was written, read it again: %w", err)
		}
		fmt.Fprintf(e.stderr, "twok arbiter claim: %s: %v\n", arb, err)
		return exitFailed
	}
	if err := printJSON(e.stdout, recordOutput{Found: true, Version: next, Record: &rec}); err != nil {
		return exitFailed
	}
	return exitOK
}

func runArbiterServe(ctx context.Context, args []string, e env) int {
	fs := flag.NewFlagSet("arbiter serve", flag.ContinueOnError)
	dir := fs.String("dir", "", "directory that keeps the records; it must exist")
	listen := fs.String("listen", ":8080", "address to listen on")
	if code, ok := parse(fs, args, e); !ok {
		return code
	}
	if !required(e, fs.Name(), [2]string{"dir", *dir}) {
		return exitInvalid
	}
	logger := newLogger(e.stderr).With("component", "arbiter")
	l, err := net.Listen("tcp", *listen)
	if err != nil {
		logger.Error("listen", "error", err)
		return exitFailed
	}
	srv := &http.Server{Handler: httparb.NewServer(*dir), ReadHeaderTimeout: 10 * time.Second}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(l) }()
	logger.Info("arbiter serving", "address", l.Addr().String(), "dir", *dir, "version", version.Version)
	select {
	case err := <-served:
		logger.Error("arbiter stopped", "error", err)
		return exitFailed
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		// Connections that never sent a request count as active for a
		// while; nothing is lost by closing them.
		_ = srv.Close()
		logger.Info("closed the remaining connections", "error", err)
	}
	logger.Info("arbiter stopped")
	return exitOK
}
