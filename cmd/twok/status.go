package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/aenix-io/twok/internal/agent"
	"github.com/aenix-io/twok/internal/arbiter"
	"github.com/aenix-io/twok/internal/engine"
)

type memberOutput struct {
	ID         string   `json:"id"`
	Name       string   `json:"name,omitempty"`
	PeerURLs   []string `json:"peerURLs,omitempty"`
	ClientURLs []string `json:"clientURLs,omitempty"`
}

type membersOutput struct {
	Voters   []memberOutput `json:"voters"`
	Learners []memberOutput `json:"learners"`
}

type localOutput struct {
	ClusterID    string `json:"clusterID"`
	MemberID     string `json:"memberID"`
	Leader       string `json:"leader,omitempty"`
	Term         uint64 `json:"term"`
	CommitIndex  uint64 `json:"commitIndex"`
	AppliedIndex uint64 `json:"appliedIndex"`
	IsLearner    bool   `json:"isLearner"`
}

type statusOutput struct {
	// Role is the role a single look can tell. Isolation needs the time
	// without a leader, which only the agent tracks, so a member without a
	// leader shows as pair here.
	Role       engine.Role    `json:"role"`
	Note       string         `json:"note"`
	Generation *uint64        `json:"generation,omitempty"`
	Local      *localOutput   `json:"local,omitempty"`
	Members    *membersOutput `json:"members,omitempty"`
	Arbiter    *recordOutput  `json:"arbiter,omitempty"`
	WAL        any            `json:"wal,omitempty"`
	Errors     []string       `json:"errors,omitempty"`
}

func runStatus(ctx context.Context, args []string, e env) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	var m memberFlags
	m.register(fs)
	arbiterURL := fs.String("arbiter", "", "the arbiter URL; without it the role cannot be stale")
	if code, ok := parse(fs, args, e); !ok {
		return code
	}
	ctx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()
	out := status(ctx, m, *arbiterURL)
	if err := printJSON(e.stdout, out); err != nil {
		return exitFailed
	}
	if len(out.Errors) > 0 {
		return exitFailed
	}
	return exitOK
}

func status(ctx context.Context, m memberFlags, arbiterURL string) statusOutput {
	var out statusOutput
	fail := func(format string, args ...any) { out.Errors = append(out.Errors, fmt.Sprintf(format, args...)) }
	obs := engine.Observation{Now: time.Now(), ArbiterReady: true, CanControl: true}

	if c, err := dialMember(m.endpoint, "", m.tls); err != nil {
		fail("%v", err)
	} else {
		defer func() { _ = c.Close() }()
		observeMember(ctx, c, &obs, &out, fail)
	}
	if arbiterURL != "" {
		obs.Arbiter = readArbiter(ctx, arbiterURL, &out, fail)
	}
	if m.dataDir != "" {
		if w, err := inspectWAL(m.dataDir); err != nil {
			fail("WAL: %v", err)
		} else {
			out.WAL = w
		}
	}
	p := engine.DefaultParams()
	p.Mode = engine.ModeDryRun
	d := engine.New(p).Observe(obs)
	out.Role, out.Note = d.Role, d.Note
	return out
}

func observeMember(ctx context.Context, c agent.Client, obs *engine.Observation, out *statusOutput, fail func(string, ...any)) {
	st, err := c.Status(ctx)
	if err != nil {
		fail("%v", err)
		return
	}
	obs.Local = &st
	out.Local = &localOutput{
		ClusterID: engine.Hex(st.ClusterID), MemberID: engine.Hex(st.MemberID), Term: st.Term,
		CommitIndex: st.CommitIndex, AppliedIndex: st.AppliedIndex, IsLearner: st.IsLearner,
	}
	if st.Leader != 0 {
		out.Local.Leader = engine.Hex(st.Leader)
	}
	switch ms, err := c.Members(ctx); {
	case err == nil:
		obs.Members = &ms
		out.Members = &membersOutput{Voters: membersJSON(ms.Voters), Learners: membersJSON(ms.Learners)}
	case !errors.Is(err, agent.ErrLearner):
		fail("%v", err)
	}
	if g, err := c.Generation(ctx); err != nil {
		fail("%v", err)
	} else {
		obs.Generation, obs.GenerationKnown = g, true
		out.Generation = &g
	}
}

func readArbiter(ctx context.Context, arbiterURL string, out *statusOutput, fail func(string, ...any)) *engine.ArbiterRead {
	arb, err := arbiter.Open(arbiterURL)
	if err != nil {
		fail("%v", err)
		return nil
	}
	rec, v, err := arb.Read(ctx)
	switch {
	case errors.Is(err, arbiter.ErrNotFound):
		out.Arbiter = &recordOutput{Version: v}
		return &engine.ArbiterRead{Version: v}
	case err != nil:
		fail("%s: %v", arb, err)
		return nil
	}
	out.Arbiter = &recordOutput{Found: true, Version: v, Record: &rec}
	return &engine.ArbiterRead{Found: true, Record: rec, Version: v}
}

func membersJSON(ms []engine.Member) []memberOutput {
	out := make([]memberOutput, 0, len(ms))
	for _, m := range ms {
		out = append(out, memberOutput{ID: engine.Hex(m.ID), Name: m.Name, PeerURLs: m.PeerURLs, ClientURLs: m.ClientURLs})
	}
	return out
}
