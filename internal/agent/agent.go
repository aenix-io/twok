// Package agent runs the decision engine against the local etcd member: it
// polls the member, asks the arbiter when the engine wants to know, and
// executes the engine's actions through narrow interfaces to etcd's client
// API, the WAL and the etcd process.
package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aenix-io/twok/internal/arbiter"
	"github.com/aenix-io/twok/internal/config"
	"github.com/aenix-io/twok/internal/engine"
	"github.com/aenix-io/twok/internal/etcdargs"
	"github.com/aenix-io/twok/internal/version"
)

const (
	// pollTimeout bounds the calls of one poll to the local member, and
	// every call to the other member.
	pollTimeout = 3 * time.Second
	// remoteTimeout bounds the status calls to learners, so that a learner
	// across a slow link cannot hold up the poll.
	remoteTimeout = time.Second
	// arbiterTimeout bounds every call to the arbiter.
	arbiterTimeout = 5 * time.Second
	// stallTimeout is how long the loop may go without progress before the
	// agent exits for the platform to restart it. Every call has a
	// timeout, and the long waits of a takeover report progress, so only a
	// bug gets near it.
	stallTimeout = 30 * time.Second
	// stopTimeout bounds the wait for etcd to exit after SIGTERM.
	stopTimeout = time.Minute
	// restartTimeout bounds the wait for etcd to answer again after an
	// edit. The kubelet restarts a container with a growing backoff, so a
	// member stopped several times in a row can take minutes to return.
	restartTimeout = 5 * time.Minute
	// walAttempts bounds how often etcd is stopped again when it was
	// started before the edit could take the WAL lock.
	walAttempts = 5
	// maxVerifyBackoff bounds the wait between arbiter checks.
	maxVerifyBackoff = time.Minute
)

// Options are what an Agent needs from outside.
type Options struct {
	// Config is the configuration until the first reload.
	Config config.Config
	// Load, when set, is called before every poll; an error keeps the
	// previous configuration.
	Load func() (config.Config, error)
	// Local is the local member.
	Local Client
	// Dial reaches other members: the survivor in a rejoin, learners on
	// the survivor.
	Dial    Dialer
	Arbiter arbiter.Arbiter
	WAL     WAL
	Process Process
	// DataDir is the member's data directory as twok sees it.
	DataDir string
	// PeerEndpoint is the other member's client URL, for a rejoin when
	// the member list does not tell.
	PeerEndpoint  string
	MetricsListen string
	Logger        *slog.Logger
	// Now is time.Now outside tests.
	Now func() time.Time
}

// Agent is the poll loop of one member.
type Agent struct {
	cfg           config.Config
	load          func() (config.Config, error)
	local         Client
	dial          Dialer
	arb           arbiter.Arbiter
	wal           WAL
	proc          Process
	dataDir       string
	peerEndpoint  string
	metricsListen string
	logger        *slog.Logger
	now           func() time.Time
	engine        *engine.Engine
	metrics       *metrics

	pollTimeout    time.Duration
	arbiterTimeout time.Duration
	stallTimeout   time.Duration
	stopTimeout    time.Duration
	restartTimeout time.Duration
	waitInterval   time.Duration
	exit           func(code int)
	started        time.Time
	lastLoop       atomic.Int64

	verified      bool
	verifyAt      time.Time
	verifyBackoff time.Duration
	// controlled is set once the etcd process was found: from then on the
	// PID namespace is known to be shared, even while etcd restarts.
	controlled bool
	args       etcdargs.Args
	self       uint64
	result     *engine.Result
	role       engine.Role
	peers      map[string]Client
	lastLog    map[string]string
}

// New returns an agent. It does not start anything.
func New(o Options) *Agent {
	if o.Now == nil {
		o.Now = time.Now
	}
	a := &Agent{
		cfg:            o.Config,
		load:           o.Load,
		local:          o.Local,
		dial:           o.Dial,
		arb:            o.Arbiter,
		wal:            o.WAL,
		proc:           o.Process,
		dataDir:        o.DataDir,
		peerEndpoint:   o.PeerEndpoint,
		metricsListen:  o.MetricsListen,
		logger:         o.Logger,
		now:            o.Now,
		metrics:        newMetrics(),
		pollTimeout:    pollTimeout,
		arbiterTimeout: arbiterTimeout,
		stallTimeout:   stallTimeout,
		stopTimeout:    stopTimeout,
		restartTimeout: restartTimeout,
		waitInterval:   500 * time.Millisecond,
		exit:           os.Exit,
		started:        time.Now(),
		role:           engine.RoleUnknown,
		peers:          map[string]Client{},
		lastLog:        map[string]string{},
	}
	a.engine = engine.New(a.engineParams())
	a.setMode()
	a.metrics.configValid.Set(1)
	return a
}

func (a *Agent) engineParams() engine.Params {
	p := engine.DefaultParams()
	p.Mode = a.cfg.Mode
	p.ClaimAfter = a.cfg.ClaimAfter
	p.ClaimDelay = a.cfg.ClaimDelay
	p.FollowerExtraDelay = a.cfg.FollowerExtraDelay
	p.PeerEndpoint = a.peerEndpoint
	p.Version = version.Version
	return p
}

// Run polls every PollInterval until ctx is done.
func (a *Agent) Run(ctx context.Context) error {
	if a.metricsListen != "" {
		stop, err := a.metrics.serve(ctx, a.metricsListen)
		if err != nil {
			return err
		}
		defer stop()
	}
	defer a.closePeers()
	a.reload()
	a.logger.Info("agent started", "mode", a.cfg.Mode, "version", version.Version, "arbiter", a.arb.String(),
		"endpoint", a.local.Endpoint(), "dataDir", a.dataDir)
	a.markLoop()
	watchdogCtx, stopWatchdog := context.WithCancel(ctx)
	var watchdog sync.WaitGroup
	watchdog.Go(func() { a.watchdog(watchdogCtx) })
	defer watchdog.Wait()
	defer stopWatchdog()

	interval := a.cfg.PollInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		a.Tick(ctx)
		a.markLoop()
		if a.cfg.PollInterval != interval {
			interval = a.cfg.PollInterval
			ticker.Reset(interval)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (a *Agent) reload() {
	if a.load == nil {
		return
	}
	cfg, err := a.load()
	if err != nil {
		a.metrics.configValid.Set(0)
		a.logChange("config", slog.LevelError, "invalid configuration, keeping the previous one", err.Error(), "error", err)
		return
	}
	a.metrics.configValid.Set(1)
	delete(a.lastLog, "config")
	if cfg == a.cfg {
		return
	}
	a.logger.Info("configuration changed", "mode", cfg.Mode, "pollInterval", cfg.PollInterval.String(),
		"claimAfter", cfg.ClaimAfter.String(), "claimDelay", cfg.ClaimDelay.String(), "followerExtraDelay", cfg.FollowerExtraDelay.String())
	wasOff := a.cfg.Mode == config.ModeOff
	a.cfg = cfg
	// Timers started while the agent was off say nothing about now, and a
	// claim the arbiter holds for this member is found again by the read
	// at start, so switching on starts the engine over.
	if wasOff != (cfg.Mode == config.ModeOff) {
		a.engine = engine.New(a.engineParams())
	} else {
		a.engine.SetParams(a.engineParams())
	}
	a.setMode()
}

func (a *Agent) setMode() {
	for _, m := range []config.Mode{config.ModeOff, config.ModeDryRun, config.ModeEnforce} {
		v := 0.0
		if m == a.cfg.Mode {
			v = 1
		}
		a.metrics.mode.WithLabelValues(string(m)).Set(v)
	}
}

func (a *Agent) markLoop() {
	a.lastLoop.Store(int64(time.Since(a.started)))
}

// watchdog ends the process when the loop stops making progress. A stalled
// agent never touches etcd, but it silently stops protecting it; the
// platform restarts an agent that exits.
func (a *Agent) watchdog(ctx context.Context) {
	ticker := time.NewTicker(a.stallTimeout / 4)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stalled := time.Since(a.started) - time.Duration(a.lastLoop.Load())
			if stalled > a.stallTimeout {
				a.logger.Error("agent stalled, exiting", "stalled", stalled.String())
				a.exit(1)
				return
			}
		}
	}
}

// Tick polls once, decides and acts.
func (a *Agent) Tick(ctx context.Context) {
	a.reload()
	now := a.now()
	if a.cfg.Mode != config.ModeOff {
		a.verify(ctx, now)
	}
	pollCtx, cancel := context.WithTimeout(ctx, a.pollTimeout)
	obs := a.observe(pollCtx, now)
	cancel()
	obs.Result, a.result = a.result, nil
	d := a.engine.Observe(obs)
	if d.Action.Kind == engine.ActionReadArbiter {
		obs.Result = nil
		obs.Arbiter = a.readArbiter(ctx)
		d = a.engine.Observe(obs)
	}
	a.report(d)
	a.act(ctx, d)
}

// verify checks the arbiter's conditional writes once, and again with a
// growing backoff while the check fails.
func (a *Agent) verify(ctx context.Context, now time.Time) {
	if a.verified || now.Before(a.verifyAt) {
		return
	}
	vctx, cancel := context.WithTimeout(ctx, a.arbiterTimeout)
	defer cancel()
	if err := a.arb.Verify(vctx); err != nil {
		a.verifyBackoff = min(max(2*a.verifyBackoff, time.Second), maxVerifyBackoff)
		a.verifyAt = now.Add(a.verifyBackoff)
		a.logChange("verify", slog.LevelError, "the arbiter check failed, no action until it passes", err.Error(),
			"arbiter", a.arb.String(), "error", err)
		return
	}
	a.verified = true
	a.metrics.arbiterVerified.Set(1)
	delete(a.lastLog, "verify")
	a.logger.Info("the arbiter honours conditional writes", "arbiter", a.arb.String())
}

func (a *Agent) observe(ctx context.Context, now time.Time) engine.Observation {
	o := engine.Observation{Now: now, ArbiterReady: a.verified}
	switch args, err := a.proc.Args(); {
	case err == nil:
		if !a.controlled {
			a.logger.Info("found the etcd process", "name", args.Name, "peerURLs", args.InitialAdvertisePeerURLs, "dataDir", args.DataDir)
		}
		a.args, a.controlled = args, true
		delete(a.lastLog, "process")
	case !a.controlled:
		a.logChange("process", slog.LevelError,
			"cannot find the etcd process; without a PID namespace shared with etcd, twok takes no action that needs to stop it",
			err.Error(), "error", err)
	}
	o.CanControl = a.controlled
	a.metrics.processControlled.Set(boolValue(a.controlled))
	if a.controlled {
		o.Name, o.PeerURLs = a.args.Name, a.args.InitialAdvertisePeerURLs
	}

	st, err := a.local.Status(ctx)
	if err != nil {
		a.metrics.pollFailures.Inc()
		a.metrics.leaderSeen.Set(0)
		a.logChange("poll", slog.LevelWarn, "etcd does not answer", err.Error(), "endpoint", a.local.Endpoint(), "error", err)
		return o
	}
	delete(a.lastLog, "poll")
	o.Local = &st
	a.self = st.MemberID
	a.metrics.lastPoll.Set(float64(now.Unix()))
	a.metrics.leaderSeen.Set(boolValue(st.Leader != 0))

	switch ms, err := a.local.Members(ctx); {
	case err == nil:
		o.Members = &ms
		delete(a.lastLog, "members")
	case errors.Is(err, ErrLearner):
	default:
		a.logChange("members", slog.LevelWarn, "cannot read the member list", err.Error(), "error", err)
	}
	if g, err := a.local.Generation(ctx); err == nil {
		o.Generation, o.GenerationKnown = g, true
		a.metrics.generation.Set(float64(g))
		delete(a.lastLog, "generation")
	} else {
		a.logChange("generation", slog.LevelWarn, "cannot read "+GenerationKey, err.Error(), "error", err)
	}
	if st.Leader == st.MemberID && o.Members != nil && len(o.Members.Learners) > 0 {
		o.LearnerStatus = a.learnerStatus(ctx, o.Members.Learners)
	}
	return o
}

// learnerStatus asks every learner that has a client URL for its status.
func (a *Agent) learnerStatus(ctx context.Context, learners []engine.Member) map[uint64]engine.Status {
	out := map[uint64]engine.Status{}
	for _, l := range learners {
		for _, u := range l.ClientURLs {
			c, err := a.peer(u)
			if err != nil {
				continue
			}
			rctx, cancel := context.WithTimeout(ctx, remoteTimeout)
			st, err := c.Status(rctx)
			cancel()
			if err == nil && st.MemberID == l.ID {
				out[l.ID] = st
				break
			}
		}
	}
	return out
}

func (a *Agent) readArbiter(ctx context.Context) *engine.ArbiterRead {
	rctx, cancel := context.WithTimeout(ctx, a.arbiterTimeout)
	defer cancel()
	rec, v, err := a.arb.Read(rctx)
	switch {
	case errors.Is(err, arbiter.ErrNotFound):
		a.arbiterReached()
		return &engine.ArbiterRead{Version: v}
	case err != nil:
		a.metrics.arbiterReachable.Set(0)
		a.logChange("arbiter", slog.LevelWarn, "the arbiter is unreachable", err.Error(), "arbiter", a.arb.String(), "error", err)
		return &engine.ArbiterRead{Err: err}
	}
	a.arbiterReached()
	a.logChange("record", slog.LevelInfo, "arbiter record", fmt.Sprintf("%d/%s/%s", rec.Generation, rec.Survivor, v),
		"generation", rec.Generation, "survivor", rec.Survivor, "survivorMemberID", rec.SurvivorMemberID, "since", rec.Since,
		"reason", rec.Reason, "version", string(v))
	return &engine.ArbiterRead{Found: true, Record: rec, Version: v}
}

func (a *Agent) arbiterReached() {
	a.metrics.arbiterReachable.Set(1)
	delete(a.lastLog, "arbiter")
}

func (a *Agent) report(d engine.Decision) {
	for _, r := range engine.Roles() {
		a.metrics.role.WithLabelValues(string(r)).Set(boolValue(r == d.Role))
	}
	attrs := []any{"role", d.Role, "note", d.Note, "generation", d.Generation, "mode", a.cfg.Mode}
	if d.Role != a.role {
		attrs = append(attrs, "from", a.role)
		if d.Role == engine.RolePair {
			// A member in a pair is a voter: a rejoin, if there was one,
			// is done, and the WAL it kept to undo the edit is not needed
			// any more. After a restart of twok the previous role is
			// unknown, so this runs on every entry into pair.
			if err := a.wal.Cleanup(a.dataDir); err != nil {
				a.logger.Warn("removing the WAL kept by the rejoin failed", "error", err)
			}
		}
		a.role = d.Role
	}
	if d.Action.Kind != engine.ActionNone {
		attrs = append(attrs, "action", d.Action.Kind)
	}
	if d.DryRun {
		attrs = append(attrs, "dryRun", true)
	}
	if d.Wait > 0 {
		attrs = append(attrs, "wait", d.Wait.Round(time.Second).String())
	}
	level := slog.LevelInfo
	if d.Problem {
		level = slog.LevelError
	}
	key := fmt.Sprintf("%s|%s|%s|%t", d.Role, d.Note, d.Action.Kind, d.DryRun)
	if a.logChange("decision", level, "decision", key, attrs...) && d.DryRun {
		a.metrics.actions.WithLabelValues(string(d.Action.Kind), "dry-run").Inc()
	}
}

func (a *Agent) act(ctx context.Context, d engine.Decision) {
	if d.DryRun {
		return
	}
	var r *engine.Result
	switch d.Action.Kind {
	case engine.ActionClaim:
		r = a.claim(ctx, d.Action)
	case engine.ActionTakeover:
		r = a.takeover(ctx, d.Action)
	case engine.ActionWriteGeneration:
		r = a.writeGeneration(ctx, d.Action)
	case engine.ActionRejoin:
		r = a.rejoin(ctx, d.Action)
	case engine.ActionPromote:
		r = a.promote(ctx, d.Action)
	default:
		return
	}
	result := "success"
	if r.Err != nil {
		result = "error"
	}
	a.metrics.actions.WithLabelValues(string(d.Action.Kind), result).Inc()
	a.result = r
}

func (a *Agent) claim(ctx context.Context, act engine.Action) *engine.Result {
	r := &engine.Result{Action: engine.ActionClaim, Record: act.Record}
	cctx, cancel := context.WithTimeout(ctx, a.arbiterTimeout)
	defer cancel()
	attrs := []any{"generation", act.Record.Generation, "survivor", act.Record.Survivor, "expected", string(act.Expected)}
	v, err := a.arb.CompareAndSwap(cctx, act.Expected, act.Record)
	switch {
	case err == nil:
		a.metrics.claims.WithLabelValues("won").Inc()
		a.logger.Info("claim won", append(attrs, "version", string(v))...)
	case errors.Is(err, arbiter.ErrConflict):
		r.Err, r.Conflict = err, true
		a.metrics.claims.WithLabelValues("lost").Inc()
		a.logger.Info("claim lost: the record changed since it was read", attrs...)
	default:
		r.Err = err
		a.metrics.claims.WithLabelValues("error").Inc()
		a.logger.Warn("claim result unknown, reading the arbiter before deciding", append(attrs, "error", err)...)
	}
	return r
}

func (a *Agent) takeover(ctx context.Context, act engine.Action) *engine.Result {
	self := a.self
	remove := hexes(act.Remove)
	a.logger.Info("taking over: etcd is stopped and restarted as a cluster of one", "member", engine.Hex(self), "remove", remove)
	err := a.withStopped(ctx, "takeover", func() error { return a.wal.Standalone(a.dataDir, self, act.Remove) })
	if err != nil {
		a.logger.Error("takeover failed", "error", err)
		return &engine.Result{Action: engine.ActionTakeover, Err: err}
	}
	a.logger.Info("takeover done, etcd answers again", "member", engine.Hex(self), "removed", remove)
	return &engine.Result{Action: engine.ActionTakeover}
}

func (a *Agent) writeGeneration(ctx context.Context, act engine.Action) *engine.Result {
	wctx, cancel := context.WithTimeout(ctx, a.pollTimeout)
	defer cancel()
	if err := a.local.PutGeneration(wctx, act.Generation); err != nil {
		a.logger.Warn("writing the generation failed", "generation", act.Generation, "error", err)
		return &engine.Result{Action: engine.ActionWriteGeneration, Err: err}
	}
	a.metrics.generation.Set(float64(act.Generation))
	a.logger.Info("generation written, serving alone", "generation", act.Generation)
	return &engine.Result{Action: engine.ActionWriteGeneration}
}

func (a *Agent) promote(ctx context.Context, act engine.Action) *engine.Result {
	pctx, cancel := context.WithTimeout(ctx, a.pollTimeout)
	defer cancel()
	r := &engine.Result{Action: engine.ActionPromote}
	switch err := a.local.Promote(pctx, act.Learner); {
	case err == nil:
		a.logger.Info("learner promoted", "member", engine.Hex(act.Learner))
	case errors.Is(err, ErrLearnerNotReady):
		r.Err, r.NotReady = err, true
		a.logChange("promote", slog.LevelInfo, "learner not ready yet", engine.Hex(act.Learner), "member", engine.Hex(act.Learner))
	default:
		r.Err = err
		a.logger.Warn("promoting the learner failed", "member", engine.Hex(act.Learner), "error", err)
	}
	return r
}

// rejoin adds the local member to the survivor as a learner, unless an
// earlier attempt already did, then rewrites the local log under the new
// member ID and lets etcd start again.
func (a *Agent) rejoin(ctx context.Context, act engine.Action) *engine.Result {
	r := &engine.Result{Action: engine.ActionRejoin}
	fail := func(err error) *engine.Result {
		a.metrics.rejoins.WithLabelValues("error").Inc()
		a.logChange("rejoin", slog.LevelWarn, "rejoin failed", err.Error(), "error", err)
		r.Err = err
		return r
	}
	survivor, err := a.findSurvivor(ctx, act)
	if err != nil {
		return fail(err)
	}
	rctx, cancel := context.WithTimeout(ctx, a.pollTimeout)
	ms, err := survivor.Members(rctx)
	cancel()
	if err != nil {
		return fail(fmt.Errorf("member list of the survivor %s: %w", survivor.Endpoint(), err))
	}
	if len(ms.Voters) != 1 {
		return fail(fmt.Errorf("the survivor %s has %d voters, a rejoin needs it alone", survivor.Endpoint(), len(ms.Voters)))
	}
	learner := uint64(0)
	for _, l := range ms.Learners {
		if sameURLs(l.PeerURLs, act.PeerURLs) {
			learner = l.ID
		}
	}
	if learner == 0 {
		actx, cancel := context.WithTimeout(ctx, a.pollTimeout)
		m, err := survivor.AddLearner(actx, act.PeerURLs)
		cancel()
		if err != nil {
			return fail(err)
		}
		learner = m.ID
		a.logger.Info("rejoin: added as a learner of the survivor", "survivor", survivor.Endpoint(), "learner", engine.Hex(learner), "peerURLs", act.PeerURLs)
	} else {
		a.logger.Info("rejoin: the survivor already has this member as a learner", "survivor", survivor.Endpoint(), "learner", engine.Hex(learner))
	}
	r.Learner = learner
	// The log is rewritten only for a learner the survivor's member list
	// shows with the local peer URLs: the answer to AddLearner alone could
	// be lost, or come from a member that is no longer the survivor.
	if err := a.confirmLearner(ctx, survivor, learner, act.PeerURLs); err != nil {
		return fail(err)
	}
	err = a.withStopped(ctx, "rejoin", func() error { return a.wal.Reidentify(a.dataDir, learner) })
	if err != nil {
		return fail(err)
	}
	delete(a.lastLog, "rejoin")
	a.metrics.rejoins.WithLabelValues("success").Inc()
	a.logger.Info("rejoin: etcd runs as the new learner", "learner", engine.Hex(learner))
	return r
}

func (a *Agent) confirmLearner(ctx context.Context, survivor Client, learner uint64, peerURLs []string) error {
	cctx, cancel := context.WithTimeout(ctx, a.pollTimeout)
	defer cancel()
	ms, err := survivor.Members(cctx)
	if err != nil {
		return fmt.Errorf("confirm learner %s on %s: %w", engine.Hex(learner), survivor.Endpoint(), err)
	}
	for _, l := range ms.Learners {
		if l.ID == learner && sameURLs(l.PeerURLs, peerURLs) {
			return nil
		}
	}
	return fmt.Errorf("the survivor %s does not list learner %s with peer URLs %v", survivor.Endpoint(), engine.Hex(learner), peerURLs)
}

// findSurvivor returns a client for the first endpoint that answers as the
// survivor the arbiter names: the same cluster, the expected member ID, and
// leading.
func (a *Agent) findSurvivor(ctx context.Context, act engine.Action) (Client, error) {
	var errs []error
	for _, ep := range act.SurvivorEndpoints {
		c, err := a.peer(ep)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		sctx, cancel := context.WithTimeout(ctx, a.pollTimeout)
		st, err := c.Status(sctx)
		cancel()
		switch {
		case err != nil:
			errs = append(errs, err)
		case st.ClusterID != act.ClusterID:
			errs = append(errs, fmt.Errorf("%s belongs to cluster %s, not %s", ep, engine.Hex(st.ClusterID), engine.Hex(act.ClusterID)))
		case act.SurvivorID != 0 && st.MemberID != act.SurvivorID:
			errs = append(errs, fmt.Errorf("%s is member %s, the survivor is %s", ep, engine.Hex(st.MemberID), engine.Hex(act.SurvivorID)))
		case st.Leader != st.MemberID:
			errs = append(errs, fmt.Errorf("%s does not lead", ep))
		default:
			return c, nil
		}
	}
	return nil, fmt.Errorf("no survivor found: %w", errors.Join(errs...))
}

// withStopped stops etcd, runs op on the stopped member's data and lets
// etcd start again, then waits until it answers. When etcd was started
// again before op could take the WAL lock, it is stopped again.
func (a *Agent) withStopped(ctx context.Context, what string, op func() error) error {
	for attempt := 1; ; attempt++ {
		a.markLoop()
		sctx, cancel := context.WithTimeout(ctx, a.stopTimeout)
		err := a.proc.Stop(sctx)
		cancel()
		if err != nil {
			return fmt.Errorf("%s: stop etcd: %w", what, err)
		}
		a.markLoop()
		err = op()
		if errors.Is(err, ErrWALLocked) && attempt < walAttempts {
			a.logger.Warn(what+": etcd started again before the edit, stopping it again", "attempt", attempt)
			continue
		}
		if err != nil {
			// etcd is let back whatever happened: the operations leave the
			// data as it was when they fail.
			if rerr := a.proc.Resume(ctx); rerr != nil {
				a.logger.Error(what+": letting etcd start again failed", "error", rerr)
			}
			return fmt.Errorf("%s: edit the WAL: %w", what, err)
		}
		a.logger.Info(what+": WAL edited, letting etcd start again", "attempt", attempt)
		break
	}
	if err := a.proc.Resume(ctx); err != nil {
		return fmt.Errorf("%s: let etcd start again: %w", what, err)
	}
	return a.waitForEtcd(ctx, what)
}

// waitForEtcd polls the local member until it answers.
func (a *Agent) waitForEtcd(ctx context.Context, what string) error {
	deadline := time.Now().Add(a.restartTimeout)
	for {
		a.markLoop()
		sctx, cancel := context.WithTimeout(ctx, a.pollTimeout)
		st, err := a.local.Status(sctx)
		cancel()
		if err == nil {
			a.logger.Info(what+": etcd answers", "member", engine.Hex(st.MemberID), "leader", engine.Hex(st.Leader), "learner", st.IsLearner)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s: etcd did not answer within %s: %w", what, a.restartTimeout, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(a.waitInterval):
		}
	}
}

func (a *Agent) peer(endpoint string) (Client, error) {
	if c, ok := a.peers[endpoint]; ok {
		return c, nil
	}
	c, err := a.dial(endpoint)
	if err != nil {
		return nil, err
	}
	a.peers[endpoint] = c
	return c, nil
}

func (a *Agent) closePeers() {
	for ep, c := range a.peers {
		_ = c.Close()
		delete(a.peers, ep)
	}
}

// logChange logs msg only when key differs from the previous key logged in
// the same category, so a situation that lasts is logged once. It reports
// whether it logged.
func (a *Agent) logChange(category string, level slog.Level, msg, key string, attrs ...any) bool {
	key = msg + "\x00" + key
	if a.lastLog[category] == key {
		return false
	}
	a.lastLog[category] = key
	a.logger.Log(context.Background(), level, msg, attrs...)
	return true
}

func sameURLs(x, y []string) bool {
	if len(x) != len(y) {
		return false
	}
	x, y = slices.Clone(x), slices.Clone(y)
	slices.Sort(x)
	slices.Sort(y)
	return slices.Equal(x, y)
}

func hexes(ids []uint64) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = engine.Hex(id)
	}
	return out
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
