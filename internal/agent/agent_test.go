package agent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/aenix-io/twok/internal/arbiter"
	"github.com/aenix-io/twok/internal/config"
	"github.com/aenix-io/twok/internal/engine"
)

func TestTakeover(t *testing.T) {
	h := newHarness(t, fastConfig(config.ModeEnforce))
	h.ticks(2)
	if h.arb.verifies != 1 || h.arb.reads != 1 {
		t.Fatalf("verifies %d, reads %d after start, want one each", h.arb.verifies, h.arb.reads)
	}
	h.cutLink()
	// claim-after 5s, claim-delay 1s for the last leader.
	h.ticks(8)
	rec, ok := h.record()
	if !ok || rec.Generation != 1 || rec.Survivor != "a" || rec.SurvivorMemberID != "a" || rec.ClusterID != "c1" || rec.Reason != "no-leader" {
		t.Fatalf("record = %+v, %v", rec, ok)
	}
	h.ticks(2)
	if len(h.wal.standalones) != 1 || h.wal.standalones[0] != "/var/lib/etcd a [11]" {
		t.Fatalf("standalone edits = %v", h.wal.standalones)
	}
	if h.proc.stops != 1 || h.proc.resumes != 1 {
		t.Errorf("stops %d, resumes %d, want one each", h.proc.stops, h.proc.resumes)
	}
	if len(h.local.puts) != 1 || h.local.puts[0] != 1 {
		t.Errorf("generation writes = %v, want [1]", h.local.puts)
	}
	h.ticks(5)
	if got := testutil.ToFloat64(h.agent.metrics.role.WithLabelValues("solo")); got != 1 {
		t.Errorf("twok_role{role=solo} = %v", got)
	}
	if got := testutil.ToFloat64(h.agent.metrics.generation); got != 1 {
		t.Errorf("twok_generation = %v", got)
	}
	if got := testutil.ToFloat64(h.agent.metrics.claims.WithLabelValues("won")); got != 1 {
		t.Errorf("twok_claims_total{result=won} = %v", got)
	}
	for _, msg := range []string{`"claim won"`, `"takeover: WAL edited, letting etcd start again"`, `"generation written, serving alone"`, `"from":"isolated"`} {
		if h.logCount(msg) != 1 {
			t.Errorf("log %s appears %d times, want once:\n%s", msg, h.logCount(msg), h.logs)
		}
	}
}

func TestTakeoverStopsEtcdAgainWhenItCameBack(t *testing.T) {
	h := newHarness(t, fastConfig(config.ModeEnforce))
	h.wal.locked = 2
	h.ticks(1)
	h.cutLink()
	h.ticks(10)
	if h.proc.stops != 3 || h.proc.resumes != 1 || len(h.wal.standalones) != 1 {
		t.Errorf("stops %d, resumes %d, edits %v; want three stops for two locked attempts", h.proc.stops, h.proc.resumes, h.wal.standalones)
	}
	if n := h.logCount("etcd started again before the edit, stopping it again"); n != 2 {
		t.Errorf("retry logged %d times, want 2:\n%s", n, h.logs)
	}

	// A WAL that stays locked fails the takeover after walAttempts, lets
	// etcd back, and the engine retries later.
	h2 := newHarness(t, fastConfig(config.ModeEnforce))
	h2.wal.locked = 1000
	h2.ticks(1)
	h2.cutLink()
	h2.ticks(9)
	if h2.proc.resumes == 0 || h2.proc.stops != walAttempts*h2.proc.resumes {
		t.Errorf("stops %d, resumes %d, want %d stops per attempt and etcd let back after each", h2.proc.stops, h2.proc.resumes, walAttempts)
	}
	if !strings.Contains(h2.logs.String(), `"takeover failed"`) {
		t.Errorf("failure not logged:\n%s", h2.logs)
	}
}

func TestDryRunDecidesAndDoesNothing(t *testing.T) {
	h := newHarness(t, fastConfig(config.ModeDryRun))
	h.ticks(1)
	h.cutLink()
	h.ticks(30)
	if _, ok := h.record(); ok {
		t.Fatal("dry-run wrote the arbiter")
	}
	if h.proc.stops != 0 || len(h.wal.standalones) != 0 {
		t.Fatal("dry-run stopped etcd")
	}
	if got := testutil.ToFloat64(h.agent.metrics.actions.WithLabelValues("claim", "dry-run")); got != 1 {
		t.Errorf("dry-run claims counted %v times, want once per decision", got)
	}
	if !strings.Contains(h.logs.String(), `"dryRun":true`) {
		t.Errorf("no dry-run decision logged:\n%s", h.logs)
	}
}

func TestOffTouchesNeitherArbiterNorEtcdProcess(t *testing.T) {
	h := newHarness(t, fastConfig(config.ModeOff))
	h.ticks(1)
	h.cutLink()
	h.ticks(30)
	if h.arb.verifies+h.arb.reads+h.arb.swaps != 0 || h.proc.stops != 0 {
		t.Errorf("off: verifies %d, reads %d, swaps %d, stops %d", h.arb.verifies, h.arb.reads, h.arb.swaps, h.proc.stops)
	}
	if got := testutil.ToFloat64(h.agent.metrics.role.WithLabelValues("isolated")); got != 1 {
		t.Errorf("off still reports the role: twok_role{role=isolated} = %v", got)
	}
}

func TestUnverifiedArbiterBlocksActions(t *testing.T) {
	h := newHarness(t, fastConfig(config.ModeEnforce))
	h.arb.verifyErr = errors.New("If-Match ignored")
	h.ticks(1)
	h.cutLink()
	h.ticks(60)
	if _, ok := h.record(); ok || h.arb.reads != 0 {
		t.Fatalf("acted before the arbiter passed its check: reads %d", h.arb.reads)
	}
	// 1, 2, 4, 8, 16, 32 seconds apart within a minute.
	if h.arb.verifies < 6 || h.arb.verifies > 7 {
		t.Errorf("verified %d times in 61s, want a growing backoff", h.arb.verifies)
	}
	if n := h.logCount("the arbiter check failed"); n != 1 {
		t.Errorf("failure logged %d times, want once", n)
	}
	h.arb.mu.Lock()
	h.arb.verifyErr = nil
	h.arb.mu.Unlock()
	h.ticks(70)
	if rec, ok := h.record(); !ok || rec.Survivor != "a" {
		t.Errorf("no claim after the arbiter passed: %+v", rec)
	}
}

func TestWithoutTheEtcdProcessNoClaim(t *testing.T) {
	h := newHarness(t, fastConfig(config.ModeEnforce))
	h.proc.argsErr = ErrNoProcess
	h.ticks(1)
	h.cutLink()
	h.ticks(30)
	if _, ok := h.record(); ok {
		t.Fatal("claimed without a way to stop etcd")
	}
	if n := h.logCount("cannot find the etcd process"); n != 1 {
		t.Errorf("logged %d times, want once:\n%s", n, h.logs)
	}
	if !strings.Contains(h.logs.String(), `"level":"ERROR","msg":"decision"`) {
		t.Errorf("the refusal is not an error:\n%s", h.logs)
	}
}

func TestRejoin(t *testing.T) {
	h := newHarness(t, fastConfig(config.ModeEnforce))
	// b won generation 1 while a was cut off.
	if _, err := h.arb.CompareAndSwap(context.Background(), arbiter.Zero, arbiter.Record{
		SchemaVersion: 1, ClusterID: "c1", Generation: 1, Survivor: "b", SurvivorMemberID: "b", Reason: "no-leader",
	}); err != nil {
		t.Fatal(err)
	}
	h.cutLink()
	h.ticks(1)
	if got := testutil.ToFloat64(h.agent.metrics.role.WithLabelValues("stale")); got != 1 {
		t.Fatalf("not stale after the start-up read: %s", h.logs)
	}
	h.ticks(1)
	if len(h.peer.added) != 1 || h.peer.added[0][0] != "https://a:2380" {
		t.Fatalf("added learners = %v", h.peer.added)
	}
	if len(h.wal.reids) != 1 || h.wal.reids[0] != 0x101 {
		t.Fatalf("reidentify = %x", h.wal.reids)
	}
	if h.dialed[0] != "https://b:2379" {
		t.Errorf("dialed %v, want b's client URL from the member list", h.dialed)
	}
	h.ticks(5)
	if got := testutil.ToFloat64(h.agent.metrics.role.WithLabelValues("joining")); got != 1 {
		t.Errorf("not joining as a learner:\n%s", h.logs)
	}
	if got := testutil.ToFloat64(h.agent.metrics.rejoins.WithLabelValues("success")); got != 1 {
		t.Errorf("twok_rejoins_total{result=success} = %v", got)
	}
}

func TestRejoinReusesTheLearnerAndChecksTheSurvivor(t *testing.T) {
	h := newHarness(t, fastConfig(config.ModeEnforce))
	_, _ = h.arb.CompareAndSwap(context.Background(), arbiter.Zero, arbiter.Record{ClusterID: "c1", Generation: 1, Survivor: "b", SurvivorMemberID: "b"})
	h.cutLink()
	// The first attempt added the learner and failed before the edit.
	h.peer.set(func(f *fakeMember) {
		f.members.Learners = []engine.Member{{ID: 0x177, PeerURLs: []string{"https://a:2380"}}}
	})
	h.ticks(2)
	if len(h.peer.added) != 0 || len(h.wal.reids) != 1 || h.wal.reids[0] != 0x177 {
		t.Fatalf("added %v, reidentify %x; want the existing learner reused", h.peer.added, h.wal.reids)
	}

	// A survivor that does not lead alone is not joined.
	h2 := newHarness(t, fastConfig(config.ModeEnforce))
	_, _ = h2.arb.CompareAndSwap(context.Background(), arbiter.Zero, arbiter.Record{ClusterID: "c1", Generation: 1, Survivor: "b", SurvivorMemberID: "b"})
	h2.cutLink()
	h2.peer.set(func(f *fakeMember) { f.members.Voters = append(f.members.Voters, memberEntry(0xa, "a")) })
	h2.ticks(20)
	if len(h2.peer.added) != 0 || len(h2.wal.reids) != 0 || h2.proc.stops != 0 {
		t.Fatalf("joined a survivor with two voters: added %v", h2.peer.added)
	}
	if !strings.Contains(h2.logs.String(), "has 2 voters, a rejoin needs it alone") {
		t.Errorf("the refusal is not logged:\n%s", h2.logs)
	}
	// Retried every RetryDelay, each attempt after a fresh arbiter read.
	if h2.arb.reads < 4 || h2.arb.reads > 6 {
		t.Errorf("arbiter read %d times in 20s of failed rejoins", h2.arb.reads)
	}

	// A learner the survivor does not list afterwards is not trusted: the
	// log is not rewritten for it.
	h4 := newHarness(t, fastConfig(config.ModeEnforce))
	_, _ = h4.arb.CompareAndSwap(context.Background(), arbiter.Zero, arbiter.Record{ClusterID: "c1", Generation: 1, Survivor: "b", SurvivorMemberID: "b"})
	h4.cutLink()
	h4.peer.set(func(f *fakeMember) { f.forget = true })
	h4.ticks(3)
	if len(h4.peer.added) != 1 || len(h4.wal.reids) != 0 || h4.proc.stops != 0 {
		t.Fatalf("added %v, reidentify %x, stops %d; want no edit for an unconfirmed learner", h4.peer.added, h4.wal.reids, h4.proc.stops)
	}
	if !strings.Contains(h4.logs.String(), "does not list learner 101") {
		t.Errorf("the refusal is not logged:\n%s", h4.logs)
	}

	// A member at the endpoint that belongs to another cluster is not
	// joined either.
	h3 := newHarness(t, fastConfig(config.ModeEnforce))
	_, _ = h3.arb.CompareAndSwap(context.Background(), arbiter.Zero, arbiter.Record{ClusterID: "c1", Generation: 1, Survivor: "b", SurvivorMemberID: "b"})
	h3.cutLink()
	h3.peer.set(func(f *fakeMember) { f.st.ClusterID = 0xdead })
	h3.ticks(3)
	if len(h3.peer.added) != 0 || !strings.Contains(h3.logs.String(), "belongs to cluster dead") {
		t.Fatalf("joined another cluster:\n%s", h3.logs)
	}
}

// removedHarness is member a after b took over generation 1 while a was cut
// off: a's etcd learned from b that it was removed, before a's agent read
// the arbiter, and now exits in a loop.
func removedHarness(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t, fastConfig(config.ModeEnforce))
	h.ticks(1)
	if _, err := h.arb.CompareAndSwap(context.Background(), arbiter.Zero, arbiter.Record{
		SchemaVersion: 1, ClusterID: "c1", Generation: 1, Survivor: "b", SurvivorMemberID: "b", Reason: "no-leader",
	}); err != nil {
		t.Fatal(err)
	}
	h.local.set(func(f *fakeMember) { f.down = true })
	return h
}

func TestRejoinWhenEtcdExitedAsRemoved(t *testing.T) {
	h := removedHarness(t)
	h.ticks(10)
	if len(h.peer.added) != 1 || len(h.wal.reids) != 1 || h.wal.reids[0] != 0x101 {
		t.Fatalf("added %v, reidentify %x; want a rejoin while etcd does not answer:\n%s", h.peer.added, h.wal.reids, h.logs)
	}
	if h.wal.inspects == 0 {
		t.Error("the WAL was not read while etcd did not answer")
	}
	if got := testutil.ToFloat64(h.agent.metrics.rejoins.WithLabelValues("success")); got != 1 {
		t.Errorf("twok_rejoins_total{result=success} = %v", got)
	}
}

func TestRejoinAfterTwokRestartedDuringTheCrashLoop(t *testing.T) {
	h := removedHarness(t)
	// A new agent: it never saw etcd answer, so it knows neither the
	// generation nor the member list. The WAL says who it is, the flag
	// where the survivor is.
	h.agent = New(Options{
		Config: fastConfig(config.ModeEnforce), Local: h.local, Arbiter: h.arb, WAL: h.wal, Process: h.proc,
		DataDir: "/var/lib/etcd", PeerEndpoint: h.peer.endpoint, Logger: h.agent.logger, Now: h.agent.now,
		Dial: func(string) (Client, error) { return h.peer, nil },
	})
	h.agent.waitInterval = time.Millisecond
	h.ticks(10)
	if len(h.wal.reids) != 1 {
		t.Fatalf("reidentify %x, want one rejoin:\n%s", h.wal.reids, h.logs)
	}
	if !strings.Contains(h.logs.String(), "etcd does not answer and the arbiter names b the survivor of generation 1") {
		t.Errorf("logs:\n%s", h.logs)
	}

	// Without the peer endpoint there is no way to the survivor: an error,
	// and nothing changes.
	h2 := removedHarness(t)
	h2.agent = New(Options{
		Config: fastConfig(config.ModeEnforce), Local: h2.local, Arbiter: h2.arb, WAL: h2.wal, Process: h2.proc,
		DataDir: "/var/lib/etcd", Logger: h2.agent.logger, Now: h2.agent.now,
		Dial: func(string) (Client, error) { return h2.peer, nil },
	})
	h2.ticks(10)
	if len(h2.peer.added)+len(h2.wal.reids) != 0 || !strings.Contains(h2.logs.String(), "set --peer-endpoint") {
		t.Errorf("added %v, reidentify %x:\n%s", h2.peer.added, h2.wal.reids, h2.logs)
	}
}

func TestRejoinAlreadyDoneOnlyWaits(t *testing.T) {
	h := removedHarness(t)
	// The edit was done before twok restarted: the WAL carries the
	// learner's ID, and the survivor lists it.
	h.local.set(func(f *fakeMember) { f.st.MemberID = 0x177 })
	h.peer.set(func(f *fakeMember) {
		f.members.Learners = []engine.Member{{ID: 0x177, PeerURLs: []string{"https://a:2380"}}}
	})
	h.ticks(10)
	if len(h.peer.added) != 0 || len(h.wal.reids) != 0 || h.proc.stops != 0 {
		t.Fatalf("added %v, reidentify %x, stops %d; want only a wait", h.peer.added, h.wal.reids, h.proc.stops)
	}
	if h.proc.resumes == 0 || !strings.Contains(h.logs.String(), "already the survivor's learner") {
		t.Errorf("resumes %d:\n%s", h.proc.resumes, h.logs)
	}
}

func TestDownMemberStillListedByTheSurvivorIsNotStale(t *testing.T) {
	h := removedHarness(t)
	// The record names b, but b leads with this member's ID as its only
	// voter: b is this member, reached through a wrong endpoint, or the
	// record is wrong. Either way it is no proof of a takeover.
	h.peer.set(func(f *fakeMember) {
		f.st.MemberID, f.st.Leader = 0xb, 0xb
		f.members.Voters = []engine.Member{memberEntry(0xa, "a")}
	})
	h.wal.mu.Lock()
	h.wal.local.st.MemberID = 0xa
	h.wal.mu.Unlock()
	h.ticks(10)
	if len(h.peer.added)+len(h.wal.reids) != 0 {
		t.Fatalf("added %v, reidentify %x", h.peer.added, h.wal.reids)
	}
	if !strings.Contains(h.logs.String(), "is this member, a") {
		t.Errorf("the refusal is not logged:\n%s", h.logs)
	}
}

func TestPromote(t *testing.T) {
	h := newHarness(t, fastConfig(config.ModeEnforce))
	h.local.set(func(f *fakeMember) {
		f.gen = 1
		f.members = engine.Members{Voters: []engine.Member{memberEntry(0xa, "a")}, Learners: []engine.Member{memberEntry(0x101, "b")}}
		f.promoteErr = ErrLearnerNotReady
	})
	h.ticks(10)
	h.local.mu.Lock()
	h.local.promoteErr = nil
	h.local.mu.Unlock()
	h.ticks(20)
	if len(h.local.promoted) != 1 || h.local.promoted[0] != 0x101 {
		t.Errorf("promoted = %x", h.local.promoted)
	}
	if !strings.Contains(h.logs.String(), "learner not ready yet") || !strings.Contains(h.logs.String(), `"learner promoted"`) {
		t.Errorf("logs:\n%s", h.logs)
	}
	// The learner's client URL was asked for its status before promoting.
	found := false
	for _, d := range h.dialed {
		found = found || d == "https://b:2379"
	}
	if !found {
		t.Errorf("the learner's status was not read: dialed %v", h.dialed)
	}
}

func TestEtcdDownCountsFailedPolls(t *testing.T) {
	h := newHarness(t, fastConfig(config.ModeEnforce))
	h.ticks(1)
	ok := h.now.Add(-time.Second)
	h.local.set(func(f *fakeMember) { f.down = true })
	h.ticks(3)
	if got := testutil.ToFloat64(h.agent.metrics.pollFailures); got != 3 {
		t.Errorf("poll failures = %v", got)
	}
	if got := testutil.ToFloat64(h.agent.metrics.lastPoll); got != float64(ok.Unix()) {
		t.Errorf("last poll = %v, want %d", got, ok.Unix())
	}
	if n := h.logCount(`"msg":"etcd does not answer"`); n != 1 {
		t.Errorf("logged %d times", n)
	}
}

type configSource struct {
	mu  sync.Mutex
	cfg config.Config
	err error
}

func (s *configSource) set(cfg config.Config, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cfg, s.err = cfg, err
}

func (s *configSource) load() (config.Config, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg, s.err
}

func TestReload(t *testing.T) {
	h := newHarness(t, fastConfig(config.ModeOff))
	src := &configSource{cfg: fastConfig(config.ModeDryRun)}
	h.agent.load = src.load
	h.ticks(1)
	h.cutLink()
	h.ticks(10)
	if _, ok := h.record(); ok {
		t.Fatal("dry-run claimed")
	}
	src.set(config.Config{}, errors.New("mode: must be one of off, dry-run, enforce"))
	h.ticks(3)
	if got := testutil.ToFloat64(h.agent.metrics.configValid); got != 0 {
		t.Errorf("twok_config_valid = %v", got)
	}
	if n := h.logCount("invalid configuration"); n != 1 {
		t.Errorf("invalid configuration logged %d times", n)
	}
	src.set(fastConfig(config.ModeEnforce), nil)
	h.ticks(2)
	if rec, ok := h.record(); !ok || rec.Survivor != "a" {
		t.Errorf("no claim right after the switch to enforce: %+v", rec)
	}
	if got := testutil.ToFloat64(h.agent.metrics.mode.WithLabelValues("enforce")); got != 1 {
		t.Errorf("twok_mode{mode=enforce} = %v", got)
	}
}

func TestSwitchingOnStartsOver(t *testing.T) {
	h := newHarness(t, fastConfig(config.ModeEnforce))
	src := &configSource{cfg: fastConfig(config.ModeOff)}
	h.agent.load = src.load
	h.cutLink()
	h.ticks(30)
	src.set(fastConfig(config.ModeEnforce), nil)
	h.ticks(1)
	if _, ok := h.record(); ok {
		t.Fatal("claimed on the first poll after switching on, from timers started while off")
	}
	h.ticks(8)
	if _, ok := h.record(); !ok {
		t.Fatal("no claim after claim-after once on")
	}
}

func TestWatchdogExitsStalledAgent(t *testing.T) {
	cfg := fastConfig(config.ModeEnforce)
	cfg.PollInterval = 10 * time.Millisecond
	h := newHarness(t, cfg)
	h.agent.stallTimeout = 100 * time.Millisecond
	exited := make(chan int, 1)
	h.agent.exit = func(code int) { exited <- code }
	hang := make(chan struct{})
	h.local.set(func(f *fakeMember) { f.hang = hang })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.agent.Run(ctx) }()
	defer func() {
		close(hang)
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run() error = %v", err)
		}
	}()
	select {
	case code := <-exited:
		if code != 1 {
			t.Errorf("exit code = %d", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the watchdog did not end a stalled agent")
	}
	if !strings.Contains(h.logs.String(), "agent stalled") {
		t.Errorf("logs:\n%s", h.logs)
	}
}

func TestWatchdogQuietWhileTheLoopRuns(t *testing.T) {
	cfg := fastConfig(config.ModeEnforce)
	cfg.PollInterval = 10 * time.Millisecond
	h := newHarness(t, cfg)
	h.agent.stallTimeout = 200 * time.Millisecond
	h.agent.exit = func(code int) { t.Errorf("exit(%d) while the loop runs", code) }
	h.local.set(func(f *fakeMember) { f.down = true })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.agent.Run(ctx); err != nil {
		t.Errorf("Run() error = %v", err)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	h := newHarness(t, fastConfig(config.ModeOff))
	h.agent.metricsListen = "127.0.0.1:0"
	stop, err := h.agent.metrics.serve(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stop()
	h.ticks(1)
	n, err := testutil.GatherAndCount(h.agent.metrics.registry, "twok_role", "twok_generation", "twok_leader_seen",
		"twok_arbiter_reachable", "twok_claims_total", "twok_rejoins_total", "twok_last_poll_timestamp_seconds")
	if err != nil {
		t.Fatal(err)
	}
	// six roles, three claim results, two rejoin results, four gauges.
	if n != 6+3+2+4 {
		t.Errorf("%d series of the design's metrics, want 15", n)
	}
}

func TestSameURLs(t *testing.T) {
	if !sameURLs([]string{"b", "a"}, []string{"a", "b"}) || sameURLs([]string{"a"}, []string{"a", "b"}) {
		t.Error("sameURLs")
	}
}
