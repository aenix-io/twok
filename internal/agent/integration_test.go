package agent_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/aenix-io/twok/internal/agent"
	"github.com/aenix-io/twok/internal/arbiter"
	"github.com/aenix-io/twok/internal/arbiter/memarb"
	"github.com/aenix-io/twok/internal/config"
	"github.com/aenix-io/twok/internal/engine"
	"github.com/aenix-io/twok/internal/etcdargs"
	"github.com/aenix-io/twok/internal/etcdtest"
	"github.com/aenix-io/twok/internal/walops"
)

// The integration tests run two real etcd members, each with an agent in
// this process, against an in-memory arbiter. The members talk to each
// other through the proxies of etcdtest, which the tests cut. Each member's
// etcd is supervised as the kubelet would: when it exits, it is started
// again with the same flags a moment later, unless the test has taken its
// site down.

var errLinkDown = errors.New("test: the link to the other site is down")

// site is one member, its supervisor and its agent.
type site struct {
	t      *testing.T
	m      *etcdtest.Member
	extra  []string
	logs   *lockedBuffer
	arb    *switchArbiter
	cancel context.CancelFunc
	done   chan struct{}

	mu sync.Mutex
	// down keeps etcd stopped: the site has lost power.
	down bool
	// restarts counts the supervisor's restarts of etcd.
	restarts int
}

// world is the two sites, the link between them and the arbiter.
type world struct {
	t     *testing.T
	cl    *etcdtest.Cluster
	sites []*site
	// linkDown blocks the client calls one site's agent makes to the
	// other: the proxies cut the peer traffic, this cuts the rest.
	linkDown atomic.Bool
	arbName  string
}

var worlds atomic.Int64

func newWorld(t *testing.T, cfg config.Config) *world {
	t.Helper()
	return newWorldPerSite(t, cfg, cfg)
}

// newWorldPerSite starts the agent of m0 with cfg0 and the one of m1 with
// cfg1, as two ConfigMaps or two sets of flags would.
func newWorldPerSite(t *testing.T, cfg0, cfg1 config.Config) *world {
	t.Helper()
	w := &world{t: t, cl: etcdtest.StartPair(t), arbName: fmt.Sprintf("integration/%s/%d", t.Name(), worlds.Add(1))}
	for _, m := range w.cl.Members {
		s := &site{t: t, m: m, extra: m.ExtraArgs(), logs: &lockedBuffer{}, arb: &switchArbiter{Arbiter: memarb.New(w.arbName)}}
		w.sites = append(w.sites, s)
	}
	for i, s := range w.sites {
		w.startAgent(s, []config.Config{cfg0, cfg1}[i])
	}
	t.Cleanup(func() {
		for _, s := range w.sites {
			s.stopAgent()
		}
		if t.Failed() {
			for _, s := range w.sites {
				t.Logf("%s agent log:\n%s", s.m.Name, tail(s.logs.String(), 80))
			}
		}
	})
	return w
}

func (w *world) startAgent(s *site, cfg config.Config, tweaks ...func(*agent.Options)) {
	t := w.t
	tlsFiles := s.m.TLS
	dial := func(endpoint, serverName string) (agent.Client, error) {
		c, err := agent.NewEtcdClient(agent.EtcdConfig{
			Endpoint: endpoint, CertFile: tlsFiles.CertFile, KeyFile: tlsFiles.KeyFile, CAFile: tlsFiles.CAFile, ServerName: serverName,
		})
		if err != nil {
			return nil, err
		}
		return &remoteClient{Client: c, down: &w.linkDown}, nil
	}
	local, err := agent.NewEtcdClient(agent.EtcdConfig{Endpoint: s.m.ClientURL, CertFile: tlsFiles.CertFile, KeyFile: tlsFiles.KeyFile, CAFile: tlsFiles.CAFile})
	if err != nil {
		t.Fatal(err)
	}
	opts := agent.Options{
		Config:  cfg,
		Local:   local,
		Dial:    dial,
		Arbiter: s.arb,
		WAL:     agent.WALOps{Logger: zap.NewNop()},
		Process: &supervised{s: s},
		DataDir: s.m.DataDir,
		Logger:  slog.New(slog.NewJSONHandler(s.logs, nil)).With("member", s.m.Name),
	}
	for _, tweak := range tweaks {
		tweak(&opts)
	}
	a := agent.New(opts)
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel, s.done = cancel, make(chan struct{})
	go func() {
		defer close(s.done)
		if err := a.Run(ctx); err != nil {
			t.Errorf("%s agent: %v", s.m.Name, err)
		}
		_ = local.Close()
	}()
	go s.supervise(ctx)
}

func (s *site) stopAgent() {
	if s.cancel != nil {
		s.cancel()
		<-s.done
		s.cancel = nil
	}
}

// supervise restarts etcd after it exits, as the kubelet does: after a
// delay that doubles with every exit that follows a short run, up to
// maxRestartDelay, and starts over after a run of ten seconds.
func (s *site) supervise(ctx context.Context) {
	delay := time.Second
	for {
		started := time.Now()
		select {
		case <-ctx.Done():
			return
		case <-s.m.Exited():
		}
		if time.Since(started) > 10*time.Second {
			delay = time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		delay = min(2*delay, maxRestartDelay)
		for {
			s.mu.Lock()
			if !s.down {
				if !s.m.Running() {
					s.restarts++
					s.m.Start(s.t, s.extra...)
				}
				s.mu.Unlock()
				break
			}
			s.mu.Unlock()
			// The site is off; powerOn starts etcd.
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
}

// maxRestartDelay is the kubelet's five minutes, shortened for the tests.
const maxRestartDelay = 16 * time.Second

func (s *site) powerOff() {
	s.mu.Lock()
	s.down = true
	s.mu.Unlock()
	if s.m.Running() {
		s.m.Kill(s.t)
	}
}

func (s *site) powerOn() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.down = false
	if !s.m.Running() {
		s.m.Start(s.t, s.extra...)
	}
}

// supervised is the agent's Process for a site: it signals the etcd
// process the test owns and leaves the restart to the supervisor.
type supervised struct{ s *site }

func (p *supervised) Args() (etcdargs.Args, error) {
	if !p.s.m.Running() {
		return etcdargs.Args{}, agent.ErrNoProcess
	}
	return etcdargs.Parse(append([]string{"etcd"}, p.s.m.Args()...))
}

func (p *supervised) Stop(ctx context.Context) error {
	pid := p.s.m.Pid()
	if pid == 0 || !p.s.m.Running() {
		return nil
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	select {
	case <-p.s.m.Exited():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *supervised) Resume(context.Context) error { return nil }

// remoteClient is a client of the other site, cut with the link.
type remoteClient struct {
	agent.Client
	down *atomic.Bool
}

func (c *remoteClient) check() error {
	if c.down.Load() {
		return errLinkDown
	}
	return nil
}

func (c *remoteClient) Status(ctx context.Context) (engine.Status, error) {
	if err := c.check(); err != nil {
		return engine.Status{}, err
	}
	return c.Client.Status(ctx)
}

func (c *remoteClient) Members(ctx context.Context) (engine.Members, error) {
	if err := c.check(); err != nil {
		return engine.Members{}, err
	}
	return c.Client.Members(ctx)
}

func (c *remoteClient) AddLearner(ctx context.Context, peerURLs []string) (engine.Member, error) {
	if err := c.check(); err != nil {
		return engine.Member{}, err
	}
	return c.Client.AddLearner(ctx, peerURLs)
}

// switchArbiter is an arbiter that one site can lose.
type switchArbiter struct {
	arbiter.Arbiter
	down atomic.Bool
}

var errArbiterDown = errors.New("test: the arbiter is unreachable")

func (a *switchArbiter) Read(ctx context.Context) (arbiter.Record, arbiter.Version, error) {
	if a.down.Load() {
		return arbiter.Record{}, arbiter.Zero, errArbiterDown
	}
	return a.Arbiter.Read(ctx)
}

func (a *switchArbiter) CompareAndSwap(ctx context.Context, v arbiter.Version, r arbiter.Record) (arbiter.Version, error) {
	if a.down.Load() {
		return arbiter.Zero, errArbiterDown
	}
	return a.Arbiter.CompareAndSwap(ctx, v, r)
}

func (w *world) cut() {
	w.linkDown.Store(true)
	for _, s := range w.sites {
		s.m.Proxy.Cut()
	}
}

func (w *world) heal() {
	w.linkDown.Store(false)
	for _, s := range w.sites {
		s.m.Proxy.Heal()
	}
}

func (w *world) record() (arbiter.Record, bool) {
	rec, _, err := memarb.New(w.arbName).Read(context.Background())
	return rec, err == nil
}

// write tries one write on a member and reports whether it was
// acknowledged.
func (s *site) write(key, value string, timeout time.Duration) bool {
	if !s.m.Running() {
		return false
	}
	ctx, cancel := context.WithTimeout(clientv3.WithRequireLeader(context.Background()), timeout)
	defer cancel()
	_, err := s.m.Client(s.t).Put(ctx, key, value)
	return err == nil
}

// writer writes keys to both sites in turn until stopped and keeps the
// keys that were acknowledged.
type writer struct {
	mu    sync.Mutex
	acked map[string]string
	first map[string]time.Time
	stop  chan struct{}
	done  chan struct{}
}

func (w *world) startWriter() *writer {
	wr := &writer{acked: map[string]string{}, first: map[string]time.Time{}, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(wr.done)
		for i := 0; ; i++ {
			select {
			case <-wr.stop:
				return
			default:
			}
			for _, s := range w.sites {
				key := fmt.Sprintf("/test/%s/%06d", s.m.Name, i)
				if s.write(key, key, 500*time.Millisecond) {
					wr.mu.Lock()
					wr.acked[key] = key
					if _, ok := wr.first[s.m.Name]; !ok {
						wr.first[s.m.Name] = time.Now()
					}
					wr.mu.Unlock()
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	return wr
}

func (wr *writer) halt() map[string]string {
	close(wr.stop)
	<-wr.done
	wr.mu.Lock()
	defer wr.mu.Unlock()
	return wr.acked
}

// resetFirst forgets when each site first accepted a write.
func (wr *writer) resetFirst() {
	wr.mu.Lock()
	defer wr.mu.Unlock()
	wr.first = map[string]time.Time{}
}

func (wr *writer) firstWrite(name string) (time.Time, bool) {
	wr.mu.Lock()
	defer wr.mu.Unlock()
	at, ok := wr.first[name]
	return at, ok
}

// waitFor polls cond until it holds or the timeout passes.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) time.Duration {
	t.Helper()
	start := time.Now()
	for !cond() {
		if time.Since(start) > timeout {
			t.Fatalf("%s did not happen within %s", what, timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return time.Since(start)
}

func (s *site) status() (engine.Status, error) {
	c, err := agent.NewEtcdClient(agent.EtcdConfig{Endpoint: s.m.ClientURL, CertFile: s.m.TLS.CertFile, KeyFile: s.m.TLS.KeyFile, CAFile: s.m.TLS.CAFile})
	if err != nil {
		return engine.Status{}, err
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return c.Status(ctx)
}

func (s *site) generation() uint64 {
	c, err := agent.NewEtcdClient(agent.EtcdConfig{Endpoint: s.m.ClientURL, CertFile: s.m.TLS.CertFile, KeyFile: s.m.TLS.KeyFile, CAFile: s.m.TLS.CAFile})
	if err != nil {
		return 0
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	g, _ := c.Generation(ctx)
	return g
}

// paired reports whether both members are voters of one cluster with a
// leader, both at generation g.
func (w *world) paired(g uint64) bool {
	var leader uint64
	for _, s := range w.sites {
		st, err := s.status()
		if err != nil || st.Leader == 0 || st.IsLearner || (leader != 0 && st.Leader != leader) || s.generation() != g {
			return false
		}
		leader = st.Leader
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		resp, err := s.m.Client(s.t).MemberList(ctx)
		cancel()
		if err != nil || len(resp.Members) != 2 {
			return false
		}
		for _, m := range resp.Members {
			if m.IsLearner {
				return false
			}
		}
	}
	return true
}

// serving reports which sites accept a write now.
func (w *world) serving() []string {
	var out []string
	for _, s := range w.sites {
		if s.write("/test/probe/"+s.m.Name, "x", 2*time.Second) {
			out = append(out, s.m.Name)
		}
	}
	return out
}

// split reports whether both members accept writes under different
// leaders: two clusters at once. Both members of one healthy pair accept
// writes, a follower by forwarding them, and that is no split.
func (w *world) split() bool {
	leaders := map[uint64]bool{}
	for _, s := range w.sites {
		if !s.write("/test/probe/"+s.m.Name, "x", 2*time.Second) {
			return false
		}
		st, err := s.status()
		if err != nil {
			return false
		}
		leaders[st.Leader] = true
	}
	return len(leaders) > 1
}

// checkAcked reads every acknowledged key from every member.
func (w *world) checkAcked(acked map[string]string) {
	t := w.t
	for _, s := range w.sites {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := s.m.Client(t).Get(ctx, "/test/", clientv3.WithPrefix())
		cancel()
		if err != nil {
			t.Fatalf("read from %s: %v", s.m.Name, err)
		}
		have := map[string]bool{}
		for _, kv := range resp.Kvs {
			have[string(kv.Key)] = true
		}
		missing := 0
		for k := range acked {
			if !have[k] {
				missing++
				if missing <= 5 {
					t.Errorf("%s lost acknowledged write %s", s.m.Name, k)
				}
			}
		}
		if missing > 0 {
			t.Errorf("%s lost %d of %d acknowledged writes", s.m.Name, missing, len(acked))
		}
	}
}

func fast(mode config.Mode) config.Config {
	return config.Config{Mode: mode, PollInterval: 200 * time.Millisecond, ClaimAfter: 3 * time.Second, ClaimDelay: time.Second, FollowerExtraDelay: 2 * time.Second}
}

func defaults(mode config.Mode) config.Config {
	cfg := config.Default()
	cfg.Mode = mode
	return cfg
}

// waitPaired waits for the agents to have verified the arbiter and the pair
// to be at generation g.
func (w *world) waitPaired(g uint64, timeout time.Duration) time.Duration {
	w.t.Helper()
	return waitFor(w.t, timeout, fmt.Sprintf("the pair at generation %d", g), func() bool { return w.paired(g) })
}

func TestIntegrationLinkCutAndHeal(t *testing.T) {
	w := newWorld(t, defaults(config.ModeEnforce))
	w.waitPaired(0, 30*time.Second)
	wr := w.startWriter()
	time.Sleep(2 * time.Second)
	leader := w.cl.Leader(t)

	cut := time.Now()
	w.cut()
	// Writes in flight at the cut may still be acknowledged; the first
	// write that counts is one that started after the leader stepped down.
	time.Sleep(3 * time.Second)
	wr.resetFirst()
	var survivor *site
	var took time.Duration
	waitFor(t, 90*time.Second, "a survivor accepting writes", func() bool {
		for _, s := range w.sites {
			if at, ok := wr.firstWrite(s.m.Name); ok {
				survivor, took = s, at.Sub(cut)
				return true
			}
		}
		return false
	})
	rec, _ := w.record()
	t.Logf("TIMELINE link cut both ways, defaults: first accepted write %.1fs after the cut, by %s (last leader %s), record %+v",
		took.Seconds(), survivor.m.Name, leader.Name, rec)
	if survivor.m != leader {
		t.Errorf("the survivor is %s, want the last leader %s", survivor.m.Name, leader.Name)
	}
	if got := w.serving(); len(got) != 1 || got[0] != survivor.m.Name {
		t.Fatalf("serving during the partition: %v, want only %s", got, survivor.m.Name)
	}
	time.Sleep(10 * time.Second)
	if got := w.serving(); len(got) != 1 {
		t.Fatalf("serving during the partition: %v", got)
	}

	w.heal()
	took = w.waitPaired(1, 3*time.Minute)
	t.Logf("TIMELINE heal: pair at generation 1 %.1fs after the heal (%.1fs after the cut)", took.Seconds(), time.Since(cut).Seconds())
	w.checkAcked(wr.halt())
	if got := w.serving(); len(got) != 2 {
		t.Errorf("serving after the heal: %v, want both", got)
	}
	// The WAL the rejoin kept to undo its edit is removed once the member
	// is a voter again.
	for _, s := range w.sites {
		old := walops.OldWALDir(walops.WALDir(s.m.DataDir))
		waitFor(t, 10*time.Second, "the removal of "+old, func() bool {
			_, err := os.Stat(old)
			return errors.Is(err, os.ErrNotExist)
		})
	}
}

// TestIntegrationOneWayCut cuts one member's proxy only. etcd's rafthttp
// sends some messages over connections the sender opens and some over
// connections the receiver opened, so a one-way cut may or may not cost
// the pair its leader; the test does not assume either. It checks what
// must hold either way: never two members serving, nothing acknowledged
// lost, and the pair back after the heal.
func TestIntegrationOneWayCut(t *testing.T) {
	for _, which := range []string{"leader", "follower"} {
		t.Run(which, func(t *testing.T) {
			w := newWorld(t, fast(config.ModeEnforce))
			w.waitPaired(0, 30*time.Second)
			wr := w.startWriter()
			target := w.cl.Leader(t)
			if which == "follower" {
				target = w.cl.Follower(t)
			}
			target.Proxy.Cut()
			deadline := time.Now().Add(30 * time.Second)
			for time.Now().Before(deadline) {
				if w.split() {
					t.Fatal("two clusters accept writes during a one-way cut")
				}
				time.Sleep(time.Second)
			}
			rec, claimed := w.record()
			t.Logf("one-way cut of the %s's proxy (%s): claimed %v, record %+v, serving %v", which, target.Name, claimed, rec, w.serving())
			target.Proxy.Heal()
			g := uint64(0)
			if claimed {
				g = rec.Generation
			}
			took := w.waitPaired(g, 3*time.Minute)
			t.Logf("TIMELINE one-way cut of the %s: pair at generation %d %.1fs after the heal", which, g, took.Seconds())
			w.checkAcked(wr.halt())
		})
	}
}

func TestIntegrationArbiterUnreachable(t *testing.T) {
	w := newWorld(t, fast(config.ModeEnforce))
	w.waitPaired(0, 30*time.Second)
	for _, s := range w.sites {
		s.arb.down.Store(true)
	}
	w.cut()
	time.Sleep(15 * time.Second)
	if got := w.serving(); len(got) != 0 {
		t.Fatalf("serving without the arbiter: %v", got)
	}
	if _, ok := w.record(); ok {
		t.Fatal("a claim was written while the arbiter was unreachable")
	}
	w.heal()
	for _, s := range w.sites {
		s.arb.down.Store(false)
	}
	w.waitPaired(0, time.Minute)
}

func TestIntegrationSurvivorDiesBeforeRejoin(t *testing.T) {
	w := newWorld(t, fast(config.ModeEnforce))
	w.waitPaired(0, 30*time.Second)
	w.cut()
	var survivor, loser *site
	waitFor(t, 60*time.Second, "a survivor", func() bool {
		rec, ok := w.record()
		if !ok {
			return false
		}
		for _, s := range w.sites {
			if s.m.Name == rec.Survivor && s.generation() == 1 {
				survivor = s
			} else if s.m.Name != rec.Survivor {
				loser = s
			}
		}
		return survivor != nil
	})
	survivor.powerOff()
	w.heal()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if loser.write("/test/loser", "x", time.Second) {
			t.Fatal("the loser accepted a write after the survivor died")
		}
		time.Sleep(500 * time.Millisecond)
	}
	if rec, _ := w.record(); rec.Generation != 1 || rec.Survivor != survivor.m.Name {
		t.Errorf("record = %+v", rec)
	}
	if !strings.Contains(loser.logs.String(), `"role":"stale"`) {
		t.Errorf("the loser is not stale:\n%s", tail(loser.logs.String(), 20))
	}
	// When the survivor returns, the loser rejoins it.
	survivor.powerOn()
	w.waitPaired(1, 3*time.Minute)
}

func TestIntegrationDryRun(t *testing.T) {
	w := newWorld(t, fast(config.ModeDryRun))
	w.waitPaired(0, 30*time.Second)
	w.cut()
	time.Sleep(15 * time.Second)
	if _, ok := w.record(); ok {
		t.Fatal("dry-run wrote the arbiter")
	}
	if got := w.serving(); len(got) != 0 {
		t.Fatalf("serving in dry-run: %v", got)
	}
	for _, s := range w.sites {
		if !strings.Contains(s.logs.String(), `"action":"claim","dryRun":true`) {
			t.Errorf("%s did not report the claim it would make:\n%s", s.m.Name, tail(s.logs.String(), 20))
		}
	}
	w.heal()
	w.waitPaired(0, time.Minute)
}

// TestIntegrationRemovedBeforeTheArbiterWasRead forces the order seen on
// the Kubernetes bench with a one-way cut: the loser's etcd learns from the
// survivor that it was removed before the loser's own isolation led it to
// the arbiter, exits, and is restarted in a loop with a growing delay. The
// loser never reads the arbiter while etcd runs, because its claim-after is
// an hour; it has to find out from the arbiter while etcd is down, and
// rejoin.
func TestIntegrationRemovedBeforeTheArbiterWasRead(t *testing.T) {
	slow := fast(config.ModeEnforce)
	slow.ClaimAfter = time.Hour
	w := newWorldPerSite(t, slow, fast(config.ModeEnforce))
	loser, survivor := w.sites[0], w.sites[1]
	w.waitPaired(0, 30*time.Second)
	wr := w.startWriter()
	w.cut()
	waitFor(t, time.Minute, "m1 serving alone", func() bool {
		rec, ok := w.record()
		return ok && rec.Survivor == survivor.m.Name && survivor.generation() == 1
	})
	healed := time.Now()
	w.heal()
	waitFor(t, time.Minute, "m0's etcd exiting as removed", func() bool {
		loser.mu.Lock()
		defer loser.mu.Unlock()
		return loser.restarts > 0
	})
	w.waitPaired(1, 4*time.Minute)
	paired := time.Since(healed)
	// The agent notices etcd's return at its own pace; the rejoin is done
	// when it does.
	waitFor(t, 2*time.Minute, "m0's agent seeing etcd back", func() bool {
		return strings.Contains(loser.logs.String(), `"msg":"rejoin: etcd runs as the new learner"`)
	})
	loser.mu.Lock()
	restarts := loser.restarts
	loser.mu.Unlock()
	t.Logf("TIMELINE removed before the arbiter was read: pair at generation 1 %.1fs after the heal, m0's etcd restarted %d times",
		paired.Seconds(), restarts)
	logs := loser.logs.String()
	// m0 went from unknown, with etcd down, straight to stale: it read
	// the arbiter only because etcd stopped answering.
	if !strings.Contains(logs, `"role":"stale","note":"generation 0 is behind the arbiter's 1 (survivor m1), rejoining as a learner","generation":0,"mode":"enforce","from":"unknown"`) {
		t.Errorf("m0 did not find out it was stale while etcd was down:\n%s", tail(logs, 30))
	}
	if strings.Contains(logs, `"action":"claim"`) {
		t.Error("m0 claimed")
	}
	w.checkAcked(wr.halt())
}

// TestIntegrationSurvivorThroughAName finds the survivor only through a
// name that resolves to both members, as a headless Service's does: the
// loser's agent is restarted while it is cut off, never reads a member
// list, and has only https://localhost:<port>, which resolves to
// 127.0.0.2 and 127.0.0.3, each forwarding to one member's client port.
// etcdtest's certificates carry localhost, not those addresses, so the
// rejoin also shows that the name, not the address, is verified.
func TestIntegrationSurvivorThroughAName(t *testing.T) {
	slow := fast(config.ModeEnforce)
	slow.ClaimAfter = time.Hour
	w := newWorldPerSite(t, slow, fast(config.ModeEnforce))
	loser, survivor := w.sites[0], w.sites[1]
	port := forwardPair(t, loser.m.ClientURL, survivor.m.ClientURL)
	w.waitPaired(0, 30*time.Second)
	w.cut()
	waitFor(t, time.Minute, "m1 serving alone", func() bool {
		rec, ok := w.record()
		return ok && rec.Survivor == survivor.m.Name && survivor.generation() == 1
	})
	loser.stopAgent()
	res := &tableResolver{hosts: map[string][]string{"localhost": {"127.0.0.2", "127.0.0.3"}}}
	w.startAgent(loser, slow, func(o *agent.Options) {
		o.Local = &noMembers{Client: o.Local}
		o.PeerEndpoints = []string{"https://localhost:" + port}
		o.Resolver = res
	})
	w.heal()
	w.waitPaired(1, 4*time.Minute)
	waitFor(t, 2*time.Minute, "m0's agent seeing etcd back", func() bool {
		return strings.Contains(loser.logs.String(), `"msg":"rejoin: etcd runs as the new learner"`)
	})
	if want := `"survivor":"https://127.0.0.3:` + port + `"`; !strings.Contains(loser.logs.String(), want) {
		t.Errorf("m0 did not find the survivor through the name, want %s:\n%s", want, tail(loser.logs.String(), 30))
	}
}

// noMembers hides the member list, as for an agent that never saw it.
type noMembers struct{ agent.Client }

func (noMembers) Members(context.Context) (engine.Members, error) {
	return engine.Members{}, errors.New("test: no member list")
}

type tableResolver struct{ hosts map[string][]string }

func (r *tableResolver) LookupHost(_ context.Context, host string) ([]string, error) {
	if addrs, ok := r.hosts[host]; ok {
		return addrs, nil
	}
	return nil, fmt.Errorf("lookup %s: no such host", host)
}

// forwardPair listens on 127.0.0.2 and 127.0.0.3 on one port, forwarding
// to the two client URLs, and returns the port.
func forwardPair(t *testing.T, to0, to1 string) string {
	t.Helper()
	for range 20 {
		l0, err := net.Listen("tcp", "127.0.0.2:0")
		if err != nil {
			t.Skipf("no 127.0.0.2 on this host: %v", err)
		}
		_, port, _ := net.SplitHostPort(l0.Addr().String())
		l1, err := net.Listen("tcp", "127.0.0.3:"+port)
		if err != nil {
			_ = l0.Close()
			continue
		}
		forward(t, l0, strings.TrimPrefix(to0, "https://"))
		forward(t, l1, strings.TrimPrefix(to1, "https://"))
		return port
	}
	t.Fatal("no port free on both 127.0.0.2 and 127.0.0.3")
	return ""
}

func forward(t *testing.T, l net.Listener, target string) {
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			in, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = in.Close() }()
				out, err := net.Dial("tcp", target)
				if err != nil {
					return
				}
				defer func() { _ = out.Close() }()
				go func() { _, _ = io.Copy(out, in) }()
				_, _ = io.Copy(in, out)
			}()
		}
	}()
}

// lockedBuffer collects an agent's log.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func tail(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
