package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aenix-io/twok/internal/arbiter"
	"github.com/aenix-io/twok/internal/arbiter/memarb"
	"github.com/aenix-io/twok/internal/config"
	"github.com/aenix-io/twok/internal/engine"
	"github.com/aenix-io/twok/internal/etcdargs"
)

const clusterID = 0xc1

var errDown = errors.New("connection refused")

// arbiters numbers the in-memory arbiters, so that every harness, also in
// repeated runs of one test, gets a register of its own.
var arbiters atomic.Int64

// fakeMember is one etcd member's client API.
type fakeMember struct {
	mu         sync.Mutex
	endpoint   string
	down       bool
	st         engine.Status
	members    engine.Members
	gen        uint64
	genErr     error
	added      [][]string
	promoted   []uint64
	promoteErr error
	puts       []uint64
	statusN    int
	hang       chan struct{}
	nextID     uint64
}

func (f *fakeMember) Endpoint() string { return f.endpoint }

func (f *fakeMember) Status(context.Context) (engine.Status, error) {
	f.mu.Lock()
	hang := f.hang
	f.statusN++
	f.mu.Unlock()
	if hang != nil {
		<-hang
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return engine.Status{}, errDown
	}
	return f.st, nil
}

func (f *fakeMember) Members(context.Context) (engine.Members, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case f.down:
		return engine.Members{}, errDown
	case f.st.IsLearner:
		return engine.Members{}, ErrLearner
	}
	return engine.Members{Voters: slices.Clone(f.members.Voters), Learners: slices.Clone(f.members.Learners)}, nil
}

func (f *fakeMember) Generation(context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return 0, errDown
	}
	return f.gen, f.genErr
}

func (f *fakeMember) PutGeneration(_ context.Context, g uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down || f.st.Leader == 0 {
		return errors.New("no leader")
	}
	f.puts = append(f.puts, g)
	f.gen = g
	return nil
}

func (f *fakeMember) AddLearner(_ context.Context, peerURLs []string) (engine.Member, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return engine.Member{}, errDown
	}
	f.added = append(f.added, peerURLs)
	f.nextID++
	m := engine.Member{ID: 0x100 + f.nextID, PeerURLs: peerURLs}
	f.members.Learners = append(f.members.Learners, m)
	return m, nil
}

func (f *fakeMember) Promote(_ context.Context, id uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.promoteErr != nil {
		return f.promoteErr
	}
	f.promoted = append(f.promoted, id)
	for i, l := range f.members.Learners {
		if l.ID == id {
			f.members.Voters = append(f.members.Voters, l)
			f.members.Learners = slices.Delete(f.members.Learners, i, i+1)
			break
		}
	}
	return nil
}

func (f *fakeMember) Close() error { return nil }

func (f *fakeMember) set(fn func(f *fakeMember)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

// fakeWAL records the edits and applies them to the local fake member, as
// etcd would after its restart.
type fakeWAL struct {
	mu          sync.Mutex
	local       *fakeMember
	locked      int
	err         error
	standalones []string
	reids       []uint64
}

func (w *fakeWAL) Standalone(dataDir string, self uint64, remove []uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.locked > 0 {
		w.locked--
		return fmt.Errorf("lock %s: %w", dataDir, ErrWALLocked)
	}
	if w.err != nil {
		return w.err
	}
	w.standalones = append(w.standalones, fmt.Sprintf("%s %x %v", dataDir, self, remove))
	w.local.set(func(f *fakeMember) {
		f.members.Voters = slices.DeleteFunc(f.members.Voters, func(m engine.Member) bool { return m.ID != self })
		f.members.Learners = nil
		f.st.Leader = self
	})
	return nil
}

func (w *fakeWAL) Reidentify(_ string, newID uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.locked > 0 {
		w.locked--
		return ErrWALLocked
	}
	w.reids = append(w.reids, newID)
	w.local.set(func(f *fakeMember) {
		f.st.MemberID, f.st.IsLearner, f.st.Leader = newID, true, 0
	})
	return nil
}

// fakeProcess is the etcd process: stopping it makes the member stop
// answering, resuming it brings it back.
type fakeProcess struct {
	mu      sync.Mutex
	local   *fakeMember
	argsErr error
	stops   int
	resumes int
}

func (p *fakeProcess) Args() (etcdargs.Args, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.argsErr != nil {
		return etcdargs.Args{}, p.argsErr
	}
	return etcdargs.Parse([]string{"etcd", "--name=a", "--initial-advertise-peer-urls=https://a:2380", "--data-dir=/var/lib/etcd"})
}

func (p *fakeProcess) Stop(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stops++
	p.local.set(func(f *fakeMember) { f.down = true })
	return nil
}

func (p *fakeProcess) Resume(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resumes++
	p.local.set(func(f *fakeMember) { f.down = false })
	return nil
}

// syncBuffer is a log destination safe for the watchdog goroutine.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// flakyArbiter wraps an arbiter and fails on demand.
type flakyArbiter struct {
	arbiter.Arbiter
	mu        sync.Mutex
	down      bool
	verifyErr error
	reads     int
	verifies  int
	swaps     int
}

func (a *flakyArbiter) Read(ctx context.Context) (arbiter.Record, arbiter.Version, error) {
	a.mu.Lock()
	a.reads++
	down := a.down
	a.mu.Unlock()
	if down {
		return arbiter.Record{}, arbiter.Zero, errDown
	}
	return a.Arbiter.Read(ctx)
}

func (a *flakyArbiter) CompareAndSwap(ctx context.Context, v arbiter.Version, r arbiter.Record) (arbiter.Version, error) {
	a.mu.Lock()
	a.swaps++
	down := a.down
	a.mu.Unlock()
	if down {
		return arbiter.Zero, errDown
	}
	return a.Arbiter.CompareAndSwap(ctx, v, r)
}

func (a *flakyArbiter) Verify(ctx context.Context) error {
	a.mu.Lock()
	a.verifies++
	err := a.verifyErr
	a.mu.Unlock()
	if err != nil {
		return err
	}
	return a.Arbiter.Verify(ctx)
}

type harness struct {
	t      *testing.T
	agent  *Agent
	local  *fakeMember
	peer   *fakeMember
	wal    *fakeWAL
	proc   *fakeProcess
	arb    *flakyArbiter
	logs   *syncBuffer
	now    time.Time
	dialed []string
}

func fastConfig(mode config.Mode) config.Config {
	return config.Config{Mode: mode, PollInterval: time.Second, ClaimAfter: 5 * time.Second, ClaimDelay: time.Second, FollowerExtraDelay: 2 * time.Second}
}

func member(id uint64, name string) engine.Member {
	return engine.Member{ID: id, Name: name, PeerURLs: []string{"https://" + name + ":2380"}, ClientURLs: []string{"https://" + name + ":2379"}}
}

// newHarness is member a (0xa) of a pair with b (0xb), a leading.
func newHarness(t *testing.T, cfg config.Config) *harness {
	t.Helper()
	local := &fakeMember{
		endpoint: "https://127.0.0.1:2379",
		st:       engine.Status{ClusterID: clusterID, MemberID: 0xa, Leader: 0xa, CommitIndex: 100, AppliedIndex: 100},
		members:  engine.Members{Voters: []engine.Member{member(0xa, "a"), member(0xb, "b")}},
	}
	peer := &fakeMember{
		endpoint: "https://b:2379",
		st:       engine.Status{ClusterID: clusterID, MemberID: 0xb, Leader: 0xb},
		members:  engine.Members{Voters: []engine.Member{member(0xb, "b")}},
		gen:      1,
	}
	h := &harness{
		t: t, local: local, peer: peer,
		wal:  &fakeWAL{local: local},
		proc: &fakeProcess{local: local},
		arb:  &flakyArbiter{Arbiter: memarb.New(fmt.Sprintf("%s/%d", t.Name(), arbiters.Add(1)))},
		logs: &syncBuffer{},
		now:  time.Date(2026, 10, 21, 20, 0, 0, 0, time.UTC),
	}
	h.agent = New(Options{
		Config: cfg,
		Local:  local,
		Dial: func(endpoint string) (Client, error) {
			h.dialed = append(h.dialed, endpoint)
			if endpoint == peer.endpoint {
				return peer, nil
			}
			return &fakeMember{endpoint: endpoint, down: true}, nil
		},
		Arbiter: h.arb,
		WAL:     h.wal,
		Process: h.proc,
		DataDir: "/var/lib/etcd",
		Logger:  slog.New(slog.NewJSONHandler(h.logs, nil)),
		Now:     func() time.Time { return h.now },
	})
	h.agent.waitInterval = time.Millisecond
	return h
}

func (h *harness) ticks(n int) {
	for range n {
		h.agent.Tick(context.Background())
		h.now = h.now.Add(time.Second)
	}
}

// cutLink leaves the local member without a leader.
func (h *harness) cutLink() {
	h.local.set(func(f *fakeMember) { f.st.Leader = 0 })
}

func (h *harness) record() (arbiter.Record, bool) {
	rec, _, err := h.arb.Arbiter.Read(context.Background())
	return rec, err == nil
}

func (h *harness) logCount(s string) int {
	return strings.Count(h.logs.String(), s)
}
