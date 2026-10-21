package engine

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/aenix-io/twok/internal/arbiter"
)

const clusterID = 0xc1

var start = time.Date(2026, 10, 21, 20, 0, 0, 0, time.UTC)

var errUnreachable = errors.New("unreachable")

// node is one member as the simulation sees it: whether etcd runs, its
// own view of the membership, and the generation key in its data.
type node struct {
	name string
	id   uint64
	e    *Engine
	up   bool
	// voters and learners are the membership in this member's own data.
	voters    []uint64
	learners  []uint64
	isLearner bool
	gen       uint64
	// reachesArbiter is false while this member cannot reach the arbiter.
	reachesArbiter bool
	canControl     bool
	result         *Result
	// loseClaimResult makes the next claim write the record and report an
	// error, as a response lost on the way back does.
	loseClaimResult bool
	actions         []Decision
	last            Decision
}

// world is two members, the link between them and the arbiter.
type world struct {
	t      *testing.T
	now    time.Time
	nodes  []*node
	linkUp bool
	// pairLeader is the leader while the two members form a pair.
	pairLeader uint64
	found      bool
	record     arbiter.Record
	version    int
	nextID     uint64
	readsBy    map[string]int
}

func testParams() Params {
	p := DefaultParams()
	p.Mode = ModeEnforce
	p.Version = "test"
	return p
}

func newWorld(t *testing.T) *world {
	t.Helper()
	w := &world{t: t, now: start, linkUp: true, pairLeader: 0xa, nextID: 0x100, readsBy: map[string]int{}}
	for _, n := range []struct {
		name string
		id   uint64
	}{{"a", 0xa}, {"b", 0xb}} {
		w.nodes = append(w.nodes, &node{
			name: n.name, id: n.id, e: New(testParams()), up: true,
			voters: []uint64{0xa, 0xb}, reachesArbiter: true, canControl: true,
		})
	}
	return w
}

func (w *world) node(name string) *node {
	for _, n := range w.nodes {
		if n.name == name {
			return n
		}
	}
	w.t.Fatalf("no node %s", name)
	return nil
}

func (w *world) peer(n *node) *node {
	for _, m := range w.nodes {
		if m != n {
			return m
		}
	}
	return nil
}

// leader is the leader n sees: itself in a cluster of one, the pair leader
// while both members are up, connected and in the same two-voter cluster,
// and the survivor for a connected learner.
func (w *world) leader(n *node) uint64 {
	p := w.peer(n)
	switch {
	case !n.up:
		return 0
	case n.isLearner:
		if w.linkUp && p.up {
			return p.id
		}
		return 0
	case len(n.voters) == 1 && n.voters[0] == n.id:
		return n.id
	case w.linkUp && p.up && !p.isLearner && slices.Equal(sorted(n.voters), sorted(p.voters)) && len(n.voters) == 2:
		if slices.Contains(n.voters, w.pairLeader) {
			return w.pairLeader
		}
		return n.voters[0]
	}
	return 0
}

func sorted(ids []uint64) []uint64 {
	s := slices.Clone(ids)
	slices.Sort(s)
	return s
}

func (w *world) nameOf(id uint64) string {
	for _, n := range w.nodes {
		if n.id == id {
			return n.name
		}
	}
	return ""
}

func (w *world) members(n *node) *Members {
	if n.isLearner {
		return nil
	}
	ms := &Members{}
	for _, id := range n.voters {
		ms.Voters = append(ms.Voters, Member{ID: id, Name: w.nameOf(id), PeerURLs: []string{peerURL(w.nameOf(id))}, ClientURLs: []string{clientURL(w.nameOf(id))}})
	}
	for _, id := range n.learners {
		ms.Learners = append(ms.Learners, Member{ID: id, Name: w.nameOf(id), PeerURLs: []string{peerURL(w.nameOf(id))}, ClientURLs: []string{clientURL(w.nameOf(id))}})
	}
	return ms
}

func peerURL(name string) string   { return "https://" + name + ":2380" }
func clientURL(name string) string { return "https://" + name + ":2379" }

func (w *world) observation(n *node) Observation {
	o := Observation{Now: w.now, CanControl: n.canControl, ArbiterReady: true, Result: n.result}
	n.result = nil
	if !n.up {
		return o
	}
	o.Local = &Status{ClusterID: clusterID, MemberID: n.id, Leader: w.leader(n), IsLearner: n.isLearner, CommitIndex: 100, AppliedIndex: 100}
	o.Members = w.members(n)
	o.Generation, o.GenerationKnown = n.gen, true
	return o
}

func (w *world) read(n *node) *ArbiterRead {
	w.readsBy[n.name]++
	if !n.reachesArbiter {
		return &ArbiterRead{Err: errUnreachable}
	}
	return &ArbiterRead{Found: w.found, Record: w.record, Version: w.ver()}
}

func (w *world) ver() arbiter.Version {
	if !w.found {
		return arbiter.Zero
	}
	return arbiter.Version(strconv.Itoa(w.version))
}

// poll runs one poll of n: observe, read the arbiter when asked and decide
// again, then execute the action.
func (w *world) poll(n *node) Decision {
	o := w.observation(n)
	d := n.e.Observe(o)
	if d.Action.Kind == ActionReadArbiter {
		n.actions = append(n.actions, d)
		o.Result = nil
		o.Arbiter = w.read(n)
		d = n.e.Observe(o)
	}
	n.last = d
	if d.Action.Kind != ActionNone {
		n.actions = append(n.actions, d)
		if !d.DryRun {
			n.result = w.execute(n, d.Action)
		}
	}
	return d
}

func (w *world) execute(n *node, a Action) *Result {
	r := &Result{Action: a.Kind}
	p := w.peer(n)
	switch a.Kind {
	case ActionClaim:
		r.Record = a.Record
		switch {
		case !n.reachesArbiter:
			r.Err = errUnreachable
		case a.Expected != w.ver():
			r.Err, r.Conflict = arbiter.ErrConflict, true
		default:
			w.found, w.record = true, a.Record
			w.version++
			if n.loseClaimResult {
				n.loseClaimResult = false
				r.Err = errors.New("response lost")
			}
		}
	case ActionTakeover:
		n.voters = []uint64{n.id}
		n.learners = nil
	case ActionWriteGeneration:
		if w.leader(n) != n.id {
			r.Err = errors.New("no leader")
			break
		}
		n.gen = a.Generation
	case ActionRejoin:
		if !w.linkUp || !p.up || !slices.Contains(a.SurvivorEndpoints, clientURL(p.name)) {
			r.Err = errUnreachable
			break
		}
		if len(p.voters) != 1 {
			r.Err = errors.New("peer URL exists")
			break
		}
		w.nextID++
		id := w.nextID
		p.learners = append(p.learners, id)
		n.id, n.isLearner = id, true
		n.voters, n.learners = slices.Clone(p.voters), slices.Clone(p.learners)
		n.gen = p.gen
		r.Learner = id
	case ActionPromote:
		learner := w.byID(a.Learner)
		if learner == nil || !w.linkUp || !learner.up {
			r.Err, r.NotReady = errors.New("not ready"), true
			break
		}
		p.learners = slices.DeleteFunc(p.learners, func(id uint64) bool { return id == a.Learner })
		n.learners = slices.DeleteFunc(n.learners, func(id uint64) bool { return id == a.Learner })
		n.voters = append(n.voters, a.Learner)
		learner.isLearner = false
		learner.voters, learner.learners = slices.Clone(n.voters), nil
		learner.gen = n.gen
	}
	return r
}

func (w *world) byID(id uint64) *node {
	for _, n := range w.nodes {
		if n.id == id {
			return n
		}
	}
	return nil
}

// sync lets a connected learner catch up and a pair share the key.
func (w *world) sync() {
	a, b := w.nodes[0], w.nodes[1]
	if !w.linkUp || !a.up || !b.up {
		return
	}
	if l := w.leader(a); l != 0 && l == w.leader(b) {
		g := max(a.gen, b.gen)
		a.gen, b.gen = g, g
	}
}

// step polls both members once and advances the clock by a second.
func (w *world) step() {
	for _, n := range w.nodes {
		w.poll(n)
	}
	w.sync()
	w.now = w.now.Add(time.Second)
}

func (w *world) run(seconds int) {
	for range seconds {
		w.step()
	}
}

// runUntil steps until cond holds and returns the seconds it took, or fails
// after limit seconds.
func (w *world) runUntil(limit int, what string, cond func() bool) int {
	w.t.Helper()
	for i := range limit {
		if cond() {
			return i
		}
		w.step()
	}
	w.t.Fatalf("%s did not happen within %ds; a: %+v; b: %+v", what, limit, w.node("a").last, w.node("b").last)
	return limit
}

func (w *world) restartEngine(n *node) {
	n.e = New(testParams())
	n.result = nil
	n.actions = nil
}

func (n *node) count(kind ActionKind) int {
	c := 0
	for _, d := range n.actions {
		if d.Action.Kind == kind {
			c++
		}
	}
	return c
}

func (n *node) serving(w *world) bool {
	return n.up && !n.isLearner && w.leader(n) == n.id && len(n.voters) == 1
}

func (w *world) String() string {
	var s string
	for _, n := range w.nodes {
		s += fmt.Sprintf("%s: role=%s note=%q voters=%v gen=%d; ", n.name, n.last.Role, n.last.Note, n.voters, n.gen)
	}
	return s
}
