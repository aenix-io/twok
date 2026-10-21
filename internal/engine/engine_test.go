package engine

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aenix-io/twok/internal/arbiter"
)

// settle runs a healthy pair long enough for both engines to read the
// arbiter at start.
func settle(t *testing.T, w *world) {
	t.Helper()
	w.run(3)
	for _, n := range w.nodes {
		if n.last.Role != RolePair {
			t.Fatalf("%s: role %s before any failure, want pair: %s", n.name, n.last.Role, w)
		}
		if n.count(ActionClaim)+n.count(ActionTakeover) > 0 {
			t.Fatalf("%s acted on a healthy pair: %v", n.name, n.actions)
		}
	}
}

// expectPairAgain heals what the test broke and waits for the loser to
// rejoin and be promoted, with both members at generation want.
func expectPairAgain(t *testing.T, w *world, want uint64) {
	t.Helper()
	w.runUntil(120, "the pair forming again", func() bool {
		a, b := w.node("a"), w.node("b")
		return a.last.Role == RolePair && b.last.Role == RolePair && len(a.voters) == 2 && len(b.voters) == 2 &&
			a.gen == want && b.gen == want
	})
}

// The failure model table of docs/design.md, row by row.

func TestLinkDownBothReachTheArbiter(t *testing.T) {
	w := newWorld(t)
	settle(t, w)
	w.linkUp = false
	cut := w.now
	a, b := w.node("a"), w.node("b")
	took := w.runUntil(60, "a serving alone", func() bool { return a.serving(w) && a.gen == 1 })
	// claim-after 20s, then claim-delay 3s for the last leader; the
	// takeover and the generation write take a poll each.
	if took < 23 || took > 27 {
		t.Errorf("a served alone %ds after the cut, want about 24s", took)
	}
	if w.record.Survivor != "a" || w.record.Generation != 1 || w.record.Reason != ReasonNoLeader || w.record.ClusterID != "c1" ||
		w.record.SurvivorMemberID != "a" || w.record.TwokVersion != "test" || !w.record.Since.After(cut) {
		t.Errorf("record = %+v", w.record)
	}
	w.runUntil(20, "b stale", func() bool { return b.last.Role == RoleStale })
	if b.count(ActionTakeover) != 0 || b.serving(w) {
		t.Fatalf("the loser took over: %v", b.actions)
	}
	// The loser keeps trying to rejoin a survivor it cannot reach, and
	// never claims again.
	w.run(60)
	if b.last.Role != RoleStale || b.serving(w) {
		t.Fatalf("b left stale while cut off: %s", w)
	}
	w.linkUp = true
	expectPairAgain(t, w, 1)
	if a.count(ActionPromote) == 0 || b.count(ActionRejoin) == 0 {
		t.Errorf("rejoin and promote expected: a %v, b %v", a.actions, b.actions)
	}
}

func TestLinkDownOnlyOneReachesTheArbiter(t *testing.T) {
	w := newWorld(t)
	w.pairLeader = 0xb
	settle(t, w)
	a, b := w.node("a"), w.node("b")
	w.linkUp = false
	b.reachesArbiter = false
	// a was a follower: claim-delay plus follower-extra-delay.
	took := w.runUntil(60, "a serving alone", func() bool { return a.serving(w) })
	if took < 28 || took > 32 {
		t.Errorf("a served alone %ds after the cut, want about 29s", took)
	}
	w.run(30)
	if b.count(ActionClaim) != 0 || b.serving(w) {
		t.Fatalf("b, which cannot reach the arbiter, acted: %v", b.actions)
	}
	if b.last.Role != RoleIsolated {
		t.Errorf("b role = %s, want isolated while it cannot read the arbiter", b.last.Role)
	}
	b.reachesArbiter = true
	w.runUntil(10, "b stale", func() bool { return b.last.Role == RoleStale })
	w.linkUp = true
	expectPairAgain(t, w, 1)
}

func TestLinkDownArbiterUnreachable(t *testing.T) {
	w := newWorld(t)
	settle(t, w)
	a, b := w.node("a"), w.node("b")
	w.linkUp = false
	a.reachesArbiter, b.reachesArbiter = false, false
	w.run(120)
	for _, n := range w.nodes {
		if n.count(ActionClaim) != 0 || n.serving(w) {
			t.Fatalf("%s acted without the arbiter: %v", n.name, n.actions)
		}
		if n.last.Role != RoleIsolated {
			t.Errorf("%s role = %s, want isolated", n.name, n.last.Role)
		}
	}
	// Reads are retried every poll while isolated.
	if w.readsBy["a"] < 90 {
		t.Errorf("a read the arbiter %d times in 100s of isolation, want every poll", w.readsBy["a"])
	}
	a.reachesArbiter, b.reachesArbiter = true, true
	w.runUntil(15, "a survivor once the arbiter is back", func() bool { return a.serving(w) })
	w.linkUp = true
	expectPairAgain(t, w, 1)
}

func TestSiteDown(t *testing.T) {
	w := newWorld(t)
	settle(t, w)
	a, b := w.node("a"), w.node("b")
	a.up = false
	w.runUntil(60, "b serving alone", func() bool { return b.serving(w) && b.gen == 1 })
	w.run(10)
	a.up = true
	expectPairAgain(t, w, 1)
	if a.id == 0xa {
		t.Errorf("a rejoined under its old ID")
	}
}

func TestSiteDownThenSurvivorDown(t *testing.T) {
	w := newWorld(t)
	settle(t, w)
	a, b := w.node("a"), w.node("b")
	a.up = false
	w.runUntil(60, "b serving alone", func() bool { return b.serving(w) && b.gen == 1 })
	b.up = false
	a.up = true
	w.run(300)
	if a.serving(w) || a.count(ActionClaim) != 0 || a.count(ActionTakeover) != 0 {
		t.Fatalf("a went on alone with older data: %v", a.actions)
	}
	if a.last.Role != RoleStale {
		t.Errorf("a role = %s, want stale", a.last.Role)
	}
	if n := a.count(ActionRejoin); n == 0 || n > 300/5+1 {
		t.Errorf("a tried to rejoin %d times in 300s, want about one per retry delay", n)
	}
}

func TestArbiterDownClusterHealthy(t *testing.T) {
	w := newWorld(t)
	a, b := w.node("a"), w.node("b")
	a.reachesArbiter, b.reachesArbiter = false, false
	w.run(120)
	for _, n := range w.nodes {
		if n.last.Role != RolePair || n.count(ActionClaim) != 0 {
			t.Errorf("%s: role %s, actions %v", n.name, n.last.Role, n.actions)
		}
		// Only the start-up read, retried every RetryDelay.
		if r := w.readsBy[n.name]; r < 20 || r > 26 {
			t.Errorf("%s read the arbiter %d times in 120s, want one per retry delay", n.name, r)
		}
	}
	a.reachesArbiter, b.reachesArbiter = true, true
	w.run(10)
	reads := w.readsBy["a"]
	w.run(60)
	if w.readsBy["a"] != reads {
		t.Errorf("a kept reading the arbiter after the start-up read succeeded")
	}
}

func TestOldSurvivorReturnsAfterNewerGeneration(t *testing.T) {
	w := newWorld(t)
	settle(t, w)
	a, b := w.node("a"), w.node("b")
	w.linkUp = false
	w.runUntil(60, "a serving alone", func() bool { return a.serving(w) && a.gen == 1 })
	w.linkUp = true
	expectPairAgain(t, w, 1)
	a.up = false
	w.runUntil(60, "b serving alone at generation 2", func() bool { return b.serving(w) && b.gen == 2 })
	if w.record.Survivor != "b" || w.record.Generation != 2 {
		t.Fatalf("record = %+v", w.record)
	}
	a.up = true
	expectPairAgain(t, w, 2)
}

func TestBothRestartSameGeneration(t *testing.T) {
	w := newWorld(t)
	settle(t, w)
	for _, n := range w.nodes {
		w.restartEngine(n)
		n.up = false
	}
	w.run(5)
	for _, n := range w.nodes {
		if n.last.Role != RoleUnknown {
			t.Errorf("%s role = %s while etcd is down and nothing is known, want unknown", n.name, n.last.Role)
		}
		n.up = true
	}
	w.run(60)
	for _, n := range w.nodes {
		if n.last.Role != RolePair || len(n.actions) == 0 || n.count(ActionReadArbiter) != 1 || len(n.actions) != 1 {
			t.Errorf("%s: role %s, actions %v; want pair after one start-up read", n.name, n.last.Role, n.actions)
		}
	}
}

func TestPartitionFlaps(t *testing.T) {
	w := newWorld(t)
	settle(t, w)
	for range 10 {
		w.linkUp = false
		w.run(19)
		w.linkUp = true
		w.run(2)
	}
	for _, n := range w.nodes {
		if n.count(ActionClaim) != 0 || n.count(ActionReadArbiter) != 1 {
			t.Fatalf("%s acted on a flapping link: %v", n.name, n.actions)
		}
	}
	// A claim that happened is final, even if the link heals right after.
	a := w.node("a")
	w.linkUp = false
	w.runUntil(30, "a claim", func() bool { return a.count(ActionClaim) > 0 })
	w.linkUp = true
	w.runUntil(10, "a serving alone", func() bool { return a.serving(w) && a.gen == 1 })
	expectPairAgain(t, w, 1)
}

func TestOneWayLinkFailure(t *testing.T) {
	// A one-way failure leaves both members without a leader, as a full
	// one does: Raft needs both directions. The engine sees the same
	// observations, so the outcome is the same.
	TestLinkDownBothReachTheArbiter(t)
}

// Further situations.

func TestLearnerNeverClaims(t *testing.T) {
	e := New(testParams())
	now := start
	for i := range 120 {
		d := e.Observe(Observation{
			Now: now.Add(time.Duration(i) * time.Second), ArbiterReady: true, CanControl: true,
			Local:           &Status{ClusterID: clusterID, MemberID: 0x100, IsLearner: true},
			GenerationKnown: true,
			Arbiter:         &ArbiterRead{Found: true, Record: arbiter.Record{Generation: 0, ClusterID: "c1"}, Version: "1"},
		})
		if d.Action.Kind != ActionNone {
			t.Fatalf("learner decided %v", d.Action)
		}
		if d.Role != RoleJoining {
			t.Fatalf("learner role %s, want joining", d.Role)
		}
	}
}

func TestClaimNeedsExactlyTwoVoters(t *testing.T) {
	e := New(testParams())
	three := &Members{Voters: []Member{{ID: 0xa, Name: "a"}, {ID: 0xb, Name: "b"}, {ID: 0xc, Name: "c"}}}
	var last Decision
	for i := range 60 {
		last = e.Observe(Observation{
			Now: start.Add(time.Duration(i) * time.Second), ArbiterReady: true, CanControl: true,
			Local: &Status{ClusterID: clusterID, MemberID: 0xa}, Members: three, GenerationKnown: true,
			Arbiter: &ArbiterRead{},
		})
		if last.Action.Kind == ActionClaim {
			t.Fatalf("claimed with three voters")
		}
	}
	if !strings.Contains(last.Note, "3 voters") {
		t.Errorf("note = %q", last.Note)
	}
}

func TestLearnerPresentOnTheSurvivor(t *testing.T) {
	e := New(testParams())
	e.startupRead = true
	solo := func(learner Member, st map[uint64]Status, r *Result, at int) Decision {
		return e.Observe(Observation{
			Now: start.Add(time.Duration(at) * time.Second), ArbiterReady: true, CanControl: true,
			Local:   &Status{ClusterID: clusterID, MemberID: 0xb, Leader: 0xb, CommitIndex: 1000},
			Members: &Members{Voters: []Member{{ID: 0xb, Name: "b"}}, Learners: []Member{learner}}, GenerationKnown: true, Generation: 1,
			LearnerStatus: st, Result: r,
		})
	}
	if d := solo(Member{ID: 0x100}, nil, nil, 0); d.Action.Kind != ActionNone || d.Role != RoleSolo {
		t.Fatalf("promoted a learner that has not started: %+v", d)
	}
	started := Member{ID: 0x100, Name: "a"}
	if d := solo(started, map[uint64]Status{0x100: {AppliedIndex: 500}}, nil, 1); d.Action.Kind != ActionNone {
		t.Fatalf("promoted a learner at half the log: %+v", d)
	}
	d := solo(started, map[uint64]Status{0x100: {AppliedIndex: 950}}, nil, 2)
	if d.Action.Kind != ActionPromote || d.Action.Learner != 0x100 {
		t.Fatalf("decision = %+v, want promote 100", d)
	}
	// Refusals back off exponentially up to the maximum.
	notReady := &Result{Action: ActionPromote, Err: errors.New("not ready"), NotReady: true}
	at := 3
	var waits []time.Duration
	for range 8 {
		d := solo(started, nil, notReady, at)
		waits = append(waits, d.Wait)
		at += int(d.Wait / time.Second)
		if d := solo(started, nil, nil, at); d.Action.Kind != ActionPromote {
			t.Fatalf("no promotion after the backoff: %+v", d)
		}
	}
	want := []time.Duration{1, 2, 4, 8, 16, 30, 30, 30}
	for i := range want {
		if waits[i] != want[i]*time.Second {
			t.Errorf("backoffs = %v, want %v seconds", waits, want)
			break
		}
	}
	if d := solo(started, nil, &Result{Action: ActionPromote}, at+1); d.Action.Kind != ActionPromote {
		t.Errorf("after a success the backoff did not reset: %+v", d)
	}
}

func TestLostClaimResult(t *testing.T) {
	for _, tc := range []struct {
		name string
		// unreachableAfter makes the arbiter unreachable for some polls
		// after the lost response.
		unreachableAfter int
	}{
		{"read right after", 0},
		{"arbiter unreachable for a while", 15},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			settle(t, w)
			a := w.node("a")
			a.loseClaimResult = true
			w.linkUp = false
			w.runUntil(30, "a claim", func() bool { return a.count(ActionClaim) == 1 })
			if w.record.Survivor != "a" {
				t.Fatalf("the claim did not write: %+v", w.record)
			}
			a.reachesArbiter = tc.unreachableAfter == 0
			w.run(tc.unreachableAfter)
			if a.count(ActionTakeover) != 0 {
				t.Fatalf("took over before the arbiter confirmed the claim: %v", a.actions)
			}
			a.reachesArbiter = true
			w.runUntil(10, "a serving alone", func() bool { return a.serving(w) && a.gen == 1 })
			if n := a.count(ActionClaim); n != 1 {
				t.Errorf("a claimed %d times, want once: the read shows the claim won", n)
			}
		})
	}
}

func TestLostClaimResultOtherWon(t *testing.T) {
	w := newWorld(t)
	settle(t, w)
	a, b := w.node("a"), w.node("b")
	w.linkUp = false
	w.runUntil(30, "a claim", func() bool { return a.count(ActionClaim) == 1 })
	// b's claim was lost on the way: it read version 0, a wrote version 1.
	w.runUntil(10, "b claim", func() bool { return b.count(ActionClaim) == 1 })
	w.run(3)
	if b.last.Role != RoleStale || b.count(ActionTakeover) != 0 {
		t.Fatalf("b after a conflicting claim: %s, %v", b.last.Role, b.actions)
	}
}

func TestTakeoverInterruptedAfterTheClaim(t *testing.T) {
	w := newWorld(t)
	settle(t, w)
	a := w.node("a")
	w.linkUp = false
	w.runUntil(30, "a claim", func() bool { return a.count(ActionClaim) == 1 })
	// twok restarts between the claim and the takeover: the new engine
	// finds the claim in the arbiter at start and finishes it.
	w.restartEngine(a)
	w.runUntil(10, "a serving alone", func() bool { return a.serving(w) && a.gen == 1 })
	if a.count(ActionClaim) != 0 || a.count(ActionTakeover) != 1 {
		t.Errorf("after the restart: %v, want the takeover and no second claim", a.actions)
	}

	// Interrupted after the takeover, before the generation is written.
	w2 := newWorld(t)
	settle(t, w2)
	a2 := w2.node("a")
	w2.linkUp = false
	w2.runUntil(40, "a takeover", func() bool { return a2.count(ActionTakeover) == 1 })
	w2.restartEngine(a2)
	w2.runUntil(10, "the generation written", func() bool { return a2.gen == 1 })
	if a2.count(ActionTakeover) != 0 || a2.count(ActionWriteGeneration) != 1 {
		t.Errorf("after the restart: %v, want only the generation written", a2.actions)
	}
}

func TestClusterIDMismatchStopsEverything(t *testing.T) {
	w := newWorld(t)
	w.found, w.version = true, 1
	w.record = arbiter.Record{ClusterID: "ffff", Generation: 0, Survivor: "x"}
	w.run(5)
	w.linkUp = false
	w.run(120)
	for _, n := range w.nodes {
		if len(n.actions) != 1 || n.actions[0].Action.Kind != ActionReadArbiter {
			t.Errorf("%s acted with another cluster's record: %v", n.name, n.actions)
		}
		if !n.last.Problem || !strings.Contains(n.last.Note, "cluster ffff") {
			t.Errorf("%s decision = %+v, want a problem naming the cluster", n.name, n.last)
		}
	}
}

func TestArbiterBehindTheLocalGeneration(t *testing.T) {
	w := newWorld(t)
	for _, n := range w.nodes {
		n.gen = 3
	}
	settle(t, w)
	w.linkUp = false
	w.run(60)
	for _, n := range w.nodes {
		if n.count(ActionClaim) != 0 || !n.last.Problem {
			t.Errorf("%s with an empty arbiter at generation 3: %+v, %v", n.name, n.last, n.actions)
		}
	}
}

func TestWithoutProcessControlNoClaim(t *testing.T) {
	w := newWorld(t)
	settle(t, w)
	a, b := w.node("a"), w.node("b")
	a.canControl = false
	w.linkUp = false
	w.runUntil(60, "b serving alone", func() bool { return b.serving(w) })
	if a.count(ActionClaim) != 0 {
		t.Errorf("a claimed without being able to stop etcd: %v", a.actions)
	}
	w.linkUp = true
	w.run(30)
	if a.count(ActionRejoin) != 0 || !a.last.Problem || a.last.Role != RoleStale {
		t.Errorf("a without process control: %+v, %v", a.last, a.actions)
	}
}

func TestManualClaim(t *testing.T) {
	w := newWorld(t)
	settle(t, w)
	a, b := w.node("a"), w.node("b")
	a.up = false
	w.runUntil(60, "b serving alone", func() bool { return b.serving(w) && b.gen == 1 })
	b.up = false
	a.up = true
	w.run(30)
	// The operator accepts the loss and names a.
	w.record = arbiter.Record{ClusterID: "c1", Generation: 2, Survivor: "a", Reason: ReasonManual}
	w.version++
	w.runUntil(30, "a serving alone", func() bool { return a.serving(w) && a.gen == 2 })
}

func TestModes(t *testing.T) {
	t.Run("off does nothing and reads nothing", func(t *testing.T) {
		w := newWorld(t)
		for _, n := range w.nodes {
			n.e.SetParams(Params{Mode: ModeOff, ClaimAfter: 20 * time.Second, RetryDelay: 5 * time.Second})
		}
		w.linkUp = false
		w.run(120)
		for _, n := range w.nodes {
			if len(n.actions) != 0 || w.readsBy[n.name] != 0 {
				t.Errorf("%s acted while off: %v", n.name, n.actions)
			}
			if n.last.Role != RoleIsolated || !strings.HasPrefix(n.last.Note, "off, would read-arbiter") {
				t.Errorf("%s while off: %+v", n.name, n.last)
			}
		}
	})
	t.Run("dry-run reads and reports, executes nothing", func(t *testing.T) {
		w := newWorld(t)
		for _, n := range w.nodes {
			p := testParams()
			p.Mode = ModeDryRun
			n.e.SetParams(p)
		}
		settle(t, w)
		w.linkUp = false
		w.run(60)
		a := w.node("a")
		if w.found {
			t.Fatalf("dry-run wrote the arbiter: %+v", w.record)
		}
		if a.last.Action.Kind != ActionClaim || !a.last.DryRun {
			t.Errorf("a in dry-run: %+v", a.last)
		}
		for _, d := range a.actions {
			if d.Action.Kind != ActionReadArbiter && !d.DryRun {
				t.Errorf("an action not marked dry-run: %+v", d)
			}
		}
		// Switching to enforce acts on the next poll.
		for _, n := range w.nodes {
			n.e.SetParams(testParams())
		}
		w.runUntil(5, "a serving alone", func() bool { return a.serving(w) })
	})
	t.Run("unverified arbiter blocks actions", func(t *testing.T) {
		e := New(testParams())
		d := e.Observe(Observation{Now: start, Local: &Status{ClusterID: clusterID, MemberID: 0xa, Leader: 0xa}})
		if d.Action.Kind != ActionNone || !strings.Contains(d.Note, "has not passed its check") {
			t.Errorf("decision = %+v", d)
		}
	})
}

func TestClaimRecordFromAFollowerOfAnotherLeader(t *testing.T) {
	// A follower that saw the other member lead waits the extra delay; the
	// leader waits claim-delay only. Both claim the same generation, the
	// leader first.
	w := newWorld(t)
	settle(t, w)
	w.linkUp = false
	a, b := w.node("a"), w.node("b")
	w.run(40)
	if a.count(ActionClaim) != 1 || b.count(ActionClaim) != 1 {
		t.Fatalf("claims: a %d, b %d", a.count(ActionClaim), b.count(ActionClaim))
	}
	ca, cb := firstClaim(a), firstClaim(b)
	if ca.Action.Record.Generation != 1 || cb.Action.Record.Generation != 1 || ca.Action.Expected != arbiter.Zero {
		t.Errorf("claims: %+v / %+v", ca.Action, cb.Action)
	}
}

func firstClaim(n *node) Decision {
	for _, d := range n.actions {
		if d.Action.Kind == ActionClaim {
			return d
		}
	}
	return Decision{}
}
