// Package engine decides what twok does about the local etcd member. It is
// a pure state machine: the agent feeds it observations, executes the one
// action it returns, and feeds the result back in the next observation. The
// engine never calls etcd, the arbiter or the clock itself, so every rule of
// docs/design.md can be tested as a sequence of observations.
package engine

import (
	"fmt"
	"slices"
	"time"

	"github.com/aenix-io/twok/internal/arbiter"
)

// Reasons written into the arbiter record.
const (
	ReasonNoLeader = "no-leader"
	ReasonManual   = "manual"
)

// learnerReady is the share of the survivor's commit index a learner must
// have applied before it is promoted. etcd refuses a promotion below 90% of
// the leader's log; asking earlier only produces refusals.
const learnerReady = 0.9

// Params are the engine's settings. They may change between observations.
type Params struct {
	Mode Mode
	// ClaimAfter is how long the local member must see no leader before
	// the arbiter is read.
	ClaimAfter time.Duration
	// ClaimDelay is the wait before a claim once the arbiter shows equal
	// generations; FollowerExtraDelay is added for a member that was not
	// the last leader it saw, so that the last leader claims first.
	ClaimDelay         time.Duration
	FollowerExtraDelay time.Duration
	// RetryDelay paces what is retried outside an isolation: arbiter reads
	// at start, rejoin attempts, and failed takeovers and generation
	// writes.
	RetryDelay time.Duration
	// JoinTimeout is how long a member that rejoined under a new ID waits
	// to see itself as a learner before it tries again.
	JoinTimeout time.Duration
	// PromoteBackoff and MaxPromoteBackoff bound the wait between
	// promotions of a learner that is not ready.
	PromoteBackoff    time.Duration
	MaxPromoteBackoff time.Duration
	// PeerEndpoint is a client URL of the other member, used for a rejoin
	// when the member list does not say where the survivor is.
	PeerEndpoint string
	// Version goes into the records this member writes.
	Version string
}

// DefaultParams are the defaults of docs/design.md, with mode off.
func DefaultParams() Params {
	return Params{
		Mode:               ModeOff,
		ClaimAfter:         20 * time.Second,
		ClaimDelay:         3 * time.Second,
		FollowerExtraDelay: 5 * time.Second,
		RetryDelay:         5 * time.Second,
		JoinTimeout:        2 * time.Minute,
		PromoteBackoff:     time.Second,
		MaxPromoteBackoff:  30 * time.Second,
		Version:            "dev",
	}
}

// Engine is the state machine of one member.
type Engine struct {
	p Params

	// What the local member said about itself, kept across polls in which
	// it does not answer: a removed member's etcd exits and restarts, and
	// the rejoin still needs to know who it was.
	self            uint64
	clusterID       uint64
	name            string
	peerURLs        []string
	members         *Members
	generation      uint64
	generationKnown bool
	lastLeader      uint64
	noLeaderSince   time.Time

	// arb is the last successful read of the arbiter, at arbAt.
	arb         *ArbiterRead
	arbAt       time.Time
	startupRead bool
	readRetryAt time.Time
	// needRead asks for a read before the next claim: after a conflict, or
	// after a claim whose outcome is unknown.
	needRead bool
	mismatch string

	candidateSince time.Time
	// won is the record of a claim this member won, here or by an
	// operator, until its takeover is finished.
	won *arbiter.Record

	retryAt        map[ActionKind]time.Time
	promoteBackoff time.Duration

	// joinedAs is the learner ID of the last rejoin, while the member waits
	// to see itself as that learner; learner is a learner an attempt added
	// before it failed.
	joinedAs uint64
	joinedAt time.Time
	learner  uint64
}

// New returns an engine that has seen nothing yet.
func New(p Params) *Engine {
	return &Engine{p: p, retryAt: map[ActionKind]time.Time{}}
}

// SetParams replaces the parameters and keeps everything observed.
func (e *Engine) SetParams(p Params) {
	e.p = p
}

// Params returns the current parameters.
func (e *Engine) Params() Params {
	return e.p
}

// Observe takes one observation and decides. The agent may call it a
// second time in the same poll, with the same Now and the arbiter read the
// first decision asked for, and without the Result it already passed.
func (e *Engine) Observe(o Observation) Decision {
	e.learn(o)
	if o.Result != nil {
		e.result(o.Now, *o.Result)
	}
	if o.Arbiter != nil {
		e.arbiterRead(o.Now, *o.Arbiter)
	}
	d := e.decide(o)
	d.Generation = e.generation
	return e.gate(d, o)
}

func (e *Engine) learn(o Observation) {
	if o.Name != "" {
		e.name = o.Name
	}
	if len(o.PeerURLs) > 0 {
		e.peerURLs = slices.Clone(o.PeerURLs)
	}
	if o.GenerationKnown {
		e.generation, e.generationKnown = o.Generation, true
	}
	l := o.Local
	if l == nil {
		return
	}
	e.self, e.clusterID = l.MemberID, l.ClusterID
	if l.Leader != 0 {
		e.lastLeader = l.Leader
		e.noLeaderSince = time.Time{}
		e.candidateSince = time.Time{}
	} else if e.noLeaderSince.IsZero() {
		e.noLeaderSince = o.Now
	}
	if o.Members == nil {
		return
	}
	e.members = o.Members.clone()
	if me, ok := e.members.voter(l.MemberID); ok {
		if me.Name != "" {
			e.name = me.Name
		}
		if len(me.PeerURLs) > 0 {
			e.peerURLs = slices.Clone(me.PeerURLs)
		}
		if l.Leader != 0 {
			// A voter in a cluster with a leader has finished any rejoin.
			e.joinedAs, e.learner = 0, 0
		}
	}
}

func (e *Engine) result(now time.Time, r Result) {
	switch r.Action {
	case ActionClaim:
		if r.Err == nil {
			rec := r.Record
			e.won = &rec
			return
		}
		// A conflict means someone else moved the record; any other error
		// means the write may or may not have happened. Either way the
		// next step is a read: the generation and the survivor in it say
		// who won.
		e.needRead = true
	case ActionRejoin:
		if r.Learner != 0 {
			e.learner = r.Learner
		}
		if r.Err != nil {
			e.retryAt[ActionRejoin] = now.Add(e.p.RetryDelay)
			return
		}
		e.joinedAs, e.joinedAt = r.Learner, now
	case ActionPromote:
		if r.Err == nil {
			e.promoteBackoff = 0
			delete(e.retryAt, ActionPromote)
			return
		}
		e.promoteBackoff = min(max(2*e.promoteBackoff, e.p.PromoteBackoff), e.p.MaxPromoteBackoff)
		e.retryAt[ActionPromote] = now.Add(e.promoteBackoff)
	case ActionTakeover, ActionWriteGeneration:
		if r.Err != nil {
			e.retryAt[r.Action] = now.Add(e.p.RetryDelay)
		}
	}
}

func (e *Engine) arbiterRead(now time.Time, r ArbiterRead) {
	if r.Err != nil {
		e.readRetryAt = now.Add(e.p.RetryDelay)
		return
	}
	e.arb, e.arbAt = &r, now
	e.startupRead, e.needRead = true, false
	rec := r.Record
	e.mismatch = ""
	if r.Found && rec.ClusterID != "" && e.clusterID != 0 && rec.ClusterID != Hex(e.clusterID) {
		e.mismatch = fmt.Sprintf("the arbiter's record belongs to cluster %s, the local member to cluster %s; no action until this is resolved",
			rec.ClusterID, Hex(e.clusterID))
		return
	}
	// The record names this member the survivor of a generation its data
	// does not hold yet: a claim won here whose takeover was interrupted,
	// or an operator's manual claim. Either is final.
	if r.Found && e.generationKnown && e.survivorIsSelf(rec) && rec.Generation > e.generation &&
		(rec.Generation == e.generation+1 || rec.Reason == ReasonManual) {
		e.won = &rec
	}
}

// survivorIsSelf compares by member ID when the record has one, and by
// name for a manual claim, which names the member only.
func (e *Engine) survivorIsSelf(rec arbiter.Record) bool {
	if id := ParseHex(rec.SurvivorMemberID); id != 0 {
		return e.self != 0 && id == e.self
	}
	return rec.Survivor != "" && rec.Survivor == e.name
}

// stale reports whether the arbiter knows a newer generation than the
// local data, claimed by another member.
func (e *Engine) stale() bool {
	return e.arb != nil && e.arb.Found && e.generationKnown &&
		e.arb.Record.Generation > e.generation && !e.survivorIsSelf(e.arb.Record)
}

func (e *Engine) role(o Observation) Role {
	l := o.Local
	switch {
	case e.won != nil:
		return RoleSolo
	case l != nil && l.IsLearner:
		return RoleJoining
	case l != nil && e.joinedAs != 0 && l.MemberID == e.joinedAs && o.Now.Sub(e.joinedAt) < e.p.JoinTimeout:
		return RoleJoining
	case e.stale():
		return RoleStale
	case l == nil:
		return RoleUnknown
	case l.Leader != 0:
		if e.members != nil && len(e.members.Voters) == 1 && e.members.Voters[0].ID == l.MemberID {
			return RoleSolo
		}
		return RolePair
	case o.Now.Sub(e.noLeaderSince) >= e.p.ClaimAfter:
		return RoleIsolated
	default:
		return RolePair
	}
}

func (e *Engine) decide(o Observation) Decision {
	if e.mismatch != "" {
		return Decision{Role: e.role(o), Problem: true, Note: e.mismatch}
	}
	if e.won != nil {
		if d, ok := e.finish(o); ok {
			return d
		}
	}
	role := e.role(o)
	if d, ok := e.startup(o, role); ok {
		return d
	}
	switch role {
	case RoleUnknown:
		return Decision{Role: role, Note: "etcd does not answer"}
	case RoleJoining:
		return e.joining(o)
	case RoleStale:
		return e.rejoin(o)
	case RoleSolo:
		return e.solo(o)
	case RoleIsolated:
		return e.isolated(o)
	default:
		return e.pair(o)
	}
}

// gate applies the mode and the preconditions that hold for every action.
func (e *Engine) gate(d Decision, o Observation) Decision {
	if d.Action.Kind == "" {
		d.Action.Kind = ActionNone
	}
	if d.Action.Kind == ActionNone {
		return d
	}
	switch {
	case e.p.Mode == ModeOff:
		return Decision{Role: d.Role, Generation: d.Generation, Action: Action{Kind: ActionNone}, Note: "off, would " + string(d.Action.Kind) + ": " + d.Note}
	case !o.ArbiterReady:
		return Decision{
			Role: d.Role, Generation: d.Generation, Action: Action{Kind: ActionNone},
			Note: "the arbiter has not passed its check yet, would " + string(d.Action.Kind) + ": " + d.Note,
		}
	case d.Action.NeedsProcess() && !o.CanControl:
		return Decision{
			Role: d.Role, Generation: d.Generation, Action: Action{Kind: ActionNone}, Problem: true,
			Note: "cannot stop the etcd process, would " + string(d.Action.Kind) + ": " + d.Note,
		}
	case e.p.Mode == ModeDryRun && d.Action.Kind != ActionReadArbiter:
		d.DryRun = true
	}
	return d
}

// startup reads the arbiter once when the agent starts, so that a stale
// member and an unfinished takeover are found without waiting for an
// isolation. A failed read is retried every RetryDelay.
func (e *Engine) startup(o Observation, role Role) (Decision, bool) {
	if e.startupRead || e.p.Mode == ModeOff || o.Local == nil || role == RoleIsolated || role == RoleUnknown || o.Now.Before(e.readRetryAt) {
		return Decision{}, false
	}
	if o.Arbiter != nil {
		// This poll's read failed; the decision falls through to the role.
		return Decision{}, false
	}
	return Decision{Role: role, Action: Action{Kind: ActionReadArbiter}, Note: "reading the arbiter at start"}, true
}

func (e *Engine) pair(o Observation) Decision {
	l := o.Local
	if l.Leader != 0 {
		return Decision{Role: RolePair, Note: "leader " + Hex(l.Leader)}
	}
	wait := e.noLeaderSince.Add(e.p.ClaimAfter).Sub(o.Now)
	return Decision{Role: RolePair, Note: "no leader", Wait: wait}
}

func (e *Engine) isolated(o Observation) Decision {
	isolatedAt := e.noLeaderSince.Add(e.p.ClaimAfter)
	if e.needRead || e.arb == nil || e.arbAt.Before(isolatedAt) {
		if o.Arbiter != nil {
			return Decision{Role: RoleIsolated, Note: "no leader and the arbiter is unreachable, staying isolated"}
		}
		return Decision{Role: RoleIsolated, Action: Action{Kind: ActionReadArbiter}, Note: "no leader, reading the arbiter"}
	}
	if d, blocked := e.blocked(o); blocked {
		// A member that cannot claim still has to learn when the other
		// one did, or it would never go stale and rejoin.
		if o.Arbiter == nil && !o.Now.Before(e.arbAt.Add(e.p.RetryDelay)) {
			return Decision{Role: RoleIsolated, Action: Action{Kind: ActionReadArbiter}, Note: d.Note}
		}
		return d
	}

	d := Decision{Role: RoleIsolated}
	l := o.Local
	if e.candidateSince.IsZero() {
		e.candidateSince = o.Now
	}
	delay, who := e.p.ClaimDelay, "the last leader"
	if e.lastLeader != l.MemberID {
		delay += e.p.FollowerExtraDelay
		who = "a follower"
	}
	next := e.generation + 1
	if wait := e.candidateSince.Add(delay).Sub(o.Now); wait > 0 {
		d.Note = fmt.Sprintf("no leader, generation %d in the arbiter too, claiming %d after the delay of %s", e.generation, next, who)
		d.Wait = wait
		return d
	}
	d.Action = Action{
		Kind:     ActionClaim,
		Expected: e.arb.Version,
		Record: arbiter.Record{
			SchemaVersion:    arbiter.SchemaVersion,
			ClusterID:        Hex(l.ClusterID),
			Generation:       next,
			Survivor:         e.name,
			SurvivorMemberID: Hex(l.MemberID),
			Since:            o.Now.UTC(),
			TwokVersion:      e.p.Version,
			Reason:           ReasonNoLeader,
		},
	}
	if !e.arb.Found {
		d.Action.Expected = arbiter.Zero
	}
	d.Note = fmt.Sprintf("no leader, claiming generation %d", next)
	return d
}

// blocked returns the reason an isolated member does not claim, if any.
func (e *Engine) blocked(o Observation) (Decision, bool) {
	d := Decision{Role: RoleIsolated}
	rec := e.arb.Record
	l := o.Local
	switch {
	case !e.generationKnown:
		d.Note = "no leader, the local generation is unknown"
	case !e.arb.Found && e.generation > 0:
		d.Problem = true
		d.Note = fmt.Sprintf("the arbiter has no record but the local generation is %d; it was reset or points elsewhere", e.generation)
	case e.arb.Found && rec.Generation != e.generation:
		d.Problem = true
		d.Note = fmt.Sprintf("the arbiter's generation %d (survivor %s) cannot follow from the local generation %d", rec.Generation, rec.Survivor, e.generation)
	case !o.GenerationKnown:
		d.Note = "no leader, /twok/generation could not be read in this poll, not claiming"
	case e.members == nil || o.Members == nil:
		d.Note = "no leader, the member list could not be read, not claiming"
	case len(o.Members.Voters) != 2:
		d.Note = fmt.Sprintf("no leader and %d voters, a claim needs exactly two", len(o.Members.Voters))
	case !slices.ContainsFunc(o.Members.Voters, func(m Member) bool { return m.ID == l.MemberID }):
		d.Note = "no leader and the local member is not a voter in its own member list, not claiming"
	case e.name == "":
		d.Note = "no leader and the local member's name is unknown, not claiming"
	case !o.CanControl:
		d.Problem = true
		d.Note = "no leader, and twok cannot stop the etcd process, so it does not claim"
	default:
		return Decision{}, false
	}
	return d, true
}

// finish takes a won claim to its end: remove the other members while any
// is left, then write the generation once the member leads alone. ok is
// false once the generation is written.
func (e *Engine) finish(o Observation) (Decision, bool) {
	w := e.won
	if e.generationKnown && e.generation >= w.Generation {
		e.won = nil
		return Decision{}, false
	}
	d := Decision{Role: RoleSolo}
	base := fmt.Sprintf("survivor of generation %d", w.Generation)
	l := o.Local
	switch {
	case l == nil:
		d.Note = base + ", waiting for etcd to answer"
		return d, true
	case l.IsLearner:
		d.Problem = true
		d.Note = base + ", but the local member is a learner and cannot take over"
		return d, true
	case o.Members == nil:
		d.Note = base + ", the member list could not be read"
		return d, true
	}
	var remove []uint64
	for _, m := range o.Members.Voters {
		if m.ID != l.MemberID {
			remove = append(remove, m.ID)
		}
	}
	if len(remove) > 0 {
		if wait := e.until(o.Now, ActionTakeover); wait > 0 {
			d.Note, d.Wait = base+", the takeover failed, retrying", wait
			return d, true
		}
		for _, m := range o.Members.Learners {
			remove = append(remove, m.ID)
		}
		d.Action = Action{Kind: ActionTakeover, Remove: remove}
		d.Note = base + ", removing the other members"
		return d, true
	}
	switch {
	case l.Leader != l.MemberID:
		d.Note = base + ", waiting to lead alone"
	case !o.GenerationKnown:
		d.Note = base + ", /twok/generation could not be read"
	default:
		if wait := e.until(o.Now, ActionWriteGeneration); wait > 0 {
			d.Note, d.Wait = base+", writing the generation failed, retrying", wait
			return d, true
		}
		d.Action = Action{Kind: ActionWriteGeneration, Generation: w.Generation}
		d.Note = base + ", writing the generation"
	}
	return d, true
}

// rejoin takes a stale member back to the survivor. Every attempt starts
// from a read of the arbiter in the same poll, so that the member rejoins
// only the survivor the arbiter names now, and only while it is stale.
func (e *Engine) rejoin(o Observation) Decision {
	rec := e.arb.Record
	d := Decision{Role: RoleStale}
	base := fmt.Sprintf("generation %d is behind the arbiter's %d (survivor %s)", e.generation, rec.Generation, rec.Survivor)
	if wait := e.until(o.Now, ActionRejoin); wait > 0 {
		d.Note, d.Wait = base+", the last rejoin attempt failed, retrying", wait
		return d
	}
	if o.Arbiter == nil {
		if wait := e.readRetryAt.Sub(o.Now); wait > 0 {
			d.Note, d.Wait = base+", the arbiter is unreachable, cannot confirm", wait
			return d
		}
		d.Action = Action{Kind: ActionReadArbiter}
		d.Note = base + ", confirming with the arbiter before rejoining"
		return d
	}
	if o.Arbiter.Err != nil {
		d.Note, d.Wait = base+", the arbiter is unreachable, cannot confirm", e.p.RetryDelay
		return d
	}
	endpoints := e.survivorEndpoints(rec)
	switch {
	case len(endpoints) == 0:
		d.Problem = true
		d.Note = base + ", but no client URL of the survivor is known; set --peer-endpoint"
		return d
	case len(e.peerURLs) == 0:
		d.Note = base + ", but the local peer URLs are unknown"
		return d
	case e.clusterID == 0:
		d.Note = base + ", but the local cluster ID is unknown"
		return d
	}
	d.Action = Action{
		Kind:              ActionRejoin,
		SurvivorEndpoints: endpoints,
		SurvivorID:        ParseHex(rec.SurvivorMemberID),
		ClusterID:         e.clusterID,
		PeerURLs:          slices.Clone(e.peerURLs),
		Learner:           e.learner,
	}
	d.Note = base + ", rejoining as a learner"
	return d
}

// survivorEndpoints are the client URLs of the survivor as the last member
// list showed them, then the configured peer endpoint.
func (e *Engine) survivorEndpoints(rec arbiter.Record) []string {
	var endpoints []string
	if e.members != nil {
		id := ParseHex(rec.SurvivorMemberID)
		for _, m := range e.members.Voters {
			if m.ID == e.self {
				continue
			}
			if (id != 0 && m.ID == id) || (id == 0 && m.Name == rec.Survivor) {
				endpoints = append(endpoints, m.ClientURLs...)
			}
		}
	}
	if e.p.PeerEndpoint != "" && !slices.Contains(endpoints, e.p.PeerEndpoint) {
		endpoints = append(endpoints, e.p.PeerEndpoint)
	}
	return endpoints
}

func (e *Engine) joining(o Observation) Decision {
	d := Decision{Role: RoleJoining}
	if o.Local != nil && o.Local.IsLearner {
		d.Note = "learner, catching up with the survivor"
		return d
	}
	d.Note = fmt.Sprintf("rejoined as %s, waiting to become a learner of the survivor", Hex(e.joinedAs))
	d.Wait = e.joinedAt.Add(e.p.JoinTimeout).Sub(o.Now)
	return d
}

func (e *Engine) solo(o Observation) Decision {
	d := Decision{Role: RoleSolo, Note: "serving alone"}
	if o.Members == nil || len(o.Members.Learners) == 0 {
		return d
	}
	learner := o.Members.Learners[0]
	id := Hex(learner.ID)
	if learner.Name == "" {
		d.Note = "serving alone, learner " + id + " has not started yet"
		return d
	}
	if wait := e.until(o.Now, ActionPromote); wait > 0 {
		d.Note, d.Wait = "serving alone, learner "+id+" is not ready yet", wait
		return d
	}
	if st, ok := o.LearnerStatus[learner.ID]; ok && float64(st.AppliedIndex) < float64(o.Local.CommitIndex)*learnerReady {
		d.Note = "serving alone, learner " + id + " is catching up"
		return d
	}
	d.Action = Action{Kind: ActionPromote, Learner: learner.ID}
	d.Note = "serving alone, promoting learner " + id
	return d
}

// until returns how long a failed action waits before it is tried again.
func (e *Engine) until(now time.Time, kind ActionKind) time.Duration {
	return max(e.retryAt[kind].Sub(now), 0)
}
