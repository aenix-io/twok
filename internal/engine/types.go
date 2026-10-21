package engine

import (
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/aenix-io/twok/internal/arbiter"
)

// Mode says how far the agent goes with a decision.
type Mode string

const (
	// ModeOff observes and logs the role. The arbiter is not contacted and
	// nothing is done.
	ModeOff Mode = "off"
	// ModeDryRun decides and logs every action, and executes none but
	// reads of the arbiter, which are needed to know what would be done.
	ModeDryRun Mode = "dry-run"
	// ModeEnforce acts.
	ModeEnforce Mode = "enforce"
)

// Role is the local member's place in the model of docs/design.md.
type Role string

const (
	// RoleUnknown is a member twok knows too little about: etcd does not
	// answer and no earlier observation says what it is.
	RoleUnknown Role = "unknown"
	// RolePair is two voters with a leader, or without one for less than
	// claim-after.
	RolePair Role = "pair"
	// RoleIsolated is a voter that has seen no leader for claim-after.
	RoleIsolated Role = "isolated"
	// RoleSolo is the survivor: the only voter, or the winner of a claim
	// that is finishing its takeover.
	RoleSolo Role = "solo"
	// RoleStale is a member whose data belongs to an older generation than
	// the arbiter's.
	RoleStale Role = "stale"
	// RoleJoining is a member that rejoined the survivor as a learner and
	// has not been promoted yet.
	RoleJoining Role = "joining"
)

// Roles lists every role, for metrics that report one series per role.
func Roles() []Role {
	return []Role{RoleUnknown, RolePair, RoleIsolated, RoleSolo, RoleStale, RoleJoining}
}

// Status is what the local member reports about itself, served without a
// quorum.
type Status struct {
	ClusterID uint64
	MemberID  uint64
	// Leader is 0 when the member knows of no leader.
	Leader       uint64
	Term         uint64
	CommitIndex  uint64
	AppliedIndex uint64
	IsLearner    bool
}

// Member is one entry of the member list.
type Member struct {
	ID uint64
	// Name is empty for a member that was added and has not started yet.
	Name       string
	PeerURLs   []string
	ClientURLs []string
}

// Members is the member list, split by role.
type Members struct {
	Voters   []Member
	Learners []Member
}

// voter returns the voter with id.
func (m *Members) voter(id uint64) (Member, bool) {
	i := slices.IndexFunc(m.Voters, func(v Member) bool { return v.ID == id })
	if i < 0 {
		return Member{}, false
	}
	return m.Voters[i], true
}

// clone copies the lists, so that the engine keeps no reference into an
// observation.
func (m *Members) clone() *Members {
	c := &Members{Voters: slices.Clone(m.Voters), Learners: slices.Clone(m.Learners)}
	return c
}

// ArbiterRead is the outcome of one read of the arbiter.
type ArbiterRead struct {
	// Err is set when the arbiter could not be read. A missing record is
	// not an error here: it is a read with Found false.
	Err     error
	Found   bool
	Record  arbiter.Record
	Version arbiter.Version
}

// Result is the outcome of the last action the agent executed, fed back in
// the next observation.
type Result struct {
	Action ActionKind
	// Err is nil on success.
	Err error
	// Conflict is set for a claim the arbiter refused with ErrConflict: the
	// write did not happen. Any other claim error leaves the outcome
	// unknown.
	Conflict bool
	// Record is the record a claim wrote.
	Record arbiter.Record
	// NotReady is set for a promotion etcd refused because the learner is
	// behind.
	NotReady bool
	// Learner is the member ID the survivor assigned in a rejoin.
	Learner uint64
}

// Observation is one poll of the local member.
type Observation struct {
	Now time.Time
	// Local is nil when etcd did not answer.
	Local *Status
	// Members is nil when the member list could not be read, which is
	// always the case on a learner.
	Members *Members
	// Generation is the value of /twok/generation, 0 when the key is
	// absent. GenerationKnown is false when it could not be read.
	Generation      uint64
	GenerationKnown bool
	// Name and PeerURLs come from etcd's command line, when twok can read
	// it. The member list takes precedence for both.
	Name     string
	PeerURLs []string
	// CanControl reports whether twok can stop the etcd process. Without
	// it there is no claim, no takeover and no rejoin: a claim that cannot
	// be followed by a takeover would leave nobody serving.
	CanControl bool
	// ArbiterReady reports whether the arbiter passed its startup check.
	// Until it has, the engine takes no action.
	ArbiterReady bool
	// LearnerStatus is the status of learners the agent could reach, by
	// member ID.
	LearnerStatus map[uint64]Status
	// Arbiter is set when the agent read the arbiter in this poll.
	Arbiter *ArbiterRead
	// Result is the outcome of the action executed after the previous
	// decision, if any.
	Result *Result
}

// ActionKind names an action.
type ActionKind string

// The actions, in the order a takeover and a rejoin use them.
const (
	ActionNone            ActionKind = "none"
	ActionReadArbiter     ActionKind = "read-arbiter"
	ActionClaim           ActionKind = "claim"
	ActionTakeover        ActionKind = "takeover"
	ActionWriteGeneration ActionKind = "write-generation"
	ActionRejoin          ActionKind = "rejoin"
	ActionPromote         ActionKind = "promote"
)

// Action is what the agent should do next. Only the fields of its kind are
// set.
type Action struct {
	Kind ActionKind
	// Expected and Record are the claim's compare-and-swap.
	Expected arbiter.Version
	Record   arbiter.Record
	// Remove are the members a takeover removes.
	Remove []uint64
	// Generation is the value write-generation writes.
	Generation uint64
	// SurvivorEndpoints are the client URLs of the survivor to rejoin;
	// SurvivorID and ClusterID are what the survivor must report there.
	// PeerURLs are the local member's.
	SurvivorEndpoints []string
	SurvivorID        uint64
	ClusterID         uint64
	PeerURLs          []string
	// Learner is the learner to promote, or for a rejoin the one an
	// earlier attempt already added, 0 when none is known.
	Learner uint64
}

// NeedsProcess reports whether the action stops etcd.
func (a Action) NeedsProcess() bool {
	return a.Kind == ActionTakeover || a.Kind == ActionRejoin
}

// Decision is the engine's answer to an observation. Note explains it for
// the log and stays the same while the situation does; Wait counts down to
// a pending action.
type Decision struct {
	Role   Role
	Action Action
	Note   string
	Wait   time.Duration
	// DryRun marks an action that is reported but must not be executed.
	DryRun bool
	// Problem marks a situation that needs an operator: the engine takes
	// no action while it lasts.
	Problem bool
	// Generation is the local generation as the engine knows it.
	Generation uint64
}

// Hex renders a member or cluster ID as etcd prints it.
func Hex(id uint64) string {
	return fmt.Sprintf("%x", id)
}

// ParseHex reads an ID printed by Hex. An empty or malformed string is 0,
// which is never a valid ID.
func ParseHex(s string) uint64 {
	id, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return 0
	}
	return id
}
