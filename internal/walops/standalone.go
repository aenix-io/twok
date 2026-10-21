package walops

import (
	"fmt"
	"slices"

	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

// StandaloneResult reports what Standalone found and did.
type StandaloneResult struct {
	// Before is the WAL as Standalone found it.
	Before State
	// Removed are the members for which a RemoveNode entry was appended, in
	// the order of the entries. It is empty when there was nothing to do.
	Removed []uint64
	// LastIndex and Commit are the WAL's after Standalone. Both equal the
	// index of the last appended entry, or Before's when nothing changed.
	LastIndex uint64
	Commit    uint64
	// Term is the term of the appended entries and of the new hard state,
	// or Before's when nothing changed.
	Term uint64
}

// Changed reports whether Standalone wrote to the WAL.
func (r StandaloneResult) Changed() bool {
	return len(r.Removed) > 0
}

// Standalone makes the member self, whose WAL is in walDir, a cluster of one
// by removing the members in remove. etcd must be stopped. Started again
// with its usual flags, etcd applies the removals and becomes the leader of
// a cluster of one.
//
// It writes what etcd's --force-new-cluster writes, with two differences.
// --force-new-cluster discards the entries above the member's commit index
// and appends one ConfChangeRemoveNode entry per other member after it, in
// the hard state's term. Standalone keeps the whole log, appends the same
// entries after the last index, and records a hard state whose commit index
// is the last appended entry, so that the log tail and the removals are
// committed together. The package documentation says why keeping the tail
// is safe with two voters.
//
// The appended entries and the new hard state are in the next term, with a
// vote for self, as if the member had won an election alone. Raft matches
// logs by index and term, and the other member may hold entries in the
// current term that this member never received: the last leader's proposals
// that were cut off. An entry of ours at the same index and term would look
// identical to raft, and the other member, if it ever rejoins with its data,
// would keep its own entry. No leader can have existed in the next term,
// because electing one needs this member's vote, which it persists before it
// gives it; so no entry anywhere carries that term, and the other member's
// conflicting tail is overwritten when it catches up.
//
// remove must name every member except self that the log knows about; a
// member the log no longer knows about, because an earlier run already
// removed it, is skipped. Running Standalone again is therefore a no-op, as
// is running it on a member that is already alone.
func Standalone(lg *zap.Logger, walDir string, self uint64, remove []uint64) (StandaloneResult, error) {
	return standalone(lg, walDir, self, remove, false)
}

// standalone is Standalone; sameTerm keeps the hard state's term, as etcd's
// --force-new-cluster does, for the test that shows why Standalone does not.
func standalone(lg *zap.Logger, walDir string, self uint64, remove []uint64, sameTerm bool) (StandaloneResult, error) {
	lg = orNop(lg)
	if slices.Contains(remove, self) {
		return StandaloneResult{}, fmt.Errorf("member %x cannot remove itself", self)
	}
	w, l, err := openForWrite(lg, walDir)
	if err != nil {
		return StandaloneResult{}, err
	}
	res := StandaloneResult{Before: l.state, LastIndex: l.state.LastIndex, Commit: l.state.Commit, Term: l.state.Term}
	fail := func(err error) (StandaloneResult, error) {
		_ = w.Close()
		return res, err
	}
	if l.state.MemberID != self {
		return fail(fmt.Errorf("WAL %s belongs to member %x, not %x", walDir, l.state.MemberID, self))
	}
	toRemove, err := removals(l.state.Members, self, remove)
	if err != nil {
		return fail(fmt.Errorf("WAL %s: %w", walDir, err))
	}
	if len(toRemove) == 0 {
		lg.Info("member is already alone in its log", zap.String("wal-dir", walDir), zap.Uint64s("members", l.state.Members))
		return res, w.Close()
	}

	term, vote := l.state.Term+1, self
	if sameTerm {
		term, vote = l.state.Term, l.state.Vote
	}
	ents := make([]raftpb.Entry, 0, len(toRemove))
	next := l.state.LastIndex + 1
	for _, id := range toRemove {
		// The same entry etcd's createConfigChangeEnts builds for a
		// removal: a v1 conf change with the node ID and nothing else.
		cc := raftpb.ConfChange{Type: raftpb.ConfChangeRemoveNode, NodeID: id}
		data, err := cc.Marshal()
		if err != nil {
			return fail(err)
		}
		ents = append(ents, raftpb.Entry{
			Type:  raftpb.EntryConfChange,
			Term:  term,
			Index: next,
			Data:  data,
		})
		next++
	}
	last := next - 1
	hs := raftpb.HardState{Term: term, Vote: vote, Commit: last}
	if err := w.Save(hs, ents); err != nil {
		return fail(fmt.Errorf("append to WAL %s: %w", walDir, err))
	}
	// Save syncs when it writes entries. Sync anyway, so that durability
	// does not hang on that detail.
	if err := w.Sync(); err != nil {
		return fail(fmt.Errorf("sync WAL %s: %w", walDir, err))
	}
	if err := w.Close(); err != nil {
		return res, fmt.Errorf("close WAL %s: %w", walDir, err)
	}
	res.Removed, res.LastIndex, res.Commit, res.Term = toRemove, last, last, term
	lg.Info("appended member removals to the WAL",
		zap.String("wal-dir", walDir),
		zap.Uint64s("removed", toRemove),
		zap.Uint64("previous-commit", l.state.Commit),
		zap.Uint64("previous-last-index", l.state.LastIndex),
		zap.Uint64("commit", last),
		zap.Uint64("term", term),
	)
	return res, nil
}

// removals checks remove against the members the log knows, and returns
// the ones to remove. When the snapshot record carries no ConfState, the
// members are unknown and remove is taken as it is.
func removals(members []uint64, self uint64, remove []uint64) ([]uint64, error) {
	if members == nil {
		return slices.Compact(slices.Sorted(slices.Values(remove))), nil
	}
	if !slices.Contains(members, self) {
		return nil, fmt.Errorf("member %x is not in its own log's configuration %x", self, members)
	}
	var out []uint64
	for _, id := range members {
		if id == self {
			continue
		}
		if !slices.Contains(remove, id) {
			return nil, fmt.Errorf("member %x is in the log's configuration %x but not in the members to remove %x", id, members, remove)
		}
		out = append(out, id)
	}
	return out, nil
}
