package walops

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

// ErrLocked is returned when another process, normally etcd, holds the WAL.
var ErrLocked = errors.New("the WAL is locked by another process; stop etcd first")

// State is what the WAL of one member says.
type State struct {
	// MemberID and ClusterID are the WAL's metadata. etcd takes its own
	// member ID from here at every start.
	MemberID  uint64
	ClusterID uint64
	// SnapshotIndex is the index of the WAL snapshot record the log was read
	// from: the newest one at or below the commit index, as etcd picks it.
	SnapshotIndex uint64
	// LastIndex is the index of the last entry, or SnapshotIndex when there
	// is no entry after the snapshot.
	LastIndex uint64
	// Term, Vote and Commit are the last hard state record.
	Term   uint64
	Vote   uint64
	Commit uint64
	// Members are the IDs of the voters and learners in the configuration
	// that results from the snapshot's ConfState and the conf changes after
	// it, committed or not, sorted. It is nil when the snapshot record has
	// no ConfState, which etcd writes since 3.5.
	Members []uint64
}

// Tail is the number of entries above the commit index: the entries
// --force-new-cluster would discard.
func (s State) Tail() uint64 {
	if s.LastIndex < s.Commit {
		return 0
	}
	return s.LastIndex - s.Commit
}

// WALDir is where etcd keeps the WAL of dataDir unless --wal-dir says
// otherwise.
func WALDir(dataDir string) string {
	return filepath.Join(dataDir, "member", "wal")
}

// Inspect reads the WAL in walDir without writing. It takes no lock, so it
// also works while etcd runs, and then reports a moment of a log that keeps
// changing. A torn record at the end of the WAL, which etcd repairs at its
// next start, is ignored.
func Inspect(lg *zap.Logger, walDir string) (State, error) {
	lg = orNop(lg)
	snap, err := newestSnapshot(lg, walDir)
	if err != nil {
		return State{}, err
	}
	w, err := wal.OpenForRead(lg, walDir, snap)
	if err != nil {
		return State{}, fmt.Errorf("open WAL %s for reading: %w", walDir, err)
	}
	defer w.Close()
	l, err := readAll(w, walDir, snap)
	if err != nil {
		return State{}, err
	}
	return l.state, nil
}

// walLog is a WAL read to the end.
type walLog struct {
	snap  walpb.Snapshot
	meta  pb.Metadata
	hs    raftpb.HardState
	ents  []raftpb.Entry
	state State
}

// openForWrite opens the WAL at its newest valid snapshot with etcd's locks
// held, and reads it to the end, so that records can be appended. A torn
// record at the end, left by a crash during a write, is repaired the way
// etcd repairs it at start: the last segment is cut at the last complete
// record, after a copy of it is kept next to it with a .broken suffix. A
// torn record was never fsynced, so nothing acknowledged is lost.
func openForWrite(lg *zap.Logger, walDir string) (*wal.WAL, walLog, error) {
	w, l, err := tryOpenForWrite(lg, walDir)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		return w, l, err
	}
	lg.Info("repairing a torn record at the end of the WAL", zap.String("wal-dir", walDir))
	if !wal.Repair(lg, walDir) {
		return nil, walLog{}, fmt.Errorf("repair WAL %s: %w", walDir, err)
	}
	return tryOpenForWrite(lg, walDir)
}

func tryOpenForWrite(lg *zap.Logger, walDir string) (*wal.WAL, walLog, error) {
	snap, err := newestSnapshot(lg, walDir)
	if err != nil {
		return nil, walLog{}, err
	}
	w, err := wal.Open(lg, walDir, snap)
	if err != nil {
		if errors.Is(err, fileutil.ErrLocked) {
			return nil, walLog{}, fmt.Errorf("open WAL %s: %w", walDir, ErrLocked)
		}
		return nil, walLog{}, fmt.Errorf("open WAL %s: %w", walDir, err)
	}
	l, err := readAll(w, walDir, snap)
	if err != nil {
		// Close releases the locks, which a repair takes again.
		_ = w.Close()
		return nil, walLog{}, err
	}
	return w, l, nil
}

func readAll(w *wal.WAL, walDir string, snap walpb.Snapshot) (walLog, error) {
	metadata, hs, ents, err := w.ReadAll()
	if err != nil {
		return walLog{}, fmt.Errorf("read WAL %s: %w", walDir, err)
	}
	var meta pb.Metadata
	if err := meta.Unmarshal(metadata); err != nil {
		return walLog{}, fmt.Errorf("WAL %s metadata: %w", walDir, err)
	}
	l := walLog{snap: snap, meta: meta, hs: hs, ents: ents}
	l.state = State{
		MemberID:      meta.NodeID,
		ClusterID:     meta.ClusterID,
		SnapshotIndex: snap.Index,
		LastIndex:     snap.Index,
		Term:          hs.Term,
		Vote:          hs.Vote,
		Commit:        hs.Commit,
	}
	if len(ents) > 0 {
		l.state.LastIndex = ents[len(ents)-1].Index
	}
	// raft refuses to start with a commit index beyond its log. Such a WAL
	// is damaged, and none of the operations here can fix it.
	if l.state.Commit > l.state.LastIndex {
		return l, fmt.Errorf("WAL %s: commit index %d is beyond the last index %d", walDir, l.state.Commit, l.state.LastIndex)
	}
	members, err := membership(snap, ents)
	if err != nil {
		return l, fmt.Errorf("WAL %s: %w", walDir, err)
	}
	l.state.Members = members
	return l, nil
}

// membership replays the conf changes after snap on its ConfState. etcd
// writes only v1 conf changes; v2 ones are read for completeness.
func membership(snap walpb.Snapshot, ents []raftpb.Entry) ([]uint64, error) {
	cs := snap.ConfState
	if cs == nil && snap.Index > 0 {
		return nil, nil
	}
	ids := map[uint64]bool{}
	if cs != nil {
		for _, id := range slices.Concat(cs.Voters, cs.Learners) {
			ids[id] = true
		}
	}
	apply := func(typ raftpb.ConfChangeType, id uint64) {
		switch typ {
		case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
			ids[id] = true
		case raftpb.ConfChangeRemoveNode:
			delete(ids, id)
		case raftpb.ConfChangeUpdateNode:
		}
	}
	for _, e := range ents {
		switch e.Type {
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(e.Data); err != nil {
				return nil, fmt.Errorf("conf change at index %d: %w", e.Index, err)
			}
			apply(cc.Type, cc.NodeID)
		case raftpb.EntryConfChangeV2:
			var cc raftpb.ConfChangeV2
			if err := cc.Unmarshal(e.Data); err != nil {
				return nil, fmt.Errorf("conf change at index %d: %w", e.Index, err)
			}
			for _, c := range cc.Changes {
				apply(c.Type, c.NodeID)
			}
		case raftpb.EntryNormal:
		}
	}
	members := make([]uint64, 0, len(ids))
	for id := range ids {
		members = append(members, id)
	}
	slices.Sort(members)
	return members, nil
}

// newestSnapshot returns the newest snapshot record that etcd itself would
// accept to start from: one at or below the WAL's commit index. etcd purges
// old WAL segments, so the record written when the WAL was created, at
// index 0, is often gone.
func newestSnapshot(lg *zap.Logger, walDir string) (walpb.Snapshot, error) {
	if !wal.Exist(walDir) {
		return walpb.Snapshot{}, fmt.Errorf("no WAL in %s: %w", walDir, os.ErrNotExist)
	}
	snaps, err := wal.ValidSnapshotEntries(lg, walDir)
	if err != nil {
		return walpb.Snapshot{}, fmt.Errorf("find snapshot records in WAL %s: %w", walDir, err)
	}
	if len(snaps) == 0 {
		return walpb.Snapshot{}, fmt.Errorf("WAL %s holds no valid snapshot record", walDir)
	}
	return snaps[len(snaps)-1], nil
}

func orNop(lg *zap.Logger) *zap.Logger {
	if lg == nil {
		return zap.NewNop()
	}
	return lg
}
