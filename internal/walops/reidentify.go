package walops

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

const (
	oldSuffix = ".twok-old"
	newSuffix = ".twok-new"
	// saveBatch bounds the entries per Save, so that the new WAL is cut
	// into segments as etcd cuts its own.
	saveBatch = 1000
)

// OldWALDir is where Reidentify keeps the previous WAL of walDir until
// Cleanup.
func OldWALDir(walDir string) string {
	return walDir + oldSuffix
}

// Reidentify rewrites the WAL in walDir so that its metadata names member
// newID, keeping the cluster ID, the snapshot record, the hard state and
// every entry. etcd must be stopped. etcd takes its member ID from the WAL
// at every start, so the member then starts as newID: the ID the survivor
// assigned when it added this member back as a learner.
//
// This is experimental: whether etcd accepts a member whose ID is not in its
// own snapshot and backend is the question the rejoin test answers.
//
// The new WAL is written next to the old one and swapped in with two
// renames; the old WAL stays in OldWALDir until Cleanup, so that it can be
// put back. Reidentify refuses to run while an old WAL from a previous run
// is still there. Running it with the ID the WAL already has is a no-op.
func Reidentify(lg *zap.Logger, walDir string, newID uint64) (State, error) {
	lg = orNop(lg)
	if newID == 0 {
		return State{}, errors.New("the new member ID is 0")
	}
	oldDir, newDir := OldWALDir(walDir), walDir+newSuffix
	if _, err := os.Stat(oldDir); err == nil {
		return State{}, fmt.Errorf("%s exists from an earlier run; clean it up first", oldDir)
	}
	// The old WAL stays open, with etcd's locks held, until the new one is
	// complete.
	old, l, err := openForWrite(lg, walDir)
	if err != nil {
		return State{}, err
	}
	if l.state.MemberID == newID {
		return l.state, old.Close()
	}
	if err := writeWAL(lg, newDir, newID, l); err != nil {
		_ = old.Close()
		_ = os.RemoveAll(newDir)
		return State{}, err
	}
	if err := old.Close(); err != nil {
		_ = os.RemoveAll(newDir)
		return State{}, fmt.Errorf("close WAL %s: %w", walDir, err)
	}
	if err := swap(walDir, oldDir, newDir); err != nil {
		return State{}, err
	}
	st := l.state
	st.MemberID = newID
	lg.Info("rewrote the WAL with a new member ID",
		zap.String("wal-dir", walDir),
		zap.String("old-wal-dir", oldDir),
		zap.String("previous-member-id", fmt.Sprintf("%x", l.state.MemberID)),
		zap.String("member-id", fmt.Sprintf("%x", newID)),
	)
	return st, nil
}

// Cleanup removes the old WAL that Reidentify kept, once the member has
// rejoined. It does nothing if there is none.
func Cleanup(walDir string) error {
	return os.RemoveAll(OldWALDir(walDir))
}

func writeWAL(lg *zap.Logger, dir string, newID uint64, l walLog) error {
	// A leftover from an interrupted run is ours and incomplete.
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	meta := pb.Metadata{NodeID: newID, ClusterID: l.meta.ClusterID}
	metadata, err := meta.Marshal()
	if err != nil {
		return err
	}
	// Create writes the metadata and a snapshot record at index 0.
	w, err := wal.Create(lg, dir, metadata)
	if err != nil {
		return fmt.Errorf("create WAL %s: %w", dir, err)
	}
	fail := func(err error) error {
		_ = w.Close()
		return fmt.Errorf("write WAL %s: %w", dir, err)
	}
	if l.snap.Index > 0 {
		if err := w.SaveSnapshot(l.snap); err != nil {
			return fail(err)
		}
	}
	for ents := l.ents; len(ents) > 0; {
		n := min(len(ents), saveBatch)
		if err := w.Save(raftpb.HardState{}, ents[:n]); err != nil {
			return fail(err)
		}
		ents = ents[n:]
	}
	if err := w.Save(l.hs, nil); err != nil {
		return fail(err)
	}
	if err := w.Sync(); err != nil {
		return fail(err)
	}
	return w.Close()
}

// swap moves walDir to oldDir and newDir to walDir, and makes both renames
// durable.
func swap(walDir, oldDir, newDir string) error {
	if err := os.Rename(walDir, oldDir); err != nil {
		return fmt.Errorf("move %s aside: %w", walDir, err)
	}
	if err := os.Rename(newDir, walDir); err != nil {
		// Put the old WAL back, so that etcd finds one.
		if rerr := os.Rename(oldDir, walDir); rerr != nil {
			return fmt.Errorf("move %s into place: %w; and put the old WAL back: %w", newDir, err, rerr)
		}
		return fmt.Errorf("move %s into place: %w", newDir, err)
	}
	parent, err := fileutil.OpenDir(filepath.Dir(filepath.Clean(walDir)))
	if err != nil {
		return err
	}
	defer parent.Close()
	return fileutil.Fsync(parent)
}
