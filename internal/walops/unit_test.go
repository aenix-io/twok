package walops_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap/zaptest"

	"github.com/aenix-io/twok/internal/walops"
)

const (
	memberA   = 0xa
	memberB   = 0xb
	clusterID = 0xc
)

// save is one Save call on a synthetic WAL: entries from..to (none when
// from is 0) and the hard state term/vote/commit. Entries 1 and 2 add
// members A and B, as a bootstrap of a pair does.
type save struct {
	from, to           uint64
	term, vote, commit uint64
}

func confChange(t *testing.T, typ raftpb.ConfChangeType, id uint64) []byte {
	t.Helper()
	cc := raftpb.ConfChange{Type: typ, NodeID: id}
	b, err := cc.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func entry(t *testing.T, term, index uint64) raftpb.Entry {
	t.Helper()
	switch index {
	case 1:
		return raftpb.Entry{Term: term, Index: index, Type: raftpb.EntryConfChange, Data: confChange(t, raftpb.ConfChangeAddNode, memberA)}
	case 2:
		return raftpb.Entry{Term: term, Index: index, Type: raftpb.EntryConfChange, Data: confChange(t, raftpb.ConfChangeAddNode, memberB)}
	default:
		return raftpb.Entry{Term: term, Index: index, Data: []byte("x")}
	}
}

// writeWAL creates the WAL of member A in a new directory with the given
// saves, the way raft would have written them, and closes it.
func writeWAL(t *testing.T, saves ...save) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "wal")
	md := pb.Metadata{NodeID: memberA, ClusterID: clusterID}
	meta, err := md.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	w, err := wal.Create(zaptest.NewLogger(t), dir, meta)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range saves {
		var ents []raftpb.Entry
		for i := s.from; s.from != 0 && i <= s.to; i++ {
			ents = append(ents, entry(t, s.term, i))
		}
		hs := raftpb.HardState{Term: s.term, Vote: s.vote, Commit: s.commit}
		if err := w.Save(hs, ents); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

// readWAL returns the metadata, the hard state and the entries.
func readWAL(t *testing.T, dir string) (pb.Metadata, raftpb.HardState, []raftpb.Entry) {
	t.Helper()
	w, err := wal.OpenForRead(zaptest.NewLogger(t), dir, walpb.Snapshot{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	metadata, st, ents, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	var meta pb.Metadata
	if err := meta.Unmarshal(metadata); err != nil {
		t.Fatal(err)
	}
	return meta, st, ents
}

func indexes(ents []raftpb.Entry) []uint64 {
	var idx []uint64
	for _, e := range ents {
		idx = append(idx, e.Index)
	}
	return idx
}

func TestInspect(t *testing.T) {
	dir := writeWAL(t, save{1, 3, 2, 7, 1}, save{4, 5, 2, 7, 3})
	got, err := walops.Inspect(zaptest.NewLogger(t), dir)
	if err != nil {
		t.Fatal(err)
	}
	want := walops.State{
		MemberID: memberA, ClusterID: clusterID, LastIndex: 5, Term: 2, Vote: 7, Commit: 3,
		Members: []uint64{memberA, memberB},
	}
	if !equalState(got, want) || got.Tail() != 2 {
		t.Fatalf("Inspect = %+v, want %+v", got, want)
	}
}

func TestStandalone(t *testing.T) {
	tests := []struct {
		name       string
		saves      []save
		wantLast   uint64
		wantBefore uint64 // commit before
	}{
		{"commit behind the last entries", []save{{1, 3, 2, 7, 1}, {4, 5, 2, 7, 3}}, 5, 3},
		{"commit-only record lost", []save{{1, 5, 3, 0, 4}}, 5, 4},
		{"already committed", []save{{1, 4, 2, 1, 4}}, 4, 4},
		// A new leader in term 3 replaced entries 4 and 5 with a shorter
		// log; the last index is 4, not 5.
		{"overwritten tail", []save{{1, 5, 2, 1, 3}, {4, 4, 3, 2, 3}}, 4, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := writeWAL(t, tt.saves...)
			_, hsBefore, entsBefore := readWAL(t, dir)

			res, err := walops.Standalone(zaptest.NewLogger(t), dir, memberA, []uint64{memberB})
			if err != nil {
				t.Fatal(err)
			}
			if !res.Changed() || !slices.Equal(res.Removed, []uint64{memberB}) ||
				res.Before.LastIndex != tt.wantLast || res.Before.Commit != tt.wantBefore ||
				res.LastIndex != tt.wantLast+1 || res.Commit != tt.wantLast+1 {
				t.Fatalf("Standalone = %+v", res)
			}

			meta, hs, ents := readWAL(t, dir)
			if meta.NodeID != memberA || meta.ClusterID != clusterID {
				t.Fatalf("metadata changed: %v", meta)
			}
			if hs.Term != hsBefore.Term+1 || hs.Vote != memberA || hs.Commit != tt.wantLast+1 || res.Term != hs.Term {
				t.Fatalf("hard state %v, was %v", hs, hsBefore)
			}
			// Every entry is kept, and one removal follows them.
			if !slices.Equal(indexes(ents[:len(ents)-1]), indexes(entsBefore)) {
				t.Fatalf("entries %v, were %v", indexes(ents), indexes(entsBefore))
			}
			last := ents[len(ents)-1]
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(last.Data); err != nil {
				t.Fatal(err)
			}
			if last.Type != raftpb.EntryConfChange || last.Term != hsBefore.Term+1 || last.Index != tt.wantLast+1 ||
				cc.Type != raftpb.ConfChangeRemoveNode || cc.NodeID != memberB || len(cc.Context) != 0 {
				t.Fatalf("appended entry %v, conf change %v", last, cc)
			}

			st, err := walops.Inspect(zaptest.NewLogger(t), dir)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(st.Members, []uint64{memberA}) || st.Tail() != 0 {
				t.Fatalf("after Standalone: %+v", st)
			}
		})
	}
}

// TestStandaloneIsIdempotent runs Standalone twice and checks that the
// second run reports no change and leaves every WAL file byte for byte as
// it was.
func TestStandaloneIsIdempotent(t *testing.T) {
	dir := writeWAL(t, save{1, 9, 2, 1, 6})
	if _, err := walops.Standalone(zaptest.NewLogger(t), dir, memberA, []uint64{memberB}); err != nil {
		t.Fatal(err)
	}
	before := digests(t, dir)
	second, err := walops.Standalone(zaptest.NewLogger(t), dir, memberA, []uint64{memberB})
	if err != nil {
		t.Fatal(err)
	}
	if second.Changed() || second.Commit != 10 {
		t.Fatalf("second Standalone: %+v", second)
	}
	if !bytes.Equal(digests(t, dir), before) {
		t.Fatal("the second Standalone changed the WAL files")
	}
}

func TestStandaloneRefuses(t *testing.T) {
	tests := []struct {
		name   string
		saves  []save
		self   uint64
		remove []uint64
	}{
		{"remove self", []save{{1, 3, 1, 1, 2}}, memberA, []uint64{memberA, memberB}},
		{"another member's WAL", []save{{1, 3, 1, 1, 2}}, memberB, []uint64{memberA}},
		{"a member left in the configuration", []save{{1, 3, 1, 1, 2}}, memberA, nil},
		{"commit beyond the log", []save{{1, 3, 1, 1, 9}}, memberA, []uint64{memberB}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := writeWAL(t, tt.saves...)
			before := digests(t, dir)
			if res, err := walops.Standalone(zaptest.NewLogger(t), dir, tt.self, tt.remove); err == nil {
				t.Fatalf("Standalone succeeded: %+v", res)
			}
			if !bytes.Equal(digests(t, dir), before) {
				t.Fatal("a refused Standalone changed the WAL files")
			}
		})
	}
}

func TestRefusesLockedWAL(t *testing.T) {
	dir := writeWAL(t, save{1, 3, 1, 1, 2})
	// An open WAL holds the same locks a running etcd holds.
	w, err := wal.Open(zaptest.NewLogger(t), dir, walpb.Snapshot{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := walops.Standalone(zaptest.NewLogger(t), dir, memberA, []uint64{memberB}); !errors.Is(err, walops.ErrLocked) {
		t.Fatalf("Standalone on a locked WAL: %v, want ErrLocked", err)
	}
	if _, err := walops.Reidentify(zaptest.NewLogger(t), dir, 0xd); !errors.Is(err, walops.ErrLocked) {
		t.Fatalf("Reidentify on a locked WAL: %v, want ErrLocked", err)
	}
	if _, err := walops.Inspect(zaptest.NewLogger(t), dir); err != nil {
		t.Fatalf("Inspect on a locked WAL: %v", err)
	}
}

func TestWithoutWAL(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	if _, err := walops.Standalone(zaptest.NewLogger(t), dir, memberA, nil); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Standalone without a WAL: %v, want ErrNotExist", err)
	}
	if _, err := walops.Inspect(zaptest.NewLogger(t), dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Inspect without a WAL: %v, want ErrNotExist", err)
	}
}

// TestRepairsTornTail cuts the last record of the WAL in half, as a crash in
// the middle of a write would, and expects Standalone to repair the WAL as
// etcd does and then append after the entries that are whole.
func TestRepairsTornTail(t *testing.T) {
	// The last record is the hard state with commit 3; once it is torn,
	// the previous hard state, commit 2, is the one on disk.
	dir := writeWAL(t, save{1, 3, 2, 1, 2}, save{4, 5, 2, 1, 3})
	segment := lastSegment(t, dir)
	b, err := os.ReadFile(segment)
	if err != nil {
		t.Fatal(err)
	}
	end := len(bytes.TrimRight(b, "\x00"))
	if err := os.Truncate(segment, int64(end-2)); err != nil {
		t.Fatal(err)
	}

	res, err := walops.Standalone(zaptest.NewLogger(t), dir, memberA, []uint64{memberB})
	if err != nil {
		t.Fatal(err)
	}
	if res.Before.Commit != 2 || res.Before.LastIndex != 5 || res.Commit != 6 {
		t.Fatalf("Standalone = %+v", res)
	}
	if _, err := os.Stat(segment + ".broken"); err != nil {
		t.Fatalf("no copy of the torn segment: %v", err)
	}
	if _, hs, ents := readWAL(t, dir); hs.Commit != 6 || !slices.Equal(indexes(ents), []uint64{1, 2, 3, 4, 5, 6}) {
		t.Fatalf("after repair: %v, %v", hs, indexes(ents))
	}
}

func TestReidentify(t *testing.T) {
	dir := writeWAL(t, save{1, 3, 2, 7, 1}, save{4, 5, 2, 7, 3})
	_, hsBefore, entsBefore := readWAL(t, dir)

	st, err := walops.Reidentify(zaptest.NewLogger(t), dir, 0xd)
	if err != nil {
		t.Fatal(err)
	}
	if st.MemberID != 0xd || st.ClusterID != clusterID {
		t.Fatalf("Reidentify = %+v", st)
	}
	meta, hs, ents := readWAL(t, dir)
	if meta.NodeID != 0xd || meta.ClusterID != clusterID {
		t.Fatalf("metadata %v", meta)
	}
	if !reflect.DeepEqual(hs, hsBefore) {
		t.Fatalf("hard state %v, was %v", hs, hsBefore)
	}
	if !reflect.DeepEqual(ents, entsBefore) {
		t.Fatalf("entries %v, were %v", ents, entsBefore)
	}

	// The old WAL is kept until Cleanup, and blocks a second run.
	if meta, _, _ := readWAL(t, walops.OldWALDir(dir)); meta.NodeID != memberA {
		t.Fatalf("old WAL metadata %v", meta)
	}
	if _, err := walops.Reidentify(zaptest.NewLogger(t), dir, 0xe); err == nil {
		t.Fatal("Reidentify ran with an old WAL in place")
	}
	if err := walops.Cleanup(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(walops.OldWALDir(dir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old WAL after Cleanup: %v", err)
	}
	// The same ID again is a no-op.
	before := digests(t, dir)
	if _, err := walops.Reidentify(zaptest.NewLogger(t), dir, 0xd); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(digests(t, dir), before) {
		t.Fatal("Reidentify with the same ID changed the WAL")
	}
}

func equalState(a, b walops.State) bool {
	return slices.Equal(a.Members, b.Members) &&
		a.MemberID == b.MemberID && a.ClusterID == b.ClusterID && a.SnapshotIndex == b.SnapshotIndex &&
		a.LastIndex == b.LastIndex && a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

func lastSegment(t *testing.T, dir string) string {
	t.Helper()
	names, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	if err != nil || len(names) == 0 {
		t.Fatalf("no WAL segment in %s: %v", dir, err)
	}
	slices.Sort(names)
	return names[len(names)-1]
}

// digests returns one SHA-256 over the names and contents of the files in
// dir.
func digests(t *testing.T, dir string) []byte {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		h.Write([]byte(e.Name()))
		h.Write(b)
	}
	return h.Sum(nil)
}
