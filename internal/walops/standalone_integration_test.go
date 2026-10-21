package walops_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap/zaptest"

	"github.com/aenix-io/twok/internal/etcdtest"
	"github.com/aenix-io/twok/internal/walops"
)

const keyPrefix = "/acked/"

// takeover is the outcome of one trial of TestStandaloneKeepsAcknowledgedWrites.
type takeover struct {
	acked       int
	inspected   walops.State
	leaderAfter time.Duration
	missing     int // acknowledged keys missing after Standalone
	missingCtrl int // acknowledged keys missing after etcd's --force-new-cluster
	extra       int // keys present although their put was not acknowledged
}

// TestStandaloneKeepsAcknowledgedWrites reproduces the follower's last
// entries: a follower that holds acknowledged entries above its on-disk
// commit index. The follower is killed with SIGKILL after the last
// acknowledged write. Raft has not asked for the commit-only hard state to
// be synced, and etcd's WAL encoder still holds it in its buffer, so it is
// lost with the process.
//
// The follower's data directory is then recovered twice: once as twok does
// it, Standalone and a restart with the original flags, and once, on a copy,
// with etcd's own --force-new-cluster, as the control. The test requires
// that every acknowledged key is present after the first, that the member
// leads a cluster of one within seconds, and that the old leader, started
// again, is told it was removed. It reports what the control lost.
//
// Modes:
//   - immediate: sequential writes, kill right after the last answer;
//   - quiet: sequential writes, kill a second after the last answer, when
//     heartbeats have long told the follower about the commit;
//   - concurrent: writes in flight, kill at a random moment.
func TestStandaloneKeepsAcknowledgedWrites(t *testing.T) {
	etcdtest.Binary(t)
	trials := 3
	if s := os.Getenv("TWOK_TAKEOVER_TRIALS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 {
			t.Fatalf("TWOK_TAKEOVER_TRIALS=%q is not a positive number", s)
		}
		trials = n
	}
	modes := []struct {
		name  string
		write func(t *testing.T, kv *clientv3.Client, kill func()) []string
	}{
		{"immediate", func(t *testing.T, kv *clientv3.Client, kill func()) []string {
			acked := writeSequential(t, kv, 200)
			kill()
			return acked
		}},
		{"quiet", func(t *testing.T, kv *clientv3.Client, kill func()) []string {
			acked := writeSequential(t, kv, 200)
			time.Sleep(time.Second)
			kill()
			return acked
		}},
		{"concurrent", writeConcurrent},
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			var results []takeover
			for i := range trials {
				t.Run(strconv.Itoa(i), func(t *testing.T) {
					r := runTakeover(t, mode.write)
					t.Logf("RESULT mode=%s acked=%d snapshot=%d last=%d commit=%d tail=%d leader_after=%s missing=%d missing_force_new_cluster=%d unacked_present=%d",
						mode.name, r.acked, r.inspected.SnapshotIndex, r.inspected.LastIndex, r.inspected.Commit,
						r.inspected.Tail(), r.leaderAfter.Round(10*time.Millisecond), r.missing, r.missingCtrl, r.extra)
					results = append(results, r)
				})
			}
			var behind, lost int
			for _, r := range results {
				if r.inspected.Tail() > 0 {
					behind++
				}
				if r.missingCtrl > 0 {
					lost++
				}
			}
			t.Logf("SUMMARY mode=%s trials=%d commit_behind=%d force_new_cluster_lost_acked=%d", mode.name, len(results), behind, lost)
		})
	}
}

func runTakeover(t *testing.T, write func(*testing.T, *clientv3.Client, func()) []string) takeover {
	t.Helper()
	cl := etcdtest.StartPair(t)
	leader := cl.Leader(t)
	follower := cl.Follower(t)
	original := follower.ExtraArgs()
	leaderID, followerID := leader.ID(t), follower.ID(t)

	acked := write(t, leader.Client(t), func() { follower.Kill(t) })
	if follower.Running() {
		t.Fatal("the write function did not kill the follower")
	}
	// The old leader is the loser. It is started again later, to see that
	// the survivor turns it away.
	leader.Kill(t)

	lg := zaptest.NewLogger(t)
	walDir := walops.WALDir(follower.DataDir)
	inspected, err := walops.Inspect(lg, walDir)
	if err != nil {
		t.Fatal(err)
	}
	control := filepath.Join(cl.Dir, "control.etcd")
	follower.CopyDataDir(t, control)

	res, err := walops.Standalone(lg, walDir, followerID, []uint64{leaderID})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Changed() || res.Before.Commit != inspected.Commit || res.Commit != inspected.LastIndex+1 {
		t.Fatalf("Standalone %+v does not match Inspect %+v", res, inspected)
	}
	assertNoop(t, walDir, followerID, leaderID)

	r := takeover{acked: len(acked), inspected: inspected}
	started := time.Now()
	follower.Start(t, original...)
	if got := follower.WaitLeader(t, 15*time.Second); got != followerID {
		t.Fatalf("leader %x after Standalone, want %x", got, followerID)
	}
	r.leaderAfter = time.Since(started)
	assertAlone(t, follower, followerID)
	r.missing, r.extra = countKeys(t, follower, acked)
	if r.missing != 0 {
		t.Errorf("%d of %d acknowledged keys are missing after Standalone", r.missing, len(acked))
	}

	// The loser comes back with its original flags and its two-voter data.
	leader.Start(t, original...)
	select {
	case <-leader.Exited():
	case <-time.After(30 * time.Second):
		t.Fatal("the removed member is still running 30s after it was started")
	}
	if !strings.Contains(leader.Log(t), "the member has been permanently removed from the cluster") {
		t.Errorf("the removed member exited (%v) without saying it was removed", leader.ExitErr())
	}
	if !strings.Contains(follower.Log(t), "because it was removed") {
		t.Error("the survivor did not log that it rejected the removed member")
	}

	follower.Stop(t)
	// The member is alone now; another Standalone changes nothing.
	assertNoop(t, walDir, followerID, leaderID)

	follower.Start(t, "--data-dir="+control, "--force-new-cluster")
	follower.WaitLeader(t, 30*time.Second)
	r.missingCtrl, _ = countKeys(t, follower, acked)
	follower.Stop(t)
	return r
}

// assertNoop runs Standalone and checks that it changed nothing.
func assertNoop(t *testing.T, walDir string, self, other uint64) {
	t.Helper()
	before := digests(t, walDir)
	res, err := walops.Standalone(zaptest.NewLogger(t), walDir, self, []uint64{other})
	if err != nil {
		t.Fatal(err)
	}
	if res.Changed() || !bytes.Equal(digests(t, walDir), before) {
		t.Fatalf("Standalone on a member that is alone changed something: %+v", res)
	}
}

// assertAlone checks that m serves a member list with only itself.
func assertAlone(t *testing.T, m *etcdtest.Member, id uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := m.Client(t).MemberList(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Members) != 1 || resp.Members[0].GetID() != id || resp.Members[0].GetIsLearner() {
		t.Fatalf("members after Standalone: %v", resp.Members)
	}
}

func writeSequential(t *testing.T, kv *clientv3.Client, n int) []string {
	t.Helper()
	acked := make([]string, 0, n)
	for i := range n {
		key := fmt.Sprintf("%s%06d", keyPrefix, i)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := kv.Put(ctx, key, "v")
		cancel()
		if err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		acked = append(acked, key)
	}
	return acked
}

// writeConcurrent runs four writers and kills the follower after a random
// delay. A put that returned an error has an unknown outcome and is not
// counted as acknowledged.
func writeConcurrent(t *testing.T, kv *clientv3.Client, kill func()) []string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var (
		mu    sync.Mutex
		acked []string
		wg    sync.WaitGroup
	)
	for w := range 4 {
		wg.Go(func() {
			for i := 0; ctx.Err() == nil; i++ {
				key := fmt.Sprintf("%s%d-%06d", keyPrefix, w, i)
				pctx, pcancel := context.WithTimeout(ctx, 2*time.Second)
				_, err := kv.Put(pctx, key, "v")
				pcancel()
				if err != nil {
					continue
				}
				mu.Lock()
				acked = append(acked, key)
				mu.Unlock()
			}
		})
	}
	time.Sleep(time.Duration(300+rand.IntN(1200)) * time.Millisecond)
	kill()
	// Writes the follower acknowledged before it died may still complete;
	// give them a moment, then stop the writers.
	time.Sleep(500 * time.Millisecond)
	cancel()
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	return slices.Clone(acked)
}

// countKeys returns how many of acked are missing on m, and how many keys
// are present that are not in acked.
func countKeys(t *testing.T, m *etcdtest.Member, acked []string) (missing, extra int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := m.Client(t).Get(ctx, keyPrefix, clientv3.WithPrefix(), clientv3.WithKeysOnly(), clientv3.WithSerializable())
	if err != nil {
		t.Fatal(err)
	}
	present := make(map[string]bool, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		present[string(kv.Key)] = true
	}
	for _, k := range acked {
		if !present[k] {
			missing++
		}
		delete(present, k)
	}
	return missing, len(present)
}

// TestRefusesRunningEtcd checks that the WAL lock of a running etcd stops
// the operations that write, and that Inspect still works next to it.
func TestRefusesRunningEtcd(t *testing.T) {
	cl := etcdtest.Start(t, 1)
	m := cl.Members[0]
	id := m.ID(t)
	walDir := walops.WALDir(m.DataDir)
	if _, err := walops.Standalone(zaptest.NewLogger(t), walDir, id, nil); !errors.Is(err, walops.ErrLocked) {
		t.Fatalf("Standalone under a running etcd: %v, want ErrLocked", err)
	}
	if _, err := walops.Reidentify(zaptest.NewLogger(t), walDir, id+1); !errors.Is(err, walops.ErrLocked) {
		t.Fatalf("Reidentify under a running etcd: %v, want ErrLocked", err)
	}
	st, err := walops.Inspect(zaptest.NewLogger(t), walDir)
	if err != nil {
		t.Fatalf("Inspect under a running etcd: %v", err)
	}
	if st.MemberID != id || !slices.Equal(st.Members, []uint64{id}) {
		t.Fatalf("Inspect under a running etcd: %+v, want member %x alone", st, id)
	}
	// etcd is unaffected.
	if _, err := m.Client(t).Put(context.Background(), "k", "v"); err != nil {
		t.Fatal(err)
	}
}

// TestStandaloneAfterSnapshotsAndPurge writes enough to a pair to make etcd
// snapshot often and purge its first WAL segments, so that the WAL no longer
// holds the snapshot record at index 0 and the membership comes from a
// snapshot's ConfState, and checks that Standalone opens it at the newest
// snapshot record and that the member starts from the result.
func TestStandaloneAfterSnapshotsAndPurge(t *testing.T) {
	cl := etcdtest.StartPair(t, "--snapshot-count=10", "--max-wals=2")
	leader := cl.Leader(t)
	follower := cl.Follower(t)
	original := follower.ExtraArgs()
	leaderID, followerID := leader.ID(t), follower.ID(t)
	walDir := walops.WALDir(follower.DataDir)
	kv := leader.Client(t)

	// A WAL segment is 64 MiB. etcd keeps --max-wals segments that are
	// behind a snapshot and purges the rest every 30 seconds.
	value := strings.Repeat("x", 1<<20)
	deadline := time.Now().Add(3 * time.Minute)
	var last string
	for i := 0; !purged(t, walDir); i++ {
		if time.Now().After(deadline) {
			t.Fatalf("etcd did not purge a WAL segment; segments: %v", segments(t, walDir))
		}
		if len(segments(t, walDir)) > 2 {
			// Enough segments; wait for the purge loop instead of writing.
			time.Sleep(time.Second)
			continue
		}
		last = fmt.Sprintf("/big/%04d", i%16)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := kv.Put(ctx, last, value[:len(value)-i])
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := kv.Put(ctx, "/final", "acked"); err != nil {
		t.Fatal(err)
	}
	follower.Kill(t)
	leader.Kill(t)
	t.Logf("segments after the purge: %v", segments(t, walDir))

	lg := zaptest.NewLogger(t)
	res, err := walops.Standalone(lg, walDir, followerID, []uint64{leaderID})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RESULT snapshot=%d last=%d commit=%d tail=%d members=%x", res.Before.SnapshotIndex, res.Before.LastIndex, res.Before.Commit, res.Before.Tail(), res.Before.Members)
	if res.Before.SnapshotIndex == 0 || !res.Changed() {
		t.Fatalf("Standalone %+v: want a snapshot above 0 and a removal", res)
	}

	follower.Start(t, original...)
	if got := follower.WaitLeader(t, 15*time.Second); got != followerID {
		t.Fatalf("leader %x after Standalone, want %x", got, followerID)
	}
	assertAlone(t, follower, followerID)
	fc := follower.Client(t)
	resp, err := fc.Get(ctx, "/final")
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "acked" {
		t.Fatalf("/final after recovery: %v", resp.Kvs)
	}
	if resp, err := fc.Get(ctx, last, clientv3.WithKeysOnly()); err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("%s after recovery: %v, %v", last, resp, err)
	}
}

func segments(t *testing.T, walDir string) []string {
	t.Helper()
	names, err := filepath.Glob(filepath.Join(walDir, "*.wal"))
	if err != nil {
		t.Fatal(err)
	}
	for i, n := range names {
		names[i] = filepath.Base(n)
	}
	slices.Sort(names)
	return names
}

// purged reports whether the first segment, the one that starts at index 0,
// is gone.
func purged(t *testing.T, walDir string) bool {
	t.Helper()
	s := segments(t, walDir)
	return len(s) > 0 && s[0] != "0000000000000000-0000000000000000.wal"
}
