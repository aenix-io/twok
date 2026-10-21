package walops_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/aenix-io/twok/internal/etcdtest"
	"github.com/aenix-io/twok/internal/walops"
)

// TestRejoinWithNewID is the rejoin experiment. The follower of a pair
// survives, the leader loses: both stop, the survivor becomes a cluster of
// one with Standalone and takes more writes, then adds the loser back as a
// learner, which assigns a new member ID. The loser's WAL is rewritten with
// Reidentify to carry that ID, and the loser starts with its original
// flags, its data directory otherwise untouched.
//
// The question is whether etcd accepts this: the loser has to connect to
// the survivor, receive what it is missing, apply the removal of its old ID
// and the addition of its new one, serve as a learner with the survivor's
// data, and be promoted. Each case logs what happens at each step.
//
// Cases:
//   - incremental: the survivor's log still holds everything the loser
//     lacks, so the loser receives entries;
//   - snapshot: the survivor has compacted its log past the loser's last
//     index, so the loser receives a snapshot;
//   - divergent: the loser, as leader, appended entries the survivor never
//     received, cut off by a partition; they must not survive the rejoin;
//   - divergent-same-term: the same, with the removals appended in the
//     current term as --force-new-cluster does. This is the control for the
//     term change in Standalone; it is expected to leave the loser
//     inconsistent, and the test only reports what it finds.
func TestRejoinWithNewID(t *testing.T) {
	type standaloneFunc func(*zap.Logger, string, uint64, []uint64) (walops.StandaloneResult, error)
	tests := []struct {
		name       string
		args       []string
		phase2     int
		divergent  bool
		standalone standaloneFunc
		control    bool
		// fresh wipes the loser's data and joins it as a new learner, the
		// path etcd documents, instead of Reidentify. It is the control for
		// the snapshot case.
		fresh bool
	}{
		{name: "incremental", phase2: 100, standalone: walops.Standalone},
		// etcd keeps 5000 entries after a snapshot for slow followers.
		{name: "snapshot", args: []string{"--snapshot-count=100"}, phase2: 5300, standalone: walops.Standalone},
		{name: "snapshot-fresh-learner", args: []string{"--snapshot-count=100"}, phase2: 5300, standalone: walops.Standalone, fresh: true},
		{name: "divergent", phase2: 100, divergent: true, standalone: walops.Standalone},
		{name: "divergent-same-term", phase2: 100, divergent: true, standalone: walops.StandaloneSameTerm, control: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl := etcdtest.StartPair(t, tt.args...)
			loser := cl.Leader(t)
			survivor := cl.Follower(t)
			original := survivor.ExtraArgs()
			idSurvivor, idLoser := survivor.ID(t), loser.ID(t)
			writeKeys(t, loser, "/phase1/", 100)

			if tt.divergent {
				proposeCutOff(t, cl, loser)
			}
			loser.Stop(t)
			survivor.Stop(t)
			for _, m := range cl.Members {
				m.Proxy.Heal()
			}
			lg := zaptest.NewLogger(t)
			loserBefore, err := walops.Inspect(lg, walops.WALDir(loser.DataDir))
			if err != nil {
				t.Fatal(err)
			}
			res, err := tt.standalone(lg, walops.WALDir(survivor.DataDir), idSurvivor, []uint64{idLoser})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("step 1: Standalone on the survivor: last %d, commit %d, term %d before; removed %x at index %d, term %d. Loser's WAL: last %d, commit %d, term %d",
				res.Before.LastIndex, res.Before.Commit, res.Before.Term, res.Removed, res.LastIndex, res.Term,
				loserBefore.LastIndex, loserBefore.Commit, loserBefore.Term)
			survivor.Start(t, original...)
			if got := survivor.WaitLeader(t, 15*time.Second); got != idSurvivor {
				t.Fatalf("leader %x, want the survivor %x", got, idSurvivor)
			}
			writeKeys(t, survivor, "/phase2/", tt.phase2)

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			add, err := survivor.Client(t).MemberAddAsLearner(ctx, []string{loser.AdvertisePeerURL})
			if err != nil {
				t.Fatal(err)
			}
			newID := add.Member.GetID()
			t.Logf("step 2: the survivor added the loser back as learner %x (old ID %x)", newID, idLoser)

			startArgs := original
			if tt.fresh {
				if err := os.RemoveAll(loser.DataDir); err != nil {
					t.Fatal(err)
				}
				startArgs = append(slices.Clone(original),
					"--initial-cluster="+cl.InitialCluster(survivor, loser), "--initial-cluster-state=existing")
				t.Logf("step 3: the loser's data is wiped; it joins as a new learner")
			} else {
				if _, err := walops.Reidentify(lg, walops.WALDir(loser.DataDir), newID); err != nil {
					t.Fatal(err)
				}
				t.Logf("step 3: Reidentify on the loser: member %x", newID)
			}

			loser.Start(t, startArgs...)
			t.Cleanup(func() {
				// A goroutine dump in the log shows where a stuck etcd waits.
				if t.Failed() && loser.Running() {
					_ = syscall.Kill(loser.Pid(), syscall.SIGQUIT)
					<-loser.Exited()
				}
			})
			st, err := waitCaughtUp(t, survivor, loser, newID)
			if err != nil {
				if tt.control {
					t.Logf("step 4: control outcome: %v; log lines of interest:\n%s", err, interesting(loser.Log(t)))
					return
				}
				t.Fatal(err)
			}
			t.Logf("step 4: the loser started with its original flags and is learner %x at applied index %d", newID, st.RaftAppliedIndex)
			log := loser.Log(t)
			snapshotted := strings.Contains(log, "applied snapshot") || strings.Contains(strings.ToLower(log), "applied incoming raft snapshot")
			t.Logf("step 5: the loser received a snapshot: %v; removed its old ID: %v",
				snapshotted, strings.Contains(log, fmt.Sprintf(`"removed-remote-peer-id":"%x"`, idLoser)))

			promote(ctx, t, survivor, newID)
			members, err := survivor.Client(t).MemberList(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var voters []string
			for _, m := range members.Members {
				if !m.GetIsLearner() {
					voters = append(voters, fmt.Sprintf("%s=%x", m.GetName(), m.GetID()))
				}
			}
			t.Logf("step 6: promoted; voters: %v", voters)
			if len(voters) != 2 {
				t.Fatalf("voters after promotion: %v", voters)
			}
			// A write now needs the loser's acknowledgement.
			promoted := time.Now()
			writeKeysPatient(t, survivor, "/phase3/", 10)
			t.Logf("step 6a: 10 writes after the promotion took %s", time.Since(promoted).Round(10*time.Millisecond))
			waitCount(t, loser, "/phase3/", 10)

			onSurvivor, onLoser := allKeys(t, survivor), allKeys(t, loser)
			same := maps.Equal(onSurvivor, onLoser)
			t.Logf("step 7: keys: survivor %d, loser %d, identical %v; /divergent/ keys: survivor %d, loser %d",
				len(onSurvivor), len(onLoser), same, countPrefix(t, survivor, "/divergent/"), countPrefix(t, loser, "/divergent/"))
			if tt.control {
				return
			}
			if !same {
				t.Fatal("the loser's keys differ from the survivor's after the rejoin")
			}
			for _, prefix := range []string{"/phase1/", "/phase2/"} {
				if n, want := countPrefix(t, loser, prefix), map[string]int{"/phase1/": 100, "/phase2/": tt.phase2}[prefix]; n != want {
					t.Errorf("the loser serves %d keys under %s, want %d", n, prefix, want)
				}
			}
			if tt.fresh {
				return
			}
			if err := walops.Cleanup(walops.WALDir(loser.DataDir)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// proposeCutOff cuts both peer proxies and proposes writes on the leader
// until it steps down. The leader appends them to its own log; the
// follower never receives them.
func proposeCutOff(t *testing.T, cl *etcdtest.Cluster, leader *etcdtest.Member) {
	t.Helper()
	for _, m := range cl.Members {
		m.Proxy.Cut()
	}
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if _, err := leader.Client(t).Put(ctx, fmt.Sprintf("/divergent/%02d", i), "v"); err == nil {
				t.Errorf("a put was acknowledged during a full partition")
			}
		})
	}
	wg.Wait()
}

// waitCaughtUp waits until the loser serves as learner newID and has
// applied everything the survivor had committed.
//
// While the loser is not there yet, the survivor takes a small write every
// second, as a cluster in use would. etcd 3.6 needs one: a member that
// installs a snapshot more than 5000 entries ahead of what it had applied
// refuses its own publish request with "too many requests" until it applies
// an entry after the snapshot, and serves no client request before it has
// published. Without traffic it waits forever; the fresh-learner case shows
// that this is etcd's behaviour, not a consequence of Reidentify.
func waitCaughtUp(t *testing.T, survivor, loser *etcdtest.Member, newID uint64) (*clientv3.StatusResponse, error) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	nudges := 0
	defer func() { t.Logf("writes to the survivor while the loser caught up: %d", nudges) }()
	for i := 0; ; i++ {
		select {
		case <-loser.Exited():
			return nil, fmt.Errorf("the loser exited: %w", loser.ExitErr())
		default:
		}
		sst, serr := survivor.Status(t)
		lst, lerr := loser.Status(t)
		// Status is answered before the member serves reads; a read tells
		// that it has published itself and is ready.
		if serr == nil && lerr == nil && lst.Header.MemberId == newID && lst.IsLearner && lst.RaftAppliedIndex >= sst.RaftIndex && serves(t, loser) {
			return lst, nil
		}
		if i%5 == 4 {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if _, err := survivor.Client(t).Put(ctx, "/nudge", strconv.Itoa(i)); err != nil {
				t.Logf("write to the survivor: %v", err)
			} else {
				nudges++
			}
			cancel()
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("the loser did not catch up as learner %x: %+v, %w; survivor %+v, %w; log lines of interest:\n%s",
				newID, lst, lerr, sst, serr, interesting(loser.Log(t)))
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// serves reports whether m answers a serializable read within a second.
func serves(t *testing.T, m *etcdtest.Member) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := m.Client(t).Get(ctx, "/", clientv3.WithSerializable(), clientv3.WithCountOnly())
	return err == nil
}

func promote(ctx context.Context, t *testing.T, survivor *etcdtest.Member, id uint64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, err := survivor.Client(t).MemberPromote(ctx, id)
		if err == nil {
			return
		}
		if !errors.Is(err, rpctypes.ErrMemberLearnerNotReady) || time.Now().After(deadline) {
			t.Fatalf("promote %x: %v", id, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func writeKeys(t *testing.T, m *etcdtest.Member, prefix string, n int) {
	t.Helper()
	for i := range n {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := m.Client(t).Put(ctx, fmt.Sprintf("%s%04d", prefix, i), "v")
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
}

// writeKeysPatient writes like writeKeys, with up to 30 seconds per write:
// the first writes after a promotion wait for the new voter's connections.
func writeKeysPatient(t *testing.T, m *etcdtest.Member, prefix string, n int) {
	t.Helper()
	for i := range n {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, err := m.Client(t).Put(ctx, fmt.Sprintf("%s%04d", prefix, i), "v")
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func countPrefix(t *testing.T, m *etcdtest.Member, prefix string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := m.Client(t).Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly(), clientv3.WithSerializable())
	if err != nil {
		t.Fatal(err)
	}
	return int(resp.Count)
}

// allKeys returns every key with its value and mod revision, read
// serializably.
func allKeys(t *testing.T, m *etcdtest.Member) map[string]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := m.Client(t).Get(ctx, "\x00", clientv3.WithFromKey(), clientv3.WithSerializable())
	if err != nil {
		t.Fatal(err)
	}
	keys := make(map[string]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		keys[string(kv.Key)] = fmt.Sprintf("%s@%d", kv.Value, kv.ModRevision)
	}
	return keys
}

func waitCount(t *testing.T, m *etcdtest.Member, prefix string, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for countPrefix(t, m, prefix) != n {
		if time.Now().After(deadline) {
			t.Fatalf("%s serves %d keys under %s, want %d", m.Name, countPrefix(t, m, prefix), prefix, n)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// interesting returns the log lines that say something about membership,
// identity, snapshots or failures.
func interesting(log string) string {
	keys := []string{
		"panic", "fatal", "removed", "snapshot", "sender", "not found", "mismatch",
		"learner", "rejected", "error", "conf", "local-member-id", "warn",
	}
	var out []string
	for line := range strings.SplitSeq(log, "\n") {
		l := strings.ToLower(line)
		for _, k := range keys {
			if strings.Contains(l, k) {
				out = append(out, line)
				break
			}
		}
	}
	if len(out) > 120 {
		out = out[len(out)-120:]
	}
	return strings.Join(out, "\n")
}
