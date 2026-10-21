package member_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aenix-io/twok/internal/etcdtest"
	"github.com/aenix-io/twok/internal/member"
)

func clientFor(t *testing.T, m *etcdtest.Member) *member.Client {
	t.Helper()
	c, err := member.New(member.Config{
		Endpoint: m.ClientURL,
		CertFile: m.TLS.CertFile,
		KeyFile:  m.TLS.KeyFile,
		CAFile:   m.TLS.CAFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

func TestNewRejectsBadConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  member.Config
	}{
		{"no endpoint", member.Config{}},
		{"certificate without CA", member.Config{Endpoint: "https://127.0.0.1:2379", CertFile: "c", KeyFile: "k"}},
		{"certificate without key", member.Config{Endpoint: "https://127.0.0.1:2379", CertFile: "c", CAFile: "ca"}},
		{"missing CA file", member.Config{Endpoint: "https://127.0.0.1:2379", CAFile: "/nonexistent/ca.crt"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if c, err := member.New(tt.cfg); err == nil {
				_ = c.Close()
				t.Fatal("New accepted the configuration")
			}
		})
	}
}

func TestStatusOfHealthyPair(t *testing.T) {
	cl := etcdtest.StartPair(t)
	var clusterID, leader uint64
	seen := map[uint64]bool{}
	for _, m := range cl.Members {
		st, err := clientFor(t, m).Status(ctx(t))
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: %+v", m.Name, st)
		if !st.HasLeader() || st.IsLearner || len(st.Errors) != 0 || st.Term == 0 || st.CommitIndex == 0 {
			t.Fatalf("%s: unexpected status %+v", m.Name, st)
		}
		if clusterID != 0 && st.ClusterID != clusterID || leader != 0 && st.Leader != leader {
			t.Fatalf("%s disagrees on cluster or leader: %+v", m.Name, st)
		}
		clusterID, leader = st.ClusterID, st.Leader
		seen[st.MemberID] = true
	}
	if len(seen) != 2 || !seen[leader] {
		t.Fatalf("member IDs %v do not include the leader %x", seen, leader)
	}
}

// TestPeerStopped stops one member of a pair and checks what the survivor
// reports, which is what twok's engine bases the claim on.
func TestPeerStopped(t *testing.T) {
	cl := etcdtest.StartPair(t)
	leader := cl.Leader(t)
	follower := cl.Follower(t)
	local := clientFor(t, leader)
	peer := clientFor(t, follower)
	if err := peer.Reachable(ctx(t)); err != nil {
		t.Fatalf("running peer is not reachable: %v", err)
	}

	follower.Stop(t)
	stopped := time.Now()
	var st member.Status
	for {
		var err error
		if st, err = local.Status(ctx(t)); err != nil {
			t.Fatal(err)
		}
		if !st.HasLeader() {
			break
		}
		if time.Since(stopped) > 10*time.Second {
			t.Fatalf("the leader still reports a leader 10s after its peer stopped: %+v", st)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("no leader %s after the peer stopped: %+v", time.Since(stopped).Round(10*time.Millisecond), st)
	if !slices.Contains(st.Errors, member.ErrNoLeader) {
		t.Fatalf("errors %q do not contain %q", st.Errors, member.ErrNoLeader)
	}

	if err := peer.Reachable(context.Background()); err == nil {
		t.Fatal("a stopped peer is reachable")
	}

	// The member list is still served, from the local copy.
	ms, err := local.Members(ctx(t))
	if err != nil {
		t.Fatalf("members without a leader: %v", err)
	}
	if len(ms.Voters) != 2 || len(ms.Learners) != 0 {
		t.Fatalf("members without a leader: %+v", ms)
	}
	for _, v := range ms.Voters {
		m := byName(t, cl, v.Name)
		if !slices.Equal(v.PeerURLs, []string{m.AdvertisePeerURL}) || !slices.Equal(v.ClientURLs, []string{m.ClientURL}) {
			t.Fatalf("member %s: %+v, want peer %s client %s", v.Name, v, m.AdvertisePeerURL, m.ClientURL)
		}
	}
}

func TestLearnerAddPromoteRemove(t *testing.T) {
	cl := etcdtest.Start(t, 1)
	survivor := cl.Members[0]
	local := clientFor(t, survivor)
	joiner := cl.NewMember(t, "m1")

	added, err := local.AddLearner(ctx(t), []string{joiner.AdvertisePeerURL})
	if err != nil {
		t.Fatal(err)
	}
	ms, err := local.Members(ctx(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(ms.Voters) != 1 || len(ms.Learners) != 1 || ms.Learners[0].ID != added.ID || ms.Learners[0].Name != "" {
		t.Fatalf("after adding a learner: %+v", ms)
	}

	// A learner that has not started has replicated nothing, so the
	// promotion is refused, deterministically.
	if err := local.Promote(ctx(t), added.ID); !errors.Is(err, member.ErrLearnerNotReady) {
		t.Fatalf("promote a learner that has not started: %v, want ErrLearnerNotReady", err)
	}

	joiner.Start(t,
		"--initial-cluster="+cl.InitialCluster(survivor, joiner),
		"--initial-cluster-state=existing",
	)
	learner := clientFor(t, joiner)
	st := waitStatus(t, learner)
	if !st.IsLearner || st.MemberID != added.ID {
		t.Fatalf("learner status: %+v, want learner %x", st, added.ID)
	}
	if _, err := learner.Members(ctx(t)); !errors.Is(err, member.ErrLearner) {
		t.Fatalf("members on a learner: %v, want ErrLearner", err)
	}

	attempts := promote(t, local, added.ID)
	t.Logf("promoted after %d attempts", attempts)
	ms, err = local.Members(ctx(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(ms.Voters) != 2 || len(ms.Learners) != 0 {
		t.Fatalf("after promotion: %+v", ms)
	}

	// etcd refuses to remove a voter until the remaining members have been
	// connected to a quorum for its health interval (5s), so a removal right
	// after a promotion fails with "unhealthy cluster" for a few seconds.
	promoted := time.Now()
	for {
		err := local.Remove(ctx(t), added.ID)
		if err == nil {
			break
		}
		if !strings.Contains(err.Error(), "unhealthy cluster") || time.Since(promoted) > 30*time.Second {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("removed %s after the promotion", time.Since(promoted).Round(100*time.Millisecond))
	ms, err = local.Members(ctx(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(ms.Voters) != 1 || ms.Voters[0].Name != survivor.Name {
		t.Fatalf("after removal: %+v", ms)
	}
}

// TestPromoteRightAfterStart promotes a learner as soon as its process has
// started, on a cluster with some data, and logs what etcd answers. Both
// answers are legitimate: the learner may already be within 90% of the
// leader's log, or not. Either way the typed error must come through, and
// the promotion must succeed once the learner has caught up.
func TestPromoteRightAfterStart(t *testing.T) {
	cl := etcdtest.Start(t, 1, "--snapshot-count=100")
	survivor := cl.Members[0]
	local := clientFor(t, survivor)
	kv := survivor.Client(t)
	value := strings.Repeat("x", 4096)
	for i := range 500 {
		if _, err := kv.Put(ctx(t), fmt.Sprintf("/data/%04d", i), value); err != nil {
			t.Fatal(err)
		}
	}

	joiner := cl.NewMember(t, "m1")
	added, err := local.AddLearner(ctx(t), []string{joiner.AdvertisePeerURL})
	if err != nil {
		t.Fatal(err)
	}
	joiner.Start(t,
		"--initial-cluster="+cl.InitialCluster(survivor, joiner),
		"--initial-cluster-state=existing",
	)
	err = local.Promote(ctx(t), added.ID)
	switch {
	case err == nil:
		t.Log("promotion right after start: accepted")
	case errors.Is(err, member.ErrLearnerNotReady):
		t.Logf("promotion right after start: refused, %v", err)
		t.Logf("promoted after %d more attempts", promote(t, local, added.ID))
	default:
		t.Fatalf("promotion right after start: %v", err)
	}
	st := waitStatus(t, clientFor(t, joiner))
	if st.IsLearner {
		t.Fatalf("still a learner after promotion: %+v", st)
	}
}

// promote retries the promotion until the learner is ready and returns the
// number of attempts.
func promote(t *testing.T, c *member.Client, id uint64) int {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for attempt := 1; ; attempt++ {
		err := c.Promote(ctx(t), id)
		if err == nil {
			return attempt
		}
		if !errors.Is(err, member.ErrLearnerNotReady) || time.Now().After(deadline) {
			t.Fatalf("promote: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func waitStatus(t *testing.T, c *member.Client) member.Status {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		st, err := c.Status(ctx(t))
		if err == nil {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("status: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func byName(t *testing.T, cl *etcdtest.Cluster, name string) *etcdtest.Member {
	t.Helper()
	for _, m := range cl.Members {
		if m.Name == name {
			return m
		}
	}
	t.Fatalf("no member %q", name)
	return nil
}
