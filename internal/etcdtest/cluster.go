package etcdtest

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// leaderTimeout bounds the first election of a new cluster.
const leaderTimeout = 30 * time.Second

// Cluster is a set of members that share a CA and a directory.
type Cluster struct {
	CA      *CA
	Dir     string
	Members []*Member
}

// NewCluster prepares an empty cluster: a temporary directory and a CA.
// Members are added with NewMember.
func NewCluster(t testing.TB) *Cluster {
	t.Helper()
	Binary(t)
	dir := t.TempDir()
	return &Cluster{CA: NewCA(t, dir), Dir: dir}
}

// Start starts a new cluster of n members, named m0, m1 and so on, with
// --initial-cluster-state=new, and waits until every member reports a leader.
// extra is passed to every member. twok runs with n = 2; n = 1 is the
// starting point for learner tests.
func Start(t testing.TB, n int, extra ...string) *Cluster {
	t.Helper()
	cl := NewCluster(t)
	for i := range n {
		cl.NewMember(t, fmt.Sprintf("m%d", i))
	}
	args := append([]string{
		"--initial-cluster=" + cl.InitialCluster(cl.Members...),
		"--initial-cluster-state=new",
		"--initial-cluster-token=etcdtest",
	}, extra...)
	for _, m := range cl.Members {
		m.Start(t, args...)
	}
	for _, m := range cl.Members {
		m.WaitLeader(t, leaderTimeout)
	}
	return cl
}

// StartPair starts the two-member cluster twok is about.
func StartPair(t testing.TB, extra ...string) *Cluster {
	t.Helper()
	return Start(t, 2, extra...)
}

// NewMember prepares a member in the cluster's directory, with a certificate
// from the cluster's CA, and adds it to Members. It is not started.
func (cl *Cluster) NewMember(t testing.TB, name string) *Member {
	t.Helper()
	m := NewMember(t, cl.CA, cl.Dir, name)
	cl.Members = append(cl.Members, m)
	return m
}

// InitialCluster is the --initial-cluster value for members.
func (cl *Cluster) InitialCluster(members ...*Member) string {
	entries := make([]string, 0, len(members))
	for _, m := range members {
		entries = append(entries, m.InitialClusterEntry())
	}
	return strings.Join(entries, ",")
}

// Leader returns the leader, once every running member reports the same one.
// Members briefly disagree while leadership moves.
func (cl *Cluster) Leader(t testing.TB) *Member {
	t.Helper()
	deadline := time.Now().Add(leaderTimeout)
	for {
		if id, ok := cl.agreedLeader(t); ok {
			if m := cl.ByID(t, id); m != nil {
				return m
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("members do not agree on a leader")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Follower returns a running member that is not the leader.
func (cl *Cluster) Follower(t testing.TB) *Member {
	t.Helper()
	leader := cl.Leader(t)
	for _, m := range cl.Members {
		if m != leader && m.Running() {
			return m
		}
	}
	t.Fatal("no running follower")
	return nil
}

func (cl *Cluster) agreedLeader(t testing.TB) (uint64, bool) {
	t.Helper()
	var leader uint64
	for _, m := range cl.Members {
		if !m.Running() {
			continue
		}
		st, err := m.Status(t)
		if err != nil || st.Leader == 0 || leader != 0 && st.Leader != leader {
			return 0, false
		}
		leader = st.Leader
	}
	return leader, leader != 0
}

// ByID returns the running member with member ID id, or nil.
func (cl *Cluster) ByID(t testing.TB, id uint64) *Member {
	t.Helper()
	for _, m := range cl.Members {
		if m.Running() && m.ID(t) == id {
			return m
		}
	}
	return nil
}
