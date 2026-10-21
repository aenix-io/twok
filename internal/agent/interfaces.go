package agent

import (
	"context"
	"errors"

	"github.com/aenix-io/twok/internal/engine"
	"github.com/aenix-io/twok/internal/etcdargs"
)

// GenerationKey is where the survivor writes its generation, in the
// cluster's own keyspace, so that it reaches the other member only through
// the Raft log together with the membership changes of the takeover.
const GenerationKey = "/twok/generation"

var (
	// ErrLearner is returned by Client.Members on a learner, which etcd
	// does not serve the member list.
	ErrLearner = errors.New("the member is a learner and does not serve the member list")
	// ErrLearnerNotReady is returned by Client.Promote while the learner is
	// too far behind the leader.
	ErrLearnerNotReady = errors.New("the learner is not in sync with the leader yet")
	// ErrWALLocked is returned by a WAL operation that found the WAL locked:
	// etcd was started again before the edit.
	ErrWALLocked = errors.New("the WAL is locked by a running etcd")
	// ErrNoProcess is returned by Process.Args when no etcd process uses
	// the data directory.
	ErrNoProcess = errors.New("no etcd process uses the data directory")
)

// Client is one etcd member's client API, bound to one endpoint. Every
// call is answered by that member alone: Status always, Members and
// Generation serializably, so they work without a leader.
type Client interface {
	Endpoint() string
	Status(ctx context.Context) (engine.Status, error)
	// Members returns ErrLearner on a learner.
	Members(ctx context.Context) (engine.Members, error)
	// Generation reads GenerationKey serializably; 0 when it is absent.
	Generation(ctx context.Context) (uint64, error)
	// PutGeneration writes GenerationKey; it needs a leader.
	PutGeneration(ctx context.Context, generation uint64) error
	// AddLearner adds a learner with peerURLs and returns its entry.
	AddLearner(ctx context.Context, peerURLs []string) (engine.Member, error)
	// Promote returns ErrLearnerNotReady while the learner is behind.
	Promote(ctx context.Context, id uint64) error
	Close() error
}

// Dialer returns a client for another member's endpoint.
type Dialer func(endpoint string) (Client, error)

// WAL edits a stopped member's write-ahead log with etcd's own WAL package.
// Each operation takes the WAL's file lock first and returns ErrWALLocked
// when it is held, which means etcd is running again.
type WAL interface {
	// Standalone appends the configuration changes that remove every
	// member in remove, and sets the commit index at the last entry, so
	// that self restarts as a cluster of one with nothing truncated.
	Standalone(dataDir string, self uint64, remove []uint64) error
	// Reidentify makes the log belong to member newID, keeping the data.
	Reidentify(dataDir string, newID uint64) error
}

// Process is the etcd process that uses the data directory.
type Process interface {
	// Args reads its command line. It returns ErrNoProcess when no such
	// process runs, which is also what twok sees without a shared PID
	// namespace.
	Args() (etcdargs.Args, error)
	// Stop sends SIGTERM and returns once the process is gone. It returns
	// nil when no process runs.
	Stop(ctx context.Context) error
	// Resume lets etcd start again after an edit. On Kubernetes and Talos
	// the platform restarts etcd by itself and Resume does nothing.
	Resume(ctx context.Context) error
}
