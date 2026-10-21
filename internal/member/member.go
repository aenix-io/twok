// Package member talks to one etcd member over its client API: the local
// member, which twok supervises, or the peer, which it probes and asks to add
// and promote a learner.
//
// A Client is bound to exactly one endpoint. etcd's client balances over
// every endpoint it is given and may also learn more from the member list;
// twok needs the opposite, an answer from one particular member, because the
// question is what that member thinks. The member list sync of clientv3 is
// therefore left off.
//
// Everything twok asks while the cluster has no leader is served by the
// member locally: Status always is, and Members reads the member list
// serializably. Both work on a member that is cut off from its peer.
package member

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	// DefaultDialTimeout is used when Config.DialTimeout is zero.
	DefaultDialTimeout = 5 * time.Second
	// ReachableTimeout bounds the probe in Reachable.
	ReachableTimeout = time.Second
)

var (
	// ErrLearner is returned by Members when the member is a learner. etcd
	// serves the member list only on voters; a learner can still answer
	// Status, which reports IsLearner.
	ErrLearner = errors.New("the member is a learner and does not serve the member list")
	// ErrLearnerNotReady is returned by Promote while the learner's log is
	// too far behind the leader's. The caller retries later.
	ErrLearnerNotReady = errors.New("the learner is not in sync with the leader yet")
)

// errNotSupportedForLearner is what etcd answers on a learner for RPCs that
// only voters serve. clientv3 converts gRPC errors to rpctypes.EtcdError
// values, which compare equal by code and description.
var errNotSupportedForLearner = rpctypes.Error(rpctypes.ErrGRPCNotSupportedForLearner)

// Config says how to reach one member.
type Config struct {
	// Endpoint is the member's client URL, http:// or https://.
	Endpoint string
	// CertFile and KeyFile are the client certificate, optional. CAFile
	// verifies the member and is required for https://. All three are read
	// again on every TLS handshake, so rotated files are picked up.
	CertFile string
	KeyFile  string
	CAFile   string
	// DialTimeout bounds each connection attempt; zero means
	// DefaultDialTimeout.
	DialTimeout time.Duration
	// Logger receives clientv3's own logs; nil discards them.
	Logger *zap.Logger
}

// Client is a connection to one member. It is safe for concurrent use. The
// connection is established lazily and re-established by gRPC when the
// member restarts, so a Client can be created before etcd is up.
type Client struct {
	endpoint string
	cli      *clientv3.Client
}

// Status is what a member reports about itself and its view of the
// cluster. It is served by the member alone, without a quorum.
type Status struct {
	ClusterID uint64
	MemberID  uint64
	// Leader is the member ID of the leader, 0 when the member knows of no
	// leader.
	Leader uint64
	// Term is the member's raft term.
	Term uint64
	// CommitIndex is the member's raft commit index, as far as it knows.
	CommitIndex uint64
	// AppliedIndex is the last index applied to the member's store.
	AppliedIndex uint64
	IsLearner    bool
	// Errors are the member's own complaints, such as "etcdserver: no
	// leader" (ErrNoLeader) and active alarms.
	Errors []string
}

// ErrNoLeader is the entry etcd puts in Status.Errors when it knows of no
// leader.
var ErrNoLeader = rpctypes.ErrorDesc(rpctypes.ErrGRPCNoLeader)

// HasLeader reports whether the member knows of a leader.
func (s Status) HasLeader() bool {
	return s.Leader != 0
}

// Member is one entry of the member list.
type Member struct {
	ID uint64
	// Name is empty for a member that was added but has not started yet.
	Name       string
	PeerURLs   []string
	ClientURLs []string
}

// Members is the member list, split by role.
type Members struct {
	Voters   []Member
	Learners []Member
}

// New returns a client for cfg.Endpoint. It does not connect yet.
func New(cfg Config) (*Client, error) {
	if cfg.Endpoint == "" {
		return nil, errors.New("member endpoint is empty")
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = DefaultDialTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	ccfg := clientv3.Config{
		Endpoints:   []string{cfg.Endpoint},
		DialTimeout: cfg.DialTimeout,
		Logger:      cfg.Logger,
		// AutoSyncInterval stays zero: no member list sync, no other
		// endpoints, ever.
	}
	if cfg.CAFile != "" || cfg.CertFile != "" {
		u, err := url.Parse(cfg.Endpoint)
		if err != nil {
			return nil, fmt.Errorf("member endpoint %q: %w", cfg.Endpoint, err)
		}
		tlsCfg, err := tlsConfig(cfg.CertFile, cfg.KeyFile, cfg.CAFile, u.Hostname())
		if err != nil {
			return nil, fmt.Errorf("member %s: %w", cfg.Endpoint, err)
		}
		ccfg.TLS = tlsCfg
	}
	cli, err := clientv3.New(ccfg)
	if err != nil {
		return nil, fmt.Errorf("member %s: %w", cfg.Endpoint, err)
	}
	return &Client{endpoint: cfg.Endpoint, cli: cli}, nil
}

// Endpoint is the member's client URL.
func (c *Client) Endpoint() string {
	return c.endpoint
}

// Close closes the connection.
func (c *Client) Close() error {
	return c.cli.Close()
}

// Status asks the member for its status. A member without a leader answers
// normally, with Leader 0 and ErrNoLeader in Errors.
func (c *Client) Status(ctx context.Context) (Status, error) {
	// The raw stub reuses the client's connection. Maintenance.Status of
	// clientv3 dials a new connection on every call.
	resp, err := pb.NewMaintenanceClient(c.cli.ActiveConnection()).Status(ctx, &pb.StatusRequest{})
	if err != nil {
		return Status{}, fmt.Errorf("status of %s: %w", c.endpoint, clientv3.ContextError(ctx, err))
	}
	return Status{
		ClusterID:    resp.GetHeader().GetClusterId(),
		MemberID:     resp.GetHeader().GetMemberId(),
		Leader:       resp.GetLeader(),
		Term:         resp.GetRaftTerm(),
		CommitIndex:  resp.GetRaftIndex(),
		AppliedIndex: resp.GetRaftAppliedIndex(),
		IsLearner:    resp.GetIsLearner(),
		Errors:       resp.GetErrors(),
	}, nil
}

// Reachable reports whether the member's client API answers within
// ReachableTimeout. It says nothing about the member's health beyond that.
func (c *Client) Reachable(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, ReachableTimeout)
	defer cancel()
	_, err := c.Status(ctx)
	return err
}

// Members returns the member list as this member knows it. The list is read
// serializably, so it works without a leader, and may lag a membership
// change that the member has not applied yet. On a learner it returns
// ErrLearner.
func (c *Client) Members(ctx context.Context) (Members, error) {
	resp, err := c.cli.MemberList(ctx, clientv3.WithSerializable())
	if err != nil {
		if errors.Is(err, errNotSupportedForLearner) {
			return Members{}, fmt.Errorf("member list of %s: %w", c.endpoint, ErrLearner)
		}
		return Members{}, fmt.Errorf("member list of %s: %w", c.endpoint, err)
	}
	var ms Members
	for _, m := range resp.Members {
		entry := Member{
			ID:         m.GetID(),
			Name:       m.GetName(),
			PeerURLs:   m.GetPeerURLs(),
			ClientURLs: m.GetClientURLs(),
		}
		if m.GetIsLearner() {
			ms.Learners = append(ms.Learners, entry)
		} else {
			ms.Voters = append(ms.Voters, entry)
		}
	}
	return ms, nil
}

// AddLearner adds a learner with peerURLs and returns its entry, with the ID
// the cluster assigned. The new member has no name until it starts.
func (c *Client) AddLearner(ctx context.Context, peerURLs []string) (Member, error) {
	resp, err := c.cli.MemberAddAsLearner(ctx, peerURLs)
	if err != nil {
		return Member{}, fmt.Errorf("add learner %v through %s: %w", peerURLs, c.endpoint, err)
	}
	m := resp.Member
	return Member{ID: m.GetID(), Name: m.GetName(), PeerURLs: m.GetPeerURLs(), ClientURLs: m.GetClientURLs()}, nil
}

// Promote makes learner id a voter. It returns ErrLearnerNotReady while the
// learner is behind: etcd promotes only a learner whose log has reached 90%
// of the leader's.
func (c *Client) Promote(ctx context.Context, id uint64) error {
	if _, err := c.cli.MemberPromote(ctx, id); err != nil {
		if errors.Is(err, rpctypes.ErrMemberLearnerNotReady) {
			return fmt.Errorf("promote %x through %s: %w", id, c.endpoint, ErrLearnerNotReady)
		}
		return fmt.Errorf("promote %x through %s: %w", id, c.endpoint, err)
	}
	return nil
}

// Remove removes member id from the cluster.
func (c *Client) Remove(ctx context.Context, id uint64) error {
	if _, err := c.cli.MemberRemove(ctx, id); err != nil {
		return fmt.Errorf("remove %x through %s: %w", id, c.endpoint, err)
	}
	return nil
}
