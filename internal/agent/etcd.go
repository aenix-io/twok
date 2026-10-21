package agent

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/aenix-io/twok/internal/engine"
	"github.com/aenix-io/twok/internal/member"
	"github.com/aenix-io/twok/internal/walops"
)

// EtcdConfig says how to reach one member's client API.
type EtcdConfig struct {
	Endpoint string
	CertFile string
	KeyFile  string
	CAFile   string
}

// etcdClient is Client over the member package, with a second connection
// for the generation key: the member package covers what twok asks about
// membership, and the key is the only data twok reads or writes.
type etcdClient struct {
	m  *member.Client
	kv *clientv3.Client
}

// NewEtcdClient returns a Client for one member. It does not connect yet.
func NewEtcdClient(cfg EtcdConfig) (Client, error) {
	m, err := member.New(member.Config{Endpoint: cfg.Endpoint, CertFile: cfg.CertFile, KeyFile: cfg.KeyFile, CAFile: cfg.CAFile})
	if err != nil {
		return nil, err
	}
	kcfg := clientv3.Config{
		Endpoints:   []string{cfg.Endpoint},
		DialTimeout: member.DefaultDialTimeout,
		Logger:      zap.NewNop(),
	}
	if cfg.CAFile != "" || cfg.CertFile != "" {
		info := transport.TLSInfo{CertFile: cfg.CertFile, KeyFile: cfg.KeyFile, TrustedCAFile: cfg.CAFile}
		tlsCfg, err := info.ClientConfig()
		if err != nil {
			_ = m.Close()
			return nil, fmt.Errorf("member %s: %w", cfg.Endpoint, err)
		}
		kcfg.TLS = tlsCfg
	}
	kv, err := clientv3.New(kcfg)
	if err != nil {
		_ = m.Close()
		return nil, fmt.Errorf("member %s: %w", cfg.Endpoint, err)
	}
	return &etcdClient{m: m, kv: kv}, nil
}

func (c *etcdClient) Endpoint() string { return c.m.Endpoint() }

func (c *etcdClient) Status(ctx context.Context) (engine.Status, error) {
	st, err := c.m.Status(ctx)
	if err != nil {
		return engine.Status{}, err
	}
	return engine.Status{
		ClusterID: st.ClusterID, MemberID: st.MemberID, Leader: st.Leader, Term: st.Term,
		CommitIndex: st.CommitIndex, AppliedIndex: st.AppliedIndex, IsLearner: st.IsLearner,
	}, nil
}

func (c *etcdClient) Members(ctx context.Context) (engine.Members, error) {
	ms, err := c.m.Members(ctx)
	if errors.Is(err, member.ErrLearner) {
		return engine.Members{}, fmt.Errorf("%w: %w", ErrLearner, err)
	}
	if err != nil {
		return engine.Members{}, err
	}
	return engine.Members{Voters: toEngine(ms.Voters), Learners: toEngine(ms.Learners)}, nil
}

func toEngine(ms []member.Member) []engine.Member {
	out := make([]engine.Member, 0, len(ms))
	for _, m := range ms {
		out = append(out, engine.Member{ID: m.ID, Name: m.Name, PeerURLs: m.PeerURLs, ClientURLs: m.ClientURLs})
	}
	return out
}

// Generation reads the key serializably: the member answers from its own
// data, with or without a leader, also as a learner.
func (c *etcdClient) Generation(ctx context.Context) (uint64, error) {
	resp, err := c.kv.Get(ctx, GenerationKey, clientv3.WithSerializable())
	if err != nil {
		return 0, fmt.Errorf("read %s from %s: %w", GenerationKey, c.Endpoint(), err)
	}
	if len(resp.Kvs) == 0 {
		return 0, nil
	}
	g, err := strconv.ParseUint(string(resp.Kvs[0].Value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s on %s: %w", GenerationKey, c.Endpoint(), err)
	}
	return g, nil
}

func (c *etcdClient) PutGeneration(ctx context.Context, generation uint64) error {
	if _, err := c.kv.Put(ctx, GenerationKey, strconv.FormatUint(generation, 10)); err != nil {
		return fmt.Errorf("write %s to %s: %w", GenerationKey, c.Endpoint(), err)
	}
	return nil
}

func (c *etcdClient) AddLearner(ctx context.Context, peerURLs []string) (engine.Member, error) {
	m, err := c.m.AddLearner(ctx, peerURLs)
	if err != nil {
		return engine.Member{}, err
	}
	return engine.Member{ID: m.ID, Name: m.Name, PeerURLs: m.PeerURLs, ClientURLs: m.ClientURLs}, nil
}

func (c *etcdClient) Promote(ctx context.Context, id uint64) error {
	err := c.m.Promote(ctx, id)
	if errors.Is(err, member.ErrLearnerNotReady) {
		return fmt.Errorf("%w: %w", ErrLearnerNotReady, err)
	}
	return err
}

func (c *etcdClient) Close() error {
	return errors.Join(c.m.Close(), c.kv.Close())
}

// WALOps is WAL over the walops package. The WAL is in the data
// directory's member/wal, where etcd keeps it without --wal-dir.
type WALOps struct {
	Logger *zap.Logger
}

func (w WALOps) Standalone(dataDir string, self uint64, remove []uint64) error {
	_, err := walops.Standalone(w.Logger, walops.WALDir(dataDir), self, remove)
	return lockErr(err)
}

// Reidentify is a no-op when the WAL already has newID, which is how an
// attempt that was interrupted after the edit is resumed: walops refuses
// to run while the previous WAL of an earlier run is kept.
func (w WALOps) Reidentify(dataDir string, newID uint64) error {
	dir := walops.WALDir(dataDir)
	if st, err := walops.Inspect(w.Logger, dir); err == nil && st.MemberID == newID {
		return nil
	}
	_, err := walops.Reidentify(w.Logger, dir, newID)
	return lockErr(err)
}

// Cleanup removes the previous WAL a rejoin kept.
func (w WALOps) Cleanup(dataDir string) error {
	return walops.Cleanup(walops.WALDir(dataDir))
}

// Inspect describes the WAL for twok status.
func (w WALOps) Inspect(dataDir string) (walops.State, error) {
	return walops.Inspect(w.Logger, walops.WALDir(dataDir))
}

func lockErr(err error) error {
	if errors.Is(err, walops.ErrLocked) {
		return fmt.Errorf("%w: %w", ErrWALLocked, err)
	}
	return err
}
