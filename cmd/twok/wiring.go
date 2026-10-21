package main

import (
	"go.uber.org/zap"

	"github.com/aenix-io/twok/internal/agent"
	"github.com/aenix-io/twok/internal/engine"
)

// dialMember connects to one member's client API.
func dialMember(endpoint string, tls tlsFiles) (agent.Client, error) {
	return agent.NewEtcdClient(agent.EtcdConfig{Endpoint: endpoint, CertFile: tls.certFile, KeyFile: tls.keyFile, CAFile: tls.caFile})
}

// newWAL returns the WAL operations, logging through etcd's logger as the
// WAL package does.
func newWAL() (agent.WAL, error) {
	lg, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}
	return agent.WALOps{Logger: lg.With(zap.String("logger", "twok"), zap.String("component", "walops"))}, nil
}

// walOutput is the WAL as twok status prints it.
type walOutput struct {
	MemberID      string   `json:"memberID"`
	ClusterID     string   `json:"clusterID"`
	SnapshotIndex uint64   `json:"snapshotIndex"`
	LastIndex     uint64   `json:"lastIndex"`
	Term          uint64   `json:"term"`
	Vote          string   `json:"vote,omitempty"`
	Commit        uint64   `json:"commit"`
	Tail          uint64   `json:"tail"`
	Members       []string `json:"members"`
}

// inspectWAL describes the member's WAL for twok status. It takes no lock
// and works while etcd runs.
func inspectWAL(dataDir string) (any, error) {
	st, err := agent.WALOps{Logger: zap.NewNop()}.Inspect(dataDir)
	if err != nil {
		return nil, err
	}
	out := walOutput{
		MemberID: engine.Hex(st.MemberID), ClusterID: engine.Hex(st.ClusterID), SnapshotIndex: st.SnapshotIndex,
		LastIndex: st.LastIndex, Term: st.Term, Commit: st.Commit, Tail: st.Tail(), Members: []string{},
	}
	if st.Vote != 0 {
		out.Vote = engine.Hex(st.Vote)
	}
	for _, id := range st.Members {
		out.Members = append(out.Members, engine.Hex(id))
	}
	return out, nil
}
