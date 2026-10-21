package main

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/aenix-io/twok/internal/etcdtest"
)

// TestStatusAgainstRealEtcd runs twok status against a real pair and
// checks what it prints.
func TestStatusAgainstRealEtcd(t *testing.T) {
	cl := etcdtest.StartPair(t)
	m := cl.Members[0]
	url := "file://" + filepath.Join(t.TempDir(), "record.json")
	r := runWith(t, nil, "status",
		"--endpoint", m.ClientURL, "--cert-file", m.TLS.CertFile, "--key-file", m.TLS.KeyFile, "--trusted-ca-file", m.TLS.CAFile,
		"--data-dir", m.DataDir, "--arbiter", url)
	if r.code != exitOK {
		t.Fatalf("status: %+v", r)
	}
	var out struct {
		Role       string `json:"role"`
		Generation *uint64
		Local      struct {
			MemberID string `json:"memberID"`
			Leader   string `json:"leader"`
		} `json:"local"`
		Members struct {
			Voters []struct {
				Name string `json:"name"`
			} `json:"voters"`
		} `json:"members"`
		Arbiter struct {
			Found bool `json:"found"`
		} `json:"arbiter"`
		WAL struct {
			MemberID string   `json:"memberID"`
			Members  []string `json:"members"`
		} `json:"wal"`
	}
	if err := json.Unmarshal([]byte(r.stdout), &out); err != nil {
		t.Fatalf("%v:\n%s", err, r.stdout)
	}
	if out.Role != "pair" || out.Generation == nil || *out.Generation != 0 || out.Local.Leader == "" || len(out.Members.Voters) != 2 ||
		out.Arbiter.Found || out.WAL.MemberID != out.Local.MemberID || len(out.WAL.Members) != 2 {
		t.Errorf("status:\n%s", r.stdout)
	}
}
