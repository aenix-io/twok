package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aenix-io/twok/internal/arbiter"
	"github.com/aenix-io/twok/internal/config"
)

type result struct {
	code   int
	stdout string
	stderr string
}

func runWith(t *testing.T, environ map[string]string, args ...string) result {
	t.Helper()
	var stdout, stderr bytes.Buffer
	e := env{stdout: &stdout, stderr: &stderr, lookupEnv: func(k string) (string, bool) {
		v, ok := environ[k]
		return v, ok
	}}
	code := run(context.Background(), args, e)
	return result{code, stdout.String(), stderr.String()}
}

func TestCommands(t *testing.T) {
	for _, tc := range []struct {
		args       []string
		code       int
		wantStdout string
		wantStderr string
	}{
		{nil, exitInvalid, "", "Usage: twok"},
		{[]string{"version"}, exitOK, "twok dev", ""},
		{[]string{"help"}, exitOK, "Usage: twok", ""},
		{[]string{"frobnicate"}, exitInvalid, "", `unknown command "frobnicate"`},
		{[]string{"arbiter"}, exitInvalid, "", "Usage: twok"},
		{[]string{"arbiter", "delete"}, exitInvalid, "", `unknown command "arbiter delete"`},
		{[]string{"arbiter", "get"}, exitInvalid, "", "--arbiter is required"},
		{[]string{"arbiter", "get", "--help"}, exitOK, "", "-arbiter"},
		{[]string{"arbiter", "get", "--arbiter=memory://x", "extra"}, exitInvalid, "", `unexpected argument "extra"`},
		{[]string{"arbiter", "get", "--arbiter=gopher://x"}, exitFailed, "", `unknown scheme "gopher"`},
		{[]string{"arbiter", "claim", "--arbiter=memory://x"}, exitInvalid, "", "--survivor is required"},
		{[]string{"arbiter", "claim", "--arbiter=memory://x", "--survivor=a"}, exitInvalid, "", "--accept-data-loss"},
		{[]string{"arbiter", "serve"}, exitInvalid, "", "--dir is required"},
		{[]string{"agent"}, exitInvalid, "", "--arbiter is required"},
		{[]string{"agent", "--arbiter=memory://x"}, exitInvalid, "", "--data-dir is required"},
		{[]string{"agent", "--mode=on"}, exitInvalid, "", "must be one of off, dry-run, enforce"},
		{[]string{"agent", "--bogus"}, exitInvalid, "", "flag provided but not defined"},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			r := runWith(t, nil, tc.args...)
			if r.code != tc.code || !strings.Contains(r.stdout, tc.wantStdout) || !strings.Contains(r.stderr, tc.wantStderr) {
				t.Errorf("code %d, stdout %q, stderr %q; want %d, %q, %q", r.code, r.stdout, r.stderr, tc.code, tc.wantStdout, tc.wantStderr)
			}
		})
	}
}

func TestArbiterGetAndClaim(t *testing.T) {
	url := "file://" + filepath.Join(t.TempDir(), "record.json")
	r := runWith(t, nil, "arbiter", "get", "--arbiter", url)
	if r.code != exitOK || !strings.Contains(r.stdout, `"found": false`) {
		t.Fatalf("get on an empty arbiter: %+v", r)
	}
	r = runWith(t, nil, "arbiter", "claim", "--arbiter", url, "--survivor", "a", "--accept-data-loss")
	if r.code != exitOK {
		t.Fatalf("claim: %+v", r)
	}
	// The arbiter URL comes from the environment as well.
	r = runWith(t, map[string]string{"TWOK_ARBITER": url}, "arbiter", "claim", "--survivor", "b", "--accept-data-loss")
	if r.code != exitOK {
		t.Fatalf("second claim: %+v", r)
	}
	r = runWith(t, nil, "arbiter", "get", "--arbiter", url)
	var out recordOutput
	if err := json.Unmarshal([]byte(r.stdout), &out); err != nil {
		t.Fatalf("get output %q: %v", r.stdout, err)
	}
	if !out.Found || out.Version == "" || out.Record.Generation != 2 || out.Record.Survivor != "b" || out.Record.Reason != "manual" {
		t.Errorf("record after two manual claims = %+v", out.Record)
	}
}

func TestArbiterServe(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"arbiter", "serve", "--dir", t.TempDir(), "--listen", addr},
			env{stdout: io.Discard, stderr: io.Discard, lookupEnv: func(string) (string, bool) { return "", false }})
	}()
	url := "http://" + addr + "/twok/test"
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound {
				t.Errorf("GET of a missing record: %s", resp.Status)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	r := runWith(t, nil, "arbiter", "claim", "--arbiter", url, "--survivor", "a", "--accept-data-loss")
	if r.code != exitOK {
		t.Errorf("claim against the served arbiter: %+v", r)
	}
	cancel()
	if code := <-done; code != exitOK {
		t.Errorf("serve exited with %d", code)
	}
}

func TestAgentSettings(t *testing.T) {
	dir := t.TempDir()
	for name, value := range map[string]string{"mode": "dry-run", "claim-after": "30s"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var stderr bytes.Buffer
	e := env{stdout: io.Discard, stderr: &stderr, lookupEnv: func(k string) (string, bool) {
		v, ok := map[string]string{"TWOK_ARBITER": "memory://x", "TWOK_CLAIM_DELAY": "7s"}[k]
		return v, ok
	}}
	opts, code, ok := parseAgentOptions([]string{
		"--data-dir=/var/lib/etcd", "--config-dir=" + dir, "--mode=enforce",
		"--peer-endpoints=https://etcd.ns.svc:2379, https://10.0.0.9:2379", "--peer-endpoint=https://old:2379",
	}, e)
	if !ok {
		t.Fatalf("parse: %d, %s", code, stderr.String())
	}
	if want := []string{"https://etcd.ns.svc:2379", "https://10.0.0.9:2379", "https://old:2379"}; !slices.Equal(opts.peerEndpoints, want) {
		t.Errorf("peer endpoints = %v, want %v", opts.peerEndpoints, want)
	}
	if opts.arbiter != "memory://x" || opts.member.endpoint != "https://127.0.0.1:2379" {
		t.Errorf("options = %+v", opts)
	}
	cfg, err := opts.load()
	if err != nil {
		t.Fatal(err)
	}
	want := config.Default()
	want.Mode = config.ModeEnforce     // the flag wins over the file
	want.ClaimAfter = 30 * time.Second // from the file
	want.ClaimDelay = 7 * time.Second  // from the environment
	if cfg != want {
		t.Errorf("load() = %+v, want %+v", cfg, want)
	}

	// A missing ConfigMap directory is mode off, not an error.
	opts, _, _ = parseAgentOptions([]string{"--arbiter=memory://x", "--data-dir=/d", "--config-dir=" + filepath.Join(dir, "absent")}, e)
	if cfg, err := opts.load(); err != nil || cfg.Mode != config.ModeOff {
		t.Errorf("load() without the ConfigMap = %+v, %v", cfg, err)
	}

	// An invalid file is an error, which keeps the previous configuration.
	if err := os.WriteFile(filepath.Join(dir, "mode"), []byte("on"), 0o600); err != nil {
		t.Fatal(err)
	}
	opts, _, _ = parseAgentOptions([]string{"--data-dir=/d", "--config-dir=" + dir}, e)
	if _, err := opts.load(); err == nil {
		t.Error("load() accepted mode=on")
	}
}

func TestAgentRetriesWhatCannotStart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var stderr bytes.Buffer
	code := run(ctx, []string{"agent", "--arbiter=gopher://x", "--data-dir=/d"},
		env{stdout: io.Discard, stderr: &stderr, lookupEnv: func(string) (string, bool) { return "", false }})
	if code != exitOK || !strings.Contains(stderr.String(), "agent stopped, restarting") {
		t.Errorf("code %d, stderr %s", code, stderr.String())
	}
}

func TestClaimKeepsTheClusterID(t *testing.T) {
	url := "file://" + filepath.Join(t.TempDir(), "record.json")
	a, err := arbiter.Open(url)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.CompareAndSwap(context.Background(), arbiter.Zero, arbiter.Record{Generation: 4, ClusterID: "c1"}); err != nil {
		t.Fatal(err)
	}
	r := runWith(t, nil, "arbiter", "claim", "--arbiter", url, "--survivor", "a", "--accept-data-loss")
	if r.code != exitOK || !strings.Contains(r.stdout, `"generation": 5`) || !strings.Contains(r.stdout, `"clusterID": "c1"`) {
		t.Errorf("claim: %+v", r)
	}
}
