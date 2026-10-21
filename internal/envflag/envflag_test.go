package envflag

import (
	"flag"
	"io"
	"strings"
	"testing"
	"time"
)

func TestApply(t *testing.T) {
	env := map[string]string{
		"TWOK_ARBITER":     "file:///tmp/arbiter.json",
		"TWOK_CLAIM_AFTER": "40s",
		"TWOK_NAME":        "",
	}
	lookupEnv := func(name string) (string, bool) {
		v, ok := env[name]
		return v, ok
	}
	fs := flag.NewFlagSet("twok", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	arbiter := fs.String("arbiter", "", "")
	claimAfter := fs.Duration("claim-after", 20*time.Second, "")
	pollInterval := fs.Duration("poll-interval", time.Second, "")
	name := fs.String("name", "default", "")
	if err := fs.Parse([]string{"--arbiter", "s3://bucket/key"}); err != nil {
		t.Fatal(err)
	}
	if err := Apply(fs, lookupEnv); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if *arbiter != "s3://bucket/key" {
		t.Errorf("arbiter = %q, want the flag to win over the environment", *arbiter)
	}
	if *claimAfter != 40*time.Second {
		t.Errorf("claim-after = %v, want 40s from the environment", *claimAfter)
	}
	if *pollInterval != time.Second {
		t.Errorf("poll-interval = %v, want the default", *pollInterval)
	}
	if *name != "" {
		t.Errorf("name = %q, want the empty value of a set variable", *name)
	}
}

func TestApplyInvalid(t *testing.T) {
	fs := flag.NewFlagSet("twok", flag.ContinueOnError)
	fs.Duration("claim-after", 20*time.Second, "")
	err := Apply(fs, func(string) (string, bool) { return "soon", true })
	if err == nil || !strings.Contains(err.Error(), "TWOK_CLAIM_AFTER") {
		t.Errorf("Apply() error = %v, want an error naming TWOK_CLAIM_AFTER", err)
	}
}

func TestName(t *testing.T) {
	if got := Name("follower-extra-delay"); got != "TWOK_FOLLOWER_EXTRA_DELAY" {
		t.Errorf("Name() = %q", got)
	}
}
