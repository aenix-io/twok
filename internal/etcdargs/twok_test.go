package etcdargs

import (
	"maps"
	"slices"
	"testing"
)

func TestSplitTwok(t *testing.T) {
	tests := []struct {
		name      string
		cmd       []string
		wantFlags TwokFlags
		wantRest  []string
	}{
		{
			name:      "no twok flags",
			cmd:       []string{"etcd", "--name=a", "--data-dir", "/d"},
			wantFlags: TwokFlags{},
			wantRest:  []string{"etcd", "--name=a", "--data-dir", "/d"},
		},
		{
			name:      "every form",
			cmd:       []string{"etcd", "--twok-arbiter=s3://b/k", "--name=a", "--twok-claim-after", "30s", "-twok-mode=off", "--twok-commit-tail", "--data-dir", "/d", "--twok-dry-run"},
			wantFlags: TwokFlags{"arbiter": "s3://b/k", "claim-after": "30s", "mode": "off", "commit-tail": "true", "dry-run": "true"},
			wantRest:  []string{"etcd", "--name=a", "--data-dir", "/d"},
		},
		{
			name:      "last wins",
			cmd:       []string{"etcd", "--twok-mode=enforce", "--twok-mode", "off"},
			wantFlags: TwokFlags{"mode": "off"},
			wantRest:  []string{"etcd"},
		},
		{
			name:      "value with an equals sign",
			cmd:       []string{"etcd", "--twok-arbiter=http://h/x?a=b"},
			wantFlags: TwokFlags{"arbiter": "http://h/x?a=b"},
			wantRest:  []string{"etcd"},
		},
		{
			name:      "an etcd flag value that looks like a twok flag stays",
			cmd:       []string{"etcd", "--initial-cluster-token=--twok-x", "--initial-cluster-token", "--twok-y", "--client-cert-auth", "--twok-mode=off"},
			wantFlags: TwokFlags{"mode": "off"},
			wantRest:  []string{"etcd", "--initial-cluster-token=--twok-x", "--initial-cluster-token", "--twok-y", "--client-cert-auth"},
		},
		{
			name:      "after the terminator",
			cmd:       []string{"etcd", "--twok-mode=off", "--", "--twok-mode=enforce"},
			wantFlags: TwokFlags{"mode": "off"},
			wantRest:  []string{"etcd", "--", "--twok-mode=enforce"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flags, rest, err := SplitTwok(tt.cmd)
			if err != nil {
				t.Fatalf("SplitTwok() error = %v", err)
			}
			if !maps.Equal(flags, tt.wantFlags) {
				t.Errorf("SplitTwok() flags = %v, want %v", flags, tt.wantFlags)
			}
			if !slices.Equal(rest, tt.wantRest) {
				t.Errorf("SplitTwok() rest = %q, want %q", rest, tt.wantRest)
			}
		})
	}
}

func TestSplitTwokRejects(t *testing.T) {
	for _, arg := range []string{"--twok-", "--twok-=x"} {
		if _, _, err := SplitTwok([]string{"etcd", arg}); err == nil {
			t.Errorf("SplitTwok(%q) succeeded, want an error", arg)
		}
	}
}

func TestLookup(t *testing.T) {
	env := map[string]string{"TWOK_CLAIM_AFTER": "40s", "TWOK_ARBITER": "file:///tmp/a", "TWOK_EMPTY": ""}
	lookupEnv := func(name string) (string, bool) {
		v, ok := env[name]
		return v, ok
	}
	flags := TwokFlags{"arbiter": "s3://b/k"}
	tests := []struct {
		name   string
		want   string
		wantOK bool
	}{
		{name: "arbiter", want: "s3://b/k", wantOK: true},
		{name: "claim-after", want: "40s", wantOK: true},
		{name: "empty", want: "", wantOK: true},
		{name: "mode", want: "", wantOK: false},
	}
	for _, tt := range tests {
		got, ok := flags.Lookup(tt.name, lookupEnv)
		if got != tt.want || ok != tt.wantOK {
			t.Errorf("Lookup(%q) = %q, %v, want %q, %v", tt.name, got, ok, tt.want, tt.wantOK)
		}
	}
	if _, ok := flags.Lookup("claim-after", nil); ok {
		t.Error("Lookup() without an environment found a value only the environment has")
	}
	if got := EnvName("follower-extra-delay"); got != "TWOK_FOLLOWER_EXTRA_DELAY" {
		t.Errorf("EnvName() = %q", got)
	}
}
