package etcdargs

import (
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
)

func operatorArgs(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile("testdata/etcd-operator-member-args.txt")
	if err != nil {
		t.Fatal(err)
	}
	return strings.Fields(string(data))
}

func TestParseOperator(t *testing.T) {
	cmd := operatorArgs(t)
	a, err := Parse(cmd)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	want := Args{
		Binary:                   "etcd",
		Name:                     "etcd-f5sw9",
		DataDir:                  "/var/lib/etcd",
		ListenClientURLs:         []string{"https://0.0.0.0:2379"},
		AdvertiseClientURLs:      []string{"https://etcd-f5sw9.etcd.cozy-freedom.svc:2379"},
		ListenPeerURLs:           []string{"https://0.0.0.0:2380"},
		InitialAdvertisePeerURLs: []string{"https://etcd-f5sw9.etcd.cozy-freedom.svc:2380"},
		InitialCluster:           "etcd-f5sw9=https://etcd-f5sw9.etcd.cozy-freedom.svc:2380",
		InitialClusterState:      "new",
		ClientTLS: TLS{
			CertFile:       "/etc/etcd/tls/client/tls.crt",
			KeyFile:        "/etc/etcd/tls/client/tls.key",
			TrustedCAFile:  "/etc/etcd/tls/client/ca.crt",
			ClientCertAuth: true,
		},
		PeerTLS: TLS{
			CertFile:       "/etc/etcd/tls/peer/tls.crt",
			KeyFile:        "/etc/etcd/tls/peer/tls.key",
			TrustedCAFile:  "/etc/etcd/tls/peer/ca.crt",
			ClientCertAuth: true,
		},
		Raw: cmd,
	}
	expectArgs(t, a, want)
	// Nothing changes without overrides.
	if got := a.With(); !slices.Equal(got, cmd) {
		t.Errorf("With() = %q, want the command line unchanged", got)
	}
}

func TestWithOperator(t *testing.T) {
	cmd := operatorArgs(t)
	a, err := Parse(cmd)
	if err != nil {
		t.Fatal(err)
	}
	pair := "etcd-f5sw9=https://etcd-f5sw9.etcd.cozy-freedom.svc:2380,etcd-x2k4p=https://etcd-x2k4p.etcd.cozy-freedom.svc:2380"
	got := a.With(InitialCluster(pair), InitialClusterState("existing"))
	want := slices.Clone(cmd)
	for i, arg := range want {
		switch {
		case strings.HasPrefix(arg, "--initial-cluster="):
			want[i] = "--initial-cluster=" + pair
		case strings.HasPrefix(arg, "--initial-cluster-state="):
			want[i] = "--initial-cluster-state=existing"
		}
	}
	if !slices.Equal(got, want) {
		t.Errorf("With() =\n%q\nwant\n%q", got, want)
	}

	got = a.With(ForceNewCluster(true))
	want = append(slices.Clone(cmd), "--force-new-cluster")
	if !slices.Equal(got, want) {
		t.Errorf("With(ForceNewCluster(true)) =\n%q\nwant\n%q", got, want)
	}
}

func TestParseDefaults(t *testing.T) {
	tests := []struct {
		name string
		cmd  []string
		want Args
	}{
		{
			name: "no flags",
			cmd:  []string{"/usr/local/bin/etcd"},
			want: Args{
				Binary:                   "/usr/local/bin/etcd",
				Name:                     "default",
				DataDir:                  "default.etcd",
				ListenClientURLs:         []string{"http://localhost:2379"},
				AdvertiseClientURLs:      []string{"http://localhost:2379"},
				ListenPeerURLs:           []string{"http://localhost:2380"},
				InitialAdvertisePeerURLs: []string{"http://localhost:2380"},
				InitialCluster:           "default=http://localhost:2380",
				InitialClusterState:      "new",
			},
		},
		{
			name: "only a name",
			cmd:  []string{"etcd", "--name", "a"},
			want: Args{
				Binary:                   "etcd",
				Name:                     "a",
				DataDir:                  "a.etcd",
				ListenClientURLs:         []string{"http://localhost:2379"},
				AdvertiseClientURLs:      []string{"http://localhost:2379"},
				ListenPeerURLs:           []string{"http://localhost:2380"},
				InitialAdvertisePeerURLs: []string{"http://localhost:2380"},
				InitialCluster:           "a=http://localhost:2380",
				InitialClusterState:      "new",
			},
		},
		{
			name: "a name and several advertise peer URLs",
			cmd:  []string{"etcd", "-name=a", "--initial-advertise-peer-urls", "https://10.0.0.1:2380,https://a.example:2380", "--data-dir="},
			want: Args{
				Binary:                   "etcd",
				Name:                     "a",
				DataDir:                  "a.etcd",
				ListenClientURLs:         []string{"http://localhost:2379"},
				AdvertiseClientURLs:      []string{"http://localhost:2379"},
				ListenPeerURLs:           []string{"http://localhost:2380"},
				InitialAdvertisePeerURLs: []string{"https://10.0.0.1:2380", "https://a.example:2380"},
				InitialCluster:           "a=https://10.0.0.1:2380,a=https://a.example:2380",
				InitialClusterState:      "new",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, err := Parse(tt.cmd)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			tt.want.Raw = tt.cmd
			expectArgs(t, a, tt.want)
		})
	}
}

func TestParseForms(t *testing.T) {
	tests := []struct {
		name  string
		cmd   []string
		check func(Args) bool
	}{
		{
			name:  "separate value",
			cmd:   []string{"etcd", "--name", "a", "--data-dir", "/d"},
			check: func(a Args) bool { return a.Name == "a" && a.DataDir == "/d" },
		},
		{
			name:  "single dash",
			cmd:   []string{"etcd", "-name", "a", "-data-dir=/d"},
			check: func(a Args) bool { return a.Name == "a" && a.DataDir == "/d" },
		},
		{
			name: "last wins",
			cmd:  []string{"etcd", "--name=a", "--name", "b", "--listen-client-urls=http://1.1.1.1:2379", "--listen-client-urls=http://2.2.2.2:2379"},
			check: func(a Args) bool {
				return a.Name == "b" && slices.Equal(a.ListenClientURLs, []string{"http://2.2.2.2:2379"})
			},
		},
		{
			name:  "boolean without value does not take the next argument",
			cmd:   []string{"etcd", "--client-cert-auth", "--name", "a"},
			check: func(a Args) bool { return a.ClientTLS.ClientCertAuth && a.Name == "a" },
		},
		{
			name:  "boolean set to false",
			cmd:   []string{"etcd", "--peer-client-cert-auth=false", "--auto-tls=false", "--force-new-cluster=false"},
			check: func(a Args) bool { return !a.PeerTLS.ClientCertAuth && !a.ForceNewCluster },
		},
		{
			name:  "value that starts with a dash",
			cmd:   []string{"etcd", "--initial-cluster-token", "-x-", "--name=a"},
			check: func(a Args) bool { return a.Name == "a" },
		},
		{
			name:  "value with an equals sign",
			cmd:   []string{"etcd", "--initial-cluster", "a=http://a:2380,b=http://b:2380"},
			check: func(a Args) bool { return a.InitialCluster == "a=http://a:2380,b=http://b:2380" },
		},
		{
			name:  "terminator at the end",
			cmd:   []string{"etcd", "--name=a", "--"},
			check: func(a Args) bool { return a.Name == "a" },
		},
		{
			name:  "empty config file is no config file",
			cmd:   []string{"etcd", "--config-file="},
			check: func(Args) bool { return true },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, err := Parse(tt.cmd)
			if err != nil {
				t.Fatalf("Parse(%q) error = %v", tt.cmd, err)
			}
			if !tt.check(a) {
				t.Errorf("Parse(%q) = %+v", tt.cmd, a)
			}
		})
	}
}

func TestParseRejects(t *testing.T) {
	tests := []struct {
		name string
		cmd  []string
		want []string
	}{
		{name: "empty", cmd: nil, want: []string{"empty"}},
		{name: "positional argument", cmd: []string{"etcd", "--name=a", "extra"}, want: []string{`"extra" is not a valid flag`}},
		{name: "argument after the terminator", cmd: []string{"etcd", "--", "--name=a"}, want: []string{`"--name=a" is not a valid flag`}},
		{name: "missing value", cmd: []string{"etcd", "--data-dir"}, want: []string{"needs an argument"}},
		{name: "three dashes", cmd: []string{"etcd", "---name=a"}, want: []string{"bad flag syntax"}},
		{name: "bad boolean", cmd: []string{"etcd", "--client-cert-auth=yes"}, want: []string{"--client-cert-auth=yes: not a boolean"}},
		{name: "bad cluster state", cmd: []string{"etcd", "--initial-cluster-state=old"}, want: []string{"want new or existing"}},
		{
			name: "every unsupported flag is listed",
			cmd:  []string{"etcd", "--config-file=/etc/etcd.yaml", "--auto-tls", "--peer-auto-tls=true", "--force-new-cluster"},
			want: []string{"--config-file", "--auto-tls", "--peer-auto-tls", "--force-new-cluster"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.cmd)
			if err == nil {
				t.Fatalf("Parse(%q) succeeded, want an error", tt.cmd)
			}
			for _, w := range tt.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("Parse(%q) error = %q, want it to contain %q", tt.cmd, err, w)
				}
			}
		})
	}
}

func TestWith(t *testing.T) {
	tests := []struct {
		name string
		cmd  []string
		opts []Override
		want []string
	}{
		{
			name: "replace in place, separate value form",
			cmd:  []string{"etcd", "--name", "a", "--initial-cluster", "a=http://a:2380", "--initial-cluster-state", "new", "--data-dir", "/d"},
			opts: []Override{InitialCluster("a=http://a:2380,b=http://b:2380"), InitialClusterState("existing")},
			want: []string{"etcd", "--name", "a", "--initial-cluster=a=http://a:2380,b=http://b:2380", "--initial-cluster-state=existing", "--data-dir", "/d"},
		},
		{
			name: "repeated flag replaced once, at its first place",
			cmd:  []string{"etcd", "--initial-cluster=x=http://x:2380", "--name=a", "-initial-cluster", "a=http://a:2380"},
			opts: []Override{InitialCluster("a=http://a:2380")},
			want: []string{"etcd", "--initial-cluster=a=http://a:2380", "--name=a"},
		},
		{
			name: "absent flags appended",
			cmd:  []string{"etcd", "--name=a"},
			opts: []Override{ForceNewCluster(true), InitialClusterState("existing"), InitialCluster("a=http://a:2380")},
			want: []string{"etcd", "--name=a", "--initial-cluster=a=http://a:2380", "--initial-cluster-state=existing", "--force-new-cluster"},
		},
		{
			name: "force-new-cluster removed in every form",
			cmd:  []string{"etcd", "--force-new-cluster=false", "--name=a", "-force-new-cluster=false"},
			opts: []Override{ForceNewCluster(false)},
			want: []string{"etcd", "--name=a"},
		},
		{
			name: "force-new-cluster added at its place",
			cmd:  []string{"etcd", "--force-new-cluster=false", "--name=a"},
			opts: []Override{ForceNewCluster(true)},
			want: []string{"etcd", "--force-new-cluster", "--name=a"},
		},
		{
			name: "unrelated flags and terminator kept",
			cmd:  []string{"etcd", "--client-cert-auth", "--snapshot-count", "10000", "--"},
			opts: []Override{InitialClusterState("existing")},
			want: []string{"etcd", "--client-cert-auth", "--snapshot-count", "10000", "--", "--initial-cluster-state=existing"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, err := Parse(tt.cmd)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if got := a.With(tt.opts...); !slices.Equal(got, tt.want) {
				t.Errorf("With() =\n%q\nwant\n%q", got, tt.want)
			}
			// With never changes the parsed command line.
			if !slices.Equal(a.Raw, tt.cmd) {
				t.Errorf("Raw = %q after With(), want %q", a.Raw, tt.cmd)
			}
		})
	}
}

func TestInitialCluster(t *testing.T) {
	s := "a=https://10.0.0.1:2380,b=https://10.0.0.2:2380,a=https://a.example:2380"
	members, err := ParseInitialCluster(s)
	if err != nil {
		t.Fatalf("ParseInitialCluster() error = %v", err)
	}
	want := []Member{
		{Name: "a", PeerURLs: []string{"https://10.0.0.1:2380", "https://a.example:2380"}},
		{Name: "b", PeerURLs: []string{"https://10.0.0.2:2380"}},
	}
	if !reflect.DeepEqual(members, want) {
		t.Errorf("ParseInitialCluster() = %+v, want %+v", members, want)
	}
	if got, want := FormatInitialCluster(members), "a=https://10.0.0.1:2380,a=https://a.example:2380,b=https://10.0.0.2:2380"; got != want {
		t.Errorf("FormatInitialCluster() = %q, want %q", got, want)
	}
	for _, bad := range []string{"", "a", "=http://a:2380", "a="} {
		if _, err := ParseInitialCluster(bad); err == nil {
			t.Errorf("ParseInitialCluster(%q) succeeded, want an error", bad)
		}
	}
}

// TestBoolFlagsMatchEtcd checks the list of boolean flags against the etcd
// binary the tests run with: a boolean flag missing from the list would
// make the parser take the next argument as its value.
func TestBoolFlagsMatchEtcd(t *testing.T) {
	binary := os.Getenv("TEST_ETCD_BINARY")
	if binary == "" {
		t.Skip("TEST_ETCD_BINARY is not set")
	}
	out, err := exec.Command(binary, "--help").CombinedOutput()
	if err != nil && len(out) == 0 {
		t.Fatalf("%s --help: %v", binary, err)
	}
	re := regexp.MustCompile(`(?m)^  --([a-z0-9-]+) '(true|false)'$`)
	matches := re.FindAllStringSubmatch(string(out), -1)
	if len(matches) == 0 {
		t.Fatalf("no boolean flags found in %s --help", binary)
	}
	for _, m := range matches {
		if !boolFlags[m[1]] {
			t.Errorf("--%s is a boolean flag of %s and missing from boolFlags", m[1], binary)
		}
	}
}

func expectArgs(t *testing.T, got, want Args) {
	t.Helper()
	got.flags = nil
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Parse() =\n%+v\nwant\n%+v", got, want)
	}
}
