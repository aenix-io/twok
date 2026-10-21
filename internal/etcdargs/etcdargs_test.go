package etcdargs

import (
	"os"
	"os/exec"
	"path/filepath"
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
			name:  "force-new-cluster is reported, not refused",
			cmd:   []string{"etcd", "--force-new-cluster"},
			check: func(a Args) bool { return a.ForceNewCluster },
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
			cmd:  []string{"etcd", "--config-file=/etc/etcd.yaml", "--auto-tls", "--peer-auto-tls=true"},
			want: []string{"--config-file", "--auto-tls", "--peer-auto-tls"},
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

func TestFromCmdline(t *testing.T) {
	cmd := operatorArgs(t)
	// /proc/<pid>/cmdline ends every argument with a NUL byte.
	data := []byte(strings.Join(cmd, "\x00") + "\x00")
	a, err := FromCmdline(data)
	if err != nil {
		t.Fatalf("FromCmdline() error = %v", err)
	}
	if !slices.Equal(a.Raw, cmd) || a.Name != "etcd-f5sw9" {
		t.Errorf("FromCmdline() = %+v, want the operator's command line", a)
	}
	// An argument may be empty, and must stay an argument of its own.
	a, err = FromCmdline([]byte("etcd\x00--name\x00\x00--data-dir=/d\x00"))
	if err != nil {
		t.Fatalf("FromCmdline() with an empty argument error = %v", err)
	}
	if a.Name != "" || a.DataDir != "/d" {
		t.Errorf("FromCmdline() = name %q, data dir %q, want an empty name and /d", a.Name, a.DataDir)
	}
	for _, empty := range [][]byte{nil, {0}} {
		if _, err := FromCmdline(empty); err == nil {
			t.Errorf("FromCmdline(%q) succeeded, want an error", empty)
		}
	}
}

func TestFromProc(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "42"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "42", "cmdline"), []byte("/usr/local/bin/etcd\x00--name=a\x00"), 0o600); err != nil {
		t.Fatal(err)
	}
	a, err := FromProc(root, 42)
	if err != nil {
		t.Fatalf("FromProc() error = %v", err)
	}
	if a.Binary != "/usr/local/bin/etcd" || a.Name != "a" || a.DataDir != "a.etcd" {
		t.Errorf("FromProc() = %+v", a)
	}
	if _, err := FromProc(root, 43); err == nil {
		t.Error("FromProc() of a missing process succeeded")
	}
}
