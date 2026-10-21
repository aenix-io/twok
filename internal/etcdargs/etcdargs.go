// Package etcdargs reads the configuration of the local etcd member from its
// command line.
//
// twok runs next to etcd, as a sidecar container in a Pod that shares the
// process namespace or as a static Pod on the same host, and reads etcd's
// command line from /proc/<pid>/cmdline. That gives it the member's name,
// data directory, URLs and TLS files with no configuration of its own, and
// it never changes the command line.
//
// The command line is read as etcd's flag parser reads it: the etcd binary
// first, then flags as --flag=value, --flag value or -flag, with the last of
// repeated flags winning, and etcd's defaults for absent flags.
//
// Configuration that does not come from the command line is out of scope.
// etcd also reads every flag from an ETCD_<FLAG> environment variable and
// from a --config-file; twok reads neither, so a member configured that way
// would be seen with etcd's defaults. --config-file is refused for that
// reason. Environment variables are not checked, because container runtimes
// set ETCD_* variables of their own; keep the etcd configuration in flags.
package etcdargs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// Defaults of etcd's flags, from etcd --help of v3.5 and v3.6.
const (
	DefaultName                = "default"
	DefaultClientURL           = "http://localhost:2379"
	DefaultPeerURL             = "http://localhost:2380"
	DefaultInitialClusterState = "new"
	defaultInitialCluster      = DefaultName + "=" + DefaultPeerURL
)

// boolFlags are etcd's boolean flags, from etcd --help of v3.5, v3.6 and
// v3.7. Go's flag parser, which etcd uses, never takes the next argument as
// the value of a boolean flag, so "--client-cert-auth true" is the flag
// followed by a stray argument. The parser has to know them to split the
// command line as etcd does. A boolean flag of a later etcd that is missing
// here is still parsed correctly in the --flag=value form.
var boolFlags = map[string]bool{
	"auto-tls":                                       true,
	"client-cert-auth":                               true,
	"discovery-insecure-skip-tls-verify":             true,
	"discovery-insecure-transport":                   true,
	"enable-distributed-tracing":                     true,
	"enable-grpc-gateway":                            true,
	"enable-log-rotation":                            true,
	"enable-pprof":                                   true,
	"enable-v2":                                      true,
	"experimental-compact-hash-check-enabled":        true,
	"experimental-enable-distributed-tracing":        true,
	"experimental-enable-lease-checkpoint":           true,
	"experimental-enable-lease-checkpoint-persist":   true,
	"experimental-initial-corrupt-check":             true,
	"experimental-memory-mlock":                      true,
	"experimental-peer-skip-client-san-verification": true,
	"experimental-stop-grpc-service-on-defrag":       true,
	"experimental-txn-mode-write-with-shared-buffer": true,
	"force-new-cluster":                              true,
	"h":                                              true,
	"help":                                           true,
	"initial-election-tick-advance":                  true,
	"memory-mlock":                                   true,
	"peer-auto-tls":                                  true,
	"peer-client-cert-auth":                          true,
	"peer-skip-client-san-verification":              true,
	"pre-vote":                                       true,
	"socket-reuse-address":                           true,
	"socket-reuse-port":                              true,
	"strict-reconfig-check":                          true,
	"unsafe-no-fsync":                                true,
	"version":                                        true,
}

// TLS names the certificate files of one listener, the client or the peer
// one.
type TLS struct {
	CertFile       string
	KeyFile        string
	TrustedCAFile  string
	ClientCertAuth bool
}

// Args is the etcd command line and what twok reads from it. Every field
// holds the value etcd would use: the last occurrence of the flag, or
// etcd's default when the flag is absent.
type Args struct {
	// Binary is the etcd executable, the first element of the command line.
	Binary string
	Name   string
	// DataDir defaults to "<name>.etcd", relative to etcd's working
	// directory, as in etcd.
	DataDir                  string
	ListenClientURLs         []string
	AdvertiseClientURLs      []string
	ListenPeerURLs           []string
	InitialAdvertisePeerURLs []string
	// InitialCluster defaults as in etcd: "default=http://localhost:2380",
	// or "<name>=<initial advertise peer URLs>" when only the name is set.
	// etcd replaces a localhost advertise URL with the host's address when
	// it listens on 0.0.0.0; twok does not, set the advertise URLs.
	InitialCluster      string
	InitialClusterState string
	ForceNewCluster     bool
	ClientTLS           TLS
	PeerTLS             TLS
	// Raw is the command line as given, binary first.
	Raw []string

	// flags are the parsed flags in order.
	flags []flag
}

// flag is one flag occurrence.
type flag struct {
	name  string
	value string
}

// Parse reads an etcd command line, binary first. It returns an error for
// a command line etcd would reject as malformed, and for configuration twok
// cannot learn from it or cannot work with, listing every such flag.
func Parse(cmd []string) (Args, error) {
	if len(cmd) == 0 {
		return Args{}, errors.New("etcd command line is empty")
	}
	a := Args{Binary: cmd[0], Raw: slices.Clone(cmd)}
	for i := 1; i < len(cmd); i++ {
		arg := cmd[i]
		if arg == "--" {
			// etcd's parser stops here and refuses anything after it.
			if i+1 < len(cmd) {
				return Args{}, fmt.Errorf("%q is not a valid flag", cmd[i+1])
			}
			break
		}
		name, value, hasValue, err := split(arg)
		if err != nil {
			return Args{}, err
		}
		f := flag{name: name}
		switch {
		case hasValue:
			f.value = value
		case boolFlags[name]:
			f.value = "true"
		case i+1 < len(cmd):
			i++
			f.value = cmd[i]
		default:
			return Args{}, fmt.Errorf("flag needs an argument: %s", arg)
		}
		a.flags = append(a.flags, f)
	}
	if err := a.resolve(); err != nil {
		return Args{}, err
	}
	return a, nil
}

// split takes one argument apart as Go's flag parser does.
func split(arg string) (name, value string, hasValue bool, err error) {
	s, ok := strings.CutPrefix(arg, "-")
	if !ok || s == "" {
		return "", "", false, fmt.Errorf("%q is not a valid flag", arg)
	}
	s = strings.TrimPrefix(s, "-")
	if s == "" || s[0] == '-' || s[0] == '=' {
		return "", "", false, fmt.Errorf("bad flag syntax: %s", arg)
	}
	name, value, hasValue = strings.Cut(s, "=")
	return name, value, hasValue, nil
}

// lookup returns the value of the last occurrence of a flag.
func (a *Args) lookup(name string) (string, bool) {
	for i := len(a.flags) - 1; i >= 0; i-- {
		if a.flags[i].name == name {
			return a.flags[i].value, true
		}
	}
	return "", false
}

func (a *Args) resolve() error {
	var problems []string
	str := func(name, def string) string {
		if v, ok := a.lookup(name); ok {
			return v
		}
		return def
	}
	urls := func(name, def string) []string {
		var out []string
		for u := range strings.SplitSeq(str(name, def), ",") {
			if u = strings.TrimSpace(u); u != "" {
				out = append(out, u)
			}
		}
		return out
	}
	boolean := func(name string) bool {
		v, ok := a.lookup(name)
		if !ok {
			return false
		}
		b, err := strconv.ParseBool(v)
		if err != nil {
			problems = append(problems, fmt.Sprintf("--%s=%s: not a boolean", name, v))
		}
		return b
	}

	a.Name = str("name", DefaultName)
	a.DataDir = str("data-dir", "")
	if a.DataDir == "" {
		a.DataDir = a.Name + ".etcd"
	}
	a.ListenClientURLs = urls("listen-client-urls", DefaultClientURL)
	a.AdvertiseClientURLs = urls("advertise-client-urls", DefaultClientURL)
	a.ListenPeerURLs = urls("listen-peer-urls", DefaultPeerURL)
	a.InitialAdvertisePeerURLs = urls("initial-advertise-peer-urls", DefaultPeerURL)
	a.InitialCluster = str("initial-cluster", defaultInitialCluster)
	if a.InitialCluster == defaultInitialCluster && a.Name != DefaultName {
		members := make([]string, 0, len(a.InitialAdvertisePeerURLs))
		for _, u := range a.InitialAdvertisePeerURLs {
			members = append(members, a.Name+"="+u)
		}
		a.InitialCluster = strings.Join(members, ",")
	}
	a.InitialClusterState = str("initial-cluster-state", DefaultInitialClusterState)
	a.ForceNewCluster = boolean("force-new-cluster")
	a.ClientTLS = TLS{
		CertFile:       str("cert-file", ""),
		KeyFile:        str("key-file", ""),
		TrustedCAFile:  str("trusted-ca-file", ""),
		ClientCertAuth: boolean("client-cert-auth"),
	}
	a.PeerTLS = TLS{
		CertFile:       str("peer-cert-file", ""),
		KeyFile:        str("peer-key-file", ""),
		TrustedCAFile:  str("peer-trusted-ca-file", ""),
		ClientCertAuth: boolean("peer-client-cert-auth"),
	}

	if v := str("config-file", ""); v != "" {
		problems = append(problems, "--config-file: twok does not read etcd configuration files, pass the flags instead")
	}
	if boolean("auto-tls") {
		problems = append(problems, "--auto-tls: twok cannot trust self-signed certificates it never sees, pass --cert-file and --key-file")
	}
	if boolean("peer-auto-tls") {
		problems = append(problems, "--peer-auto-tls: twok cannot trust self-signed peer certificates it never sees, pass --peer-cert-file and --peer-key-file")
	}
	switch a.InitialClusterState {
	case "new", "existing":
	default:
		problems = append(problems, fmt.Sprintf("--initial-cluster-state=%s: want new or existing", a.InitialClusterState))
	}
	if len(problems) > 0 {
		return fmt.Errorf("etcd arguments twok cannot work with:\n  %s", strings.Join(problems, "\n  "))
	}
	return nil
}

// Member is one entry of --initial-cluster.
type Member struct {
	Name     string
	PeerURLs []string
}

// ParseInitialCluster reads an --initial-cluster value, the members etcd
// was bootstrapped or joined with. A member with
// several peer URLs is listed once per URL in etcd's format and comes back
// as one Member, in the order of first appearance.
func ParseInitialCluster(s string) ([]Member, error) {
	var members []Member
	index := map[string]int{}
	for entry := range strings.SplitSeq(s, ",") {
		name, u, ok := strings.Cut(strings.TrimSpace(entry), "=")
		if !ok || name == "" || u == "" {
			return nil, fmt.Errorf("initial cluster entry %q: want name=URL", entry)
		}
		i, seen := index[name]
		if !seen {
			i = len(members)
			index[name] = i
			members = append(members, Member{Name: name})
		}
		members[i].PeerURLs = append(members[i].PeerURLs, u)
	}
	return members, nil
}

// FromCmdline reads the content of /proc/<pid>/cmdline: the arguments,
// each ended by a NUL byte.
func FromCmdline(data []byte) (Args, error) {
	s := strings.TrimSuffix(string(data), "\x00")
	if s == "" {
		// A kernel thread, or a process that has already exited.
		return Args{}, errors.New("etcd command line is empty")
	}
	return Parse(strings.Split(s, "\x00"))
}

// FromProc reads the command line of the process pid under procRoot, which
// is "/proc" outside tests.
func FromProc(procRoot string, pid int) (Args, error) {
	path := filepath.Join(procRoot, strconv.Itoa(pid), "cmdline")
	data, err := os.ReadFile(path) // #nosec G304 -- a file under /proc
	if err != nil {
		return Args{}, err
	}
	a, err := FromCmdline(data)
	if err != nil {
		return Args{}, fmt.Errorf("%s: %w", path, err)
	}
	return a, nil
}
