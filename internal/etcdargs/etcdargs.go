// Package etcdargs reads the etcd command line twok supervises and writes
// the variants twok needs for a takeover and a rejoin.
//
// twok is given the etcd command line as it would run without twok: the
// etcd binary first, then flags in the forms etcd's flag parser accepts,
// --flag=value, --flag value and -flag, with the last of repeated flags
// winning. twok learns the member's name, data directory, URLs and TLS
// files from it, and changes nothing in it except --initial-cluster,
// --initial-cluster-state and --force-new-cluster.
//
// Configuration that does not come from the command line is out of scope.
// etcd also reads every flag from an ETCD_<FLAG> environment variable and
// from a --config-file; twok reads neither, so a member configured that way
// would be seen with etcd's defaults. --config-file is refused because twok
// cannot rewrite it. Environment variables are not checked, because
// container runtimes set ETCD_* variables of their own; keep the etcd
// configuration in flags.
package etcdargs

import (
	"errors"
	"fmt"
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

// Flags twok rewrites.
const (
	flagInitialCluster      = "initial-cluster"
	flagInitialClusterState = "initial-cluster-state"
	flagForceNewCluster     = "force-new-cluster"
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

	// flags are the parsed flags in order, with the arguments they came
	// from.
	flags []flag
}

// flag is one flag occurrence and the range of Raw it occupies.
type flag struct {
	name  string
	value string
	// start and end index Raw: the flag is Raw[start:end], one argument or
	// two.
	start, end int
}

// Parse reads an etcd command line, binary first. It returns an error for
// a command line etcd would reject as malformed, and for configuration twok
// cannot work with, listing every such flag.
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
		f := flag{name: name, start: i, end: i + 1}
		switch {
		case hasValue:
			f.value = value
		case boolFlags[name]:
			f.value = "true"
		case i+1 < len(cmd):
			i++
			f.value, f.end = cmd[i], i+1
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
	a.InitialCluster = str(flagInitialCluster, defaultInitialCluster)
	if a.InitialCluster == defaultInitialCluster && a.Name != DefaultName {
		members := make([]string, 0, len(a.InitialAdvertisePeerURLs))
		for _, u := range a.InitialAdvertisePeerURLs {
			members = append(members, a.Name+"="+u)
		}
		a.InitialCluster = strings.Join(members, ",")
	}
	a.InitialClusterState = str(flagInitialClusterState, DefaultInitialClusterState)
	a.ForceNewCluster = boolean(flagForceNewCluster)
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
		problems = append(problems, "--config-file: twok cannot read or rewrite an etcd configuration file, pass the flags instead")
	}
	if boolean("auto-tls") {
		problems = append(problems, "--auto-tls: twok cannot authenticate to self-signed certificates it never sees, pass --cert-file and --key-file")
	}
	if boolean("peer-auto-tls") {
		problems = append(problems, "--peer-auto-tls: a member rebuilt by twok could not prove its peer identity, pass --peer-cert-file and --peer-key-file")
	}
	if a.ForceNewCluster {
		problems = append(problems, "--force-new-cluster: twok adds it for one start during a takeover; kept on every start it would remove a joining member again")
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

// Override changes one flag in a command line built by With.
type Override func(*overrides)

type overrides struct {
	initialCluster      *string
	initialClusterState *string
	forceNewCluster     *bool
}

// InitialCluster sets --initial-cluster.
func InitialCluster(v string) Override {
	return func(o *overrides) { o.initialCluster = &v }
}

// InitialClusterState sets --initial-cluster-state, "new" or "existing".
func InitialClusterState(v string) Override {
	return func(o *overrides) { o.initialClusterState = &v }
}

// ForceNewCluster adds --force-new-cluster when on and removes every
// occurrence of it when off.
//
// etcd v3.5.34, v3.6.15 and v3.7.2 have no --force-new-cluster-bump-amount;
// the revision bump exists only in etcdutl snapshot restore. twok therefore
// adds no such flag.
func ForceNewCluster(on bool) Override {
	return func(o *overrides) { o.forceNewCluster = &on }
}

// With returns a new command line, binary first, with the overrides
// applied and every other argument kept as it was, in its order. An
// overridden flag takes the place of its first occurrence, and its other
// occurrences are dropped, which is what etcd's last-wins rule makes of
// them anyway. A flag that was absent is appended at the end.
func (a Args) With(opts ...Override) []string {
	var o overrides
	for _, opt := range opts {
		opt(&o)
	}
	replacement := map[string]*string{}
	if o.initialCluster != nil {
		replacement[flagInitialCluster] = o.initialCluster
	}
	if o.initialClusterState != nil {
		replacement[flagInitialClusterState] = o.initialClusterState
	}
	var force *string
	if o.forceNewCluster != nil {
		if *o.forceNewCluster {
			s := ""
			force = &s
		}
		replacement[flagForceNewCluster] = force
	}

	out := []string{a.Binary}
	emitted := map[string]bool{}
	emit := func(name string) {
		emitted[name] = true
		v := replacement[name]
		switch {
		case v == nil:
			// Removed.
		case name == flagForceNewCluster:
			out = append(out, "--"+name)
		default:
			out = append(out, "--"+name+"="+*v)
		}
	}
	next := 1
	for _, f := range a.flags {
		out = append(out, a.Raw[next:f.start]...)
		next = f.end
		if _, overridden := replacement[f.name]; !overridden {
			out = append(out, a.Raw[f.start:f.end]...)
			continue
		}
		if !emitted[f.name] {
			emit(f.name)
		}
	}
	out = append(out, a.Raw[next:]...)
	for _, name := range []string{flagInitialCluster, flagInitialClusterState, flagForceNewCluster} {
		if _, overridden := replacement[name]; overridden && !emitted[name] {
			emit(name)
		}
	}
	return out
}

// Member is one entry of --initial-cluster.
type Member struct {
	Name     string
	PeerURLs []string
}

// ParseInitialCluster reads an --initial-cluster value. A member with
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

// FormatInitialCluster writes members as an --initial-cluster value.
func FormatInitialCluster(members []Member) string {
	var entries []string
	for _, m := range members {
		for _, u := range m.PeerURLs {
			entries = append(entries, m.Name+"="+u)
		}
	}
	return strings.Join(entries, ",")
}
