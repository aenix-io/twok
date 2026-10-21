// Package etcdarb keeps the arbiter record under a key of another etcd
// cluster, one that does not depend on the pair it arbitrates for.
//
// Two URL schemes select the transport, so that whether the connection is
// encrypted is visible in the URL rather than inferred from the
// environment:
//
//	etcd://host1:2379,host2:2379/twok/prod      plaintext
//	etcd+tls://host1:2379,host2:2379/twok/prod  TLS
//
// The host part is a comma-separated list of endpoints; the port defaults
// to 2379, but Go's URL parser rejects a list whose last endpoint has no
// port while an earlier one has, so give every port or none. IPv6
// addresses are not supported in the list. The path, leading slash
// included, is the key.
//
// With etcd+tls the server is verified against TWOK_ETCD_ARBITER_CA, or
// the system roots when it is not set, and TWOK_ETCD_ARBITER_CERT and
// TWOK_ETCD_ARBITER_KEY give a client certificate. Setting any of them with etcd:// is an error
// rather than a silent upgrade or downgrade. etcd user names and passwords
// are not supported; use client certificates.
//
// The version is the key's mod revision as a decimal string. A key that
// does not exist has version Zero. CompareAndSwap is a single transaction:
//
//	If(ModRevision(key) = expected)   or, for Zero, If(CreateRevision(key) = 0)
//	Then(Put(key, record))
//	Else(Get(key))
//
// The client retries a transaction only when no connection was available
// (isSafeRetryMutableRPC in client/v3/retry.go), never once it may have
// reached the server, so an error other than a failed comparison leaves
// the outcome unknown, as the Arbiter contract allows.
package etcdarb

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/aenix-io/twok/internal/arbiter"
)

// The URL schemes this backend registers.
const (
	Scheme    = "etcd"
	SchemeTLS = "etcd+tls"
)

// The environment variables that hold the TLS files for etcd+tls.
const (
	CertEnv = "TWOK_ETCD_ARBITER_CERT"
	KeyEnv  = "TWOK_ETCD_ARBITER_KEY"
	CAEnv   = "TWOK_ETCD_ARBITER_CA"
)

const (
	defaultPort = "2379"
	dialTimeout = 5 * time.Second
	// timeout bounds one request when the caller's context has a later
	// deadline or none. A claim that hangs delays the takeover more than one
	// that fails and is retried.
	timeout = 10 * time.Second
)

func init() {
	arbiter.Register(Scheme, New)
	arbiter.Register(SchemeTLS, New)
}

// Arbiter is the etcd backend. It is safe for concurrent use.
type Arbiter struct {
	client    *clientv3.Client
	kv        clientv3.KV
	key       string
	endpoints []string
}

var _ arbiter.Arbiter = (*Arbiter)(nil)

// New builds the backend from an etcd:// or etcd+tls:// URL. It does not
// connect: the client dials on the first request, so that a member can
// start while the arbiter is unreachable, as decision 7 requires.
func New(u *url.URL) (arbiter.Arbiter, error) {
	endpoints, key, err := parseURL(u)
	if err != nil {
		return nil, err
	}
	tlsConfig, err := tlsFromEnv(u.Scheme == SchemeTLS)
	if err != nil {
		return nil, err
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:            endpoints,
		TLS:                  tlsConfig,
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    10 * time.Second,
		DialKeepAliveTimeout: 5 * time.Second,
		// The client's own log is noise next to twok's; every failure it
		// would log comes back as an error from the call that hit it.
		Logger: zap.NewNop(),
	})
	if err != nil {
		return nil, fmt.Errorf("etcd arbiter: %w", err)
	}
	return &Arbiter{client: client, kv: client.KV, key: key, endpoints: endpoints}, nil
}

func parseURL(u *url.URL) (endpoints []string, key string, err error) {
	const form = "etcd://host:2379,host2:2379/key/path or etcd+tls://..."
	if u.Scheme != Scheme && u.Scheme != SchemeTLS {
		return nil, "", fmt.Errorf("etcd arbiter: scheme %q, want %q or %q", u.Scheme, Scheme, SchemeTLS)
	}
	switch {
	case u.User != nil:
		return nil, "", fmt.Errorf("etcd arbiter: credentials in the URL are not accepted; use client certificates through %s and %s (URL form %s)", CertEnv, KeyEnv, form)
	case u.Opaque != "":
		return nil, "", fmt.Errorf("etcd arbiter: URL must be %s", form)
	case u.RawQuery != "" || u.Fragment != "":
		return nil, "", fmt.Errorf("etcd arbiter: URL must have no query or fragment (URL form %s)", form)
	case u.Host == "":
		return nil, "", fmt.Errorf("etcd arbiter: no endpoints in the URL (URL form %s)", form)
	case u.Path == "" || u.Path == "/":
		return nil, "", fmt.Errorf("etcd arbiter: no key in the URL (URL form %s)", form)
	}
	httpScheme := "http://"
	if u.Scheme == SchemeTLS {
		httpScheme = "https://"
	}
	for ep := range strings.SplitSeq(u.Host, ",") {
		if ep == "" {
			return nil, "", fmt.Errorf("etcd arbiter: empty endpoint in %q (URL form %s)", u.Host, form)
		}
		host, port := ep, defaultPort
		if h, p, err := net.SplitHostPort(ep); err == nil {
			host, port = h, p
		} else if strings.ContainsAny(ep, ":[]") {
			return nil, "", fmt.Errorf("etcd arbiter: endpoint %q is not host or host:port (URL form %s)", ep, form)
		}
		if host == "" {
			return nil, "", fmt.Errorf("etcd arbiter: endpoint %q has no host (URL form %s)", ep, form)
		}
		if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
			return nil, "", fmt.Errorf("etcd arbiter: endpoint %q has an invalid port (URL form %s)", ep, form)
		}
		endpoints = append(endpoints, httpScheme+net.JoinHostPort(host, port))
	}
	return endpoints, u.Path, nil
}

func tlsFromEnv(useTLS bool) (*tls.Config, error) {
	info := transport.TLSInfo{
		CertFile:      os.Getenv(CertEnv),
		KeyFile:       os.Getenv(KeyEnv),
		TrustedCAFile: os.Getenv(CAEnv),
	}
	if !useTLS {
		if !info.Empty() || info.TrustedCAFile != "" { // Empty looks at the certificate and key only.
			return nil, fmt.Errorf("etcd arbiter: %s, %s or %s is set but the URL scheme is %s://; use %s:// for TLS", CertEnv, KeyEnv, CAEnv, Scheme, SchemeTLS)
		}
		return nil, nil
	}
	if (info.CertFile == "") != (info.KeyFile == "") {
		return nil, fmt.Errorf("etcd arbiter: %s and %s must be set together", CertEnv, KeyEnv)
	}
	if info.Empty() && info.TrustedCAFile == "" {
		// Server verification against the system roots, no client
		// certificate.
		return &tls.Config{MinVersion: tls.VersionTLS12}, nil
	}
	cfg, err := info.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("etcd arbiter: TLS: %w", err)
	}
	return cfg, nil
}

// Close releases the client's connections.
func (a *Arbiter) Close() error {
	return a.client.Close()
}

// String names the endpoints and the key.
func (a *Arbiter) String() string {
	return fmt.Sprintf("etcd %s key %s", strings.Join(a.endpoints, ","), a.key)
}

// Read gets the key and returns its mod revision as the version.
func (a *Arbiter) Read(ctx context.Context) (arbiter.Record, arbiter.Version, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	resp, err := a.kv.Get(ctx, a.key)
	if err != nil {
		return arbiter.Record{}, arbiter.Zero, fmt.Errorf("etcd arbiter: get %s: %w", a.key, err)
	}
	if len(resp.Kvs) == 0 {
		return arbiter.Record{}, arbiter.Zero, arbiter.ErrNotFound
	}
	kv := resp.Kvs[0]
	var rec arbiter.Record
	if err := json.Unmarshal(kv.Value, &rec); err != nil {
		return arbiter.Record{}, arbiter.Zero, fmt.Errorf("etcd arbiter: %s is not a record: %w", a.key, err)
	}
	return rec, revisionVersion(kv.ModRevision), nil
}

// CompareAndSwap writes the record in a transaction conditioned on the
// key's mod revision, or on its absence for Zero.
func (a *Arbiter) CompareAndSwap(ctx context.Context, expected arbiter.Version, record arbiter.Record) (arbiter.Version, error) {
	var cmp clientv3.Cmp
	if expected == arbiter.Zero {
		cmp = clientv3.Compare(clientv3.CreateRevision(a.key), "=", 0)
	} else {
		rev, err := strconv.ParseInt(string(expected), 10, 64)
		if err != nil || rev <= 0 {
			return arbiter.Zero, fmt.Errorf("etcd arbiter: version %q is not a mod revision", expected)
		}
		cmp = clientv3.Compare(clientv3.ModRevision(a.key), "=", rev)
	}
	data, err := json.Marshal(record)
	if err != nil {
		return arbiter.Zero, fmt.Errorf("etcd arbiter: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	resp, err := a.kv.Txn(ctx).
		If(cmp).
		Then(clientv3.OpPut(a.key, string(data))).
		Else(clientv3.OpGet(a.key)).
		Commit()
	if err != nil {
		return arbiter.Zero, fmt.Errorf("etcd arbiter: transaction on %s: %w", a.key, err)
	}
	if !resp.Succeeded {
		current := arbiter.Zero
		if len(resp.Responses) == 1 {
			if r := resp.Responses[0].GetResponseRange(); r != nil && len(r.Kvs) == 1 {
				current = revisionVersion(r.Kvs[0].ModRevision)
			}
		}
		return arbiter.Zero, fmt.Errorf("%w: %s is at version %q, not %q", arbiter.ErrConflict, a.key, current, expected)
	}
	// The put is the transaction's only write, so the key's new mod
	// revision is the revision the transaction created.
	return revisionVersion(resp.Header.Revision), nil
}

// Verify runs the probe on the key with the probe suffix.
func (a *Arbiter) Verify(ctx context.Context) error {
	p := *a
	p.key += arbiter.ProbeSuffix
	return arbiter.VerifyProbe(ctx, &p)
}

func revisionVersion(rev int64) arbiter.Version {
	return arbiter.Version(strconv.FormatInt(rev, 10))
}
