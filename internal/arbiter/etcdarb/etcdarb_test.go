package etcdarb

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/aenix-io/twok/internal/arbiter"
	"github.com/aenix-io/twok/internal/arbiter/castest"
)

// binaryEnv names the etcd binary the tests run. There is no fake of etcd:
// the backend's correctness is the server's transaction semantics, and
// only the server has them.
const binaryEnv = "TEST_ETCD_BINARY"

func TestArbiter(t *testing.T) {
	addr := startEtcd(t, nil)
	a := open(t, "etcd://"+addr+"/twok/test")
	castest.Exercise(t, a)
}

func TestArbiterTLS(t *testing.T) {
	pki := newPKI(t)
	addr := startEtcd(t, pki)
	t.Setenv(CAEnv, pki.ca)
	t.Setenv(CertEnv, pki.clientCert)
	t.Setenv(KeyEnv, pki.clientKey)
	a := open(t, "etcd+tls://"+addr+"/twok/test")
	castest.Exercise(t, a)
}

func TestTLSWithoutClientCertificateIsRefused(t *testing.T) {
	pki := newPKI(t)
	addr := startEtcd(t, pki)
	t.Setenv(CAEnv, pki.ca)
	a := open(t, "etcd+tls://"+addr+"/twok/test")
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if _, _, err := a.Read(ctx); err == nil || errors.Is(err, arbiter.ErrNotFound) {
		t.Fatalf("Read without a client certificate: err = %v, want a TLS failure", err)
	}
}

// TestVerifyRefusesStoreThatIgnoresConditions runs the backend over a real
// etcd through a KV that drops the transaction's comparison, which is what
// a store that ignores conditions looks like from the backend.
func TestVerifyRefusesStoreThatIgnoresConditions(t *testing.T) {
	addr := startEtcd(t, nil)
	tests := []struct {
		name   string
		target pb.Compare_CompareTarget
	}{
		{name: "ignores the create condition", target: pb.Compare_CREATE},
		{name: "ignores the mod revision", target: pb.Compare_MOD},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := open(t, "etcd://"+addr+"/twok/"+strings.ReplaceAll(tt.name, " ", "-"))
			a.kv = ignoringKV{KV: a.kv, target: tt.target}
			castest.ExpectUnsupported(t, a)
		})
	}
}

func TestConflictNamesCurrentVersion(t *testing.T) {
	a := open(t, "etcd://"+startEtcd(t, nil)+"/twok/test")
	v, err := a.CompareAndSwap(t.Context(), arbiter.Zero, castest.Record(1))
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.CompareAndSwap(t.Context(), arbiter.Zero, castest.Record(2))
	if !errors.Is(err, arbiter.ErrConflict) || !strings.Contains(err.Error(), string(v)) {
		t.Fatalf("err = %v, want ErrConflict naming version %s", err, v)
	}
}

func TestCompareAndSwapRejectsMalformedVersion(t *testing.T) {
	// No server needed: the version is checked before anything is sent.
	a := open(t, "etcd://127.0.0.1:1/twok/test")
	for _, v := range []arbiter.Version{"abc", "-1", "0", `"etag"`} {
		if _, err := a.CompareAndSwap(t.Context(), v, castest.Record(1)); err == nil || errors.Is(err, arbiter.ErrConflict) {
			t.Errorf("CompareAndSwap from %q: err = %v, want a malformed-version error", v, err)
		}
	}
}

func TestParseURL(t *testing.T) {
	tests := []struct {
		raw        string
		endpoints  []string
		key        string
		errContain string
	}{
		{raw: "etcd://e1:2379/twok/prod", endpoints: []string{"http://e1:2379"}, key: "/twok/prod"},
		{raw: "etcd://e1,e2:2380,10.0.0.3:2379/k", endpoints: []string{"http://e1:2379", "http://e2:2380", "http://10.0.0.3:2379"}, key: "/k"},
		{raw: "etcd://e1,e2/k", endpoints: []string{"http://e1:2379", "http://e2:2379"}, key: "/k"},
		{raw: "etcd+tls://e1:2379,e2:2379/twok/prod", endpoints: []string{"https://e1:2379", "https://e2:2379"}, key: "/twok/prod"},
		{raw: "etcd://user:secret@e1/k", errContain: "credentials"},
		{raw: "etcd:///k", errContain: "no endpoints"},
		{raw: "etcd://e1:2379", errContain: "no key"},
		{raw: "etcd://e1:2379/", errContain: "no key"},
		{raw: "etcd://e1,,e2/k", errContain: "empty endpoint"},
		{raw: "etcd://e1:99999/k", errContain: "invalid port"},
		{raw: "etcd://e1:2379/k?timeout=1s", errContain: "no query"},
		{raw: "etcds://e1/k", errContain: "scheme"},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			u, err := url.Parse(tt.raw)
			if err != nil {
				t.Fatal(err)
			}
			endpoints, key, err := parseURL(u)
			if tt.errContain != "" {
				if err == nil || !strings.Contains(err.Error(), tt.errContain) {
					t.Fatalf("parseURL(%q) err = %v, want one containing %q", tt.raw, err, tt.errContain)
				}
				if strings.Contains(err.Error(), "secret") {
					t.Fatalf("error %q leaks the password", err)
				}
				return
			}
			if err != nil || !slices.Equal(endpoints, tt.endpoints) || key != tt.key {
				t.Fatalf("parseURL(%q) = %q, %q, %v; want %q, %q", tt.raw, endpoints, key, err, tt.endpoints, tt.key)
			}
		})
	}
}

func TestTLSSettings(t *testing.T) {
	tests := []struct {
		name       string
		scheme     string
		env        map[string]string
		errContain string
	}{
		{name: "plaintext", scheme: Scheme},
		{name: "plaintext with a CA set", scheme: Scheme, env: map[string]string{CAEnv: "/ca.pem"}, errContain: "etcd+tls://"},
		{name: "TLS with system roots", scheme: SchemeTLS},
		{name: "TLS with a certificate but no key", scheme: SchemeTLS, env: map[string]string{CertEnv: "/c.pem"}, errContain: "together"},
		{name: "TLS with a missing CA file", scheme: SchemeTLS, env: map[string]string{CAEnv: "/nonexistent/ca.pem"}, errContain: "TLS"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, k := range []string{CAEnv, CertEnv, KeyEnv} {
				t.Setenv(k, tt.env[k])
			}
			_, err := arbiter.Open(tt.scheme + "://127.0.0.1:1/k")
			if tt.errContain == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.errContain) {
				t.Fatalf("err = %v, want one containing %q", err, tt.errContain)
			}
		})
	}
}

func open(t *testing.T, raw string) *Arbiter {
	t.Helper()
	a, err := arbiter.Open(raw)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.(*Arbiter).Close() })
	return a.(*Arbiter)
}

// ignoringKV passes everything through to a real etcd but drops the
// comparisons of one target from transactions.
type ignoringKV struct {
	clientv3.KV
	target pb.Compare_CompareTarget
}

func (k ignoringKV) Txn(ctx context.Context) clientv3.Txn {
	return ignoringTxn{Txn: k.KV.Txn(ctx), target: k.target}
}

type ignoringTxn struct {
	clientv3.Txn
	target pb.Compare_CompareTarget
}

func (t ignoringTxn) If(cs ...clientv3.Cmp) clientv3.Txn {
	kept := slices.DeleteFunc(slices.Clone(cs), func(c clientv3.Cmp) bool { return c.Target == t.target })
	return ignoringTxn{Txn: t.Txn.If(kept...), target: t.target}
}

// pki holds the files of a test CA, a server certificate for 127.0.0.1 and
// a client certificate.
type pki struct {
	ca, serverCert, serverKey, clientCert, clientKey string
}

func newPKI(t *testing.T) *pki {
	t.Helper()
	dir := t.TempDir()
	caKey := newKey(t)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "twok test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	p := &pki{ca: writePEM(t, dir, "ca.pem", "CERTIFICATE", caDER)}
	issue := func(name string, serial int64, usage x509.ExtKeyUsage) (string, string) {
		key := newKey(t)
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: name},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{usage},
			IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		return writePEM(t, dir, name+".pem", "CERTIFICATE", der), writePEM(t, dir, name+"-key.pem", "EC PRIVATE KEY", keyDER)
	}
	p.serverCert, p.serverKey = issue("server", 2, x509.ExtKeyUsageServerAuth)
	p.clientCert, p.clientKey = issue("client", 3, x509.ExtKeyUsageClientAuth)
	return p
}

func newKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func writePEM(t *testing.T, dir, name, typ string, der []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// startEtcd starts a one-member etcd from TEST_ETCD_BINARY, with client
// TLS and client certificate authentication when p is set, and returns its
// client address. The test is skipped when the variable is not set.
func startEtcd(t *testing.T, p *pki) string {
	t.Helper()
	binary := os.Getenv(binaryEnv)
	if binary == "" {
		t.Skipf("%s is not set; set it to an etcd v3.6 binary to run the etcd arbiter tests", binaryEnv)
	}
	dir := t.TempDir()
	clientAddr, peerAddr := freeAddr(t), freeAddr(t)
	clientScheme := "http://"
	if p != nil {
		clientScheme = "https://"
	}
	args := []string{
		"--name", "arbiter",
		"--data-dir", filepath.Join(dir, "data"),
		"--listen-client-urls", clientScheme + clientAddr,
		"--advertise-client-urls", clientScheme + clientAddr,
		"--listen-peer-urls", "http://" + peerAddr,
		"--initial-advertise-peer-urls", "http://" + peerAddr,
		"--initial-cluster", "arbiter=http://" + peerAddr,
		"--log-level", "error",
	}
	if p != nil {
		args = append(args,
			"--cert-file", p.serverCert, "--key-file", p.serverKey,
			"--trusted-ca-file", p.ca, "--client-cert-auth")
	}
	logFile, err := os.Create(filepath.Join(dir, "etcd.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, args...)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = logFile.Close()
		if t.Failed() {
			if log, err := os.ReadFile(logFile.Name()); err == nil {
				t.Logf("etcd log:\n%s", log)
			}
		}
	})
	waitReady(t, clientAddr)
	return clientAddr
}

// waitReady waits until the member accepts connections. A TCP connect is
// enough: etcd opens the client listener after the member has started,
// and the first request of a test waits for the leader on its own.
func waitReady(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("etcd did not listen on %s in time", addr)
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}
