package etcdtest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TLSFiles names a certificate, its key and the CA that verifies the other
// side, as files on disk, the way etcd and twok take them.
type TLSFiles struct {
	CertFile string
	KeyFile  string
	CAFile   string
}

// CA issues certificates for test members.
type CA struct {
	// CertFile is the CA certificate in PEM.
	CertFile string
	dir      string
	cert     *x509.Certificate
	key      *ecdsa.PrivateKey
}

// NewCA creates a CA that writes its files to dir.
func NewCA(t testing.TB, dir string) *CA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "etcdtest-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	ca := &CA{CertFile: filepath.Join(dir, "ca.crt"), dir: dir, cert: cert, key: key}
	writePEM(t, ca.CertFile, "CERTIFICATE", der)
	return ca
}

// Issue creates a certificate for member name, usable as etcd server, peer
// and client certificate. It carries both the ServerAuth and the ClientAuth
// extended key usage, as the certificates etcd-operator issues do, because
// etcd presents the same peer certificate when it dials and when it accepts.
// The SANs cover 127.0.0.1, ::1, localhost and name.
func (c *CA) Issue(t testing.TB, name string) TLSFiles {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost", name},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &key.PublicKey, c.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	files := TLSFiles{
		CertFile: filepath.Join(c.dir, name+".crt"),
		KeyFile:  filepath.Join(c.dir, name+".key"),
		CAFile:   c.CertFile,
	}
	writePEM(t, files.CertFile, "CERTIFICATE", der)
	writePEM(t, files.KeyFile, "EC PRIVATE KEY", keyDER)
	return files
}

func writePEM(t testing.TB, path, typ string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}
