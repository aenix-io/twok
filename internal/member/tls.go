package member

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
)

// tlsConfig returns a client TLS configuration that reads the certificate,
// the key and the CA from disk on every handshake. etcd-operator and
// cert-manager rotate these files in place, and a supervisor that runs for
// months must pick up the new files without a restart.
//
// Go's tls.Config has no per-handshake hook for the root CAs, so the
// standard verification is turned off and VerifyConnection does the same
// verification against a freshly read CA pool. Nothing is skipped: the chain
// and the server name are checked as crypto/tls would. The server name is
// the host of the client's one endpoint; it has to be passed in, because
// crypto/tls leaves ConnectionState.ServerName empty for IP addresses.
func tlsConfig(certFile, keyFile, caFile, serverName string) (*tls.Config, error) {
	if caFile == "" {
		return nil, errors.New("TLS needs a trusted CA file to verify the member with")
	}
	if (certFile == "") != (keyFile == "") {
		return nil, errors.New("TLS needs both a certificate and a key file, or neither")
	}
	// Fail early on unreadable files, instead of on the first handshake.
	if _, err := loadPool(caFile); err != nil {
		return nil, err
	}
	if serverName == "" {
		return nil, errors.New("TLS needs a server name to verify the member's certificate against")
	}
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: serverName,
		// Verified in VerifyConnection against a CA read at handshake time.
		InsecureSkipVerify: true, //nolint:gosec // see VerifyConnection
		VerifyConnection: func(cs tls.ConnectionState) error {
			return verify(cs, caFile, serverName)
		},
	}
	if certFile != "" {
		if _, err := tls.LoadX509KeyPair(certFile, keyFile); err != nil {
			return nil, fmt.Errorf("client certificate: %w", err)
		}
		cfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				return nil, fmt.Errorf("client certificate: %w", err)
			}
			return &cert, nil
		}
	}
	return cfg, nil
}

func verify(cs tls.ConnectionState, caFile, serverName string) error {
	if len(cs.PeerCertificates) == 0 {
		return errors.New("the member presented no certificate")
	}
	roots, err := loadPool(caFile)
	if err != nil {
		return err
	}
	intermediates := x509.NewCertPool()
	for _, c := range cs.PeerCertificates[1:] {
		intermediates.AddCert(c)
	}
	_, err = cs.PeerCertificates[0].Verify(x509.VerifyOptions{
		DNSName:       serverName,
		Roots:         roots,
		Intermediates: intermediates,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	return err
}

func loadPool(caFile string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caFile) //nolint:gosec // the path is the operator's own etcd configuration
	if err != nil {
		return nil, fmt.Errorf("trusted CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("trusted CA %s holds no PEM certificate", caFile)
	}
	return pool, nil
}
