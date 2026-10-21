package member_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aenix-io/twok/internal/etcdtest"
	"github.com/aenix-io/twok/internal/member"
)

// TestTLSFilesReadOnEveryHandshake starts a client with a CA file that does
// not verify the member, replaces the file's content with the right CA, and
// expects the same client to connect: the files are read per handshake, not
// once.
func TestTLSFilesReadOnEveryHandshake(t *testing.T) {
	cl := etcdtest.Start(t, 1)
	m := cl.Members[0]

	dir := t.TempDir()
	wrong := etcdtest.NewCA(t, dir)
	caFile := filepath.Join(dir, "rotating-ca.crt")
	copyFile(t, wrong.CertFile, caFile)

	c, err := member.New(member.Config{
		Endpoint: m.ClientURL,
		CertFile: m.TLS.CertFile,
		KeyFile:  m.TLS.KeyFile,
		CAFile:   caFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Status(ctx(t)); err == nil {
		t.Fatal("status succeeded with a CA that did not issue the member's certificate")
	}

	copyFile(t, m.TLS.CAFile, caFile)
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, err := c.Status(ctx(t))
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("status after the CA file was replaced: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	b, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, b, 0o600); err != nil {
		t.Fatal(err)
	}
}
