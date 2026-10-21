package member_test

import (
	"testing"

	"github.com/aenix-io/twok/internal/etcdtest"
	"github.com/aenix-io/twok/internal/member"
)

// TestServerName dials a member by address and verifies its certificate
// against another name, as twok does with the addresses a DNS name
// resolves to. etcdtest's certificates carry localhost and the member's name.
func TestServerName(t *testing.T) {
	cl := etcdtest.Start(t, 1)
	m := cl.Members[0]
	for _, tt := range []struct {
		serverName string
		ok         bool
	}{
		{"", true},
		{"localhost", true},
		{m.Name, true},
		{"etcd.example.com", false},
	} {
		t.Run(tt.serverName, func(t *testing.T) {
			c, err := member.New(member.Config{
				Endpoint: m.ClientURL, CertFile: m.TLS.CertFile, KeyFile: m.TLS.KeyFile, CAFile: m.TLS.CAFile,
				ServerName: tt.serverName,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close() }()
			_, err = c.Status(ctx(t))
			if (err == nil) != tt.ok {
				t.Errorf("Status() with server name %q: error = %v, want ok %v", tt.serverName, err, tt.ok)
			}
		})
	}
}
