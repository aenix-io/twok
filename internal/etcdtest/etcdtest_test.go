package etcdtest

import (
	"bufio"
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// echoServer accepts connections and echoes every line back.
func echoServer(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}()
		}
	}()
	return l.Addr().String()
}

func roundTrip(addr string) error {
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(time.Second))
	if _, err := c.Write([]byte("ping\n")); err != nil {
		return err
	}
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		return err
	}
	if line != "ping\n" {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func TestProxyCutAndHeal(t *testing.T) {
	p := NewProxy(t, echoServer(t))
	if err := roundTrip(p.Addr()); err != nil {
		t.Fatalf("through a healthy proxy: %v", err)
	}

	// A connection that is open when the proxy is cut must die with it.
	open, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer open.Close()
	if _, err := open.Write([]byte("ping\n")); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(open)
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}

	p.Cut()
	_ = open.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = open.Write([]byte("ping\n"))
	if _, err := reader.ReadString('\n'); err == nil {
		t.Fatal("a connection open before the cut still works")
	}
	if err := roundTrip(p.Addr()); err == nil {
		t.Fatal("a new connection works through a cut proxy")
	}

	p.Heal()
	if err := roundTrip(p.Addr()); err != nil {
		t.Fatalf("through a healed proxy: %v", err)
	}
}

func TestPairPartition(t *testing.T) {
	cl := StartPair(t)
	leader := cl.Leader(t)
	follower := cl.Follower(t)
	if _, err := leader.Client(t).Put(context.Background(), "k", "v"); err != nil {
		t.Fatalf("put on a healthy pair: %v", err)
	}
	for _, m := range cl.Members {
		if !strings.Contains(strings.Join(m.Args(), " "), "--initial-advertise-peer-urls="+m.AdvertisePeerURL) {
			t.Fatalf("%s does not advertise its proxy: %v", m.Name, m.Args())
		}
		if m.Pid() == 0 {
			t.Fatalf("%s has no pid", m.Name)
		}
	}

	for _, m := range cl.Members {
		m.Proxy.Cut()
	}
	waitNoLeader(t, leader)
	waitNoLeader(t, follower)

	for _, m := range cl.Members {
		m.Proxy.Heal()
	}
	cl.Leader(t)

	follower.Restart(t)
	cl.Leader(t)
	if !strings.Contains(follower.Log(t), "=== etcdtest: start") {
		t.Fatal("the log does not record the starts")
	}
}

func waitNoLeader(t *testing.T, m *Member) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		st, err := m.Status(t)
		if err == nil && st.Leader == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s still reports a leader after both proxies were cut", m.Name)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
