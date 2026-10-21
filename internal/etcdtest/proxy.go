package etcdtest

import (
	"io"
	"net"
	"sync"
	"testing"
)

// Proxy forwards TCP connections from a local port to a target address. It
// does not look into the traffic, so TLS stays end to end between the two
// sides.
type Proxy struct {
	target   string
	listener net.Listener

	mu    sync.Mutex
	cut   bool
	conns map[net.Conn]struct{}
	wg    sync.WaitGroup
}

// NewProxy listens on a free port on 127.0.0.1 and forwards every accepted
// connection to target. The proxy is closed when the test ends.
func NewProxy(t testing.TB, target string) *Proxy {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &Proxy{target: target, listener: l, conns: map[net.Conn]struct{}{}}
	p.wg.Go(p.accept)
	t.Cleanup(p.close)
	return p
}

// Addr is the address the proxy listens on.
func (p *Proxy) Addr() string {
	return p.listener.Addr().String()
}

// Cut closes every forwarded connection and rejects new ones until Heal. A
// rejected connection is accepted and closed at once, so the dialer sees the
// connection reset instead of a timeout. The listener stays open, so the
// port cannot be taken by someone else while the proxy is cut.
func (p *Proxy) Cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cut = true
	for c := range p.conns {
		_ = c.Close()
	}
}

// Heal lets new connections through again. Connections closed by Cut stay
// closed; etcd redials its peers by itself.
func (p *Proxy) Heal() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cut = false
}

func (p *Proxy) accept() {
	for {
		in, err := p.listener.Accept()
		if err != nil {
			return
		}
		p.wg.Go(func() { p.forward(in) })
	}
}

func (p *Proxy) forward(in net.Conn) {
	if !p.track(in) {
		_ = in.Close()
		return
	}
	defer p.untrack(in)
	out, err := net.Dial("tcp", p.target)
	if err != nil {
		return
	}
	if !p.track(out) {
		_ = out.Close()
		return
	}
	defer p.untrack(out)

	var copies sync.WaitGroup
	pipe := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		// Half-close the other side so that a clean EOF travels on, and
		// close both when either side is gone for good.
		if tcp, ok := dst.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
	}
	copies.Go(func() { pipe(out, in) })
	copies.Go(func() { pipe(in, out) })
	copies.Wait()
}

// track registers c unless the proxy is cut.
func (p *Proxy) track(c net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cut || p.conns == nil {
		return false
	}
	p.conns[c] = struct{}{}
	return true
}

func (p *Proxy) untrack(c net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.conns, c)
	_ = c.Close()
}

func (p *Proxy) close() {
	_ = p.listener.Close()
	p.mu.Lock()
	for c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
	p.mu.Unlock()
	p.wg.Wait()
}
