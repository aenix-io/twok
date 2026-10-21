package etcdtest

import (
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
)

// The port windows. go test runs the test binaries of several packages at
// once, and each starts etcd processes on ports it picked beforehand. A
// port taken from the kernel with "127.0.0.1:0" and closed again goes back
// to the kernel's ephemeral range, where the next ":0" listener in any
// process, or an outgoing connection, may get it before etcd binds it:
// etcd then fails with "bind: address already in use".
//
// So every test process takes its ports from a window of its own, chosen
// by its PID, and hands them out one after the other. The windows lie
// below the kernel's ephemeral range (32768-60999 on Linux by default), so
// that nothing the kernel assigns lands in them. Two processes share a
// window only when their PIDs are equal modulo the number of windows; each
// candidate is still probed with a listen and close, so a port in use is
// skipped, and when the window runs out the kernel picks the port as
// before.
const (
	portsFirst  = 10000
	portsLast   = 32499
	portsWindow = 500
)

var (
	portsMu   sync.Mutex
	portsNext = windowStart(os.Getpid())
	portsEnd  = portsNext + portsWindow
)

// windowStart is the first port of the window of the process with pid.
func windowStart(pid int) int {
	windows := (portsLast - portsFirst + 1) / portsWindow
	return portsFirst + (pid%windows)*portsWindow
}

// freeAddr returns a 127.0.0.1 address for a listener that is started
// later, by another process: the next port of this process's window that
// can be bound now, or one the kernel picks once the window is used up.
func freeAddr(t testing.TB) string {
	t.Helper()
	portsMu.Lock()
	defer portsMu.Unlock()
	for portsNext < portsEnd {
		addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(portsNext))
		portsNext++
		if l, err := net.Listen("tcp", addr); err == nil {
			_ = l.Close()
			return addr
		}
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}
