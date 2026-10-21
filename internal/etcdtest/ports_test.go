package etcdtest

import (
	"net"
	"strconv"
	"testing"
)

func TestWindowStart(t *testing.T) {
	windows := (portsLast - portsFirst + 1) / portsWindow
	seen := map[int]bool{}
	for pid := range 2 * windows {
		start := windowStart(pid)
		if start < portsFirst || start+portsWindow-1 > portsLast || (start-portsFirst)%portsWindow != 0 {
			t.Fatalf("windowStart(%d) = %d, outside the windows", pid, start)
		}
		seen[start] = true
	}
	if len(seen) != windows {
		t.Errorf("%d distinct windows for %d PIDs, want %d", len(seen), 2*windows, windows)
	}
	if portsLast >= 32768 {
		t.Errorf("the windows reach the kernel's ephemeral range")
	}
}

func TestFreeAddr(t *testing.T) {
	portsMu.Lock()
	next, end := portsNext, portsEnd
	portsMu.Unlock()
	t.Cleanup(func() {
		portsMu.Lock()
		portsNext, portsEnd = next, end
		portsMu.Unlock()
	})

	// A window of three ports, the second one in use.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	_, p, _ := net.SplitHostPort(l.Addr().String())
	busy, _ := strconv.Atoi(p)
	portsMu.Lock()
	portsNext, portsEnd = busy-1, busy+2
	portsMu.Unlock()

	want := []string{
		"127.0.0.1:" + strconv.Itoa(busy-1),
		"127.0.0.1:" + strconv.Itoa(busy+1),
	}
	for _, w := range want {
		got := freeAddr(t)
		// The neighbours of a kernel-assigned port may be taken by
		// someone else; only the busy one must never come back.
		if got == l.Addr().String() {
			t.Fatalf("freeAddr() = %s, the port in use", got)
		}
		if got != w {
			t.Logf("freeAddr() = %s, want %s (taken by another process?)", got, w)
		}
	}
	// The window is used up: the kernel picks.
	got := freeAddr(t)
	_, p, _ = net.SplitHostPort(got)
	if port, _ := strconv.Atoi(p); port >= busy-1 && port <= busy+1 {
		t.Errorf("freeAddr() = %s after the window ran out, want a port from the kernel", got)
	}
}
