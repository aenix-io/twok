package etcdtest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// BinaryEnv names the environment variable with the etcd binary.
const BinaryEnv = "TEST_ETCD_BINARY"

// LogDirEnv names an optional directory that receives a copy of every
// member's log when the test ends, for logs too long for the test output.
const LogDirEnv = "TEST_ETCD_LOG_DIR"

// stopTimeout bounds a graceful stop; etcd needs well under a second.
const stopTimeout = 30 * time.Second

// Binary returns the etcd binary from TEST_ETCD_BINARY, or skips the test.
func Binary(t testing.TB) string {
	t.Helper()
	path := os.Getenv(BinaryEnv)
	if path == "" {
		t.Skipf("%s is not set; set it to an etcd binary to run tests against real etcd", BinaryEnv)
	}
	return path
}

// Member is one etcd process with its data directory, ports, certificates
// and log. The process can be stopped and started again with other
// arguments, on the same data directory and ports.
type Member struct {
	Name    string
	DataDir string
	// ClientURL is the member's real client URL. Client traffic is not
	// proxied.
	ClientURL string
	// PeerURL is where etcd listens for peers.
	PeerURL string
	// AdvertisePeerURL is the URL of Proxy, which forwards to PeerURL. It is
	// what the member advertises and what membership stores.
	AdvertisePeerURL string
	Proxy            *Proxy
	// TLS is used for the client and the peer listener, and by tests as
	// client certificate.
	TLS TLSFiles
	// LogFile receives etcd's stdout and stderr, from every start.
	LogFile string

	binary string

	mu      sync.Mutex
	cmd     *exec.Cmd
	args    []string
	extra   []string
	exited  chan struct{}
	waitErr error
	log     *os.File
	client  *clientv3.Client
}

// NewMember prepares a member in dir without starting it. The member is
// stopped when the test ends, and the tail of its log is printed if the test
// failed.
func NewMember(t testing.TB, ca *CA, dir, name string) *Member {
	t.Helper()
	m := &Member{
		Name:      name,
		DataDir:   filepath.Join(dir, name+".etcd"),
		ClientURL: "https://" + freeAddr(t),
		PeerURL:   "https://" + freeAddr(t),
		TLS:       ca.Issue(t, name),
		LogFile:   filepath.Join(dir, name+".log"),
		binary:    Binary(t),
	}
	m.Proxy = NewProxy(t, strings.TrimPrefix(m.PeerURL, "https://"))
	m.AdvertisePeerURL = "https://" + m.Proxy.Addr()
	t.Cleanup(func() {
		if m.Running() {
			m.Kill(t)
		}
		m.mu.Lock()
		if m.client != nil {
			_ = m.client.Close()
		}
		if m.log != nil {
			_ = m.log.Close()
		}
		m.mu.Unlock()
		if t.Failed() {
			t.Logf("%s log:\n%s", m.Name, tail(m.Log(t), 60))
		}
		keepLog(t, m)
	})
	return m
}

// InitialClusterEntry is the member's entry for --initial-cluster.
func (m *Member) InitialClusterEntry() string {
	return m.Name + "=" + m.AdvertisePeerURL
}

// BaseArgs are the arguments every start of the member gets: name, data
// directory, URLs and mTLS. The cluster arguments are not among them.
func (m *Member) BaseArgs() []string {
	return []string{
		"--name=" + m.Name,
		"--data-dir=" + m.DataDir,
		"--listen-client-urls=" + m.ClientURL,
		"--advertise-client-urls=" + m.ClientURL,
		"--listen-peer-urls=" + m.PeerURL,
		"--initial-advertise-peer-urls=" + m.AdvertisePeerURL,
		"--cert-file=" + m.TLS.CertFile,
		"--key-file=" + m.TLS.KeyFile,
		"--trusted-ca-file=" + m.TLS.CAFile,
		"--client-cert-auth",
		"--peer-cert-file=" + m.TLS.CertFile,
		"--peer-key-file=" + m.TLS.KeyFile,
		"--peer-trusted-ca-file=" + m.TLS.CAFile,
		"--peer-client-cert-auth",
	}
}

// Start starts etcd with BaseArgs followed by extra. Later flags win in
// etcd's flag parsing, so extra can override a base argument. Start does not
// wait for the member to be ready.
func (m *Member) Start(t testing.TB, extra ...string) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cmd != nil && !isClosed(m.exited) {
		t.Fatalf("%s is already running", m.Name)
	}
	if m.log == nil {
		f, err := os.OpenFile(m.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		m.log = f
	}
	args := append(m.BaseArgs(), extra...)
	_, _ = fmt.Fprintf(m.log, "=== etcdtest: start %s %s\n", m.binary, strings.Join(args, " "))
	cmd := exec.Command(m.binary, args...)
	cmd.Stdout, cmd.Stderr = m.log, m.log
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", m.Name, err)
	}
	m.cmd, m.args, m.extra, m.exited, m.waitErr = cmd, args, slices.Clone(extra), make(chan struct{}), nil
	exited := m.exited
	go func() {
		err := cmd.Wait()
		m.mu.Lock()
		m.waitErr = err
		m.mu.Unlock()
		close(exited)
	}()
}

// ExtraArgs returns the arguments of the last start that came after
// BaseArgs. A test that wants to restart a member "with its original flags"
// saves them after the first start.
func (m *Member) ExtraArgs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.extra)
}

// Args returns the arguments of the last start, without the binary.
func (m *Member) Args() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.args)
}

// Pid returns the process ID of the last start, or 0 if it never started.
func (m *Member) Pid() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cmd == nil {
		return 0
	}
	return m.cmd.Process.Pid
}

// Running reports whether the process of the last start is still running.
func (m *Member) Running() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cmd != nil && !isClosed(m.exited)
}

// Exited is closed when the process of the last start exits.
func (m *Member) Exited() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.exited
}

// ExitErr returns the result of waiting for the last process: nil for exit
// code 0. It is only meaningful after Exited is closed.
func (m *Member) ExitErr() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.waitErr
}

// Stop sends SIGTERM and waits for the process to exit. etcd flushes and
// closes its WAL on SIGTERM.
func (m *Member) Stop(t testing.TB) {
	t.Helper()
	m.signal(t, syscall.SIGTERM)
}

// Kill sends SIGKILL and waits for the process to exit. Whatever etcd had
// not written to the kernel, such as WAL records still in its buffer, is
// lost, as in a crash of the process.
func (m *Member) Kill(t testing.TB) {
	t.Helper()
	m.signal(t, syscall.SIGKILL)
}

// Restart stops the member gracefully and starts it with BaseArgs and extra.
func (m *Member) Restart(t testing.TB, extra ...string) {
	t.Helper()
	m.Stop(t)
	m.Start(t, extra...)
}

func (m *Member) signal(t testing.TB, sig syscall.Signal) {
	t.Helper()
	m.mu.Lock()
	cmd, exited := m.cmd, m.exited
	m.mu.Unlock()
	if cmd == nil {
		t.Fatalf("%s was never started", m.Name)
	}
	if isClosed(exited) {
		return
	}
	if err := cmd.Process.Signal(sig); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("signal %s: %v", m.Name, err)
	}
	select {
	case <-exited:
	case <-time.After(stopTimeout):
		_ = cmd.Process.Kill()
		<-exited
		t.Fatalf("%s did not exit within %s after %s", m.Name, stopTimeout, sig)
	}
}

// Log returns everything etcd wrote to its log file so far.
func (m *Member) Log(t testing.TB) string {
	t.Helper()
	b, err := os.ReadFile(m.LogFile)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return string(b)
}

// Client returns a client connected to this member only. It is kept for the
// life of the test, and keeps working across restarts of the member.
func (m *Member) Client(t testing.TB) *clientv3.Client {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.client != nil {
		return m.client
	}
	info := transport.TLSInfo{CertFile: m.TLS.CertFile, KeyFile: m.TLS.KeyFile, TrustedCAFile: m.TLS.CAFile}
	tlsConfig, err := info.ClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	c, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{m.ClientURL},
		TLS:         tlsConfig,
		DialTimeout: 5 * time.Second,
		Logger:      zap.NewNop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	m.client = c
	return c
}

// Status returns the member's status, or an error if it does not answer
// within a second.
func (m *Member) Status(t testing.TB) (*clientv3.StatusResponse, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return m.Client(t).Status(ctx, m.ClientURL)
}

// ID returns the member ID, waiting up to 30 seconds for the member to
// answer.
func (m *Member) ID(t testing.TB) uint64 {
	t.Helper()
	return m.waitStatus(t, 30*time.Second, func(*clientv3.StatusResponse) bool { return true }).Header.MemberId
}

// WaitLeader waits until the member reports a leader and returns it.
func (m *Member) WaitLeader(t testing.TB, timeout time.Duration) uint64 {
	t.Helper()
	return m.waitStatus(t, timeout, func(st *clientv3.StatusResponse) bool { return st.Leader != 0 }).Leader
}

func (m *Member) waitStatus(t testing.TB, timeout time.Duration, ok func(*clientv3.StatusResponse) bool) *clientv3.StatusResponse {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for {
		st, err := m.Status(t)
		if err == nil && ok(st) {
			return st
		}
		if err != nil {
			last = err
		} else {
			last = fmt.Errorf("status: leader %x, errors %v", st.Leader, st.Errors)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: condition not met within %s: %v", m.Name, timeout, last)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// CopyDataDir copies the member's data directory to dst, which must not
// exist. The member must be stopped. Tests use it to try two recoveries on
// the same crashed state.
func (m *Member) CopyDataDir(t testing.TB, dst string) {
	t.Helper()
	if m.Running() {
		t.Fatalf("%s is running; stop it before copying its data", m.Name)
	}
	err := filepath.WalkDir(m.DataDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(m.DataDir, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o700)
		}
		return copyFile(path, target)
	})
	if err != nil {
		t.Fatalf("copy %s: %v", m.DataDir, err)
	}
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

func keepLog(t testing.TB, m *Member) {
	dir := os.Getenv(LogDirEnv)
	if dir == "" {
		return
	}
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()) + "_" + m.Name + ".log"
	if err := copyFile(m.LogFile, filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Logf("keep the log of %s: %v", m.Name, err)
	}
}

func isClosed(c chan struct{}) bool {
	if c == nil {
		return true
	}
	select {
	case <-c:
		return true
	default:
		return false
	}
}

func tail(s string, lines int) string {
	all := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(all) > lines {
		all = all[len(all)-lines:]
	}
	return strings.Join(all, "\n")
}
