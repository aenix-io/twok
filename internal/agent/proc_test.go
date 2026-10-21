package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// fakeEtcdEnv makes the test binary stand in for etcd: it sleeps until it
// is killed, with whatever command line the test gave it.
const fakeEtcdEnv = "TWOK_TEST_FAKE_ETCD"

func TestMain(m *testing.M) {
	if os.Getenv(fakeEtcdEnv) != "" {
		time.Sleep(time.Hour)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// fakeProc builds a /proc with the given command lines by pid.
func fakeProc(t *testing.T, procs map[int][]string) string {
	t.Helper()
	root := t.TempDir()
	for pid, cmd := range procs {
		dir := filepath.Join(root, strconv.Itoa(pid))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		data := strings.Join(cmd, "\x00") + "\x00"
		if err := os.WriteFile(filepath.Join(dir, "cmdline"), []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "stat"), []byte(strconv.Itoa(pid)+" ("+filepath.Base(cmd[0])+") S 1 1"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("/work", filepath.Join(dir, "cwd")); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "uptime"), []byte("1 1"), 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestProcFind(t *testing.T) {
	root := fakeProc(t, map[int][]string{
		1:  {"/pause"},
		7:  {"twok", "agent", "--data-dir=/var/lib/etcd"},
		10: {"/usr/local/bin/etcd", "--name=other", "--data-dir=/var/lib/other"},
		11: {"/usr/local/bin/etcd", "--name=a", "--data-dir", "/var/lib/etcd/", "--initial-advertise-peer-urls=https://a:2380"},
		12: {"etcd", "--name=rel", "--data-dir=rel.etcd"},
		13: {"/usr/local/bin/etcd", "--config-file=/x"},
	})
	p := NewProcProcess("/var/lib/etcd", quietLogger())
	p.Root = root
	args, err := p.Args()
	if err != nil {
		t.Fatal(err)
	}
	if args.Name != "a" || args.InitialAdvertisePeerURLs[0] != "https://a:2380" {
		t.Errorf("Args() = %+v", args)
	}

	// A relative --data-dir is resolved against etcd's working directory.
	p.DataDir = "/work/rel.etcd"
	if args, err := p.Args(); err != nil || args.Name != "rel" {
		t.Errorf("Args() = %+v, %v; want the member with a relative data dir", args, err)
	}

	p.DataDir = "/elsewhere"
	if _, err := p.Args(); !errors.Is(err, ErrNoProcess) {
		t.Errorf("Args() error = %v, want ErrNoProcess", err)
	}
}

func TestProcStopFake(t *testing.T) {
	root := fakeProc(t, map[int][]string{11: {"etcd", "--data-dir=/var/lib/etcd"}})
	p := NewProcProcess("/var/lib/etcd", quietLogger())
	p.Root, p.Interval = root, time.Millisecond
	var signals []syscall.Signal
	p.kill = func(pid int, sig syscall.Signal) error {
		signals = append(signals, sig)
		// The process becomes a zombie: still in /proc, but gone.
		return os.WriteFile(filepath.Join(root, strconv.Itoa(pid), "stat"), []byte("11 (etcd) Z 1 1"), 0o644)
	}
	if err := p.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(signals) != 1 || signals[0] != syscall.SIGTERM {
		t.Errorf("signals = %v, want one SIGTERM", signals)
	}

	// Without a process there is nothing to stop.
	p.DataDir = "/none"
	if err := p.Stop(context.Background()); err != nil {
		t.Errorf("Stop() without a process = %v", err)
	}

	// A process that ignores SIGTERM fails Stop at the deadline.
	p.DataDir = "/var/lib/etcd"
	if err := os.WriteFile(filepath.Join(root, "11", "stat"), []byte("11 (etcd) S 1 1"), 0o644); err != nil {
		t.Fatal(err)
	}
	p.kill = func(int, syscall.Signal) error { return nil }
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := p.Stop(ctx); err == nil || !strings.Contains(err.Error(), "did not stop") {
		t.Errorf("Stop() = %v, want a timeout", err)
	}
}

// TestProcStopReal stops a real process found in the real /proc.
func TestProcStopReal(t *testing.T) {
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "etcd")
	if err := os.Symlink(bin, link); err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(t.TempDir(), "data")
	cmd := exec.Command(link, "--name=real", "--data-dir="+dataDir)
	cmd.Env = append(os.Environ(), fakeEtcdEnv+"=1")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	p := NewProcProcess(dataDir, quietLogger())
	p.Interval = 10 * time.Millisecond
	deadline := time.Now().Add(5 * time.Second)
	for {
		args, err := p.Args()
		if err == nil {
			if args.Name != "real" {
				t.Fatalf("Args() = %+v", args)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the process was not found: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waited:
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGTERM {
			t.Errorf("the process ended with %v, want SIGTERM", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the process is still running")
	}
	if _, err := p.Args(); !errors.Is(err, ErrNoProcess) {
		t.Errorf("Args() after Stop = %v, want ErrNoProcess", err)
	}
}
