package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aenix-io/twok/internal/etcdargs"
)

// ProcProcess finds the etcd process through /proc and stops it with
// SIGTERM. Whoever runs etcd starts it again: the kubelet for a container,
// Talos for its service. It needs a PID namespace shared with etcd, and the
// same user as etcd or CAP_KILL.
type ProcProcess struct {
	// Root is "/proc" outside tests.
	Root string
	// DataDir is etcd's --data-dir, as etcd's command line has it.
	DataDir string
	Logger  *slog.Logger
	// Interval paces the checks while etcd stops.
	Interval time.Duration
	// kill sends a signal; syscall.Kill outside tests.
	kill func(pid int, sig syscall.Signal) error
}

// NewProcProcess returns the controller for the etcd process that uses
// dataDir.
func NewProcProcess(dataDir string, logger *slog.Logger) *ProcProcess {
	return &ProcProcess{Root: "/proc", DataDir: dataDir, Logger: logger, Interval: 100 * time.Millisecond, kill: syscall.Kill}
}

// Args implements Process.
func (p *ProcProcess) Args() (etcdargs.Args, error) {
	_, args, err := p.find()
	return args, err
}

// find returns the process whose first argument ends in "etcd" and whose
// --data-dir is DataDir.
func (p *ProcProcess) find() (int, etcdargs.Args, error) {
	entries, err := os.ReadDir(p.Root)
	if err != nil {
		return 0, etcdargs.Args{}, err
	}
	want := filepath.Clean(p.DataDir)
	self := os.Getpid()
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == self {
			continue
		}
		data, err := os.ReadFile(filepath.Join(p.Root, e.Name(), "cmdline"))
		if err != nil || len(data) == 0 {
			continue
		}
		first, _, _ := bytes.Cut(data, []byte{0})
		if !strings.HasSuffix(filepath.Base(string(first)), "etcd") {
			continue
		}
		args, err := etcdargs.FromCmdline(data)
		if err != nil {
			continue
		}
		dir := args.DataDir
		if !filepath.IsAbs(dir) {
			// A relative --data-dir is relative to etcd's working
			// directory, which only the process itself knows.
			cwd, err := os.Readlink(filepath.Join(p.Root, e.Name(), "cwd"))
			if err != nil {
				continue
			}
			dir = filepath.Join(cwd, dir)
		}
		if filepath.Clean(dir) == want {
			return pid, args, nil
		}
	}
	return 0, etcdargs.Args{}, fmt.Errorf("%w: --data-dir=%s under %s", ErrNoProcess, p.DataDir, p.Root)
}

// Stop implements Process.
func (p *ProcProcess) Stop(ctx context.Context) error {
	pid, _, err := p.find()
	if errors.Is(err, ErrNoProcess) {
		p.Logger.Info("etcd is not running", "dataDir", p.DataDir)
		return nil
	}
	if err != nil {
		return err
	}
	p.Logger.Info("stopping etcd", "pid", pid)
	if err := p.kill(pid, syscall.SIGTERM); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return fmt.Errorf("signal etcd (pid %d): %w", pid, err)
	}
	ticker := time.NewTicker(p.Interval)
	defer ticker.Stop()
	for {
		if p.gone(pid) {
			p.Logger.Info("etcd stopped", "pid", pid)
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("etcd (pid %d) did not stop: %w", pid, ctx.Err())
		case <-ticker.C:
		}
	}
}

// gone reports whether pid has exited: it is missing from /proc, or a
// zombie that only waits for its parent.
func (p *ProcProcess) gone(pid int) bool {
	data, err := os.ReadFile(filepath.Join(p.Root, strconv.Itoa(pid), "stat"))
	if err != nil {
		return true
	}
	// The state follows the command name, which is in parentheses and may
	// contain anything, so it is found from the last ')'.
	i := bytes.LastIndexByte(data, ')')
	return i < 0 || i+2 >= len(data) || data[i+2] == 'Z' || data[i+2] == 'X'
}

// Resume implements Process. The platform starts etcd again by itself.
func (p *ProcProcess) Resume(context.Context) error {
	return nil
}
