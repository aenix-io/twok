// Package filearb is an arbiter in a local file, for tests and labs.
//
// The URL is file:///absolute/path. The record is the JSON file at that
// path, and its version is the hex SHA-256 of the file's content. A
// CompareAndSwap holds an exclusive flock on path+".lock" while it reads,
// compares and replaces the file, and the replacement is a synced rename,
// so a reader without the lock sees the old or the new record, never a mix.
//
// This is a correct compare-and-swap register only for processes on one
// host that share a filesystem on which flock works. It is not one across
// hosts, on NFS without working locks, or between containers that do not
// share the lock file. It exists so that tests and lab setups can run twok
// without a store; a real deployment needs an arbiter in a third place.
//
// Because the version is derived from the content, writing a record
// identical to an earlier one would bring back the earlier version. twok
// never does that: every claim increases the generation.
package filearb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/aenix-io/twok/internal/arbiter"
	"github.com/aenix-io/twok/internal/atomicfile"
)

// Scheme is the URL scheme of the file arbiter.
const Scheme = "file"

// lockPoll is how often a CompareAndSwap retries a lock held by another
// process. flock has no timeout of its own, so the context is honoured by
// polling a non-blocking lock instead.
const lockPoll = 10 * time.Millisecond

func init() {
	arbiter.Register(Scheme, func(u *url.URL) (arbiter.Arbiter, error) {
		if u.Host != "" && u.Host != "localhost" {
			return nil, fmt.Errorf("file arbiter: %q is not a local path, want file:///absolute/path", u.String())
		}
		return New(u.Path)
	})
}

// Arbiter is a record in a local file.
type Arbiter struct {
	path string
}

// New returns the arbiter for the record at path, which must be absolute:
// every member has to name the same file whatever its working directory.
func New(path string) (*Arbiter, error) {
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("file arbiter: path %q is not absolute", path)
	}
	return &Arbiter{path: filepath.Clean(path)}, nil
}

func (a *Arbiter) Read(ctx context.Context) (arbiter.Record, arbiter.Version, error) {
	if err := ctx.Err(); err != nil {
		return arbiter.Record{}, arbiter.Zero, err
	}
	return a.read()
}

func (a *Arbiter) read() (arbiter.Record, arbiter.Version, error) {
	data, err := os.ReadFile(a.path)
	if errors.Is(err, fs.ErrNotExist) {
		return arbiter.Record{}, arbiter.Zero, arbiter.ErrNotFound
	}
	if err != nil {
		return arbiter.Record{}, arbiter.Zero, err
	}
	var r arbiter.Record
	if err := json.Unmarshal(data, &r); err != nil {
		return arbiter.Record{}, arbiter.Zero, fmt.Errorf("file arbiter: %s: %w", a.path, err)
	}
	return r, version(data), nil
}

func (a *Arbiter) CompareAndSwap(ctx context.Context, expected arbiter.Version, record arbiter.Record) (arbiter.Version, error) {
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return arbiter.Zero, err
	}
	data = append(data, '\n')

	unlock, err := a.lock(ctx)
	if err != nil {
		return arbiter.Zero, err
	}
	defer unlock()

	current := arbiter.Zero
	switch existing, err := os.ReadFile(a.path); {
	case err == nil:
		current = version(existing)
	case !errors.Is(err, fs.ErrNotExist):
		return arbiter.Zero, err
	}
	if current != expected {
		return arbiter.Zero, arbiter.ErrConflict
	}
	if err := atomicfile.Write(a.path, data, 0o600); err != nil {
		return arbiter.Zero, err
	}
	return version(data), nil
}

// lock takes the exclusive lock next to the record and returns its release.
func (a *Arbiter) lock(ctx context.Context) (func(), error) {
	f, err := os.OpenFile(a.path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) // #nosec G115 -- a file descriptor fits in an int
		if err == nil {
			break
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EINTR) {
			_ = f.Close()
			return nil, fmt.Errorf("file arbiter: lock %s: %w", f.Name(), err)
		}
		select {
		case <-ctx.Done():
			_ = f.Close()
			return nil, ctx.Err()
		case <-time.After(lockPoll):
		}
	}
	// Closing the descriptor releases the lock.
	return func() { _ = f.Close() }, nil
}

func (a *Arbiter) Verify(ctx context.Context) error {
	probe, err := New(a.path + arbiter.ProbeSuffix)
	if err != nil {
		return err
	}
	return arbiter.VerifyProbe(ctx, probe)
}

func (a *Arbiter) String() string {
	return (&url.URL{Scheme: Scheme, Path: a.path}).String()
}

func version(data []byte) arbiter.Version {
	sum := sha256.Sum256(data)
	return arbiter.Version(hex.EncodeToString(sum[:]))
}
