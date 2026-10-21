package filearb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/aenix-io/twok/internal/arbiter"
	"github.com/aenix-io/twok/internal/arbiter/arbitertest"
)

func TestConformance(t *testing.T) {
	arbitertest.Run(t, func(t *testing.T) arbiter.Arbiter {
		a, err := New(filepath.Join(t.TempDir(), "record.json"))
		if err != nil {
			t.Fatal(err)
		}
		return a
	})
}

// TestConformanceSeparateHandles runs the suite with a new Arbiter value
// per call, as two processes would have, to make sure nothing is shared in
// memory.
func TestConformanceSeparateHandles(t *testing.T) {
	arbitertest.Run(t, func(t *testing.T) arbiter.Arbiter {
		return &handles{path: filepath.Join(t.TempDir(), "record.json")}
	})
}

type handles struct{ path string }

func (h *handles) open() *Arbiter {
	a, err := New(h.path)
	if err != nil {
		panic(err)
	}
	return a
}

func (h *handles) Read(ctx context.Context) (arbiter.Record, arbiter.Version, error) {
	return h.open().Read(ctx)
}

func (h *handles) CompareAndSwap(ctx context.Context, expected arbiter.Version, r arbiter.Record) (arbiter.Version, error) {
	return h.open().CompareAndSwap(ctx, expected, r)
}
func (h *handles) Verify(ctx context.Context) error { return h.open().Verify(ctx) }
func (h *handles) String() string                   { return h.open().String() }

func TestOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "record.json")
	tests := []struct {
		url     string
		wantErr bool
	}{
		{url: "file://" + path},
		{url: "file://localhost" + path},
		{url: "file://relative/record.json", wantErr: true},
		{url: "file:relative.json", wantErr: true},
	}
	for _, tt := range tests {
		a, err := arbiter.Open(tt.url)
		if tt.wantErr {
			if err == nil {
				t.Errorf("Open(%q) succeeded, want an error", tt.url)
			}
			continue
		}
		if err != nil {
			t.Errorf("Open(%q) error = %v", tt.url, err)
			continue
		}
		if got, want := a.String(), "file://"+path; got != want {
			t.Errorf("Open(%q).String() = %q, want %q", tt.url, got, want)
		}
	}
}

func TestFileFormat(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "record.json")
	a, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	v, err := a.CompareAndSwap(ctx, arbiter.Zero, arbiter.Record{SchemaVersion: 1, Generation: 3, Survivor: "b"})
	if err != nil {
		t.Fatalf("CompareAndSwap() error = %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if want := arbiter.Version(hex.EncodeToString(sum[:])); v != want {
		t.Errorf("version = %q, want the SHA-256 of the file %q", v, want)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("record permissions = %o, want 600", perm)
	}
	// No temporary files are left behind next to the record.
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != "record.json" && e.Name() != "record.json.lock" {
			t.Errorf("unexpected file %s next to the record", e.Name())
		}
	}
}

func TestCorruptedRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "record.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	a, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := a.Read(context.Background()); err == nil || errors.Is(err, arbiter.ErrNotFound) {
		t.Errorf("Read() of a corrupted record error = %v, want a decode error", err)
	}
}

func TestLockHonoursContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "record.json")
	a, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	// Another process holding the lock, as seen through a second open file
	// description.
	held, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if err := syscall.Flock(int(held.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := a.CompareAndSwap(ctx, arbiter.Zero, arbiter.Record{Generation: 1}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CompareAndSwap() under a held lock error = %v, want context.DeadlineExceeded", err)
	}
	if err := syscall.Flock(int(held.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	if _, err := a.CompareAndSwap(context.Background(), arbiter.Zero, arbiter.Record{Generation: 1}); err != nil {
		t.Fatalf("CompareAndSwap() after the lock was released error = %v", err)
	}
}
