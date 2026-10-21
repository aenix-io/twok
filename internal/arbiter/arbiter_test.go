package arbiter_test

import (
	"context"
	"errors"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aenix-io/twok/internal/arbiter"
)

// fake is a register whose faults the test chooses.
type fake struct {
	mu      sync.Mutex
	record  arbiter.Record
	version int
	// ignoreZero accepts a create over an existing record.
	ignoreZero bool
	// ignoreStale accepts a swap from any non-zero version.
	ignoreStale bool
	// sameVersion returns the old version after a successful update.
	sameVersion bool
	// failWrites fails every write as a store that cannot be reached.
	failWrites error
}

var errUnreachable = errors.New("store unreachable")

// registered counts the schemes the tests register. The registry lives as
// long as the process, so a scheme must not repeat, not even with -count.
var registered atomic.Uint64

func scheme(base string) string {
	return base + "-" + strconv.FormatUint(registered.Add(1), 10)
}

func (f *fake) Read(context.Context) (arbiter.Record, arbiter.Version, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.version == 0 {
		return arbiter.Record{}, arbiter.Zero, arbiter.ErrNotFound
	}
	return f.record, arbiter.Version(strconv.Itoa(f.version)), nil
}

func (f *fake) CompareAndSwap(_ context.Context, expected arbiter.Version, r arbiter.Record) (arbiter.Version, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failWrites != nil {
		return arbiter.Zero, f.failWrites
	}
	current := arbiter.Zero
	if f.version > 0 {
		current = arbiter.Version(strconv.Itoa(f.version))
	}
	switch {
	case expected == current:
	case expected == arbiter.Zero && f.ignoreZero:
	case expected != arbiter.Zero && f.ignoreStale:
	default:
		return arbiter.Zero, arbiter.ErrConflict
	}
	f.record = r
	if f.sameVersion && f.version > 0 {
		return current, nil
	}
	f.version++
	return arbiter.Version(strconv.Itoa(f.version)), nil
}

func (f *fake) Verify(ctx context.Context) error { return arbiter.VerifyProbe(ctx, f) }

func (*fake) String() string { return "fake" }

func TestOpen(t *testing.T) {
	var got *url.URL
	want := &fake{}
	name := scheme("test-open")
	arbiter.Register(name, func(u *url.URL) (arbiter.Arbiter, error) {
		got = u
		return want, nil
	})
	a, err := arbiter.Open(name + "://bucket/clusters/prod")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if a != want {
		t.Errorf("Open() = %v, want the factory's arbiter", a)
	}
	if got.Host != "bucket" || got.Path != "/clusters/prod" {
		t.Errorf("factory got %v, want host bucket and path /clusters/prod", got)
	}
	if !slices.Contains(arbiter.Schemes(), name) {
		t.Errorf("Schemes() = %v, want %s in it", arbiter.Schemes(), name)
	}
}

func TestOpenFactoryError(t *testing.T) {
	name := scheme("test-error")
	arbiter.Register(name, func(*url.URL) (arbiter.Arbiter, error) {
		return nil, errUnreachable
	})
	if _, err := arbiter.Open(name + "://x"); !errors.Is(err, errUnreachable) {
		t.Errorf("Open() error = %v, want the factory's error", err)
	}
}

func TestOpenUnknownScheme(t *testing.T) {
	known := scheme("test-known")
	arbiter.Register(known, func(*url.URL) (arbiter.Arbiter, error) { return &fake{}, nil })
	_, err := arbiter.Open("nosuch://x")
	if err == nil {
		t.Fatal("Open() of an unknown scheme succeeded")
	}
	// The error lists what would have worked.
	if !strings.Contains(err.Error(), `"nosuch"`) || !strings.Contains(err.Error(), known) {
		t.Errorf("Open() error = %q, want the scheme and the known schemes in it", err)
	}
}

func TestOpenInvalidURL(t *testing.T) {
	if _, err := arbiter.Open("s3://bucket/%zz"); err == nil {
		t.Fatal("Open() of an invalid URL succeeded")
	}
}

func TestRegisterTwice(t *testing.T) {
	f := func(*url.URL) (arbiter.Arbiter, error) { return &fake{}, nil }
	name := scheme("test-twice")
	arbiter.Register(name, f)
	defer func() {
		if recover() == nil {
			t.Error("a second Register of one scheme did not panic")
		}
	}()
	arbiter.Register(name, f)
}

func TestSchemesSorted(t *testing.T) {
	f := func(*url.URL) (arbiter.Arbiter, error) { return &fake{}, nil }
	arbiter.Register(scheme("test-sort-b"), f)
	arbiter.Register(scheme("test-sort-a"), f)
	if s := arbiter.Schemes(); !slices.IsSorted(s) {
		t.Errorf("Schemes() = %v, want sorted", s)
	}
}

func TestVerifyProbe(t *testing.T) {
	tests := []struct {
		name  string
		probe *fake
		// unsupported is true when the store must be refused, false when
		// it must pass.
		unsupported bool
		wantErr     error
	}{
		{name: "honest store", probe: &fake{}},
		{name: "stale probe from an earlier start", probe: &fake{version: 7}},
		{name: "create ignores the existing record", probe: &fake{ignoreZero: true}, unsupported: true},
		{name: "swap ignores the version", probe: &fake{ignoreStale: true}, unsupported: true},
		{name: "version does not change on a write", probe: &fake{sameVersion: true}, unsupported: true},
		{name: "unreachable store", probe: &fake{failWrites: errUnreachable}, wantErr: errUnreachable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := arbiter.VerifyProbe(context.Background(), tt.probe)
			switch {
			case tt.unsupported:
				if !errors.Is(err, arbiter.ErrUnsupported) {
					t.Errorf("VerifyProbe() error = %v, want ErrUnsupported", err)
				}
			case tt.wantErr != nil:
				if !errors.Is(err, tt.wantErr) || errors.Is(err, arbiter.ErrUnsupported) {
					t.Errorf("VerifyProbe() error = %v, want %v and not ErrUnsupported", err, tt.wantErr)
				}
			case err != nil:
				t.Errorf("VerifyProbe() error = %v", err)
			}
		})
	}
}

func TestVerifyProbeRecord(t *testing.T) {
	probe := &fake{}
	if err := arbiter.VerifyProbe(context.Background(), probe); err != nil {
		t.Fatalf("VerifyProbe() error = %v", err)
	}
	r, _, err := probe.Read(context.Background())
	if err != nil {
		t.Fatalf("Read() of the probe error = %v: the probe must stay in place", err)
	}
	// Nothing may mistake the probe for a claim.
	if r.Generation != 0 || r.Survivor != "twok-probe" || r.Reason != "probe" {
		t.Errorf("probe record = %+v, want generation 0, survivor twok-probe, reason probe", r)
	}
}
