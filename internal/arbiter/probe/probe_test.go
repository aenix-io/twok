package probe

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/aenix-io/twok/internal/arbiter"
)

// register is an in-memory register that can be told to ignore either
// condition, the two ways a store can fail the probe.
type register struct {
	mu            sync.Mutex
	exists        bool
	rev           int
	ignoreCreate  bool
	ignoreVersion bool
}

func (r *register) Read(context.Context) (arbiter.Record, arbiter.Version, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.exists {
		return arbiter.Record{}, arbiter.Zero, arbiter.ErrNotFound
	}
	return arbiter.Record{}, arbiter.Version(strconv.Itoa(r.rev)), nil
}

func (r *register) CompareAndSwap(_ context.Context, expected arbiter.Version, _ arbiter.Record) (arbiter.Version, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if expected == arbiter.Zero {
		if r.exists && !r.ignoreCreate {
			return arbiter.Zero, arbiter.ErrConflict
		}
	} else if (!r.exists || string(expected) != strconv.Itoa(r.rev)) && !r.ignoreVersion {
		return arbiter.Zero, arbiter.ErrConflict
	}
	r.exists = true
	r.rev++
	return arbiter.Version(strconv.Itoa(r.rev)), nil
}

func TestVerify(t *testing.T) {
	tests := []struct {
		name    string
		reg     *register
		wantErr error
	}{
		{name: "honours both conditions, no probe yet", reg: &register{}},
		{name: "honours both conditions, stale probe", reg: &register{exists: true, rev: 7}},
		{name: "ignores the create condition", reg: &register{ignoreCreate: true}, wantErr: arbiter.ErrUnsupported},
		{name: "ignores the version", reg: &register{ignoreVersion: true}, wantErr: arbiter.ErrUnsupported},
		{name: "ignores the version, stale probe", reg: &register{exists: true, rev: 7, ignoreVersion: true}, wantErr: arbiter.ErrUnsupported},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Verify(t.Context(), tt.reg, "999")
			if !errors.Is(err, tt.wantErr) || (tt.wantErr == nil && err != nil) {
				t.Fatalf("Verify = %v, want %v", err, tt.wantErr)
			}
			if !tt.reg.exists {
				t.Fatal("the probe was not left in place")
			}
		})
	}
}

func TestVerifyRejectsZeroWrongVersion(t *testing.T) {
	if err := Verify(t.Context(), &register{}, arbiter.Zero); err == nil {
		t.Fatal("Verify accepted Zero as the wrong version")
	}
}

// failing returns an error that is not a conflict, which Verify must pass
// on rather than mistake for an answer about conditions.
type failing struct{ register }

var errStore = errors.New("store is down")

func (*failing) CompareAndSwap(context.Context, arbiter.Version, arbiter.Record) (arbiter.Version, error) {
	return arbiter.Zero, errStore
}

func TestVerifyPassesStoreErrors(t *testing.T) {
	err := Verify(t.Context(), &failing{}, "999")
	if !errors.Is(err, errStore) || errors.Is(err, arbiter.ErrUnsupported) {
		t.Fatalf("Verify = %v, want the store's error", err)
	}
}
