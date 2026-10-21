// Package castest holds the checks every arbiter backend's unit tests run
// against their fake of the store, so that each backend is held to the
// same contract and its tests only have to provide the store.
package castest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aenix-io/twok/internal/arbiter"
)

// Record returns a record that differs for every generation, as real
// claims do.
func Record(generation uint8) arbiter.Record {
	return arbiter.Record{
		SchemaVersion:    arbiter.SchemaVersion,
		ClusterID:        "cdf818194e3a8c32",
		Generation:       uint64(generation),
		Survivor:         "a",
		SurvivorMemberID: "8e9e05c52164694d",
		Since:            time.Date(2026, 10, 21, 12, 0, int(generation), 0, time.UTC),
		TwokVersion:      "test",
		Reason:           "no-leader",
	}
}

// Exercise runs the compare-and-swap contract against a fresh, empty
// arbiter: not found, create, swap, a stale swap, a create over an
// existing record, and Verify twice, the second time over the probe the
// first one left.
func Exercise(t *testing.T, a arbiter.Arbiter) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	if _, v, err := a.Read(ctx); !errors.Is(err, arbiter.ErrNotFound) || v != arbiter.Zero {
		t.Fatalf("Read on an empty store = %q, %v; want Zero, ErrNotFound", v, err)
	}

	v1, err := a.CompareAndSwap(ctx, arbiter.Zero, Record(1))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if v1 == arbiter.Zero {
		t.Fatal("create returned the Zero version")
	}
	expect(ctx, t, a, Record(1), v1)

	if _, err := a.CompareAndSwap(ctx, arbiter.Zero, Record(9)); !errors.Is(err, arbiter.ErrConflict) {
		t.Fatalf("create over an existing record: err = %v, want ErrConflict", err)
	}
	expect(ctx, t, a, Record(1), v1)

	v2, err := a.CompareAndSwap(ctx, v1, Record(2))
	if err != nil {
		t.Fatalf("swap from the current version: %v", err)
	}
	if v2 == v1 || v2 == arbiter.Zero {
		t.Fatalf("swap returned version %q after %q", v2, v1)
	}
	expect(ctx, t, a, Record(2), v2)

	if _, err := a.CompareAndSwap(ctx, v1, Record(9)); !errors.Is(err, arbiter.ErrConflict) {
		t.Fatalf("swap from a stale version: err = %v, want ErrConflict", err)
	}
	expect(ctx, t, a, Record(2), v2)

	for i := range 2 {
		if err := a.Verify(ctx); err != nil {
			t.Fatalf("Verify #%d: %v", i+1, err)
		}
	}
	// The probe lives next to the record and must not have touched it.
	expect(ctx, t, a, Record(2), v2)

	if s := a.String(); s == "" {
		t.Error("String is empty")
	}
}

// ExpectUnsupported checks that Verify refuses a store that ignores the
// condition.
func ExpectUnsupported(t *testing.T, a arbiter.Arbiter) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := a.Verify(ctx); !errors.Is(err, arbiter.ErrUnsupported) {
		t.Fatalf("Verify on a store that ignores conditions: err = %v, want ErrUnsupported", err)
	}
}

func expect(ctx context.Context, t *testing.T, a arbiter.Arbiter, want arbiter.Record, wantV arbiter.Version) {
	t.Helper()
	got, v, err := a.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if v != wantV {
		t.Fatalf("Read version = %q, want %q", v, wantV)
	}
	if !got.Since.Equal(want.Since) {
		t.Fatalf("Read Since = %v, want %v", got.Since, want.Since)
	}
	got.Since, want.Since = time.Time{}, time.Time{}
	if got != want {
		t.Fatalf("Read = %+v, want %+v", got, want)
	}
}
