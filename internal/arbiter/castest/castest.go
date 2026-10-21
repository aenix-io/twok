// Package castest holds small helpers the backends' own tests share on
// top of the conformance suite in arbitertest: records for the error-path
// tests, and the check that Verify refuses a store that ignores its
// condition, which only a backend's fake can stage.
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
