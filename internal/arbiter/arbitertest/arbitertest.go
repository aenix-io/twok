// Package arbitertest is the conformance suite every arbiter backend runs
// in its own tests, against the real store or a fake of it.
//
// The suite checks the contract of arbiter.Arbiter, not a backend's
// internals: a missing record, a create, reading back, a swap with the
// right and with a stale version, concurrent swaps from one version, the
// probe check, and a String that is safe to log.
package arbitertest

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aenix-io/twok/internal/arbiter"
)

// Rounds and Racers size the concurrency test: Racers goroutines swap from
// the same version, Rounds times in a row.
const (
	Rounds = 5
	Racers = 8
)

// Run runs the suite. newArbiter must return an arbiter for a record that
// does not exist yet, a fresh one on every call: every subtest starts from
// an empty record.
//
// String is expected to describe the arbiter well enough to find the
// record, the store and the key, and to carry no credentials: no password
// in a URL, no signed query parameters, and none of the secrets the process
// has in its environment. The suite checks what it can see from outside;
// a backend that reads credentials from a file must not print them either.
func Run(t *testing.T, newArbiter func(t *testing.T) arbiter.Arbiter) {
	t.Helper()

	t.Run("read of a missing record", func(t *testing.T) {
		a := newArbiter(t)
		_, v, err := a.Read(context.Background())
		if !errors.Is(err, arbiter.ErrNotFound) {
			t.Fatalf("Read() error = %v, want ErrNotFound", err)
		}
		if v != arbiter.Zero {
			t.Errorf("Read() version = %q, want Zero", v)
		}
	})

	t.Run("create and read back", func(t *testing.T) {
		ctx := context.Background()
		a := newArbiter(t)
		want := record(1, "a")
		v, err := a.CompareAndSwap(ctx, arbiter.Zero, want)
		if err != nil {
			t.Fatalf("CompareAndSwap(Zero) error = %v", err)
		}
		if v == arbiter.Zero {
			t.Fatal("CompareAndSwap(Zero) returned the zero version")
		}
		expectRecord(t, a, want, v)
	})

	t.Run("create of an existing record", func(t *testing.T) {
		ctx := context.Background()
		a := newArbiter(t)
		first := record(1, "a")
		v := mustSwap(t, a, arbiter.Zero, first)
		if _, err := a.CompareAndSwap(ctx, arbiter.Zero, record(1, "b")); !errors.Is(err, arbiter.ErrConflict) {
			t.Fatalf("second CompareAndSwap(Zero) error = %v, want ErrConflict", err)
		}
		expectRecord(t, a, first, v)
	})

	t.Run("swap with the current version", func(t *testing.T) {
		a := newArbiter(t)
		v1 := mustSwap(t, a, arbiter.Zero, record(1, "a"))
		want := record(2, "b")
		v2 := mustSwap(t, a, v1, want)
		if v2 == v1 {
			t.Fatalf("CompareAndSwap(%q) returned the same version", v1)
		}
		expectRecord(t, a, want, v2)
	})

	t.Run("swap with a stale version", func(t *testing.T) {
		ctx := context.Background()
		a := newArbiter(t)
		v1 := mustSwap(t, a, arbiter.Zero, record(1, "a"))
		current := record(2, "b")
		v2 := mustSwap(t, a, v1, current)
		if _, err := a.CompareAndSwap(ctx, v1, record(3, "a")); !errors.Is(err, arbiter.ErrConflict) {
			t.Fatalf("CompareAndSwap(stale %q) error = %v, want ErrConflict", v1, err)
		}
		expectRecord(t, a, current, v2)
	})

	t.Run("concurrent swaps from one version", func(t *testing.T) {
		a := newArbiter(t)
		from := arbiter.Zero
		for round := range Rounds {
			winner, v := race(t, a, from, uint64(round+1))
			expectRecord(t, a, winner, v)
			from = v
		}
	})

	t.Run("verify", func(t *testing.T) {
		ctx := context.Background()
		a := newArbiter(t)
		if err := a.Verify(ctx); err != nil {
			t.Fatalf("Verify() error = %v", err)
		}
		// The second run finds the probe of the first one and must take it
		// over, as a restarted member does.
		if err := a.Verify(ctx); err != nil {
			t.Fatalf("second Verify() error = %v", err)
		}
		if _, _, err := a.Read(ctx); !errors.Is(err, arbiter.ErrNotFound) {
			t.Fatalf("Read() after Verify() error = %v, want ErrNotFound: the probe must not touch the record", err)
		}
	})

	t.Run("concurrent verify", func(t *testing.T) {
		// Both members of a cluster start at about the same time and
		// verify the same probe; neither may fail because of the other.
		a := newArbiter(t)
		for round := range Rounds {
			var wg sync.WaitGroup
			errs := make([]error, 2)
			for i := range errs {
				wg.Go(func() { errs[i] = a.Verify(context.Background()) })
			}
			wg.Wait()
			for i, err := range errs {
				if err != nil {
					t.Errorf("round %d: Verify() #%d error = %v", round, i, err)
				}
			}
		}
	})

	t.Run("String carries no secrets", func(t *testing.T) {
		expectSafeString(t, newArbiter(t).String())
	})
}

// race lets Racers goroutines swap from the same version and checks that
// exactly one of them won. It returns the winning record and its version.
func race(t *testing.T, a arbiter.Arbiter, from arbiter.Version, generation uint64) (arbiter.Record, arbiter.Version) {
	t.Helper()
	type result struct {
		record  arbiter.Record
		version arbiter.Version
		err     error
	}
	results := make([]result, Racers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range results {
		wg.Go(func() {
			r := record(generation, fmt.Sprintf("racer-%d", i))
			<-start
			v, err := a.CompareAndSwap(context.Background(), from, r)
			results[i] = result{r, v, err}
		})
	}
	close(start)
	wg.Wait()

	var winners []result
	for i, r := range results {
		switch {
		case r.err == nil:
			winners = append(winners, r)
		case !errors.Is(r.err, arbiter.ErrConflict):
			t.Fatalf("generation %d: racer %d: CompareAndSwap(%q) error = %v, want nil or ErrConflict", generation, i, from, r.err)
		}
	}
	if len(winners) != 1 {
		t.Fatalf("generation %d: %d racers won the swap from %q, want exactly 1", generation, len(winners), from)
	}
	return winners[0].record, winners[0].version
}

func record(generation uint64, survivor string) arbiter.Record {
	return arbiter.Record{
		SchemaVersion:    arbiter.SchemaVersion,
		ClusterID:        "e09a2cd26e182b15",
		Generation:       generation,
		Survivor:         survivor,
		SurvivorMemberID: "2a1a388f4c496b3e",
		// Stores keep time at different precisions; whole seconds in UTC
		// survive all of them.
		Since:       time.Now().UTC().Truncate(time.Second),
		TwokVersion: "test",
		Reason:      "no-leader",
	}
}

func mustSwap(t *testing.T, a arbiter.Arbiter, expected arbiter.Version, r arbiter.Record) arbiter.Version {
	t.Helper()
	v, err := a.CompareAndSwap(context.Background(), expected, r)
	if err != nil {
		t.Fatalf("CompareAndSwap(%q) error = %v", expected, err)
	}
	return v
}

// expectRecord checks that a Read returns want at version v: a successful
// swap must be visible to every later read.
func expectRecord(t *testing.T, a arbiter.Arbiter, want arbiter.Record, v arbiter.Version) {
	t.Helper()
	got, gotV, err := a.Read(context.Background())
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if gotV != v {
		t.Errorf("Read() version = %q, want %q", gotV, v)
	}
	if !got.Since.Equal(want.Since) {
		t.Errorf("Read() Since = %v, want %v", got.Since, want.Since)
	}
	got.Since, want.Since = time.Time{}, time.Time{}
	if got != want {
		t.Errorf("Read() record = %+v, want %+v", got, want)
	}
}

var (
	urlPattern     = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.-]*://[^\s]+`)
	secretName     = regexp.MustCompile(`(?i)(secret|token|password|passwd|credential|signature|session|private)`)
	minSecretBytes = 8
)

func expectSafeString(t *testing.T, s string) {
	t.Helper()
	if s == "" {
		t.Fatal("String() is empty")
	}
	for _, raw := range urlPattern.FindAllString(s, -1) {
		u, err := url.Parse(raw)
		if err != nil {
			continue
		}
		if _, hasPassword := u.User.Password(); hasPassword {
			t.Errorf("String() = %q has a password in a URL", s)
		}
		for key := range u.Query() {
			if secretName.MatchString(key) || strings.HasPrefix(strings.ToLower(key), "x-amz-") {
				t.Errorf("String() = %q has the query parameter %q", s, key)
			}
		}
	}
	for _, kv := range os.Environ() {
		name, value, _ := strings.Cut(kv, "=")
		if secretName.MatchString(name) && len(value) >= minSecretBytes && strings.Contains(s, value) {
			t.Errorf("String() contains the value of $%s", name)
		}
	}
}
