// Package probe implements the startup check that every arbiter backend
// runs before twok trusts it: some stores accept conditional headers and
// ignore them, and an arbiter that ignores its condition lets both members
// win the same generation. See decision 9 in docs/decisions.md.
//
// Every backend runs the same steps, so that "verified" means the same
// thing whatever the store.
package probe

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aenix-io/twok/internal/arbiter"
)

// Suffix is appended to the record's key to name the probe key. The probe
// lives next to the record so that it is covered by the same permissions.
const Suffix = ".probe"

// attempts bounds the loop that takes over an existing probe. The two
// members may verify at the same time and move the probe under each other,
// but not forever.
const attempts = 5

// Register is the part of arbiter.Arbiter the probe needs. A backend passes
// a copy of itself that points at the probe key.
type Register interface {
	Read(ctx context.Context) (arbiter.Record, arbiter.Version, error)
	CompareAndSwap(ctx context.Context, expected arbiter.Version, record arbiter.Record) (arbiter.Version, error)
}

// Verify checks that r honours conditional writes. wrong must be a
// non-zero Version, well-formed for the store, that the probe can never
// have; the backend picks it because only it knows the token's format.
//
// The probe is created, or taken over from its current version if a
// previous run left it, and then two writes that must fail are attempted:
// a second create, and a swap from the wrong version. If either succeeds
// the store ignores the condition and Verify returns ErrUnsupported. The
// probe is left in place, so that the next run finds it and exercises the
// takeover path as well.
func Verify(ctx context.Context, r Register, wrong arbiter.Version) error {
	if wrong == arbiter.Zero {
		return errors.New("probe: the wrong version must not be Zero")
	}
	rec := arbiter.Record{
		SchemaVersion: arbiter.SchemaVersion,
		Since:         time.Now().UTC(),
		Reason:        "probe",
	}
	if err := ensure(ctx, r, rec); err != nil {
		return err
	}
	if err := mustConflict(ctx, r, arbiter.Zero, rec, "a second create of the probe succeeded"); err != nil {
		return err
	}
	return mustConflict(ctx, r, wrong, rec, fmt.Sprintf("a swap of the probe from version %q, which it does not have, succeeded", wrong))
}

// ensure leaves the probe written by this run. A conflict on create is
// expected when an earlier run left the probe; any other error is not.
func ensure(ctx context.Context, r Register, rec arbiter.Record) error {
	for range attempts {
		_, err := r.CompareAndSwap(ctx, arbiter.Zero, rec)
		if err == nil {
			return nil
		}
		if !errors.Is(err, arbiter.ErrConflict) {
			return fmt.Errorf("probe: create: %w", err)
		}
		_, v, err := r.Read(ctx)
		if errors.Is(err, arbiter.ErrNotFound) {
			// Removed between the create and the read; create again.
			continue
		}
		if err != nil {
			return fmt.Errorf("probe: read: %w", err)
		}
		_, err = r.CompareAndSwap(ctx, v, rec)
		if err == nil {
			return nil
		}
		if !errors.Is(err, arbiter.ErrConflict) {
			return fmt.Errorf("probe: swap from %q: %w", v, err)
		}
		// The peer moved the probe in between; read it again.
	}
	return fmt.Errorf("probe: the probe kept changing in %d attempts", attempts)
}

func mustConflict(ctx context.Context, r Register, expected arbiter.Version, rec arbiter.Record, ignored string) error {
	_, err := r.CompareAndSwap(ctx, expected, rec)
	switch {
	case errors.Is(err, arbiter.ErrConflict):
		return nil
	case err == nil:
		return fmt.Errorf("%w: %s", arbiter.ErrUnsupported, ignored)
	default:
		return fmt.Errorf("probe: %w", err)
	}
}
