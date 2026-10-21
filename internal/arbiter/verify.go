package arbiter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// ProbeSuffix is appended to the record's key, path or name to get the key
// of the probe record Verify writes. The probe lives next to the record, so
// it is subject to the same permissions and the same store behaviour.
const ProbeSuffix = ".probe"

// probeAttempts bounds the retries when another member verifies the same
// store at the same time. Both members of a cluster start at about the same
// moment and share one probe key, so their probe writes may interleave.
const probeAttempts = 5

// VerifyProbe implements Verify for a backend: probe is an Arbiter of the
// same backend for the record's key with ProbeSuffix appended.
//
// It checks the two properties every claim depends on, on the probe record
// only:
//
//   - a create, a CompareAndSwap expecting Zero, fails with ErrConflict when
//     the record exists;
//   - a CompareAndSwap expecting a version that is no longer current fails
//     with ErrConflict.
//
// A store that accepts either write instead is reported as ErrUnsupported:
// it ignores the condition, and two members could both win a claim.
//
// The probe record is left in place. Deleting it would need a delete
// permission that twok otherwise never uses, and some stores reject or
// ignore conditional deletes, so a cleanup could fail a store that is
// perfectly good for claims. A probe left over from an earlier start is
// taken over by its version, which is itself part of the check.
//
// Both members may verify at the same time; every step tolerates the other
// member's writes, and no step depends on the probe staying unchanged
// between two calls.
func VerifyProbe(ctx context.Context, probe Arbiter) error {
	current, err := ensureProbe(ctx, probe)
	if err != nil {
		return err
	}

	// The probe exists from here on, and nothing ever deletes it, so a
	// second create must fail whatever the other member does meanwhile.
	if _, err := probe.CompareAndSwap(ctx, Zero, probeRecord()); err == nil {
		return fmt.Errorf("%w: %s: a second create of the probe record succeeded", ErrUnsupported, probe)
	} else if !errors.Is(err, ErrConflict) {
		return fmt.Errorf("probe create: %w", err)
	}

	// Move the probe forward, so that the version read before is stale.
	// Every probe record carries a fresh nonce, so a store whose version is
	// derived from the content cannot return to the old version either.
	for attempt := 0; ; attempt++ {
		next, err := probe.CompareAndSwap(ctx, current, probeRecord())
		if err == nil {
			if next == current {
				return fmt.Errorf("%w: %s: the version did not change on a write", ErrUnsupported, probe)
			}
			break
		}
		if !errors.Is(err, ErrConflict) || attempt+1 == probeAttempts {
			return fmt.Errorf("probe update: %w", err)
		}
		if _, current, err = probe.Read(ctx); err != nil {
			return fmt.Errorf("probe read: %w", err)
		}
	}

	// current is the version the successful write replaced.
	if _, err := probe.CompareAndSwap(ctx, current, probeRecord()); err == nil {
		return fmt.Errorf("%w: %s: a write with a stale version succeeded", ErrUnsupported, probe)
	} else if !errors.Is(err, ErrConflict) {
		return fmt.Errorf("probe stale update: %w", err)
	}
	return nil
}

// ensureProbe creates the probe record, or reads it when it already exists,
// and returns its current version.
func ensureProbe(ctx context.Context, probe Arbiter) (Version, error) {
	for attempt := 0; attempt < probeAttempts; attempt++ {
		v, err := probe.CompareAndSwap(ctx, Zero, probeRecord())
		switch {
		case err == nil:
			if v == Zero {
				return Zero, fmt.Errorf("%w: %s: a create returned the zero version", ErrUnsupported, probe)
			}
			return v, nil
		case !errors.Is(err, ErrConflict):
			return Zero, fmt.Errorf("probe create: %w", err)
		}
		_, v, err = probe.Read(ctx)
		switch {
		case err == nil:
			return v, nil
		case !errors.Is(err, ErrNotFound):
			return Zero, fmt.Errorf("probe read: %w", err)
		}
		// A create that conflicts on a record that does not exist is
		// either a store that rejects every create or a race with a
		// delete nobody should be doing. Try again; persistent failure
		// ends in the error below.
	}
	return Zero, fmt.Errorf("%w: %s: the probe record could neither be created nor read", ErrUnsupported, probe)
}

// probeRecord is a record that can never be mistaken for a cluster's: it
// has generation 0 and names no member, and its nonce makes every probe
// write distinct in content.
func probeRecord() Record {
	nonce := make([]byte, 8)
	_, _ = rand.Read(nonce)
	return Record{
		SchemaVersion:    SchemaVersion,
		Survivor:         "twok-probe",
		SurvivorMemberID: hex.EncodeToString(nonce),
		Since:            time.Now().UTC(),
		Reason:           "probe",
	}
}
