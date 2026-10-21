// Package arbiter defines the compare-and-swap register twok uses to break
// the tie between two etcd members, and the registry of its backends.
//
// A backend implements Arbiter for one URL scheme and registers itself from
// its init function. Adding a backend is adding a package; nothing in the
// core changes.
package arbiter

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"sync"
	"time"
)

// SchemaVersion is the version of Record a twok of this version writes.
// Readers ignore unknown fields; new fields are optional within a major
// version.
const SchemaVersion = 1

// Record is the arbiter's state for one cluster. It is small, has no data
// of the cluster in it, and is rewritten only by a claim.
type Record struct {
	SchemaVersion int `json:"schemaVersion"`
	// ClusterID is the etcd cluster ID in hex. A member whose cluster ID
	// differs from the record's refuses to act on it.
	ClusterID string `json:"clusterID,omitempty"`
	// Generation increases by one with every claim. A member may claim
	// only from the generation its own data belongs to.
	Generation uint64 `json:"generation"`
	// Survivor is the etcd member name that claimed this generation.
	Survivor string `json:"survivor,omitempty"`
	// SurvivorMemberID is the survivor's etcd member ID in hex at the time
	// of the claim.
	SurvivorMemberID string `json:"survivorMemberID,omitempty"`
	// Since is when the claim was written, as reported by the claimant's
	// clock. It is informational: no decision depends on it.
	Since time.Time `json:"since,omitempty"`
	// TwokVersion is the version of the twok that wrote the record.
	TwokVersion string `json:"twokVersion,omitempty"`
	// Reason says what triggered the claim: "no-leader" or "manual".
	Reason string `json:"reason,omitempty"`
}

// Version is an opaque token that identifies one state of the record in
// its store: an ETag, a mod revision, a resourceVersion. The zero Version
// means the record does not exist, and a CompareAndSwap expecting it creates
// the record.
type Version string

// Zero is the Version of a record that does not exist.
const Zero Version = ""

var (
	// ErrNotFound is returned by Read when the record does not exist. The
	// returned Version is then Zero.
	ErrNotFound = errors.New("arbiter: record not found")
	// ErrConflict is returned by CompareAndSwap when the current version is
	// not the expected one. The write did not happen.
	ErrConflict = errors.New("arbiter: version conflict")
	// ErrUnsupported is returned by Verify when the store does not honour
	// conditional writes.
	ErrUnsupported = errors.New("arbiter: store does not honour conditional writes")
)

// Arbiter is a compare-and-swap register for one cluster's record.
//
// Implementations must be linearizable: after CompareAndSwap returns a
// Version, every later Read returns at least that state, and two
// CompareAndSwap calls with the same expected Version cannot both succeed.
// On any error other than ErrConflict the caller must assume the write may
// have happened and Read again before retrying.
type Arbiter interface {
	// Read returns the record and its version, or ErrNotFound with Zero.
	Read(ctx context.Context) (Record, Version, error)
	// CompareAndSwap writes record if the current version is expected, and
	// returns the new version. It returns ErrConflict otherwise.
	CompareAndSwap(ctx context.Context, expected Version, record Record) (Version, error)
	// Verify checks, on a probe key next to the record, that the store
	// honours conditional writes: a second conditional create must fail
	// with ErrConflict. It returns ErrUnsupported when it does not.
	Verify(ctx context.Context) error
	// String describes the arbiter for logs, without credentials.
	String() string
}

// Factory builds an Arbiter from its URL. Credentials come from files or
// the environment the backend documents, never from the URL.
type Factory func(u *url.URL) (Arbiter, error)

var (
	mu        sync.RWMutex
	factories = make(map[string]Factory)
)

// Register adds a backend for a URL scheme. It panics on a duplicate, which
// is a programming error.
func Register(scheme string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := factories[scheme]; dup {
		panic(fmt.Sprintf("arbiter: scheme %q registered twice", scheme))
	}
	factories[scheme] = f
}

// Schemes lists the registered URL schemes, sorted.
func Schemes() []string {
	mu.RLock()
	defer mu.RUnlock()
	schemes := make([]string, 0, len(factories))
	for s := range factories {
		schemes = append(schemes, s)
	}
	sort.Strings(schemes)
	return schemes
}

// Open builds the arbiter for a URL from the registered backends.
func Open(rawURL string) (Arbiter, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("arbiter URL: %w", err)
	}
	mu.RLock()
	f, ok := factories[u.Scheme]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("arbiter: unknown scheme %q, known: %v", u.Scheme, Schemes())
	}
	return f(u)
}
