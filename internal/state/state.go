// Package state reads and writes the member's state file: the generation
// its data directory belongs to and whether that data is a legitimate copy
// or is being rebuilt from the survivor.
//
// The file lives on the same volume as the etcd data directory and moves
// with it. It is the member's half of every safety decision, so a file that
// cannot be read is an error, never a fresh start: a corrupted file read as
// generation 0 would let stale data claim the cluster.
package state

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"

	"github.com/aenix-io/twok/internal/atomicfile"
)

// SchemaVersion is the version of the state file a twok of this version
// writes. Readers ignore unknown fields; a newer major version is refused.
const SchemaVersion = 1

// Role says what the data directory holds.
type Role string

const (
	// RoleMember means the data directory is a legitimate copy of the
	// cluster at the generation: the member was a voter at it, or became
	// one by rejoining.
	RoleMember Role = "member"
	// RoleJoining means the data directory was set aside and the member is
	// being rebuilt from the survivor of the generation.
	RoleJoining Role = "joining"
)

// State is the content of the state file.
type State struct {
	Generation    uint64 `json:"generation"`
	Role          Role   `json:"role"`
	SchemaVersion int    `json:"schemaVersion"`
}

// Validate checks the fields a reader acts on.
func (s State) Validate() error {
	switch s.Role {
	case RoleMember, RoleJoining:
	case "":
		return errors.New("state: role is missing")
	default:
		return fmt.Errorf("state: unknown role %q, want %q or %q", s.Role, RoleMember, RoleJoining)
	}
	switch {
	case s.SchemaVersion < 1:
		return fmt.Errorf("state: schemaVersion %d is invalid", s.SchemaVersion)
	case s.SchemaVersion > SchemaVersion:
		return fmt.Errorf("state: schemaVersion %d is newer than %d, written by a newer twok", s.SchemaVersion, SchemaVersion)
	}
	return nil
}

// Read returns the state in path. A missing file is the state of a member
// that was never through a takeover: generation 0, a legitimate member,
// and exists false. Every other failure, including a file that is empty,
// truncated or not valid, is an error.
func Read(path string) (s State, exists bool, err error) {
	data, err := os.ReadFile(path) // #nosec G304 -- the path is twok's own configuration
	if errors.Is(err, fs.ErrNotExist) {
		return State{Role: RoleMember, SchemaVersion: SchemaVersion}, false, nil
	}
	if err != nil {
		return State{}, false, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&s); err != nil {
		return State{}, true, fmt.Errorf("state: %s: %w", path, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return State{}, true, fmt.Errorf("state: %s: unexpected data after the JSON object", path)
	}
	if err := s.Validate(); err != nil {
		return State{}, true, fmt.Errorf("%w in %s", err, path)
	}
	return s, true, nil
}

// Write replaces the state in path atomically and durably: when Write
// returns, a crash leaves the new state on disk, and a crash during Write
// leaves the old one. The schema version is always this twok's.
func Write(path string, s State) error {
	s.SchemaVersion = SchemaVersion
	if err := s.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return atomicfile.Write(path, append(data, '\n'), 0o600)
}
