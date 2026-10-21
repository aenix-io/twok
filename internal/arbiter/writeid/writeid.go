// Package writeid tags each write to an arbiter store with a random
// identifier, so that a backend can tell its own write from someone
// else's when the answer to the write was lost or misleading.
//
// The ambiguity is real for stores reached over HTTP. A conditional PUT
// can be applied and its response lost to a timeout, and a client that
// retries it then receives 412 for its own successful write. Without a way
// to recognise the stored object, the backend would report a conflict for
// a claim it won, and the arbiter would be stuck on a generation that no
// member believes it holds. With the identifier, the backend reads the
// object back and decides by content.
package writeid

import (
	"crypto/rand"
	"encoding/hex"

	"github.com/aenix-io/twok/internal/arbiter"
)

// Envelope is what a backend stores: the record's own fields with the
// write identifier next to them. Because the fields of Record are promoted,
// the stored JSON is still a valid Record for any reader that ignores
// unknown fields, as readers of Record must.
type Envelope struct {
	arbiter.Record
	// WriteID is random per CompareAndSwap call.
	WriteID string `json:"writeID,omitempty"`
}

// New returns a random identifier of 128 bits, which no two writes share
// by accident.
func New() string {
	var b [16]byte
	// crypto/rand.Read never returns an error since Go 1.24; it panics
	// instead if the system's random source fails.
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
