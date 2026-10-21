package main

import (
	"errors"

	"github.com/aenix-io/twok/internal/agent"
)

// errNotWired is returned until the etcd client and the WAL operations are
// connected: the agent and status need both, and they live in packages
// that are not part of this build yet.
var errNotWired = errors.New("this build has no etcd client and no WAL operations yet")

// dialMember connects to one member's client API.
func dialMember(string, tlsFiles) (agent.Client, error) {
	return nil, errNotWired
}

// newWAL returns the WAL operations.
func newWAL() (agent.WAL, error) {
	return nil, errNotWired
}

// inspectWAL describes the member's WAL for twok status.
func inspectWAL(string) (any, error) {
	return nil, errNotWired
}
