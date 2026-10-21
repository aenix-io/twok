// Package version holds the version of the twok binary.
package version

// Version is set at build time from git describe, through the linker flag
// -X github.com/aenix-io/twok/internal/version.Version. It goes into the
// arbiter record of every claim, so that a record says which twok wrote it.
var Version = "dev"
