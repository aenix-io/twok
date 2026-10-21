// Command twok is the twok agent. This placeholder only prints the version,
// so that the build and the image exist before the agent does.
package main

import (
	"fmt"

	"github.com/aenix-io/twok/internal/version"
)

func main() {
	fmt.Printf("twok %s\n", version.Version)
}
