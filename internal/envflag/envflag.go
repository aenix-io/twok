// Package envflag lets every flag of a flag set fall back to an environment
// variable, so that a setting shared by a whole Pod or node can come from
// the environment and a single member can still override it on its command
// line.
package envflag

import (
	"flag"
	"fmt"
	"strings"
)

// Prefix starts the environment variable of every twok setting.
const Prefix = "TWOK_"

// Name is the environment variable of a flag: claim-after is
// TWOK_CLAIM_AFTER.
func Name(flagName string) string {
	return Prefix + strings.ToUpper(strings.ReplaceAll(flagName, "-", "_"))
}

// Apply sets every flag of fs that was not given on the command line from
// its environment variable, read through lookupEnv, which is os.LookupEnv
// outside tests. Call it after fs.Parse. A flag given on the command line
// wins over the environment; a variable that is set but empty still counts
// as set. A value the flag rejects is an error that names the variable.
func Apply(fs *flag.FlagSet, lookupEnv func(string) (string, bool)) error {
	given := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { given[f.Name] = true })
	var err error
	fs.VisitAll(func(f *flag.Flag) {
		if err != nil || given[f.Name] {
			return
		}
		value, ok := lookupEnv(Name(f.Name))
		if !ok {
			return
		}
		if setErr := fs.Set(f.Name, value); setErr != nil {
			err = fmt.Errorf("%s: %w", Name(f.Name), setErr)
		}
	})
	return err
}
