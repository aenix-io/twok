package etcdargs

import (
	"fmt"
	"slices"
	"strings"
)

// TwokPrefix starts the flags that are twok's own when twok runs in place
// of etcd. An image that installs twok as the etcd binary is configured
// through the etcd command line its platform builds, so twok's settings
// travel in it as --twok-<name> flags and are removed before etcd starts.
const TwokPrefix = "twok-"

// EnvPrefix starts the environment variables that set twok's settings when
// no --twok-<name> flag does: --twok-claim-after reads TWOK_CLAIM_AFTER.
const EnvPrefix = "TWOK_"

// TwokFlags are the --twok-<name> flags of a command line, by name without
// the prefix. A flag given without a value is "true".
type TwokFlags map[string]string

// SplitTwok removes the --twok-<name> flags from a command line, binary
// first, and returns them with the rest of the command line, whose order is
// kept. It accepts --twok-<name>=value, --twok-<name> value and a single
// dash, as etcd's parser does for etcd's flags; the last of repeated flags
// wins.
//
// A --twok-<name> followed by an argument that does not start with a dash
// takes it as the value: etcd accepts no positional arguments, so that
// argument cannot be anything else. Followed by a flag, or last, it is a
// boolean set to "true". The separate value of an etcd flag is never taken
// for a twok flag, and arguments after "--" are left alone.
func SplitTwok(cmd []string) (TwokFlags, []string, error) {
	flags := TwokFlags{}
	if len(cmd) == 0 {
		return flags, nil, nil
	}
	rest := []string{cmd[0]}
	for i := 1; i < len(cmd); i++ {
		arg := cmd[i]
		if arg == "--" {
			rest = append(rest, cmd[i:]...)
			break
		}
		trimmed := strings.TrimPrefix(strings.TrimPrefix(arg, "-"), "-")
		body, ok := strings.CutPrefix(trimmed, TwokPrefix)
		if !strings.HasPrefix(arg, "-") || !ok {
			rest = append(rest, arg)
			// The value of an etcd flag in the separate form is never a
			// twok flag, whatever it looks like.
			if name, _, hasValue := strings.Cut(trimmed, "="); strings.HasPrefix(arg, "-") && !hasValue && !boolFlags[name] && i+1 < len(cmd) {
				i++
				rest = append(rest, cmd[i])
			}
			continue
		}
		name, value, hasValue := strings.Cut(body, "=")
		if name == "" {
			return nil, nil, fmt.Errorf("bad flag syntax: %s", arg)
		}
		switch {
		case hasValue:
		case i+1 < len(cmd) && !strings.HasPrefix(cmd[i+1], "-"):
			i++
			value = cmd[i]
		default:
			value = "true"
		}
		flags[name] = value
	}
	return flags, slices.Clip(rest), nil
}

// Lookup returns the setting name from its flag, or else from the
// TWOK_<NAME> environment variable through lookupEnv, which is
// os.LookupEnv outside tests. The flag wins over the environment, so a
// command line can override what a whole image or Pod sets.
func (f TwokFlags) Lookup(name string, lookupEnv func(string) (string, bool)) (string, bool) {
	if v, ok := f[name]; ok {
		return v, true
	}
	if lookupEnv == nil {
		return "", false
	}
	return lookupEnv(EnvName(name))
}

// EnvName is the environment variable of a setting: claim-after is
// TWOK_CLAIM_AFTER.
func EnvName(name string) string {
	return EnvPrefix + strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
}
