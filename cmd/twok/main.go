// Command twok runs next to an etcd member of a two-member cluster and
// breaks the tie through an external arbiter when the cluster loses its
// leader. Besides the agent it has the commands an operator needs around
// it: the member's status, the arbiter record, a manual claim, and the
// reference HTTP arbiter.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/aenix-io/twok/internal/envflag"
	"github.com/aenix-io/twok/internal/version"

	// The arbiter backends register their URL schemes.
	_ "github.com/aenix-io/twok/internal/arbiter/etcdarb"
	_ "github.com/aenix-io/twok/internal/arbiter/filearb"
	_ "github.com/aenix-io/twok/internal/arbiter/httparb"
	_ "github.com/aenix-io/twok/internal/arbiter/k8sarb"
	_ "github.com/aenix-io/twok/internal/arbiter/memarb"
	_ "github.com/aenix-io/twok/internal/arbiter/s3arb"
)

// Exit codes, as in threek: 0 for success and for --help, 1 for a command
// that failed, 2 for invalid arguments.
const (
	exitOK      = 0
	exitFailed  = 1
	exitInvalid = 2
)

const usage = `Usage: twok <command> [flags]

Commands:
  agent                 run next to an etcd member and break ties through the arbiter
  status                print the local member's role, generation and member list as JSON
  arbiter get           print the arbiter record and its version
  arbiter claim         move the generation forward by hand, for a double failure
  arbiter serve         run the reference HTTP arbiter
  version               print the version

Every flag can also be set through TWOK_<FLAG>, for example TWOK_ARBITER.
Run twok <command> --help for the flags of a command.
`

// env is what a command reads from its process.
type env struct {
	stdout    io.Writer
	stderr    io.Writer
	lookupEnv func(string) (string, bool)
}

func main() {
	e := env{stdout: os.Stdout, stderr: os.Stderr, lookupEnv: os.LookupEnv}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	code := run(ctx, os.Args[1:], e)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, e env) int {
	if len(args) == 0 {
		fmt.Fprint(e.stderr, usage)
		return exitInvalid
	}
	switch args[0] {
	case "agent":
		return runAgent(ctx, args[1:], e)
	case "status":
		return runStatus(ctx, args[1:], e)
	case "arbiter":
		return runArbiter(ctx, args[1:], e)
	case "version":
		fmt.Fprintf(e.stdout, "twok %s\n", version.Version)
		return exitOK
	case "help", "-h", "-help", "--help":
		fmt.Fprint(e.stdout, usage)
		return exitOK
	}
	fmt.Fprintf(e.stderr, "twok: unknown command %q\n\n%s", args[0], usage)
	return exitInvalid
}

func runArbiter(ctx context.Context, args []string, e env) int {
	if len(args) == 0 {
		fmt.Fprint(e.stderr, usage)
		return exitInvalid
	}
	switch args[0] {
	case "get":
		return runArbiterGet(ctx, args[1:], e)
	case "claim":
		return runArbiterClaim(ctx, args[1:], e)
	case "serve":
		return runArbiterServe(ctx, args[1:], e)
	}
	fmt.Fprintf(e.stderr, "twok: unknown command \"arbiter %s\"\n\n%s", args[0], usage)
	return exitInvalid
}

// parse parses a command's flags with the environment as fallback. It
// returns the exit code to end with when the command should not go on.
func parse(fs *flag.FlagSet, args []string, e env) (int, bool) {
	fs.SetOutput(e.stderr)
	err := fs.Parse(args)
	if err == nil {
		err = envflag.Apply(fs, e.lookupEnv)
		if err != nil {
			fmt.Fprintf(e.stderr, "twok %s: %v\n", fs.Name(), err)
		}
	}
	switch {
	case errors.Is(err, flag.ErrHelp):
		return exitOK, false
	case err != nil:
		return exitInvalid, false
	case fs.NArg() > 0:
		fmt.Fprintf(e.stderr, "twok %s: unexpected argument %q\n", fs.Name(), fs.Arg(0))
		return exitInvalid, false
	}
	return exitOK, true
}

// required reports the first empty value among the named flags.
func required(e env, command string, flags ...[2]string) bool {
	for _, f := range flags {
		if f[1] == "" {
			fmt.Fprintf(e.stderr, "twok %s: --%s is required\n", command, f[0])
			return false
		}
	}
	return true
}

func printJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func newLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, nil)).With("logger", "twok")
}
