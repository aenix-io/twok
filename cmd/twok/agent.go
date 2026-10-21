package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/aenix-io/twok/internal/agent"
	"github.com/aenix-io/twok/internal/arbiter"
	"github.com/aenix-io/twok/internal/config"
)

const (
	// retryDelay paces restarts of an agent that could not start. The
	// sidecar never exits on such errors: a sidecar that is not running
	// makes the whole Pod, and so the etcd member, not ready.
	retryDelay = 10 * time.Second
	// remindAfter repeats an error that keeps the agent from starting, so
	// that it stays visible in recent logs.
	remindAfter = 10 * time.Minute
)

// memberFlags are the flags that say how to reach the local member.
type memberFlags struct {
	endpoint string
	tls      tlsFiles
	dataDir  string
}

// tlsFiles are the client certificate files for etcd.
type tlsFiles struct {
	certFile string
	keyFile  string
	caFile   string
}

func (m *memberFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&m.endpoint, "endpoint", "https://127.0.0.1:2379", "client URL of the local etcd member")
	fs.StringVar(&m.tls.certFile, "cert-file", "", "client certificate for etcd")
	fs.StringVar(&m.tls.keyFile, "key-file", "", "client key for etcd")
	fs.StringVar(&m.tls.caFile, "trusted-ca-file", "", "CA that signed the etcd server certificates")
	fs.StringVar(&m.dataDir, "data-dir", "", "the member's data directory, as etcd's --data-dir names it")
}

type agentOptions struct {
	member        memberFlags
	arbiter       string
	peerEndpoints []string
	configDir     string
	metricsListen string
	// shared are the shared settings given as flags or through the
	// environment, by name. They win over the ConfigMap.
	shared map[string]string
}

func parseAgentOptions(args []string, e env) (agentOptions, int, bool) {
	opts := agentOptions{shared: map[string]string{}}
	fs := flag.NewFlagSet("agent", flag.ContinueOnError)
	opts.member.register(fs)
	fs.StringVar(&opts.arbiter, "arbiter", "", "the arbiter URL, see docs/design.md")
	peerEndpoints := func(value string) error {
		for ep := range strings.SplitSeq(value, ",") {
			if ep = strings.TrimSpace(ep); ep != "" {
				opts.peerEndpoints = append(opts.peerEndpoints, ep)
			}
		}
		return nil
	}
	fs.Func("peer-endpoints", "comma-separated client URLs that lead to the other member, for a rejoin when the member list does not tell;"+
		" a host may be a name that resolves to both members, such as a headless Service", peerEndpoints)
	fs.Func("peer-endpoint", "the same as --peer-endpoints, kept for compatibility", peerEndpoints)
	fs.StringVar(&opts.configDir, "config-dir", "", "directory of the mounted twok ConfigMap, read again before every poll")
	fs.StringVar(&opts.metricsListen, "metrics-listen", "", "address for the Prometheus endpoint; empty disables it")
	defaults := config.Default()
	for _, name := range config.Names() {
		fs.Func(name, sharedUsage(name, defaults), func(value string) error {
			probe := config.Default()
			if err := probe.Set(name, value); err != nil {
				return err
			}
			opts.shared[name] = value
			return nil
		})
	}
	if code, ok := parse(fs, args, e); !ok {
		return agentOptions{}, code, false
	}
	if !required(e, "agent", [2]string{"arbiter", opts.arbiter}, [2]string{"data-dir", opts.member.dataDir}) {
		return agentOptions{}, exitInvalid, false
	}
	return opts, exitOK, true
}

func sharedUsage(name string, d config.Config) string {
	defaults := map[string]string{
		"mode":                 string(d.Mode),
		"poll-interval":        d.PollInterval.String(),
		"claim-after":          d.ClaimAfter.String(),
		"claim-delay":          d.ClaimDelay.String(),
		"follower-extra-delay": d.FollowerExtraDelay.String(),
	}
	return fmt.Sprintf("shared setting, default %s; wins over the same key in --config-dir", defaults[name])
}

// load reads the shared settings: the ConfigMap when there is one, then
// the flags on top.
func (o agentOptions) load() (config.Config, error) {
	cfg := config.Default()
	if o.configDir != "" {
		var err error
		if cfg, err = config.FromDir(os.DirFS(o.configDir)); err != nil {
			return config.Config{}, err
		}
	}
	for _, name := range config.Names() {
		if v, ok := o.shared[name]; ok {
			if err := cfg.Set(name, v); err != nil {
				return config.Config{}, err
			}
		}
	}
	return cfg, nil
}

func runAgent(ctx context.Context, args []string, e env) int {
	logger := newLogger(e.stderr).With("component", "agent")
	opts, code, ok := parseAgentOptions(args, e)
	if !ok {
		return code
	}
	keepRunning(ctx, func(ctx context.Context) error { return startAgent(ctx, opts, logger) }, logger, retryDelay, remindAfter)
	return exitOK
}

// startAgent wires the agent to the real member, WAL, process and arbiter,
// and runs it until ctx is done.
func startAgent(ctx context.Context, opts agentOptions, logger *slog.Logger) error {
	arb, err := arbiter.Open(opts.arbiter)
	if err != nil {
		return err
	}
	local, err := dialMember(opts.member.endpoint, "", opts.member.tls)
	if err != nil {
		return err
	}
	defer func() { _ = local.Close() }()
	wal, err := newWAL()
	if err != nil {
		return err
	}
	// The agent starts off and takes its mode from the first reload, so
	// that nothing happens before the configuration is read.
	initial := config.Default()
	a := agent.New(agent.Options{
		Config: initial,
		Load:   opts.load,
		Local:  local,
		Dial: func(endpoint, serverName string) (agent.Client, error) {
			return dialMember(endpoint, serverName, opts.member.tls)
		},
		Arbiter:       arb,
		WAL:           wal,
		Process:       agent.NewProcProcess(opts.member.dataDir, logger),
		DataDir:       opts.member.dataDir,
		PeerEndpoints: opts.peerEndpoints,
		MetricsListen: opts.metricsListen,
		Logger:        logger,
	})
	return a.Run(ctx)
}

// keepRunning restarts run until ctx is done. An error that repeats, such
// as a missing certificate, is logged again only after remind.
func keepRunning(ctx context.Context, run func(context.Context) error, logger *slog.Logger, delay, remind time.Duration) {
	var last string
	var loggedAt time.Time
	for {
		err := run(ctx)
		if ctx.Err() != nil {
			return
		}
		switch {
		case err == nil:
			last = ""
			logger.Error("agent stopped unexpectedly, restarting", "after", delay.String())
		case err.Error() != last || time.Since(loggedAt) >= remind:
			last, loggedAt = err.Error(), time.Now()
			logger.Error("agent stopped, restarting", "error", err, "after", delay.String())
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}
