package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type metrics struct {
	registry          *prometheus.Registry
	mode              *prometheus.GaugeVec
	configValid       prometheus.Gauge
	generation        prometheus.Gauge
	role              *prometheus.GaugeVec
	leaderSeen        prometheus.Gauge
	arbiterReachable  prometheus.Gauge
	arbiterVerified   prometheus.Gauge
	claims            *prometheus.CounterVec
	rejoins           *prometheus.CounterVec
	actions           *prometheus.CounterVec
	pollFailures      prometheus.Counter
	lastPoll          prometheus.Gauge
	processControlled prometheus.Gauge
}

func newMetrics() *metrics {
	m := &metrics{
		registry: prometheus.NewRegistry(),
		mode: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "twok_mode",
			Help: "The agent's current mode: 1 for the active one.",
		}, []string{"mode"}),
		configValid: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "twok_config_valid",
			Help: "Whether the configuration read last was valid; 0 means the agent keeps the previous one.",
		}),
		generation: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "twok_generation",
			Help: "The local generation, the value of /twok/generation in the member's data.",
		}),
		role: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "twok_role",
			Help: "1 for the current role: pair, isolated, solo, stale, joining or unknown.",
		}, []string{"role"}),
		leaderSeen: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "twok_leader_seen",
			Help: "1 when the last poll reported a leader.",
		}),
		arbiterReachable: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "twok_arbiter_reachable",
			Help: "1 when the last arbiter read succeeded.",
		}),
		arbiterVerified: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "twok_arbiter_verified",
			Help: "1 once the arbiter passed its conditional-write check; no action is taken before.",
		}),
		claims: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "twok_claims_total",
			Help: "Claims attempted, by result: won, lost, error.",
		}, []string{"result"}),
		rejoins: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "twok_rejoins_total",
			Help: "Rejoins attempted, by result: success, error.",
		}, []string{"result"}),
		actions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "twok_actions_total",
			Help: "Actions executed, by action and result: success, error. Dry-run actions are counted with result dry-run, once per decision.",
		}, []string{"action", "result"}),
		pollFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "twok_poll_failures_total",
			Help: "Polls in which the local member did not answer.",
		}),
		lastPoll: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "twok_last_poll_timestamp_seconds",
			Help: "Unix time of the last successful poll.",
		}),
		processControlled: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "twok_process_controlled",
			Help: "1 when twok has found the etcd process and can stop it.",
		}),
	}
	m.registry.MustRegister(
		m.mode, m.configValid, m.generation, m.role, m.leaderSeen, m.arbiterReachable, m.arbiterVerified,
		m.claims, m.rejoins, m.actions, m.pollFailures, m.lastPoll, m.processControlled,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	// Every series exists from the start, so that a dashboard sees zero
	// rather than nothing.
	for _, r := range []string{"won", "lost", "error"} {
		m.claims.WithLabelValues(r)
	}
	for _, r := range []string{"success", "error"} {
		m.rejoins.WithLabelValues(r)
	}
	return m
}

// serve exposes the metrics on addr until stop is called.
func (m *metrics) serve(ctx context.Context, addr string) (stop func(), err error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics listener: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(l)
	}()
	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			_ = srv.Close()
		}
		<-done
	}, nil
}
