//go:build e2e

package e2e

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/aenix-io/twok/internal/arbiter"
	_ "github.com/aenix-io/twok/internal/arbiter/httparb"
	"github.com/aenix-io/twok/internal/etcdtest"
)

const (
	nettoolsBase = "docker.io/library/alpine:3.24.2@sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6"
	nettools     = "twok-e2e-nettools"
	pki          = "/etc/etcd/pki"
	dataDir      = "/var/lib/etcd"
	// user is the one non-root user of etcd and twok, as in the
	// StatefulSet: twok may signal etcd because it is the same user.
	user        = "65532:65532"
	arbiterPath = "/e2e"
	// putTimeout bounds every client write.
	putTimeout = time.Second
	// putInterval is the pause between two writes of one writer.
	putInterval = 200 * time.Millisecond
	// settle is how long after a cut a write may still be acknowledged:
	// a put that started before the rules were in place.
	settle = time.Second
)

var image, etcdImage string

func TestMain(m *testing.M) {
	image, etcdImage = os.Getenv("E2E_IMAGE"), os.Getenv("ETCD_IMAGE")
	if image == "" || etcdImage == "" {
		fmt.Println("E2E_IMAGE or ETCD_IMAGE is not set, skipping end-to-end tests")
		os.Exit(0)
	}
	build := exec.Command("docker", "build", "--quiet", "--tag", nettools, "-")
	build.Stdin = strings.NewReader("FROM " + nettoolsBase + "\nRUN apk add --no-cache iptables\n")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		fmt.Println("build network tools image:", err)
		os.Exit(1)
	}
	// Pulled once here rather than by five tests at the same time.
	pull := exec.Command("docker", "pull", "--quiet", etcdImage)
	pull.Stderr = os.Stderr
	if err := pull.Run(); err != nil {
		// A local image that is not in a registry is fine.
		fmt.Println("pull", etcdImage+":", err)
	}
	os.Exit(m.Run())
}

func docker(args ...string) (string, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("docker", args...)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(stdout.String()), nil
}

func mustDocker(t *testing.T, args ...string) string {
	t.Helper()
	out, err := docker(args...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

// member is one etcd member with its twok agent, laid out as a Pod: a pod
// container holds the network and PID namespaces, as the pause container
// does for the kubelet, and the etcd and twok containers join both. etcd
// runs with --restart=always, so that docker starts it again after twok
// stopped it, as the kubelet does. The namespaces cannot belong to the etcd
// container itself: when etcd, the init process of its PID namespace,
// exits, the kernel kills every other process in that namespace, twok
// included, in the middle of the takeover it stopped etcd for.
type member struct {
	name        string // etcd --name, DNS alias on the network, certificate name
	pod         string // the container that holds the namespaces; nettools image
	etcd        string
	agent       string
	volume      string
	clientPort  int
	metricsPort int
	tls         etcdtest.TLSFiles
	client      *clientv3.Client
	writer      *writer
}

type cluster struct {
	t           *testing.T
	ctx         context.Context
	prefix      string
	network     string
	pkiDir      string
	arbiter     string
	arbiterPort int
	arb         arbiter.Arbiter
	members     []*member
	tl          *timeline
	httpClient  *http.Client
}

// start runs two members with their agents in mode and the arbiter, and
// waits until the pair has a leader and both agents are ready.
func start(t *testing.T, mode string) *cluster {
	t.Helper()
	suffix := make([]byte, 3)
	_, _ = rand.Read(suffix)
	prefix := "twok-e2e-" + hex.EncodeToString(suffix)
	ctx, cancel := context.WithCancel(context.Background())
	c := &cluster{
		t: t, ctx: ctx, prefix: prefix,
		tl:         newTimeline(),
		httpClient: &http.Client{Timeout: time.Second},
	}
	// Cleanups run last in, first out: the containers go before this.
	t.Cleanup(func() {
		cancel()
		t.Logf("timeline of %s (seconds from the first cut):\n%s", t.Name(), c.timeline())
	})

	// The containers run as 65532 and read the certificates from a bind
	// mount, so the directory and the files must be readable by others.
	c.pkiDir = t.TempDir()
	ca := etcdtest.NewCA(t, c.pkiDir)
	for _, name := range []string{"a", "b"} {
		c.members = append(c.members, &member{
			name:        name,
			pod:         prefix + "-" + name,
			etcd:        prefix + "-" + name + "-etcd",
			agent:       prefix + "-" + name + "-twok",
			volume:      prefix + "-" + name + "-data",
			clientPort:  freePort(t),
			metricsPort: freePort(t),
			tls:         ca.Issue(t, name),
		})
	}
	entries, err := os.ReadDir(c.pkiDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(c.pkiDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if err := os.Chmod(c.pkiDir+"/"+e.Name(), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	c.network = mustDocker(t, "network", "create", prefix)
	t.Cleanup(func() { _, _ = docker("network", "rm", c.network) })
	c.startArbiter()

	var initial []string
	for _, m := range c.members {
		initial = append(initial, fmt.Sprintf("%s=https://%s:2380", m.name, m.name))
	}
	for _, m := range c.members {
		c.startMember(m, strings.Join(initial, ","), mode)
	}

	// Runs first: the writers and the monitor stop before the containers go.
	t.Cleanup(cancel)

	eventually(t, 2*time.Minute, "the pair elects a leader", func() (bool, string) {
		id, ok := c.agreedLeader()
		return ok && id != 0, ""
	})
	eventually(t, 2*time.Minute, "both agents are ready", func() (bool, string) {
		var detail []string
		ready := true
		for _, m := range c.members {
			text, err := c.metrics(m)
			r, mo := role(text), metricLabel(text, "twok_mode", "mode")
			v := metricValue(text, "twok_arbiter_verified")
			p := metricValue(text, "twok_process_controlled")
			detail = append(detail, fmt.Sprintf("%s: role %s, mode %s, arbiter verified %g, process controlled %g, %v", m.name, r, mo, v, p, err))
			ready = ready && err == nil && r == "pair" && mo == mode && v == 1 && p == 1
		}
		return ready, strings.Join(detail, "; ")
	})
	return c
}

func (c *cluster) startArbiter() {
	t := c.t
	c.arbiter = c.prefix + "-arbiter"
	c.arbiterPort = freePort(t)
	mustDocker(t, "run", "--detach", "--name", c.arbiter,
		"--network", c.network, "--network-alias", "arbiter",
		"--user", user, "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--read-only",
		"--tmpfs", "/var/lib/twok-arbiter:uid=65532,gid=65532,mode=0700",
		"--publish", fmt.Sprintf("127.0.0.1:%d:8080", c.arbiterPort),
		image, "arbiter", "serve", "--dir=/var/lib/twok-arbiter", "--listen=:8080")
	t.Cleanup(func() {
		c.dumpLogs(c.arbiter, 40)
		_, _ = docker("rm", "--force", c.arbiter)
	})
	arb, err := arbiter.Open(fmt.Sprintf("http://127.0.0.1:%d%s", c.arbiterPort, arbiterPath))
	if err != nil {
		t.Fatal(err)
	}
	c.arb = arb
	eventually(t, 30*time.Second, "the arbiter answers", func() (bool, string) {
		_, err := c.record()
		return err == nil, fmt.Sprint(err)
	})
}

func (c *cluster) startMember(m *member, initialCluster, mode string) {
	t := c.t
	mustDocker(t, "volume", "create", m.volume)
	t.Cleanup(func() { _, _ = docker("volume", "rm", "--force", m.volume) })
	// A new volume belongs to root; etcd creates its files as 65532.
	mustDocker(t, "run", "--rm", "--volume", m.volume+":"+dataDir, nettools,
		"sh", "-c", "chown 65532:65532 "+dataDir+" && chmod 0700 "+dataDir)

	// The pod container: namespaces, the published ports, and the network
	// tools that cut the links. --init reaps what is left of a stopped etcd,
	// as the pause container does.
	mustDocker(t, "run", "--detach", "--init", "--name", m.pod,
		"--network", c.network, "--network-alias", m.name, "--hostname", m.name,
		"--cap-drop", "ALL", "--cap-add", "NET_ADMIN", "--cap-add", "NET_RAW",
		"--publish", fmt.Sprintf("127.0.0.1:%d:2379", m.clientPort),
		"--publish", fmt.Sprintf("127.0.0.1:%d:2390", m.metricsPort),
		nettools, "sleep", "2147483647")
	t.Cleanup(func() { _, _ = docker("rm", "--force", m.pod) })

	tls := func(prefix string) []string {
		return []string{
			prefix + "cert-file=" + pki + "/" + m.name + ".crt",
			prefix + "key-file=" + pki + "/" + m.name + ".key",
			prefix + "trusted-ca-file=" + pki + "/ca.crt",
		}
	}
	shared := []string{
		"--network", "container:" + m.pod, "--pid", "container:" + m.pod,
		"--user", user, "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--read-only",
		"--restart", "always",
		"--volume", m.volume + ":" + dataDir,
		"--volume", c.pkiDir + ":" + pki + ":ro",
	}

	etcd := append([]string{"run", "--detach", "--name", m.etcd}, shared...)
	etcd = append(etcd, etcdImage, "/usr/local/bin/etcd",
		"--name="+m.name,
		"--data-dir="+dataDir,
		"--listen-client-urls=https://0.0.0.0:2379",
		"--advertise-client-urls=https://"+m.name+":2379",
		"--listen-peer-urls=https://0.0.0.0:2380",
		"--initial-advertise-peer-urls=https://"+m.name+":2380",
		"--initial-cluster="+initialCluster,
		"--initial-cluster-state=new",
		"--initial-cluster-token="+c.prefix,
		"--client-cert-auth=true",
		"--peer-client-cert-auth=true",
	)
	etcd = append(etcd, tls("--")...)
	etcd = append(etcd, tls("--peer-")...)
	mustDocker(t, etcd...)
	t.Cleanup(func() {
		if n, err := docker("inspect", "--format", "{{.RestartCount}}", m.etcd); err == nil {
			c.tl.add(time.Now(), "%s: docker restarted etcd %s times in all", m.name, n)
		}
		c.dumpLogs(m.etcd, 80)
		_, _ = docker("rm", "--force", m.etcd)
	})

	agent := append([]string{"run", "--detach", "--name", m.agent}, shared...)
	agent = append(agent, image, "agent",
		"--arbiter=http://arbiter:8080"+arbiterPath,
		"--endpoint=https://127.0.0.1:2379",
		"--data-dir="+dataDir,
		"--mode="+mode,
		"--metrics-listen=:2390",
	)
	agent = append(agent, tls("--")...)
	mustDocker(t, agent...)
	t.Cleanup(func() {
		c.agentEvents(m)
		c.dumpLogs(m.agent, 120)
		_, _ = docker("rm", "--force", m.agent)
	})

	info := transport.TLSInfo{CertFile: m.tls.CertFile, KeyFile: m.tls.KeyFile, TrustedCAFile: m.tls.CAFile}
	tlsCfg, err := info.ClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	m.client, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{fmt.Sprintf("https://127.0.0.1:%d", m.clientPort)},
		DialTimeout: time.Second,
		TLS:         tlsCfg,
		Logger:      zap.NewNop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.client.Close() })
}

func (c *cluster) dumpLogs(container string, lines int) {
	if !c.t.Failed() {
		return
	}
	logs, _ := exec.Command("docker", "logs", "--tail", strconv.Itoa(lines), container).CombinedOutput()
	c.t.Logf("logs of %s:\n%s", container, logs)
}

func (c *cluster) member(name string) *member {
	for _, m := range c.members {
		if m.name == name {
			return m
		}
	}
	c.t.Fatalf("no member %s", name)
	return nil
}

func (c *cluster) other(m *member) *member {
	for _, x := range c.members {
		if x != m {
			return x
		}
	}
	return nil
}

// status reads the member's own view: its ID, the leader it sees, whether
// it is a learner.
func (c *cluster) status(m *member) (*clientv3.StatusResponse, error) {
	ctx, cancel := context.WithTimeout(c.ctx, time.Second)
	defer cancel()
	return m.client.Status(ctx, m.client.Endpoints()[0])
}

func (c *cluster) agreedLeader() (uint64, bool) {
	var leader uint64
	for _, m := range c.members {
		st, err := c.status(m)
		if err != nil || st.Leader == 0 || leader != 0 && st.Leader != leader {
			return 0, false
		}
		leader = st.Leader
	}
	return leader, true
}

// leader returns the member the pair agrees is the leader.
func (c *cluster) leader() *member {
	c.t.Helper()
	var id uint64
	eventually(c.t, 30*time.Second, "the members agree on a leader", func() (bool, string) {
		l, ok := c.agreedLeader()
		id = l
		return ok && l != 0, ""
	})
	for _, m := range c.members {
		st, err := c.status(m)
		if err == nil && st.Header.MemberId == id {
			return m
		}
	}
	c.t.Fatalf("leader %x is neither member", id)
	return nil
}

func (c *cluster) metrics(m *member) (string, error) {
	ctx, cancel := context.WithTimeout(c.ctx, time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/metrics", m.metricsPort), http.NoBody)
	if err != nil {
		return "", err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metrics of %s: %s", m.name, resp.Status)
	}
	return string(body), nil
}

// metricValue returns the value of one series, written as in the
// exposition format: name{labels}. A missing series is 0.
func metricValue(text, series string) float64 {
	for line := range strings.Lines(text) {
		rest, ok := strings.CutPrefix(line, series+" ")
		if !ok {
			continue
		}
		v, _ := strconv.ParseFloat(strings.TrimSpace(rest), 64)
		return v
	}
	return 0
}

// metricLabel returns the value of label on the series of name that is 1.
func metricLabel(text, name, label string) string {
	prefix := name + "{" + label + "=\""
	for line := range strings.Lines(text) {
		rest, ok := strings.CutPrefix(line, prefix)
		if !ok {
			continue
		}
		value, tail, ok := strings.Cut(rest, "\"} ")
		if ok && strings.TrimSpace(tail) == "1" {
			return value
		}
	}
	return ""
}

func role(text string) string {
	return metricLabel(text, "twok_role", "role")
}

// roleOf returns the role twok reports for m, or the error.
func (c *cluster) roleOf(m *member) string {
	text, err := c.metrics(m)
	if err != nil {
		return "unreachable"
	}
	return role(text)
}

// record reads the arbiter record from the host; a missing record is a
// zero record without error.
func (c *cluster) record() (arbiter.Record, error) {
	ctx, cancel := context.WithTimeout(c.ctx, 2*time.Second)
	defer cancel()
	rec, _, err := c.arb.Read(ctx)
	if errors.Is(err, arbiter.ErrNotFound) {
		return arbiter.Record{}, nil
	}
	return rec, err
}

// ip returns the address of m's pod container on the test network.
func (c *cluster) ip(container string) string {
	c.t.Helper()
	return mustDocker(c.t, "inspect", "--format",
		fmt.Sprintf(`{{(index .NetworkSettings.Networks %q).IPAddress}}`, c.network), container)
}

// rule is one iptables DROP rule in a member's network namespace.
type rule struct {
	m     *member
	chain string // INPUT or OUTPUT
	match string // -s or -d
	addr  string
}

func (c *cluster) iptables(op string, rules []rule) {
	c.t.Helper()
	byPod := map[*member][]string{}
	var order []*member
	for _, r := range rules {
		if _, ok := byPod[r.m]; !ok {
			order = append(order, r.m)
		}
		byPod[r.m] = append(byPod[r.m], fmt.Sprintf("iptables %s %s %s %s -j DROP", op, r.chain, r.match, r.addr))
	}
	for _, m := range order {
		mustDocker(c.t, "exec", m.pod, "sh", "-c", strings.Join(byPod[m], " && "))
	}
}

// cut inserts the rules and returns the time the last one was in place.
func (c *cluster) cut(rules []rule) time.Time {
	c.t.Helper()
	c.iptables("-I", rules)
	return time.Now()
}

func (c *cluster) restore(rules []rule) time.Time {
	c.t.Helper()
	c.iptables("-D", rules)
	return time.Now()
}

// twoWay drops every packet between x and y, without resets, like a broken
// WAN link.
func (c *cluster) twoWay(x, y *member) []rule {
	ipX, ipY := c.ip(x.pod), c.ip(y.pod)
	return []rule{
		{x, "INPUT", "-s", ipY},
		{x, "OUTPUT", "-d", ipY},
		{y, "INPUT", "-s", ipX},
		{y, "OUTPUT", "-d", ipX},
	}
}

// oneWay drops the packets from x to y only; y's packets reach x.
func (c *cluster) oneWay(x, y *member) []rule {
	ipX, ipY := c.ip(x.pod), c.ip(y.pod)
	return []rule{{x, "OUTPUT", "-d", ipY}, {y, "INPUT", "-s", ipX}}
}

// arbiterCut drops every packet between both members and the arbiter.
func (c *cluster) arbiterCut() []rule {
	ip := c.ip(c.arbiter)
	var rules []rule
	for _, m := range c.members {
		rules = append(rules, rule{m, "INPUT", "-s", ip}, rule{m, "OUTPUT", "-d", ip})
	}
	return rules
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok, detail := cond()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting: %s %s", timeout, what, detail)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// put is one client write.
type put struct {
	key        string
	start, end time.Time
	ok         bool
}

// writer writes w/<member>/<seq> to one member, a put every putInterval,
// each with putTimeout, and records the outcome of every put.
type writer struct {
	m    *member
	mu   sync.Mutex
	puts []put
}

// startWriters starts a writer per member and waits until both have had
// their writes acknowledged.
func (c *cluster) startWriters() {
	c.t.Helper()
	for _, m := range c.members {
		m.writer = &writer{m: m}
		go m.writer.run(c.ctx)
	}
	eventually(c.t, 30*time.Second, "both writers succeed", func() (bool, string) {
		return len(c.members[0].writer.acked()) >= 5 && len(c.members[1].writer.acked()) >= 5, ""
	})
}

func (w *writer) run(ctx context.Context) {
	for seq := 0; ctx.Err() == nil; seq++ {
		key := fmt.Sprintf("w/%s/%06d", w.m.name, seq)
		start := time.Now()
		pctx, cancel := context.WithTimeout(ctx, putTimeout)
		_, err := w.m.client.Put(pctx, key, start.UTC().Format(time.RFC3339Nano))
		cancel()
		if ctx.Err() != nil {
			return
		}
		w.mu.Lock()
		w.puts = append(w.puts, put{key: key, start: start, end: time.Now(), ok: err == nil})
		w.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(putInterval):
		}
	}
}

func (w *writer) snapshot() []put {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.puts)
}

func (w *writer) acked() []string {
	var keys []string
	for _, p := range w.snapshot() {
		if p.ok {
			keys = append(keys, p.key)
		}
	}
	return keys
}

// accepted returns the acknowledged puts that started in [from, to).
func (w *writer) accepted(from, to time.Time) []put {
	var out []put
	for _, p := range w.snapshot() {
		if p.ok && !p.start.Before(from) && p.start.Before(to) {
			out = append(out, p)
		}
	}
	return out
}

// waitWinner waits until a member acknowledges a put that started after
// from, and fails if both do.
func (c *cluster) waitWinner(from time.Time, timeout time.Duration) *member {
	c.t.Helper()
	var winner *member
	eventually(c.t, timeout, "one member accepts writes", func() (bool, string) {
		var accepting []*member
		for _, m := range c.members {
			if len(m.writer.accepted(from, time.Now().Add(time.Hour))) > 0 {
				accepting = append(accepting, m)
			}
		}
		if len(accepting) > 1 {
			c.t.Fatalf("both members accept writes after %s", from.Format(time.RFC3339Nano))
		}
		if len(accepting) == 1 {
			winner = accepting[0]
			return true, ""
		}
		return false, ""
	})
	return winner
}

// noWrites fails if m acknowledged a put that started in [from, to).
func (c *cluster) noWrites(m *member, from, to time.Time) {
	c.t.Helper()
	if got := m.writer.accepted(from, to); len(got) > 0 {
		c.t.Errorf("%s accepted %d writes between %s and %s, the first %s at %s", m.name, len(got),
			c.tl.rel(from), c.tl.rel(to), got[0].key, c.tl.rel(got[0].start))
	}
}

// watch keeps the cluster under observation for d, failing at once if
// cond fails.
func (c *cluster) watch(d time.Duration, what string, cond func() (bool, string)) {
	c.t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if ok, detail := cond(); !ok {
			c.t.Fatalf("violated: %s %s", what, detail)
		}
		time.Sleep(time.Second)
	}
}

// waitPair waits until survivor and loser form a pair of voters again and
// the loser accepts writes that started after since.
func (c *cluster) waitPair(survivor, loser *member, since time.Time, timeout time.Duration) {
	c.t.Helper()
	eventually(c.t, timeout, "the pair is restored", func() (bool, string) {
		ctx, cancel := context.WithTimeout(c.ctx, time.Second)
		defer cancel()
		ms, err := survivor.client.MemberList(ctx, clientv3.WithSerializable())
		if err != nil {
			return false, fmt.Sprintf("(member list of %s: %v)", survivor.name, err)
		}
		voters := 0
		for _, x := range ms.Members {
			if !x.IsLearner {
				voters++
			}
		}
		st, err := c.status(loser)
		if err != nil {
			return false, fmt.Sprintf("(%d members, %d voters; status of %s: %v)", len(ms.Members), voters, loser.name, err)
		}
		rs, rl := c.roleOf(survivor), c.roleOf(loser)
		writes := len(loser.writer.accepted(since, time.Now().Add(time.Hour)))
		ok := len(ms.Members) == 2 && voters == 2 && !st.IsLearner && st.Leader != 0 && writes > 0 && rs == "pair" && rl == "pair"
		return ok, fmt.Sprintf("(%d members, %d voters; %s learner %t leader %x; roles %s %s, %s %s; %d writes on %s)",
			len(ms.Members), voters, loser.name, st.IsLearner, st.Leader, survivor.name, rs, loser.name, rl, writes, loser.name)
	})
	c.tl.add(time.Now(), "the pair is restored: two voters, both agents in pair, %s accepts writes", loser.name)
}

// checkAcked stops the writers and checks that every acknowledged key is
// on each of members, with a linearizable read.
func (c *cluster) checkAcked(members ...*member) {
	c.t.Helper()
	var acked []string
	for _, m := range c.members {
		acked = append(acked, m.writer.acked()...)
	}
	for _, m := range members {
		ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
		resp, err := m.client.Get(ctx, "w/", clientv3.WithPrefix(), clientv3.WithKeysOnly())
		cancel()
		if err != nil {
			c.t.Errorf("read the keys from %s: %v", m.name, err)
			continue
		}
		have := map[string]bool{}
		for _, kv := range resp.Kvs {
			have[string(kv.Key)] = true
		}
		var missing []string
		for _, k := range acked {
			if !have[k] {
				missing = append(missing, k)
			}
		}
		if len(missing) > 0 {
			c.t.Errorf("%s lacks %d of %d acknowledged keys, the first %v", m.name, len(missing), len(acked), missing[:min(5, len(missing))])
		} else {
			c.t.Logf("%s holds all %d acknowledged keys (%d keys in all)", m.name, len(acked), len(resp.Kvs))
		}
	}
}

// expectRecord checks the arbiter record.
func (c *cluster) expectRecord(generation uint64, survivor string) {
	c.t.Helper()
	rec, err := c.record()
	if err != nil {
		c.t.Fatal(err)
	}
	if rec.Generation != generation || rec.Survivor != survivor {
		c.t.Errorf("arbiter record: generation %d, survivor %q; want generation %d, survivor %q",
			rec.Generation, rec.Survivor, generation, survivor)
	}
}

// monitor records in the timeline what each member reports as its leader
// and how the arbiter record changes, until the test ends.
func (c *cluster) monitor() {
	go func() {
		last := map[string]string{}
		note := func(key, value string) {
			if last[key] != value {
				last[key] = value
				c.tl.add(time.Now(), "%s", value)
			}
		}
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			for _, m := range c.members {
				st, err := c.status(m)
				switch {
				case err != nil:
					note(m.name, m.name+": etcd does not answer")
				case st.Leader == 0:
					note(m.name, m.name+": no leader")
				case st.Leader == st.Header.MemberId:
					note(m.name, fmt.Sprintf("%s: leader (member %x)", m.name, st.Header.MemberId))
				default:
					note(m.name, fmt.Sprintf("%s: follows %x (member %x, learner %t)", m.name, st.Leader, st.Header.MemberId, st.IsLearner))
				}
			}
			if rec, err := c.record(); err == nil {
				if rec.Generation == 0 {
					note("arbiter", "arbiter: no record")
				} else {
					note("arbiter", fmt.Sprintf("arbiter: generation %d, survivor %s", rec.Generation, rec.Survivor))
				}
			}
			select {
			case <-c.ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// agentEvents adds the agent's role changes and actions to the timeline,
// from its JSON log.
func (c *cluster) agentEvents(m *member) {
	out, err := exec.Command("docker", "logs", m.agent).CombinedOutput()
	if err != nil {
		return
	}
	sc := bufio.NewScanner(bytes.NewReader(out))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var line map[string]any
		if json.Unmarshal(sc.Bytes(), &line) != nil {
			continue
		}
		at, err := time.Parse(time.RFC3339Nano, fmt.Sprint(line["time"]))
		if err != nil {
			continue
		}
		msg := fmt.Sprint(line["msg"])
		switch {
		case msg == "decision" && line["from"] != nil:
			text := fmt.Sprintf("%s twok: %v -> %v, generation %v: %v", m.name, line["from"], line["role"], line["generation"], line["note"])
			if a, ok := line["action"]; ok {
				text += fmt.Sprintf(" (action %v)", a)
			}
			c.tl.add(at, "%s", text)
		case msg == "decision":
		case strings.Contains(msg, "metrics"), strings.Contains(msg, "arbiter honours"):
		default:
			text := m.name + " twok: " + msg
			for _, k := range []string{"error", "attempt", "pid"} {
				if v, ok := line[k]; ok {
					text += fmt.Sprintf(", %s %v", k, v)
				}
			}
			c.tl.add(at, "%s", text)
		}
	}
}

// writerEvents adds to the timeline the edges of each writer's outcomes:
// the first failure after a run of successes and the reverse.
func (c *cluster) writerEvents() {
	for _, m := range c.members {
		if m.writer == nil {
			continue
		}
		puts := m.writer.snapshot()
		for i, p := range puts {
			if i == 0 || p.ok == puts[i-1].ok {
				continue
			}
			if p.ok {
				c.tl.add(p.end, "%s: write accepted again (%s, started %s)", m.name, p.key, c.tl.rel(p.start))
			} else {
				c.tl.add(puts[i-1].end, "%s: last accepted write before failures (%s)", m.name, puts[i-1].key)
			}
		}
	}
}

func (c *cluster) timeline() string {
	c.writerEvents()
	return c.tl.String()
}

type event struct {
	at   time.Time
	what string
}

// timeline collects events with their wall-clock time; the containers run
// on the same host, so their logs share the clock.
type timeline struct {
	mu     sync.Mutex
	zero   time.Time
	marked bool
	events []event
}

func newTimeline() *timeline { return &timeline{zero: time.Now()} }

// mark records the event the offsets count from; later marks are plain
// events.
func (tl *timeline) mark(at time.Time, what string) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	if !tl.marked {
		tl.zero, tl.marked = at, true
	}
	tl.events = append(tl.events, event{at, what})
}

func (tl *timeline) add(at time.Time, format string, args ...any) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	tl.events = append(tl.events, event{at, fmt.Sprintf(format, args...)})
}

func (tl *timeline) rel(at time.Time) string {
	return fmt.Sprintf("%+.1fs", at.Sub(tl.zero).Seconds())
}

func (tl *timeline) String() string {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	sort.SliceStable(tl.events, func(i, j int) bool { return tl.events[i].at.Before(tl.events[j].at) })
	var b strings.Builder
	for _, e := range tl.events {
		fmt.Fprintf(&b, "  %8s  %s\n", tl.rel(e.at), e.what)
	}
	return b.String()
}
