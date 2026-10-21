//go:build kubernetes

package kubernetes

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// settle is how long after the policy is applied a put may still succeed
// without counting as a put during the partition: Cilium needs a moment to
// push a new policy to the endpoints, and a put that started just before may
// still be in flight.
const settle = 3 * time.Second

// TestPartition cuts the two members from each other in both directions.
func TestPartition(t *testing.T) {
	runPartition(t, "manifests/partition.yaml")
}

// TestPartitionOneWay cuts the connections from etcd-0 to etcd-1 only.
func TestPartitionOneWay(t *testing.T) {
	runPartition(t, "manifests/partition-one-way.yaml")
}

type put struct {
	seq        int
	ok         bool
	start, end int64 // Unix ms on the local clock
}

type status struct {
	at     int64 // Unix ms on the local clock
	member string
	leader uint64
	err    string
}

func ms(t time.Time) int64 { return t.UnixMilli() }

func runPartition(t *testing.T, policy string) {
	e := &env{t: t, log: &bytes.Buffer{}}
	e.logf("# %s, %s", t.Name(), time.Now().UTC().Format(time.RFC3339))
	e.logf("mode: %s", cfg.mode())
	e.logf("namespace: %s, partition for %s, watch for at least %s and at most %s after the heal, policy %s",
		cfg.namespace, cfg.partitionFor, cfg.healFor, cfg.healTimeout, policy)

	membersManifest := e.membersManifest()
	var helpers []string
	t.Cleanup(func() {
		if cfg.results != "" {
			if err := os.WriteFile(cfg.results, e.log.Bytes(), 0o600); err != nil {
				t.Error(err)
			}
		}
		if cfg.keep {
			t.Log("K8S_KEEP is set, leaving the objects in place")
			return
		}
		for _, h := range helpers {
			if err := e.deleteManifest(h); err != nil {
				t.Error(err)
			}
		}
		for _, f := range []string{"manifests/partition.yaml", "manifests/partition-one-way.yaml", "manifests/cut-arbiter.yaml"} {
			e.deleteFile(f)
		}
		e.deleteMembers(membersManifest)
	})

	// 1. Members.
	e.apply(membersManifest)
	e.waitReady(5*time.Minute, members...)
	nodes := map[string]string{}
	for _, m := range members {
		p := e.pod(m)
		nodes[m] = p.Spec.NodeName
		e.logf("%s: node %s, IP %s", m, p.Spec.NodeName, p.Status.PodIP)
	}
	e.labelMembers()

	// 2. The probe and one writer per member, on the member's node.
	probe := e.helperManifest("manifests/probe.yaml", "", "")
	helpers = append(helpers, probe)
	e.apply(probe)
	for _, m := range members {
		w := e.helperManifest("manifests/writer.yaml", m, nodes[m])
		helpers = append(helpers, w)
		e.apply(w)
	}
	e.waitReady(5*time.Minute, "probe", "writer-etcd-0", "writer-etcd-1")
	offsets := map[string]int64{}
	for _, p := range []string{"probe", "writer-etcd-0", "writer-etcd-1"} {
		offsets[p] = e.clockOffset(p)
	}
	e.waitForPuts(10)
	e.snapshot("before the partition")

	// 3. Partition.
	monitor := e.startPolicyMonitor(nodes["etcd-0"], cfg.partitionFor)
	tApply := time.Now()
	e.applyFile(policy)
	e.logf("partition applied at %s", tApply.UTC().Format(time.RFC3339Nano))
	e.enforcement(nodes["etcd-0"], "etcd-0")
	time.Sleep(cfg.partitionFor / 2)
	e.snapshot("during the partition")
	time.Sleep(time.Until(tApply.Add(cfg.partitionFor)))

	// 4. Heal.
	tHeal := time.Now()
	e.deleteFile(policy)
	e.logf("partition removed at %s", tHeal.UTC().Format(time.RFC3339Nano))
	time.Sleep(cfg.healFor)
	e.waitHealed(tHeal, cfg.healTimeout)
	e.snapshot("after the heal")
	e.logf("## policy verdicts on %s (cilium-dbg monitor --type policy-verdict, deny lines)", nodes["etcd-0"])
	for _, l := range monitor() {
		e.logf("    %s", l)
	}
	e.etcdLogs(tApply.Add(-2 * time.Second))

	// 5. Collect and check.
	puts := map[string][]put{}
	for _, m := range members {
		puts[m] = e.writes("writer-"+m, offsets["writer-"+m])
	}
	statuses := e.statuses(offsets["probe"])
	final := e.finalKeys()
	e.verify(puts, final, ms(tApply), ms(tHeal))
	e.timeline(puts, statuses, ms(tApply), ms(tHeal))
}

// claims are the volumes the members and the HTTP arbiter write to.
var claims = []string{"data-etcd-0", "data-etcd-1", "arbiter-data"}

// deleteMembers deletes the workloads, empties their volumes as the user that
// wrote them, and then deletes everything else, the claims included.
func (e *env) deleteMembers(manifest string) {
	e.t.Helper()
	if _, err := e.kubectl("delete", "statefulset/etcd", "deployment/arbiter", "--ignore-not-found", "--wait=true"); err != nil {
		e.t.Error(err)
	}
	if _, err := e.kubectl("wait", "--for=delete", "pod", "--selector", "app.kubernetes.io/name in (etcd,twok-arbiter)", "--timeout=2m"); err != nil {
		e.t.Error(err)
	}
	for _, c := range claims {
		if _, err := e.kubectl("get", "pvc", c); err != nil {
			continue
		}
		e.wipe(c)
	}
	if err := e.deleteManifest(manifest); err != nil {
		e.t.Error(err)
	}
	if _, err := e.kubectl("delete", "pvc", "data-etcd-0", "data-etcd-1", "--ignore-not-found", "--wait=true"); err != nil {
		e.t.Error(err)
	}
}

// wipe empties a claim; see manifests/wipe.yaml for why.
func (e *env) wipe(claim string) {
	e.t.Helper()
	pod, err := render("manifests/wipe.yaml", map[string]string{"Claim": claim, "Image": fetchImage})
	if err != nil {
		e.t.Error(err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if _, err := e.run(ctx, "", pod, "apply", "--filename", "-"); err != nil {
		e.t.Error(err)
		return
	}
	if _, err := e.run(ctx, "", "", "wait", "--for=jsonpath={.status.phase}=Succeeded", "--timeout=2m", "pod/wipe-"+claim); err != nil {
		e.t.Errorf("%s was not emptied, its volume may stay Released: %v", claim, err)
	}
	if err := e.deleteManifest(pod); err != nil {
		e.t.Error(err)
	}
}

// clockOffset returns the Pod's clock minus the local clock, in ms.
func (e *env) clockOffset(pod string) int64 {
	e.t.Helper()
	var best, bestRTT int64 = 0, 1 << 62
	for range 3 {
		before := ms(time.Now())
		out, err := e.exec(pod, "date", "+%s%3N")
		after := ms(time.Now())
		if err != nil {
			e.t.Fatal(err)
		}
		remote, err := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
		if err != nil {
			e.t.Fatal(err)
		}
		if rtt := after - before; rtt < bestRTT {
			bestRTT, best = rtt, remote-(before+after)/2
		}
	}
	e.logf("%s: clock offset %+d ms (exec round trip %d ms)", pod, best, bestRTT)
	return best
}

func (e *env) waitForPuts(n int) {
	e.t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for _, m := range members {
		for {
			ok := 0
			for _, p := range e.writes("writer-"+m, 0) {
				if p.ok {
					ok++
				}
			}
			if ok >= n {
				break
			}
			if time.Now().After(deadline) {
				e.t.Fatalf("writer-%s: fewer than %d successful puts after 2m", m, n)
			}
			time.Sleep(time.Second)
		}
	}
}

// snapshot logs each member's status as the member itself reports it.
func (e *env) snapshot(when string) {
	e.t.Helper()
	e.logf("## member status %s", when)
	for _, m := range members {
		out, err := e.etcdctl(m, "endpoint", "status", "--write-out=json")
		if err != nil {
			e.logf("%s: %v", m, err)
		} else {
			e.logf("%s: %s", m, strings.TrimSpace(out))
		}
		if !cfg.baseline() {
			metrics, err := e.exec("probe", "/tools/busybox", "wget", "-q", "-O", "-", "http://"+m+".etcd:2390/metrics")
			if err != nil {
				e.logf("%s twok metrics: %v", m, err)
				continue
			}
			for _, l := range strings.Split(metrics, "\n") {
				if strings.HasPrefix(l, "twok_") {
					e.logf("%s %s", m, l)
				}
			}
		}
	}
}

// startPolicyMonitor runs cilium-dbg monitor for policy verdicts in the
// Cilium agent on node, read-only, and returns a function that waits for it
// and returns the deny lines.
func (e *env) startPolicyMonitor(node string, d time.Duration) func() []string {
	e.t.Helper()
	if cfg.ciliumNamespace == "" {
		return func() []string { return []string{"(K8S_CILIUM_NAMESPACE is -, no monitor)"} }
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	agent, err := e.run(ctx, cfg.ciliumNamespace, "", "get", "pods", "--selector", "k8s-app=cilium",
		"--field-selector", "spec.nodeName="+node, "--output", "jsonpath={.items[0].metadata.name}")
	if err != nil || agent == "" {
		e.logf("no Cilium agent found on %s: %v", node, err)
		return func() []string { return nil }
	}
	e.logf("Cilium agent on %s: %s", node, agent)
	var (
		wg  sync.WaitGroup
		out string
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), d+time.Minute)
		defer cancel()
		secs := strconv.Itoa(int((d + 5*time.Second).Seconds()))
		out, _ = e.run(ctx, cfg.ciliumNamespace, "", "exec", agent, "--container", "cilium-agent", "--",
			"timeout", secs, "cilium-dbg", "monitor", "--type", "policy-verdict")
	}()
	time.Sleep(2 * time.Second) // let the monitor attach
	return func() []string {
		wg.Wait()
		var deny []string
		for _, l := range strings.Split(out, "\n") {
			if strings.Contains(l, "action deny") && len(deny) < 20 {
				deny = append(deny, strings.TrimSpace(l))
			}
		}
		if len(deny) == 0 {
			deny = append(deny, "(no deny verdicts)")
		}
		return deny
	}
}

// enforcement logs the policy enforcement state of a member's endpoint as the
// Cilium agent on its node reports it, once the policy is realised.
func (e *env) enforcement(node, member string) {
	e.t.Helper()
	if cfg.ciliumNamespace == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	agent, err := e.run(ctx, cfg.ciliumNamespace, "", "get", "pods", "--selector", "k8s-app=cilium",
		"--field-selector", "spec.nodeName="+node, "--output", "jsonpath={.items[0].metadata.name}")
	if err != nil {
		e.logf("%v", err)
		return
	}
	id, err := e.kubectl("get", "ciliumendpoint", member, "--output", "jsonpath={.status.id}")
	if err != nil {
		e.logf("%v", err)
		return
	}
	time.Sleep(2 * time.Second)
	out, err := e.run(ctx, cfg.ciliumNamespace, "", "exec", agent, "--container", "cilium-agent", "--",
		"cilium-dbg", "endpoint", "list")
	if err != nil {
		e.logf("%v", err)
		return
	}
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, "ENDPOINT") || strings.HasPrefix(l, id+" ") {
			e.logf("    %s", strings.TrimRight(l, " "))
		}
	}
}

var etcdLogKeywords = []string{
	"lost TCP streaming", "failed to reach the peer", "prober found high clock drift", "is starting a new election",
	"became candidate", "lost leader", "elected leader", "became leader", "became follower", "peer became active",
	"peer became inactive", "established TCP streaming", "dropped internal Raft message",
}

// etcdLogs logs the lines of each member's log since the given time that say
// what Raft and the peer transport saw.
func (e *env) etcdLogs(since time.Time) {
	e.t.Helper()
	for _, m := range members {
		e.logf("## etcd log of %s since %s, filtered", m, since.UTC().Format(time.RFC3339))
		out, err := e.kubectl("logs", m, "--since-time", since.UTC().Format(time.RFC3339))
		if err != nil {
			e.logf("%v", err)
			continue
		}
		// Repeated messages, such as an election attempt every second,
		// are folded into one line with a count.
		const limit = 80
		n, repeats := 0, 0
		var prevMsg, prevTS string
		flush := func() {
			if repeats > 0 {
				e.logf("    ... repeated %d more times until %s", repeats, prevTS)
				repeats = 0
			}
		}
		scanner := bufio.NewScanner(strings.NewReader(out))
		scanner.Buffer(make([]byte, 1<<20), 1<<20)
		for scanner.Scan() {
			var line struct {
				TS  string `json:"ts"`
				Msg string `json:"msg"`
			}
			raw := scanner.Text()
			if json.Unmarshal([]byte(raw), &line) != nil || !matchesAny(line.Msg, etcdLogKeywords) {
				continue
			}
			if line.Msg == prevMsg {
				repeats++
				prevTS = line.TS
				continue
			}
			flush()
			prevMsg, prevTS = line.Msg, line.TS
			if n < limit {
				e.logf("    %s %s", line.TS, compact(raw))
			}
			n++
		}
		flush()
		if n > limit {
			e.logf("    ... %d more distinct lines", n-limit)
		}
	}
}

func matchesAny(s string, substrings []string) bool {
	for _, k := range substrings {
		if strings.Contains(s, k) {
			return true
		}
	}
	return false
}

// waitHealed waits until both members report a leader and both writers have
// had a successful put since the heal, or until timeout has passed since it.
func (e *env) waitHealed(tHeal time.Time, timeout time.Duration) {
	e.t.Helper()
	deadline := tHeal.Add(timeout)
	for {
		healed := true
		for _, m := range members {
			out, err := e.etcdctl(m, "endpoint", "status", "--write-out=json")
			if err != nil || strings.Contains(out, "no leader") || !strings.Contains(out, `"leader":`) {
				healed = false
			}
			okSince := false
			for _, p := range e.writes("writer-"+m, 0) {
				okSince = okSince || (p.ok && p.start > ms(tHeal)+1000)
			}
			healed = healed && okSince
		}
		if healed {
			e.logf("both members have a leader and both writers succeed again, %.1f s after the heal", time.Since(tHeal).Seconds())
			return
		}
		if time.Now().After(deadline) {
			e.logf("the pair has not recovered %s after the heal", timeout)
			return
		}
		time.Sleep(2 * time.Second)
	}
}

// compact drops the fields that only make a log line longer.
func compact(raw string) string {
	var fields map[string]any
	if json.Unmarshal([]byte(raw), &fields) != nil {
		return raw
	}
	for _, k := range []string{"ts", "level", "caller"} {
		delete(fields, k)
	}
	out, _ := json.Marshal(fields)
	return string(out)
}

func (e *env) writes(pod string, offset int64) []put {
	e.t.Helper()
	out, err := e.exec(pod, "cat", "/log/writes.csv")
	if err != nil {
		return nil
	}
	var puts []put
	for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
		f := strings.Split(l, ",")
		if len(f) != 4 {
			continue
		}
		seq, err1 := strconv.Atoi(f[0])
		start, err2 := strconv.ParseInt(f[2], 10, 64)
		end, err3 := strconv.ParseInt(f[3], 10, 64)
		if err1 != nil || err2 != nil || err3 != nil {
			continue
		}
		puts = append(puts, put{seq: seq, ok: f[1] == "ok", start: start - offset, end: end - offset})
	}
	return puts
}

func (e *env) statuses(offset int64) []status {
	e.t.Helper()
	out, err := e.exec("probe", "cat", "/log/status.log")
	if err != nil {
		e.t.Fatal(err)
	}
	var list []status
	for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
		f := strings.SplitN(l, " ", 3)
		if len(f) != 3 {
			continue
		}
		at, err := strconv.ParseInt(f[0], 10, 64)
		if err != nil {
			continue
		}
		s := status{at: at - offset, member: f[1]}
		var parsed []struct {
			Status struct {
				Leader uint64 `json:"leader"`
			} `json:"Status"`
		}
		if json.Unmarshal([]byte(f[2]), &parsed) == nil && len(parsed) == 1 {
			s.leader = parsed[0].Status.Leader
		} else {
			s.err = f[2]
		}
		list = append(list, s)
	}
	return list
}

func (e *env) finalKeys() map[string]bool {
	e.t.Helper()
	keys := map[string]bool{}
	var lastErr error
	for range 10 {
		for _, m := range members {
			out, err := e.etcdctl(m, "get", "--prefix", "w/", "--keys-only", "--consistency=l")
			if err != nil {
				lastErr = err
				continue
			}
			for _, k := range strings.Fields(out) {
				keys[k] = true
			}
			e.logf("final read through %s (linearizable): %d keys", m, len(keys))
			return keys
		}
		time.Sleep(3 * time.Second)
	}
	e.t.Fatalf("no linearizable read succeeded after the heal: %v", lastErr)
	return nil
}

func (e *env) verify(puts map[string][]put, final map[string]bool, tApply, tHeal int64) {
	e.t.Helper()
	e.logf("## checks")
	from := tApply + settle.Milliseconds()
	sides := 0
	for _, m := range members {
		var total, ok, missing, during, inFlight int
		for _, p := range puts[m] {
			total++
			if !p.ok {
				continue
			}
			ok++
			if !final[fmt.Sprintf("w/%s/%08d", m, p.seq)] {
				missing++
				e.t.Errorf("writer-%s: put %d returned success at %d and is missing from the final cluster", m, p.seq, p.end)
			}
			switch {
			case p.start >= from && p.start < tHeal:
				during++
			case p.start >= tApply && p.start < from:
				inFlight++
			}
		}
		if during > 0 {
			sides++
		}
		e.logf("writer-%s: %d puts, %d successful, %d successful puts missing after the heal, "+
			"%d successful puts started in the first %s of the partition, %d started later in the partition",
			m, total, ok, missing, inFlight, settle, during)
		if cfg.baseline() && during > 0 {
			e.t.Errorf("writer-%s: %d puts succeeded during the partition; plain etcd with two voters must accept none", m, during)
		}
	}
	if sides > 1 {
		e.t.Errorf("both sides accepted writes during the partition")
	}
	if !cfg.baseline() && sides == 0 && cfg.partitionFor > 40*time.Second {
		e.t.Errorf("no side accepted writes during the partition; the survivor should have")
	}
	e.logf("sides with successful puts during the partition: %d", sides)
}

func (e *env) timeline(puts map[string][]put, statuses []status, tApply, tHeal int64) {
	e.t.Helper()
	type event struct {
		at   int64
		what string
	}
	var events []event
	add := func(at int64, what string) {
		if at != 0 {
			events = append(events, event{at, what})
		}
	}
	add(tApply, "partition applied")
	add(tHeal, "partition removed")
	for _, m := range members {
		var lastOK, firstFail, firstOKAfter int64
		for _, p := range puts[m] {
			if p.ok && p.start < tHeal && p.end > lastOK {
				lastOK = p.end
			}
			if !p.ok && p.start >= tApply && firstFail == 0 {
				firstFail = p.start
			}
			if p.ok && p.start >= tHeal && firstOKAfter == 0 {
				firstOKAfter = p.end
			}
		}
		add(lastOK, "writer-"+m+": last successful put before the heal returns")
		add(firstFail, "writer-"+m+": first failed put starts")
		add(firstOKAfter, "writer-"+m+": first successful put after the heal returns")
		var lost, back int64
		for _, s := range statuses {
			if s.member != m {
				continue
			}
			if s.at >= tApply && s.at < tHeal && s.leader == 0 && lost == 0 {
				lost = s.at
			}
			if s.at >= tHeal && s.leader != 0 && back == 0 {
				back = s.at
			}
		}
		add(lost, m+": first status without a leader")
		add(back, m+": first status with a leader after the heal")
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].at < events[j].at })
	e.logf("## timeline, seconds from the partition")
	e.logf("")
	e.logf("| T | Event |")
	e.logf("| --- | --- |")
	for _, ev := range events {
		e.logf("| %+.1f s | %s |", float64(ev.at-tApply)/1000, ev.what)
	}
	e.logf("")
	e.logf("## status of each member as the probe saw it during the partition (leader 0 means none)")
	last := map[string]string{}
	for _, s := range statuses {
		if s.at < tApply-5000 {
			continue
		}
		v := fmt.Sprintf("leader %x", s.leader)
		if s.err != "" {
			v = "error: " + truncate(s.err, 120)
		}
		if last[s.member] != v {
			e.logf("    %+.1f s %s %s", float64(s.at-tApply)/1000, s.member, v)
			last[s.member] = v
		}
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
