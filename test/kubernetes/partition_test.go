//go:build kubernetes

package kubernetes

import (
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

// scenario is one partition run.
type scenario struct {
	policies []string
	// winner is the member expected to become the survivor with twok:
	// lastLeader, a member name, or "" for no expectation.
	winner string
	// restartSurvivor deletes the survivor's Pod once it serves alone, and
	// removes the partition at the same moment.
	restartSurvivor bool
	// leader, when set, is made the Raft leader before the partition.
	leader string
}

const lastLeader = "the last leader"

// TestPartition cuts the two members from each other in both directions.
func TestPartition(t *testing.T) {
	runPartition(t, scenario{policies: []string{"manifests/partition.yaml"}, winner: lastLeader})
}

// TestPartitionOneWay cuts the connections from etcd-0 to etcd-1 only.
func TestPartitionOneWay(t *testing.T) {
	runPartition(t, scenario{policies: []string{"manifests/partition-one-way.yaml"}})
}

// TestPartitionArbiterCut makes etcd-1 the leader, then cuts the members
// apart and etcd-1 from the arbiter: etcd-0, the follower, must win.
func TestPartitionArbiterCut(t *testing.T) {
	if cfg.baseline() {
		t.Skip("no arbiter in the baseline")
	}
	runPartition(t, scenario{policies: []string{"manifests/partition.yaml", "manifests/cut-arbiter.yaml"}, winner: "etcd-0", leader: "etcd-1"})
}

// TestSurvivorRestart deletes the survivor's Pod while the loser has not
// rejoined; the loser must stay out until the survivor is back.
func TestSurvivorRestart(t *testing.T) {
	if cfg.baseline() {
		t.Skip("no survivor in the baseline")
	}
	runPartition(t, scenario{policies: []string{"manifests/partition.yaml"}, winner: lastLeader, restartSurvivor: true})
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

func runPartition(t *testing.T, sc scenario) {
	e := &env{t: t, log: &bytes.Buffer{}}
	e.logf("# %s, %s", t.Name(), time.Now().UTC().Format(time.RFC3339))
	e.logf("mode: %s", cfg.mode())
	e.logf("namespace: %s, partition for %s, watch for at least %s and at most %s after the heal, policies %s",
		cfg.namespace, cfg.partitionFor, cfg.healFor, cfg.healTimeout, strings.Join(sc.policies, ", "))

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
	ids, leaderID := e.memberIDs()
	if sc.leader != "" && ids[leaderID] != sc.leader {
		e.moveLeader(ids, ids[leaderID], sc.leader)
		ids, leaderID = e.memberIDs()
	}
	expected := sc.winner
	if expected == lastLeader {
		expected = ids[leaderID]
	}
	e.logf("members %v, leader before the partition %s (%s)", ids, ids[leaderID], leaderID)

	// 3. Partition.
	monitor := e.startPolicyMonitor(nodes["etcd-0"], cfg.partitionFor)
	tApply := time.Now()
	for _, p := range sc.policies {
		e.applyFile(p)
	}
	e.logf("partition applied at %s", tApply.UTC().Format(time.RFC3339Nano))
	e.enforcement(nodes["etcd-0"], "etcd-0")
	var tHeal time.Time
	if sc.restartSurvivor {
		survivor := e.waitForSurvivor(tApply, cfg.partitionFor+time.Minute)
		if survivor == "" {
			e.t.FailNow()
		}
		e.snapshot("during the partition, survivor serving")
		e.twokStatuses()
		tKill := time.Now()
		e.must("delete", "pod", survivor, "--wait=false")
		e.mark(ms(tKill), survivor+": Pod deleted")
		tHeal = time.Now()
		for _, p := range sc.policies {
			e.deleteFile(p)
		}
		e.logf("survivor %s deleted at %s, partition removed at %s", survivor,
			tKill.UTC().Format(time.RFC3339Nano), tHeal.UTC().Format(time.RFC3339Nano))
		e.watchWhileSurvivorAway(survivor, tKill)
	} else {
		time.Sleep(cfg.partitionFor / 2)
		e.snapshot("during the partition")
		e.twokStatuses()
		time.Sleep(time.Until(tApply.Add(cfg.partitionFor)))

		// 4. Heal.
		tHeal = time.Now()
		for _, p := range sc.policies {
			e.deleteFile(p)
		}
		e.logf("partition removed at %s", tHeal.UTC().Format(time.RFC3339Nano))
	}
	time.Sleep(cfg.healFor)
	e.waitHealed(tHeal, cfg.healTimeout)
	e.snapshot("after the heal")
	e.twokStatuses()
	e.logf("## policy verdicts on %s (cilium-dbg monitor --type policy-verdict, deny lines)", nodes["etcd-0"])
	for _, l := range monitor() {
		e.logf("    %s", l)
	}
	e.restarts()
	for _, m := range members {
		e.containerLogs(m, "etcd", etcdLogKeywords)
		if !cfg.baseline() {
			e.containerLogs(m, "twok", twokLogKeywords)
			e.twokEvents(m)
		}
	}

	// 5. Collect and check.
	puts := map[string][]put{}
	for _, m := range members {
		puts[m] = e.writes("writer-"+m, offsets["writer-"+m])
	}
	statuses := e.statuses(offsets["probe"])
	final := e.finalKeys()
	e.verify(puts, final, ms(tApply), ms(tHeal), expected)
	e.timeline(puts, statuses, ms(tApply), ms(tHeal))
}

// deleteMembers stops the workloads, empties every claim in the namespace
// as the user that wrote it, and then deletes everything else, the claims
// included. The arbiter EtcdCluster is scaled to zero first: the operator
// then deletes its Pod and keeps its claim, which is emptied like the
// others before the cluster is deleted.
func (e *env) deleteMembers(manifest string) {
	e.t.Helper()
	if _, err := e.kubectl("delete", "statefulset/etcd", "deployment/arbiter", "--ignore-not-found", "--wait=true"); err != nil {
		e.t.Error(err)
	}
	if cfg.overlay == "arbiter-etcd" && !cfg.baseline() {
		if _, err := e.kubectl("patch", "etcdclusters.etcd-operator.cozystack.io", "arbiter", "--type=merge",
			"--patch", `{"spec":{"replicas":0}}`); err != nil {
			e.t.Error(err)
		}
	}
	if _, err := e.kubectl("wait", "--for=delete", "pod", "--selector",
		"app.kubernetes.io/name in (etcd,twok-arbiter)", "--timeout=2m"); err != nil {
		e.t.Error(err)
	}
	if _, err := e.kubectl("wait", "--for=delete", "pod", "--selector",
		"etcd-operator.cozystack.io/cluster=arbiter", "--timeout=3m"); err != nil {
		e.t.Error(err)
	}
	out, err := e.kubectl("get", "pvc", "--output", "name")
	if err != nil {
		e.t.Error(err)
	}
	var pvcs []string
	for _, name := range strings.Fields(out) {
		pvc := strings.TrimPrefix(name, "persistentvolumeclaim/")
		pvcs = append(pvcs, pvc)
		e.wipe(pvc)
	}
	if err := e.deleteManifest(manifest); err != nil {
		e.t.Error(err)
	}
	// twok's records in the kubernetes:// arbiter.
	if _, err := e.kubectl("delete", "configmap", "twok-arbiter", "twok-arbiter.probe", "--ignore-not-found"); err != nil {
		e.t.Error(err)
	}
	if len(pvcs) > 0 {
		args := append([]string{"delete", "pvc", "--ignore-not-found", "--wait=true"}, pvcs...)
		if _, err := e.kubectl(args...); err != nil {
			e.t.Error(err)
		}
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
			metrics, err := e.metrics(m)
			if err != nil {
				e.logf("%s twok metrics: %v", m, err)
				continue
			}
			for _, l := range strings.Split(metrics, "\n") {
				if strings.HasPrefix(l, "twok_") && !strings.HasSuffix(l, " 0") {
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
	"peer became inactive", "established TCP streaming", "dropped internal Raft message", "removed", "exit", "fatal", "panic", "failed to", "rejected",
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
		if healed && !cfg.baseline() {
			restored, list := e.pairRestored()
			if !restored {
				healed = false
			} else {
				e.logf("member list: %s", list)
			}
		}
		if healed {
			e.logf("both members have a leader and both writers succeed again, %.1f s after the heal", time.Since(tHeal).Seconds())
			e.mark(ms(time.Now()), "pair restored: both members have a leader, both writers succeed"+
				map[bool]string{true: "", false: ", two voters"}[cfg.baseline()])
			return
		}
		if time.Now().After(deadline) {
			e.logf("the pair has not recovered %s after the heal", timeout)
			if !cfg.baseline() {
				e.t.Errorf("the pair has not recovered %s after the heal", timeout)
			}
			return
		}
		time.Sleep(2 * time.Second)
	}
}

// compact drops the fields that only make a log line longer. Numbers are
// kept as written: member IDs do not fit a float64.
func compact(raw string) string {
	var fields map[string]any
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	if dec.Decode(&fields) != nil {
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

func (e *env) verify(puts map[string][]put, final map[string]bool, tApply, tHeal int64, expected string) {
	e.t.Helper()
	e.logf("## checks")
	from := tApply + settle.Milliseconds()
	sides := 0
	winner := ""
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
			winner = m
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
	if !cfg.baseline() {
		e.logf("survivor: %q, expected: %q", winner, expected)
		if expected != "" && sides == 1 && winner != expected {
			e.t.Errorf("%s became the survivor, expected %s", winner, expected)
		}
	}
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
	for _, ev := range e.events {
		add(ev.at, ev.what)
	}
	for _, m := range members {
		var lastOK, firstFail, firstOKAfter, firstOKDuring int64
		from := tApply + settle.Milliseconds()
		for _, p := range puts[m] {
			if p.ok && p.start >= from && p.start < tHeal && firstOKDuring == 0 {
				firstOKDuring = p.end
			}
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
		add(firstOKDuring, "writer-"+m+": first successful put during the partition returns")
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

// twokStatuses logs twok status from both sidecars.
func (e *env) twokStatuses() {
	e.t.Helper()
	if cfg.baseline() {
		return
	}
	for _, m := range members {
		e.twokStatus(m)
	}
}

// watchWhileSurvivorAway waits until the survivor's Pod is back and ready,
// and logs the other member's twok role while it is away.
func (e *env) watchWhileSurvivorAway(survivor string, tKill time.Time) {
	e.t.Helper()
	other := members[0]
	if other == survivor {
		other = members[1]
	}
	type podState struct {
		Metadata struct {
			UID               string  `json:"uid"`
			DeletionTimestamp *string `json:"deletionTimestamp"`
		} `json:"metadata"`
		Status struct {
			Conditions []struct {
				Type   string `json:"type"`
				Status string `json:"status"`
			} `json:"conditions"`
		} `json:"status"`
	}
	oldUID := ""
	lastRole := ""
	deadline := tKill.Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		var p podState
		out, err := e.kubectl("get", "pod", survivor, "--output", "json")
		if err == nil && json.Unmarshal([]byte(out), &p) == nil {
			if oldUID == "" && p.Metadata.DeletionTimestamp != nil {
				oldUID = p.Metadata.UID
			}
			ready := false
			for _, c := range p.Status.Conditions {
				ready = ready || (c.Type == "Ready" && c.Status == "True")
			}
			if oldUID != "" && p.Metadata.UID != oldUID && ready {
				e.logf("%s is back and ready, %.1f s after the deletion", survivor, time.Since(tKill).Seconds())
				e.mark(ms(time.Now()), survivor+": new Pod ready")
				return
			}
		}
		if role := e.role(other); role != lastRole {
			e.logf("%+.1f s after the deletion: %s twok role %s", time.Since(tKill).Seconds(), other, role)
			lastRole = role
		}
		time.Sleep(2 * time.Second)
	}
	e.t.Errorf("%s did not come back within 5m", survivor)
}

// role returns a member's twok role from its metrics.
func (e *env) role(member string) string {
	metrics, err := e.metrics(member)
	if err != nil {
		return "unknown (metrics unreachable)"
	}
	for _, l := range strings.Split(metrics, "\n") {
		if strings.HasPrefix(l, "twok_role{") && strings.HasSuffix(l, " 1") {
			return l
		}
	}
	return "unknown"
}

// metrics reads a member's twok metrics from the probe. The probe image has
// bash but no HTTP client, so it is a plain HTTP/1.0 request on /dev/tcp.
func (e *env) metrics(member string) (string, error) {
	script := `exec 3<>/dev/tcp/` + member + `.etcd/2390 && printf 'GET /metrics HTTP/1.0\r\nHost: x\r\n\r\n' >&3 && timeout 3 cat <&3`
	out, err := e.exec("probe", "bash", "-c", script)
	return strings.ReplaceAll(out, "\r", ""), err
}

// moveLeader hands the Raft leadership from one member to another.
func (e *env) moveLeader(ids map[string]string, from, to string) {
	e.t.Helper()
	target := ""
	for id, name := range ids {
		if name == to {
			target = id
		}
	}
	out, err := e.etcdctl(from, "move-leader", target)
	if err != nil {
		e.t.Fatal(err)
	}
	e.logf("move-leader %s -> %s: %s", from, to, strings.TrimSpace(out))
	time.Sleep(2 * time.Second)
}
