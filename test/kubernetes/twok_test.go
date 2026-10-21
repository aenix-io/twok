//go:build kubernetes

package kubernetes

import (
	"bufio"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// memberIDs maps the etcd member IDs to the Pod names and returns the leader
// as etcd-0 sees it, both as hex IDs.
func (e *env) memberIDs() (map[string]string, string) {
	e.t.Helper()
	ids := map[string]string{}
	leader := ""
	for _, m := range members {
		out, err := e.etcdctl(m, "endpoint", "status", "--write-out=json")
		if err != nil {
			e.logf("%s: %v", m, err)
			continue
		}
		var st []struct {
			Status struct {
				Header struct {
					MemberID uint64 `json:"member_id"`
				} `json:"header"`
				Leader uint64 `json:"leader"`
			} `json:"Status"`
		}
		if json.Unmarshal([]byte(out), &st) != nil || len(st) != 1 {
			continue
		}
		ids[strconv.FormatUint(st[0].Status.Header.MemberID, 16)] = m
		if st[0].Status.Leader != 0 {
			leader = strconv.FormatUint(st[0].Status.Leader, 16)
		}
	}
	return ids, leader
}

type memberList struct {
	Members []struct {
		ID        uint64 `json:"ID"`
		Name      string `json:"name"`
		IsLearner bool   `json:"isLearner"`
	} `json:"members"`
}

// pairRestored reports whether some member answers a member list with two
// voters and no learner.
func (e *env) pairRestored() (bool, string) {
	for _, m := range members {
		out, err := e.etcdctl(m, "member", "list", "--write-out=json", "--consistency=l")
		if err != nil {
			continue
		}
		var list memberList
		if json.Unmarshal([]byte(out), &list) != nil {
			continue
		}
		var desc []string
		voters := 0
		for _, mm := range list.Members {
			kind := "voter"
			if mm.IsLearner {
				kind = "learner"
			} else {
				voters++
			}
			desc = append(desc, fmt.Sprintf("%s %x %s", mm.Name, mm.ID, kind))
		}
		return voters == 2 && len(list.Members) == 2, strings.Join(desc, ", ")
	}
	return false, "no member answered"
}

// waitForSurvivor waits until one writer has a successful put that started
// after settle, and returns its member.
func (e *env) waitForSurvivor(tApply time.Time, timeout time.Duration) string {
	e.t.Helper()
	from := ms(tApply) + settle.Milliseconds()
	deadline := tApply.Add(timeout)
	for time.Now().Before(deadline) {
		for _, m := range members {
			for _, p := range e.writes("writer-"+m, 0) {
				if p.ok && p.start >= from {
					e.logf("%s accepts writes again, %.1f s after the partition", m, time.Since(tApply).Seconds())
					return m
				}
			}
		}
		time.Sleep(time.Second)
	}
	e.t.Errorf("no member accepted a write within %s of the partition", timeout)
	return ""
}

// twokStatus runs twok status in a member's sidecar.
func (e *env) twokStatus(member string) {
	e.t.Helper()
	out, err := e.kubectl("exec", member, "--container", "twok", "--",
		"twok", "status", "--endpoint=http://127.0.0.1:2379", "--data-dir=/var/lib/etcd", "--arbiter="+cfg.arbiterURL())
	if err != nil {
		e.logf("%s twok status: %v", member, err)
		return
	}
	e.logf("%s twok status: %s", member, compactJSON(out))
}

func compactJSON(s string) string {
	var v any
	if json.Unmarshal([]byte(s), &v) != nil {
		return strings.TrimSpace(s)
	}
	out, _ := json.Marshal(v)
	return string(out)
}

// containerLogs logs a container's log, the previous instance too when
// there is one, with repeated messages folded.
func (e *env) containerLogs(pod, container string, keywords []string) {
	e.t.Helper()
	for _, previous := range []bool{true, false} {
		args := []string{"logs", pod, "--container", container, "--timestamps"}
		label := "current"
		if previous {
			args = append(args, "--previous")
			label = "previous"
		}
		out, err := e.kubectl(args...)
		if err != nil {
			if !previous {
				e.logf("%v", err)
			}
			continue
		}
		e.logf("## %s log of %s/%s (%s instance)", container, pod, container, label)
		e.foldedLog(out, keywords, 150)
	}
}

// foldedLog logs JSON log lines, optionally only those whose message
// contains one of keywords, folding runs of the same message.
func (e *env) foldedLog(out string, keywords []string, limit int) {
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
		raw := scanner.Text()
		ts, body, _ := strings.Cut(raw, " ") // --timestamps prefix
		var line struct {
			Msg string `json:"msg"`
		}
		if json.Unmarshal([]byte(body), &line) != nil {
			line.Msg = body
		}
		if keywords != nil && !matchesAny(line.Msg, keywords) {
			continue
		}
		if line.Msg == prevMsg {
			repeats++
			prevTS = ts
			continue
		}
		flush()
		prevMsg, prevTS = line.Msg, ts
		if n < limit {
			e.logf("    %s %s", ts, compact(body))
		}
		n++
	}
	flush()
	if n > limit {
		e.logf("    ... %d more distinct lines", n-limit)
	}
}

// restarts logs the restart count, the last termination and the kubelet
// events of each member Pod.
func (e *env) restarts() {
	e.t.Helper()
	e.logf("## container restarts and kubelet events")
	for _, m := range members {
		out, err := e.kubectl("get", "pod", m, "--output", "json")
		if err != nil {
			e.logf("%v", err)
			continue
		}
		var p struct {
			Metadata struct {
				UID string `json:"uid"`
			} `json:"metadata"`
			Status struct {
				ContainerStatuses     []json.RawMessage `json:"containerStatuses"`
				InitContainerStatuses []json.RawMessage `json:"initContainerStatuses"`
			} `json:"status"`
		}
		if json.Unmarshal([]byte(out), &p) != nil {
			continue
		}
		for _, raw := range append(p.Status.ContainerStatuses, p.Status.InitContainerStatuses...) {
			var c struct {
				Name         string          `json:"name"`
				RestartCount int             `json:"restartCount"`
				LastState    json.RawMessage `json:"lastState"`
			}
			if json.Unmarshal(raw, &c) == nil {
				e.logf("%s/%s: %d restarts, last state %s", m, c.Name, c.RestartCount, compactJSON(string(c.LastState)))
			}
		}
		// By UID: events of an earlier Pod with the same name stay for an hour.
		events, err := e.kubectl("get", "events", "--field-selector", "involvedObject.uid="+p.Metadata.UID,
			"--sort-by=.lastTimestamp", "--output",
			"custom-columns=LAST:.lastTimestamp,COUNT:.count,REASON:.reason,MESSAGE:.message", "--no-headers")
		if err == nil {
			for _, l := range strings.Split(strings.TrimSpace(events), "\n") {
				if l != "" {
					e.logf("    %s", l)
				}
			}
		}
	}
}

var twokLogKeywords []string // nil: every line, repeats folded

// twokTimelineMessages are the agent's log messages that go into the
// timeline.
var twokTimelineMessages = []string{
	"claim won", "claim lost", "stopping etcd", "etcd stopped", "WAL edited", "takeover done",
	"generation written", "added as a learner", "etcd runs as the new learner", "learner promoted",
}

// twokEvents puts the agent's key log messages into the timeline, at the
// time the agent logged them. The sidecar's clock is the node's, within a
// few hundred milliseconds of the probe's, see the clock offsets.
func (e *env) twokEvents(member string) {
	e.t.Helper()
	out, err := e.kubectl("logs", member, "--container", "twok")
	if err != nil {
		return
	}
	seen := map[string]bool{}
	scanner := bufio.NewScanner(strings.NewReader(out))
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		var line struct {
			Time string `json:"time"`
			Msg  string `json:"msg"`
		}
		if json.Unmarshal(scanner.Bytes(), &line) != nil || !matchesAny(line.Msg, twokTimelineMessages) {
			continue
		}
		at, err := time.Parse(time.RFC3339Nano, line.Time)
		if err != nil {
			continue
		}
		// "generation written" repeats while the survivor serves alone.
		if seen[line.Msg] && strings.HasPrefix(line.Msg, "generation written") {
			continue
		}
		seen[line.Msg] = true
		e.mark(ms(at), member+" twok: "+line.Msg)
	}
}
