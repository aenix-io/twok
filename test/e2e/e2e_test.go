//go:build e2e

// Package e2e runs a two-member etcd in containers, each member with a twok
// agent next to it, and breaks the network between the members and towards
// the arbiter with iptables. E2E_IMAGE names the twok image under test and
// ETCD_IMAGE the upstream etcd image.
//
// Each member is laid out as a Pod with a shared process namespace: etcd and
// twok run as the same non-root user without capabilities, share the
// member's data volume at the same path, and talk mTLS with certificates
// from internal/etcdtest. docker restarts etcd after twok stops it, as the
// kubelet does. The arbiter is a third container running twok arbiter serve,
// the http backend.
//
// Every test writes to each member separately, a put every 200 ms with a
// one-second timeout, and asserts on which member acknowledged what. Each
// test logs a timeline with seconds from the first cut.
package e2e

import (
	"fmt"
	"testing"
	"time"
)

// takeoverWithin is how long after a cut the survivor must accept writes:
// claim-after (20 s), the claim delays (3 s and 8 s) and the restart of etcd
// fit with room.
const takeoverWithin = 60 * time.Second

// rejoinWithin bounds the time from a heal to the pair being restored. The
// peer streams that stayed open through the partition need tens of seconds
// of TCP retransmission backoff, and docker restarts the removed loser with
// a growing delay.
const rejoinWithin = 4 * time.Minute

// TestPartitionTwoWay: the link between the members breaks in both
// directions. Exactly one member accepts writes within a minute, the last
// leader; after the heal the loser rejoins and nothing acknowledged is lost.
func TestPartitionTwoWay(t *testing.T) {
	t.Parallel()
	runPartition(t, false)
}

// TestPartitionOneWay: the packets from the leader to the follower are
// dropped, the reverse direction works. The same safety property holds;
// which member survives is recorded.
func TestPartitionOneWay(t *testing.T) {
	t.Parallel()
	runPartition(t, true)
}

func runPartition(t *testing.T, oneWay bool) {
	c := start(t, "enforce")
	c.monitor()
	c.startWriters()
	last := c.leader()
	follower := c.other(last)
	c.tl.add(time.Now(), "%s leads, %s follows", last.name, follower.name)

	rules := c.twoWay(last, follower)
	what := "partition: every packet between a and b dropped"
	if oneWay {
		rules = c.oneWay(last, follower)
		what = fmt.Sprintf("one-way partition: packets from %s (leader) to %s dropped", last.name, follower.name)
	}
	cutAt := c.cut(rules)
	c.tl.mark(cutAt, what)

	survivor := c.waitWinner(cutAt.Add(settle), takeoverWithin)
	loser := c.other(survivor)
	first := survivor.writer.accepted(cutAt.Add(settle), time.Now())[0]
	t.Logf("%s accepts writes %s after the cut; the last leader was %s", survivor.name, c.tl.rel(first.end), last.name)
	if !oneWay && survivor != last {
		t.Errorf("the survivor is %s, want the last leader %s", survivor.name, last.name)
	}
	eventually(t, 30*time.Second, "the loser's agent reports stale", func() (bool, string) {
		r := c.roleOf(loser)
		return r == "stale", "(" + r + ")"
	})
	c.watch(10*time.Second, "only the survivor accepts writes", func() (bool, string) {
		n := len(loser.writer.accepted(cutAt.Add(settle), time.Now().Add(time.Hour)))
		return n == 0, fmt.Sprintf("(%s accepted %d writes)", loser.name, n)
	})
	c.expectRecord(1, survivor.name)
	if r := c.roleOf(survivor); r != "solo" {
		t.Errorf("the survivor's agent reports %s, want solo", r)
	}

	healAt := c.restore(rules)
	c.tl.add(healAt, "partition healed")
	c.noWrites(loser, cutAt.Add(settle), healAt)
	c.waitPair(survivor, loser, healAt, rejoinWithin)
	c.checkAcked(c.members...)
	if oneWay {
		t.Logf("one-way partition from %s to %s: %s survived", last.name, follower.name, survivor.name)
	}
}

// TestArbiterUnreachable: both members lose the arbiter, then each other.
// Nobody may accept writes. When the arbiter is back, one member takes over.
func TestArbiterUnreachable(t *testing.T) {
	t.Parallel()
	c := start(t, "enforce")
	a, b := c.member("a"), c.member("b")
	c.monitor()
	c.startWriters()
	c.tl.add(time.Now(), "%s leads", c.leader().name)

	arbiterRules := c.arbiterCut()
	c.tl.add(c.cut(arbiterRules), "arbiter cut from both members")
	partition := c.twoWay(a, b)
	cutAt := c.cut(partition)
	c.tl.mark(cutAt, "partition: every packet between a and b dropped")

	c.watch(takeoverWithin, "nobody accepts writes without the arbiter", func() (bool, string) {
		for _, m := range c.members {
			if n := len(m.writer.accepted(cutAt.Add(settle), time.Now().Add(time.Hour))); n > 0 {
				return false, fmt.Sprintf("(%s accepted %d writes)", m.name, n)
			}
		}
		return true, ""
	})
	for _, m := range c.members {
		text, err := c.metrics(m)
		if err != nil {
			t.Fatal(err)
		}
		if v := metricValue(text, "twok_arbiter_reachable"); v != 0 {
			t.Errorf("%s: twok_arbiter_reachable = %g, want 0", m.name, v)
		}
		if r := role(text); r != "isolated" {
			t.Errorf("%s: role %s, want isolated", m.name, r)
		}
	}
	c.expectRecord(0, "")

	backAt := c.restore(arbiterRules)
	c.tl.add(backAt, "arbiter reachable again")
	survivor := c.waitWinner(cutAt.Add(settle), takeoverWithin)
	first := survivor.writer.accepted(cutAt.Add(settle), time.Now())[0]
	t.Logf("%s accepts writes %.1fs after the arbiter came back", survivor.name, first.end.Sub(backAt).Seconds())
	c.noWrites(c.other(survivor), cutAt.Add(settle), time.Now())
	c.expectRecord(1, survivor.name)
	c.checkAcked(survivor)
}

// TestSurvivorLostBeforeRejoin: after a takeover the survivor's whole site
// goes down before the loser has rejoined. The loser's data is behind, so it
// stays stale and accepts nothing until the survivor is back; then the pair
// forms again with every acknowledged write.
func TestSurvivorLostBeforeRejoin(t *testing.T) {
	t.Parallel()
	c := start(t, "enforce")
	a, b := c.member("a"), c.member("b")
	c.monitor()
	c.startWriters()
	c.tl.add(time.Now(), "%s leads", c.leader().name)

	partition := c.twoWay(a, b)
	cutAt := c.cut(partition)
	c.tl.mark(cutAt, "partition: every packet between a and b dropped")
	survivor := c.waitWinner(cutAt.Add(settle), takeoverWithin)
	loser := c.other(survivor)
	eventually(t, 30*time.Second, "the loser's agent reports stale", func() (bool, string) {
		r := c.roleOf(loser)
		return r == "stale", "(" + r + ")"
	})

	// The site is gone: no restarts until it comes back.
	mustDocker(t, "update", "--restart", "no", survivor.etcd, survivor.agent)
	mustDocker(t, "kill", survivor.etcd, survivor.agent, survivor.pod)
	killedAt := time.Now()
	c.tl.add(killedAt, "%s killed: its pod, etcd and twok containers", survivor.name)
	// The survivor's rules went with its network namespace.
	var loserRules []rule
	for _, r := range partition {
		if r.m == loser {
			loserRules = append(loserRules, r)
		}
	}
	c.tl.add(c.restore(loserRules), "partition rules removed from %s", loser.name)

	c.watch(45*time.Second, "the loser stays stale and accepts no writes", func() (bool, string) {
		n := len(loser.writer.accepted(cutAt.Add(settle), time.Now().Add(time.Hour)))
		r := c.roleOf(loser)
		return n == 0 && r == "stale", fmt.Sprintf("(%s: role %s, %d writes accepted)", loser.name, r, n)
	})
	c.expectRecord(1, survivor.name)

	mustDocker(t, "start", survivor.pod)
	mustDocker(t, "update", "--restart", "always", survivor.etcd, survivor.agent)
	mustDocker(t, "start", survivor.etcd, survivor.agent)
	backAt := time.Now()
	c.tl.add(backAt, "%s started again", survivor.name)
	c.noWrites(loser, cutAt.Add(settle), backAt)
	c.waitPair(survivor, loser, backAt, rejoinWithin)
	c.expectRecord(1, survivor.name)
	c.checkAcked(c.members...)
}

// TestDryRun: in mode dry-run the agents decide but do not act. The
// partition leaves both members without writes, and the arbiter record is
// never written.
func TestDryRun(t *testing.T) {
	t.Parallel()
	c := start(t, "dry-run")
	a, b := c.member("a"), c.member("b")
	c.monitor()
	c.startWriters()
	c.tl.add(time.Now(), "%s leads", c.leader().name)

	cutAt := c.cut(c.twoWay(a, b))
	c.tl.mark(cutAt, "partition: every packet between a and b dropped")
	c.watch(takeoverWithin, "nobody accepts writes and the arbiter has no record", func() (bool, string) {
		for _, m := range c.members {
			if n := len(m.writer.accepted(cutAt.Add(settle), time.Now().Add(time.Hour))); n > 0 {
				return false, fmt.Sprintf("(%s accepted %d writes)", m.name, n)
			}
		}
		rec, err := c.record()
		if err != nil {
			return true, ""
		}
		return rec.Generation == 0, fmt.Sprintf("(arbiter generation %d, survivor %s)", rec.Generation, rec.Survivor)
	})
	claims := 0.0
	for _, m := range c.members {
		text, err := c.metrics(m)
		if err != nil {
			t.Fatal(err)
		}
		v := metricValue(text, `twok_actions_total{action="claim",result="dry-run"}`)
		claims += v
		if r := role(text); r != "isolated" {
			t.Errorf("%s: role %s, want isolated", m.name, r)
		}
	}
	if claims == 0 {
		t.Error("neither agent counted a claim it would have made")
	}
	c.expectRecord(0, "")
}
