# twok design

Status: research. The design is written down before the code and is revised as the code and the tests teach us otherwise. Nothing here is released, and nothing is meant for production.

## Problem

Two sites, A and B, each with one etcd member, and no third site that can run a full etcd member close enough to the other two.
The question from the [original thread](https://www.reddit.com/r/kubernetes/comments/1vzokft/etcd_with_2_dcs_a_lightweight_arbiter/): can an external, lightweight arbiter break the tie, so that the loss of either site does not take etcd down, without a third member that takes part in every write?

Raft needs a majority of the voting members for every commit and every election.
With two voters, the majority is both of them, so the cluster stops at the first failure: a dead node, a dead link, or a dead site.
It stops safely, which is the point of Raft, but it stops.

twok keeps upstream etcd as it is and adds the one thing that is missing: a way to decide, safely, which of the two members may go on alone.

## What twok does

twok is a small daemon that runs next to each of the two etcd members: a sidecar container in the member's Pod on Kubernetes, a static Pod on Talos.
It talks to its member through the client API, and it can stop the etcd process and edit the member's data directory while etcd is down.
It does nothing as long as the cluster has a leader.

When the cluster has had no leader for a configured time, the daemon tries to claim the right to continue alone in an **arbiter**: a record in an external system that supports an atomic compare-and-swap.
Exactly one member can win the claim.
The winner stops its etcd, rewrites the membership in its own log to a cluster of one, and lets etcd start again with the same flags as before; from then on it serves.
The loser cannot commit anything, by Raft itself, and when it meets the survivor again it is re-added as a learner and catches up.

The arbiter is involved only when the cluster has lost its leader.
It is not on the write path, it stores a few hundred bytes, and it needs no low latency.
It can be an S3 bucket, another etcd cluster, a Kubernetes API server, or any HTTP service that implements the contract in [Arbiter backends](#arbiter-backends).

This is the mechanism Red Hat ships in two-node OpenShift with fencing, with two differences.
Red Hat fences the other node through its BMC before the survivor reconfigures etcd; twok breaks the tie with the arbiter and relies on Raft to stop the other member's etcd, which it does by construction: a voter without a quorum commits nothing.
And Red Hat's survivor restarts etcd with `--force-new-cluster`, which needs control over etcd's command line; twok writes the same membership change into the member's log itself, so etcd is restarted by whoever runs it, with the flags it always had.
twok fences etcd, not the node.
Workloads and storage on the losing node are out of scope; see the [Failure model](#failure-model).

## The model

### Generations

The life of a twok cluster is a sequence of **generations**. Generation 0 is the bootstrap: two voters. Every time a member becomes the survivor the generation increases by one.

The arbiter holds one record per cluster:

```json
{
  "schemaVersion": 1,
  "clusterID": "e09a2cd26e182b15",
  "generation": 3,
  "survivor": "b",
  "survivorMemberID": "2a1a388f4c496b3e",
  "since": "2026-10-21T20:14:03Z",
  "twokVersion": "v0.1.0"
}
```

The cluster itself holds the generation too, as the key `/twok/generation`, written by the survivor right after it starts serving alone.
That key is the member's proof of which generation its data belongs to: it reaches a member only through the Raft log, together with the membership changes of the same takeover, so a member whose copy of the key is behind the arbiter's generation holds data that is behind too.
twok keeps no state file of its own; everything it needs to know about a member is in the member's data directory and in the arbiter.

### The rule

> A member may become the survivor of generation g+1 only if it is a voter whose data holds generation g, and it wins the compare-and-swap from g to g+1 in the arbiter.

The compare-and-swap is the whole tie-break. Two members that race for the same generation cannot both win. A member whose data belongs to an older generation than the arbiter's does not even try: its data is stale by definition, because a newer generation exists only when another member went on alone.

### Roles and transitions

```mermaid
stateDiagram-v2
    [*] --> Pair: two voters, leader present
    Pair --> Isolated: no leader for claim-after
    Isolated --> Pair: leader is back
    Isolated --> Solo: CAS g→g+1 won
    Isolated --> Stale: arbiter generation > local generation
    Solo --> Pair: peer promoted to voter
    Pair --> Stale: arbiter generation > local generation
    Stale --> Joining: survivor reachable, re-added as a learner
    Joining --> Pair: promoted to voter
```

**Pair.** etcd runs with two voters and has a leader. twok polls the local member's `Status` every `poll-interval` and remembers who the leader is and what the member list is. Nothing else happens; the arbiter is not contacted.

**Isolated.** The local member has reported no leader for `claim-after`. The leader of a two-voter cluster steps down as soon as it loses its follower (CheckQuorum), so both members see no leader within an election timeout, whatever the cause: a dead peer, a dead link, or a dead site. twok reads the arbiter:

- If the arbiter's generation is higher than the local one, this member is stale and goes to **Stale**.
- If the generations are equal, this member is a candidate. It waits `claim-delay`, plus `follower-extra-delay` if it was not the last leader it saw, then attempts the compare-and-swap from g to g+1 with itself as survivor. The delays give the last leader the first chance.
- If the arbiter cannot be reached, nothing happens, and the member stays isolated. etcd stays up and refuses writes on its own.
- If a leader comes back at any point, the member returns to **Pair**.

**Solo.** The member won. twok stops etcd, appends to the member's log the configuration changes that remove every other member, with the commit index at the end of the log (see [The whole log counts](#the-whole-log-counts)), and lets etcd start again with its usual flags. etcd replays the log, applies the changes, and leads a cluster of one within a couple of seconds. twok then writes `/twok/generation` with the new generation. It does not renew anything in the arbiter: a cluster of one keeps its quorum until a second voter is added, and only twok adds one.

**Stale.** The member lost. Its etcd keeps running: it cannot commit, and it serves serializable reads from data that is now behind. When it reaches the survivor's peer port it is told it was removed (HTTP 410 on the peer stream) and exits, with exit code 0 and no marker on disk; whoever runs etcd restarts it, and it exits again. twok does not parse that. It waits until the survivor's client API answers and goes to **Joining**.

**Joining.** twok asks the survivor to add the local member as a learner, under a new member ID, with the local peer URLs. It then stops the local etcd, rewrites the member's log so that it belongs to the new ID, and lets etcd start again with its usual flags. The member keeps its data: it reconnects to the survivor, receives the entries it missed, applies the removal of its old ID and its own addition as a learner, and catches up from where it was. When the learner is caught up, the survivor's twok promotes it. When the local member reports that it is a voter, it is back in **Pair**.

This rejoin, keeping the log and changing only the member's identity, is the one part of the design that rests on an experiment rather than on etcd's documented behaviour; see [Open questions](#open-questions). If it does not hold, the fallback is the rejoin Red Hat does, which discards the data directory and needs the member restarted with a different initial cluster, and so needs whoever runs etcd to cooperate.

The survivor's twok takes no part in the rejoin beyond answering the API calls and promoting the learner. It returns to **Pair** when its member list shows two voters again.

### Startup

twok never blocks etcd's start: etcd is started by whoever runs it, before twok has looked at anything.
That is safe:

- A member whose data holds two voters cannot commit without the peer. If the peer became the survivor in the meantime, this member finds out from the arbiter at the next isolation, or from the survivor itself, which rejects it as removed.
- A member whose data holds one voter is the survivor of its generation, and no other member can have claimed a newer generation. A newer claim requires a voter at this generation, which means a member promoted into this cluster of one, and that promotion is in this member's log.

twok reads the arbiter at start when it can, logs the result, and acts on it: a stale member is taken through **Stale** and **Joining**, a survivor whose takeover was interrupted after the claim finishes it.

## Safety argument

The property to prove: at any time, at most one etcd cluster derived from the original one accepts writes, and every write acknowledged to a client before a failure is present on the survivor after it.

1. **At most one survivor per generation.** The arbiter's compare-and-swap from g to g+1 succeeds for at most one writer. Every backend provides this through a single primitive: a conditional write on a version token. A backend that cannot do that is not an arbiter; see [Arbiter backends](#arbiter-backends).

2. **A loser never commits.** Until a member's log says otherwise, its membership has two voters, and Raft commits nothing without both. Linearizable reads go through the same quorum check and fail as well. Serializable reads are served from the local copy and may be stale, which is their documented contract. This holds whether or not the loser knows it lost, and whether or not it can reach the arbiter.

3. **A stale member never becomes a survivor.** The claim requires the member's generation to equal the arbiter's. A member whose data holds generation g cannot claim g+2. It can only rejoin g+1 as a learner, and it becomes a candidate for g+2 only after it is promoted, at which point its log contains everything committed at g+1 up to the promotion, and everything after the promotion needs its acknowledgement.

4. **A learner never becomes a survivor.** The claim requires `IsLearner == false` from the local member. A learner's log may lag arbitrarily, because the cluster of one commits without it.

5. **Two members never run as clusters of one at the same time.** By 1 and 3: a second survivor needs a newer generation, which needs a voter at the current one, which needs a promotion into the current survivor's cluster, after which the current survivor has two voters again and cannot commit alone.

6. **Acknowledged writes survive.** A write acknowledged at generation g was committed by the leader, so both voters hold it in their logs. The survivor is one of the two voters, so its log holds it. twok appends to that log and truncates nothing, see below.

### The whole log counts

A follower acknowledges an entry to the leader as soon as it is in its log. The leader commits it, answers the client, and tells the follower about the new commit index in its next message to it. If the partition hits in between, the follower holds an acknowledged entry that it does not know is committed, and it never learns. etcd's `--force-new-cluster` discards entries after the member's commit index, so a follower that becomes the survivor that way would lose it. Two details make the window wider than a heartbeat: the leader's notification is one message, lost with the partition, and the commit index alone is written to the WAL without a sync, so even a notification that arrived may not be on disk when the follower is killed. The fix in etcd 3.5.22 and 3.6.1, which keeps entries up to the applied index, does not cover this case: a follower cannot apply what it does not know is committed. The sources are in the research notes under [docs/research/](research/).

twok does not use `--force-new-cluster`. It writes the same configuration changes into the log itself, after the last entry, and sets the commit index at the end. Nothing is truncated. This is safe in a two-voter cluster and only there:

- Every committed entry is in both logs, because a commit needs both acknowledgements.
- No entry in either log was ever contradicted by a committed entry at the same index, because contradicting it would have required that member's acknowledgement, and the acknowledgement overwrites the entry.
- So the survivor's log is a prefix-consistent superset of the committed log. Treating all of it as committed commits some proposals that had timed out for their clients. A timed-out request is allowed to take effect; that is the contract every etcd client already lives with.

The last leader is still preferred as the survivor through `follower-extra-delay`, so that in the common case no such entries exist at all.

This is the one place where twok touches etcd's files. It uses etcd's own WAL package and nothing else, and a test produces exactly this situation and checks that the acknowledged write survives.

## Failure model

What the tests must show, with twok running on both members and the arbiter in a third place.

| Scenario | Survivor | Loser | Guaranteed by |
| --- | --- | --- | --- |
| Link A-B down, both reach the arbiter | the last leader, or the first to claim | stops committing at once, rejoins after the link heals | CAS, Raft |
| Link A-B down, only A reaches the arbiter | A | B stops committing | CAS, Raft |
| Link A-B down, arbiter unreachable from both | none: writes stop until the arbiter or the link is back | | Raft |
| Site A down | B | A rejoins when it returns | CAS, generations |
| Site A down, then B down before A returns | none: B's data is the only legitimate copy, A waits for it | | generations |
| Arbiter down, cluster healthy | nothing changes: writes continue | | design: the arbiter is off the write path |
| Old survivor returns after the other member claimed a newer generation | the newer generation | the old survivor rejoins as a learner | generations |
| Both members restart, same generation | the pair forms again | | etcd |
| Partition flaps | claims only after `claim-after` without a leader; a claim that already happened is final | | timers, generations |
| One-way link failure | the same as a full link failure: no commit without both directions | | Raft |

What is guaranteed:

- No two clusters derived from one accept writes at the same time, for any sequence of link, node, site and arbiter failures, as long as the arbiter is a correct compare-and-swap register and nobody edits the arbiter, the state files or the data directories by hand.
- No write acknowledged to a client is lost by a takeover.

What is not guaranteed:

- Availability when the arbiter is unreachable at the time of the failure. Writes stop until the arbiter, or the peer, is reachable again.
- Availability after a double failure: the survivor fails before the loser has rejoined. The loser's data is older than the survivor's, and twok refuses to serve it. An operator who accepts the loss can force it; see [Manual intervention](#manual-intervention).
- Anything about clients. A client that keeps talking to the loser gets errors until it reconnects to the survivor. Client routing is the same problem threek has and is solved the same way: list both members in the client, or route by site.
- Anything about workloads or storage on the losing node. twok stops etcd from committing; it does not stop the node.

What is measured, on a test bench, and reported in [docs/testing.md](testing.md):

- Time from a failure to the survivor accepting writes, with default settings.
- Time from a link healing to the pair being restored.
- That a write acknowledged by the leader and held by the follower survives a takeover by the follower.

What is assumed:

- The arbiter backend honours its conditional write exactly as documented. twok checks this once at startup on a probe key next to the record, and refuses the backend if the check fails.
- Clocks are not relied on for safety. Timers only decide when to try a claim; the claim itself is decided by the arbiter.
- The state file and the data directory live on the same volume and move together.

## Expected timeline with defaults

Link A-B breaks at T, A was the leader, both reach the arbiter:

| Time | Event |
| --- | --- |
| T + 1 s | A steps down (CheckQuorum, election timeout), B times out on the leader; both report no leader |
| T + 21 s | `claim-after` (20 s) passes on both; both read the arbiter and see their own generation |
| T + 24 s | A's `claim-delay` (3 s) passes, A wins the compare-and-swap |
| T + 29 s | B's extra delay passes, B's compare-and-swap fails, B goes to Stale |
| T + 30 s | A's etcd has been restarted twice and serves as a cluster of one |

The numbers are starting points. Red Hat waits minutes before fencing to ride out short outages; the right value depends on the links.

## etcd versions

twok talks to etcd through the client API and touches the data directory only with etcd's own WAL package, whose format is the same in 3.5 and 3.6. The tests run against the newest 3.5 and the newest 3.6.

Two etcd bugs matter for the takeover and the rejoin, and both are avoided or fixed in newer patch releases:

- Before 3.5.20 a learner's promotion was stored only in the v2 store, and the member becomes a learner again on an upgrade to 3.6. Run 3.5.20 or later.
- Before 3.5.25 `--force-new-cluster` does not remove a learner that is already in a snapshot, and the removal of the other member may not reach the backend when the applied index is ahead of the commit index. twok writes the commit index at the last entry before the restart, which puts it ahead of the applied index and avoids the second case; the first case does not occur in twok's flows, where a learner exists only while the survivor is already alone.

## Configuration

| Setting | Default | Meaning |
| --- | --- | --- |
| `endpoint` | `https://127.0.0.1:2379` | client URL of the local member |
| `data-dir` | | the member's data directory, as mounted into twok's container |
| `arbiter` | | the arbiter URL, see [Arbiter backends](#arbiter-backends) |
| `poll-interval` | `1s` | how often the local member's status is read |
| `claim-after` | `20s` | how long the cluster must be without a leader before a claim is considered |
| `claim-delay` | `3s` | extra wait before the claim for the member that was the last leader |
| `follower-extra-delay` | `5s` | extra wait on top of `claim-delay` for a member that was not the last leader |
| `mode` | `off` | `off`: observe and log only. `dry-run`: decide and log, never act. `enforce`: act. |
| `metrics-listen` | empty | address for Prometheus metrics |

The TLS files for the client API are flags as in threek: `--cert-file`, `--key-file`, `--trusted-ca-file`.
The member's name, peer URLs and the rest of its configuration twok reads from the etcd process itself: it runs in the same PID namespace and reads etcd's command line from `/proc`.
As in threek, the settings shared by the members can come from a mounted ConfigMap that is read again before every poll, so `mode` is switched without restarting anything.

## Arbiter backends

The arbiter is one record with a version token and one operation besides reading it:

```go
// Arbiter is a compare-and-swap register for one cluster's record.
type Arbiter interface {
    // Read returns the record and its version, or ErrNotFound and the
    // version that creates it.
    Read(ctx context.Context) (Record, Version, error)
    // CompareAndSwap writes record if the current version is expected.
    // It returns ErrConflict when it is not. On any other error the
    // caller must assume the write may have happened.
    CompareAndSwap(ctx context.Context, expected Version, record Record) (Version, error)
}
```

A backend lives in its own package, registers its URL scheme, and touches nothing else. Adding a backend is adding a package.

| Scheme | Store | Version token | Conditional write |
| --- | --- | --- | --- |
| `s3://bucket/key` | S3-compatible object store | ETag | `PUT` with `If-Match`, `If-None-Match: *` to create |
| `etcd://host:2379/key` | another etcd cluster | mod revision | transaction on `ModRevision` |
| `kubernetes://namespace/name` | a ConfigMap in a Kubernetes API server | resourceVersion | update with the resourceVersion |
| `http://host/path` | any HTTP service | ETag | `PUT` with `If-Match`, `If-None-Match: *` to create, `412` on mismatch |
| `file:///path` | a local file, for tests | content hash | lock and rename |

Not every S3-compatible store implements conditional writes, and some ignore the headers instead of rejecting them. twok verifies the conditional write on a probe key at startup and refuses to run against a store that does not honour it. The probe is a write next to the record, under the same prefix with a `.probe` suffix.

The `http` contract is small enough for a Cloudflare Worker with a Durable Object, a few lines of any web framework, or `twok arbiter serve`, a reference implementation that keeps its records in a file. That server is a single point of failure for arbitration, not for writes, which is the trade-off the arbiter model makes everywhere.

## Manual intervention

`twok arbiter get` prints the record. `twok arbiter claim --survivor <name>` moves the generation forward by hand, for the double failure above: the operator declares a member the survivor, accepts that writes after the last common point are lost, and twok on that member does the takeover at the next poll. The command refuses to run without an explicit `--accept-data-loss`.

An administrator who runs `--force-new-cluster` by hand must also claim the generation, or the other member will not know whom to follow.

## Deployment

twok is one program, run next to the etcd process, with three things from the platform: the client API of the local member, the member's data directory, and the ability to stop the etcd process and have it started again with the same flags.

| Platform | Delivery | Stopping etcd | Restarting etcd | Status |
| --- | --- | --- | --- | --- |
| Kubernetes with etcd-operator | sidecar injected by a MutatingAdmissionPolicy, as threek does; the policy also mounts the `data` volume and sets `shareProcessNamespace` | `SIGTERM` to the etcd process, found through `/proc` | kubelet restarts the container with the same command | the policy is tested on kind in CI; the test bench runs the same Pod layout as a StatefulSet because the test cluster is on Kubernetes 1.33 |
| Kubernetes, hand-written StatefulSet | the same sidecar in the Pod template | the same | the same | tested, see [testing.md](testing.md) |
| Talos | static Pod on every control-plane node with host PID, as threek's but with `/var/lib/etcd` mounted | `SIGTERM` to the etcd process | Talos restarts the service | designed, not tested |

What twok owes to the systems that run etcd:

- it never changes etcd's image, binary or command line;
- it stops etcd only during a takeover or a rejoin, and only after the arbiter decided;
- it edits the data directory only while etcd is stopped, and only through etcd's own WAL package;
- a crash, a leak or a missing image of twok never touches the etcd process; with the operator the sidecar counts toward the Pod's readiness as threek's does.

The operator needs no change to run twok, beyond what threek already relies on. What it does not do well with a cluster whose membership twok changes is listed in [etcd-operator.md](etcd-operator.md).

## Security

- twok listens on nothing but its optional metrics endpoint. It connects to the two members' client URLs and to the arbiter.
- It authenticates to etcd with the member's own certificates, and to the arbiter with whatever the backend needs, read from files or the environment.
- It sends `SIGTERM` to the etcd process, which runs as the same user, and rewrites the WAL of a stopped member. Nothing else on disk, and nothing else in the process table.
- It writes `/twok/generation` in the cluster's own keyspace, which Kubernetes API servers never read.
- The arbiter record contains member names, IDs and timestamps. No data, no keys.

## Observability

Every transition is logged with its reason and the generation.

| Metric | Meaning |
| --- | --- |
| `twok_generation` | the local generation |
| `twok_role{role}` | 1 for the current role: pair, isolated, solo, stale, joining |
| `twok_leader_seen` | 1 when the last poll reported a leader |
| `twok_arbiter_reachable` | 1 when the last arbiter read succeeded |
| `twok_claims_total{result}` | claims attempted, by result: won, lost, error |
| `twok_rejoins_total{result}` | rejoins, by result |
| `twok_last_poll_timestamp_seconds` | Unix time of the last successful poll |

## Testing

- **Unit tests** for the state machine, a pure function of observations and timers: every row of the failure model table, flapping, learners, unknown arbiter state, a stale state file.
- **Unit tests** for every arbiter backend against a fake of its store, with the conflict paths exercised.
- **Integration tests** with real etcd processes and mTLS. Two members talk to each other through TCP proxies that the test can cut, one direction at a time. The arbiter is in memory. The tests assert the failure model table, measure the timeline, and produce the follower's-last-entries situation on purpose.
- **End-to-end tests** in containers with `iptables` blackholes, as in threek, in CI.
- **Kubernetes tests** against a real cluster, with partitions made by network policies, documented with their commands and results in [docs/testing.md](testing.md).

## Rejected alternatives

| Alternative | Why not |
| --- | --- |
| A witness in Raft: a third voter that stores no log, only terms and votes | needs a Raft and etcd fork, and the witness still has to persist a vote per election; the arbiter model gets the same tie-break with upstream etcd |
| A stateless proxy over two independent etcd clusters | the proxy has to resolve the same tie, and it is on the write path of every request |
| A full third member in a third site | the question twok asks is how to avoid exactly that, because of latency and cost |
| Node fencing through a BMC, as in Red Hat's two-node OpenShift | the right tool when the node's workloads and storage must be stopped too; for etcd alone, Raft already stops the loser, and most of our targets have no BMC the cluster may use |
| A lease with a TTL instead of generations | a lease expires while the holder still serves, and the loser could take it over with stale data; the generation is monotonic and the loser's data says which generation it belongs to |
| A state file next to the data directory | the generation key in the cluster's own keyspace is tied to the log that carries the membership changes, so it cannot disagree with the data; a file can |
| twok as etcd's parent process, delivered in an image that stands in for etcd | ties etcd's security fixes to twok's releases, restarts etcd on every settings change, and has to be the image everywhere; the daemon leaves etcd's image and command line alone, as threek does |
| etcd embedded in twok as a library | every etcd flag would have to be parsed as etcd parses it, and etcd rebuilt for every release |
| `--force-new-cluster` on the survivor | needs control over the command line, and it truncates the log after the commit index; the same configuration changes written into the log by twok need neither |
| Re-adding the loser from a wiped data directory, as Red Hat does | needs a different initial cluster on the next start, which only the process's parent can provide; keeping the log and changing the member's identity needs nothing from the platform and copies only what the member missed |
| A demotion of the loser to a learner instead of a removal | Raft would accept it, but etcd's membership layer has no demotion and would panic on the entry |

## Open questions

1. Whether a member can rejoin by keeping its log under a new member ID, as described in **Joining**. The experiment and its result are in [testing.md](testing.md); until it is positive on both supported etcd versions, the rejoin is the open end of the design.
2. Whether `claim-after` should scale with the election timeout, or stay a wall-clock setting as Red Hat's fencing delay is.
3. How twok stops etcd on Talos without privileges beyond the etcd user, and whether Talos's own etcd health checks tolerate the restart.
