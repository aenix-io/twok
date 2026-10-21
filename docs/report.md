# twok: research report

Status: research. Written 2026-10-22 for the first round of the project; it says what was built, what was shown on a test bench and what was not.

## What this is and why

etcd needs a majority of its voters for every write and every election.
With two members the majority is both, so the first failure stops the cluster: a dead node, a dead link, a dead site.
A third member in a third site is the textbook answer, but it takes part in every write and so must be as close as the other two.
The question behind twok, from [this thread](https://www.reddit.com/r/kubernetes/comments/1vzokft/etcd_with_2_dcs_a_lightweight_arbiter/): can two sites run etcd with only a lightweight arbiter somewhere else, one that is not on the write path and does not need low latency?

twok is the answer tried here: a small daemon next to each member, an arbiter that is any system with an atomic compare-and-swap, and upstream etcd unmodified.
Repository: <https://github.com/aenix-io/twok>.

## The design, in short

- Every member runs a twok agent as a sidecar (Kubernetes) or a static Pod (Talos, by design only). The agent watches its own member and does nothing while the cluster has a leader.
- When the cluster has had no leader for `claim-after`, each agent tries to move a **generation** in the arbiter forward by one, with itself as the survivor, through a compare-and-swap. Exactly one can win. The last leader gets a head start.
- The winner stops its etcd, appends to the member's own WAL the configuration changes that remove the other member, with the commit index at the end of the log, and lets etcd start again with the flags it always had. etcd replays the log and leads a cluster of one within two seconds. The agent then writes the generation into the cluster's keyspace.
- The loser cannot commit anything, by Raft itself: a voter without the other voter commits nothing, whether or not it knows about the arbiter. When it reaches the survivor again it is told it was removed and exits; the agent asks the survivor to add it as a learner under a new ID, rewrites the member identity in its WAL, and lets it start again. It catches up from where it was, and the survivor promotes it.
- A member may claim only the generation its own data holds. A member whose data is behind the arbiter can never become a survivor; it can only rejoin.

The arbiter backends are S3 (conditional `PUT` on the ETag), another etcd (a transaction on the mod revision), a Kubernetes ConfigMap (resourceVersion), and any HTTP service that implements conditional `PUT`, for which a reference server ships in the binary.
Each backend is one package; the core knows only the interface.
Every backend proves at startup, on a probe key, that its store honours conditions, because some S3-compatible stores accept the headers and ignore them.

The full design, the safety argument and the failure model are in [design.md](design.md); the reasoning behind each choice is in [decisions.md](decisions.md).

## Alternatives considered and dropped

- **A witness inside Raft.** A third voter without a log, as in the original question. It needs a fork of the raft library and of etcd, and the witness still has to persist a vote in every election and be reachable for it. The arbiter gets the same tie-break without touching etcd.
- **Red Hat's two-node OpenShift with fencing.** The closest prior art: the survivor restarts etcd with `--force-new-cluster` after fencing the other node through its BMC, and the fenced node rejoins as a learner from a wiped data directory. Its tie-break is a static fencing delay by node name, and its memory of who survived lives in Pacemaker's CIB, which splits with the nodes, so cold boots and a survivor reboot need a person. twok keeps the mechanism and replaces the fencing with the arbiter, the delay with a compare-and-swap, and the CIB with a generation that lives in the arbiter and in the cluster's own log. Fencing the node is out of scope: twok fences etcd, not workloads or storage. The notes on what Red Hat does, with code references, are in [research/red-hat-two-node-fencing.md](research/red-hat-two-node-fencing.md).
- **A lease with a TTL.** A lease says who holds it now, not whose data is current; it expires while the holder still serves. The generation is monotonic and stale data disqualifies itself.
- **twok as etcd's parent process**, first in its own image, then inside an image that stands in for etcd so that Talos and etcd-operator would run it unknowingly. Dropped at the operator's request, in favour of threek's shape: a daemon next to etcd that leaves its image and command line alone. That forced the two ideas below.
- **`--force-new-cluster`.** It needs control over the command line, and it discards WAL entries after the member's commit index. A follower learns the commit index from the leader's next message, so at the moment of a partition it may hold an acknowledged entry it does not know is committed; the commit-only hard state is also written without a sync. twok writes the same configuration changes itself and truncates nothing. This was verified in the etcd source for 3.5 and 3.6 ([research/etcd-internals.md](research/etcd-internals.md)) and then reproduced: see below.
- **Rejoin from a wiped data directory**, as Red Hat does. It needs a different initial cluster on the next start, which only the process's parent can provide. Rewriting the member's identity in its WAL and keeping the log needs nothing from the platform and copies only what the member missed.
- **Demoting the loser to a learner** instead of removing it. Raft would accept it; etcd's membership layer has no demotion and panics on the entry.

## What was built

- `twok agent`: the daemon. A pure decision engine with table tests for every row of the failure model, the loop around it with a watchdog, Prometheus metrics, settings from a ConfigMap re-read before every poll, and process control through `/proc` in a shared PID namespace.
- `twok status`, `twok arbiter get`, `twok arbiter claim --accept-data-loss` (the manual override for a double failure), `twok arbiter serve` (the HTTP reference arbiter).
- The WAL operations: `Standalone` (the takeover), `Reidentify` (the rejoin), `Inspect`, using etcd's own WAL package, which reads and writes 3.5 and 3.6 data directories alike.
- Four arbiter backends plus `memory://` and `file://` for tests, a conformance suite every backend runs, and the startup probe.
- Deployment: a StatefulSet with the sidecar and three arbiter overlays, and a MutatingAdmissionPolicy for etcd-operator, modelled on threek's.
- Tests: unit tests; integration tests with real etcd processes, both supported versions, in CI; a Kubernetes bench driven through `kubectl` with partitions made by Cilium policies; container end-to-end tests with iptables (in CI).

## What was shown

All measurements are in [testing.md](testing.md) with the commands and the raw records.

**Acknowledged writes survive the takeover, and would not with `--force-new-cluster`.** A pair, a client writing through the leader, the follower killed right after an acknowledgement; its persisted commit index was behind its log in every run. twok's `Standalone` and a restart with the usual flags lost nothing; etcd's `--force-new-cluster` on a copy of the same data lost one to four acknowledged entries in every run with an immediate kill, on both 3.5.34 and 3.6.15.

**The rejoin keeps the log.** A loser re-added under a new ID and started with its old flags received only what it missed, applied the removal of its old identity and its addition as a learner, and was promoted; also through a snapshot after 5300 entries, and with a diverged tail of its own, which was overwritten. The variant that appends the removals in the current term, as `--force-new-cluster` does, made the loser panic with "member unknown" on both versions, which is why twok appends them in the next term.

**Two agents, real etcd, TCP proxies cut in both directions.** The survivor was the last leader every time, accepted its first write 29 to 31 s after the cut with the default settings, the pair was back 5 to 10 s after the heal, and every acknowledged write was present on both members. Nobody served writes while the arbiter was unreachable; a loser whose survivor died before the rejoin stayed stale until the survivor was back; `dry-run` touched nothing.

**Containers, iptables, both etcd versions, in CI.** Two members built as Pods (a pause container holding the namespaces, etcd restarted by Docker as the kubelet would, twok next to it, the HTTP reference arbiter as a third container). Partition both ways: the last leader wins, accepts writes 30 s after the cut, the loser is back as a voter 3 s after the heal. One way, packets dropped by direction: the same, the member whose packets were dropped wins. Arbiter unreachable plus the partition: nobody writes for the 60 s the arbiter is away, the claim follows within 2 s of its return. Survivor killed 2 s after its takeover: the loser stays stale and writes nothing for the 45 s the survivor is down, then rejoins within 15 s of its return. Dry run: both agents log the claim they would make and touch nothing. No run had both members accepting writes, no acknowledged write was lost, and the WAL edit won against Docker's restart on the first attempt every time.

**Kubernetes bench, plain etcd baseline.** Two members on different nodes of the dev cluster, Cilium policies cutting port 2380 in both directions for 60 s, proven by the agent's policy verdicts and etcd's own dial errors. Both members stopped accepting writes within a second, no acknowledged write was lost, and the pair needed about 48 s after the heal to elect a leader again, which looks like TCP retransmission backoff on the peer streams that stayed open through the cut.

**Kubernetes bench with twok**, nine runs on the dev cluster with the agent image, mode enforce, default timers, 60 s cuts. Both ways, with the HTTP arbiter twice, with an `EtcdCluster` from etcd-operator as the arbiter, and with a ConfigMap in the API server: the last leader won every time, claimed 26 to 27 s after the cut and accepted its first write at 29 to 30 s, the loser's claim lost with "the record changed since it was read", the loser was promoted 7 to 13 s after the heal, and 428 to 476 acknowledged writes per run were all present. Arbiter cut from one member plus the partition, with the leadership moved to the cut member first: the follower that still reached the arbiter won, 35 s after the cut, and nothing was lost. Survivor Pod deleted 4 s after its takeover: the loser stayed stale and wrote nothing, the Pod came back on its volume in 4 s as a cluster of one, and the loser was a voter again 25 s later. The WAL edit took 8 to 18 ms; stopping etcd 0.1 to 1.2 s; etcd answered again 1.5 to 3 s after the edit.

**The one-way cut found a bug.** Twice out of twice the loser did not come back. A one-way policy still lets the survivor's messages through the connections the loser dialed, so the loser's etcd learned it was removed before its own `claim-after` had elapsed, exited with "data-dir used by this member must be removed" and crash-looped under the kubelet's backoff; the agent, with no etcd to ask, stayed in role `unknown`, never read the arbiter, and never rejoined. Safety held: the loser accepted no write and all 1701 and 1728 acknowledged writes were present. The container end-to-end suite has the same scenario and passed only because the timing there went the other way. The fix (#14) lets the agent read the arbiter while etcd does not answer, take the member's identity from the WAL, and prove it is stale from the survivor's member list; an integration test reproduces the order of events from the bench and fails without the fix. Rerun on the bench twice with the fixed image: the loser went stale from the WAL and the arbiter 4 s after its etcd exited, was re-added through the connections the one-way cut leaves open, and was a voter again 19 s later, with 16 of those spent waiting for the kubelet's second restart; the other run went the ordinary way after the heal. A both-ways regression run gave the same numbers as before.

## What was not shown

- **Talos.** The static Pod is designed from the Talos code and not run: the test cluster is not touched with `talosctl`. How Talos's etcd health checks behave while twok restarts the process underneath them is open.
- **etcd-operator Pods.** The admission policy needs Kubernetes 1.36; the dev cluster runs 1.33, so the bench runs the same Pod layout as a StatefulSet. The policy is tested on kind in CI. The operator itself needs no change to run twok, but it handles a member that comes back under a new ID badly: it never refreshes the member ID it recorded, so a later deletion of that member wedges; see [etcd-operator.md](etcd-operator.md).
- **S3 against a real store.** The S3 backend is tested against an in-process fake of the conditional-write API; the known behaviour of real stores is documented from their sources, not measured here.
- **A one-way partition in the proxy tests.** A proxy cut in one direction does not stop Raft, because etcd falls back to the connections the other member dialed; only the Kubernetes bench, which drops packets by direction, exercises it.
- **Long partitions and flapping links** beyond what the timers do by construction.

## Known limitations

- Exactly two voting members. Learners are ignored.
- Availability needs the arbiter at the moment of the failure. With the arbiter unreachable, writes stop until it or the peer is back; that is the safe outcome and the chosen trade-off.
- A double failure, the survivor dying before the loser rejoined, needs a person: `twok arbiter claim --accept-data-loss`.
- The loser keeps serving serializable reads from stale data until it reaches the survivor. twok fences writes, not reads, and not the node.
- Clients must not keep talking to the loser; list both members or route by site, as with threek.
- etcd authentication (`auth enable`) is not supported, as in threek.
- On Kubernetes the kubelet restarts etcd after twok stops it. The first restart is immediate, the second within ten minutes waits ten seconds, and a crash-looping etcd waits up to five minutes, so a rejoin after a crash loop is correct but slow.
- etcd 3.5.20 or later is advised for the learner promotion to survive an upgrade to 3.6, and twok's own takeover avoids the force-new-cluster bugs fixed in 3.5.25.

## Where things are

- Repository: <https://github.com/aenix-io/twok>, `main`.
- Design: [design.md](design.md). Decisions: [decisions.md](decisions.md). Measurements: [testing.md](testing.md). Research notes: [research/](research/).
- Pull requests, in order: #1 design, #2 research notes, #3 and #5 delivery, #4 core and scaffolding, #6 backends, #7 Kubernetes bench, #8 etcd client and WAL operations, #9 experiment results, #10 engine, agent and CLI, #11 documentation cleanup, #12 container end-to-end tests and the policy job, #13 the Kubernetes runs, #14 the rejoin fix, #15 the one-way rerun.
