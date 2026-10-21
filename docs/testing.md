# Testing

What was measured, how, and with which result.
The design states what the tests must show in [design.md](design.md#failure-model); this file records what they did show.

## WAL operations on real etcd

The takeover and the rejoin are offline operations on a stopped member's WAL, in `internal/walops`.
Their tests run against real etcd processes, on etcd v3.5.34 and v3.6.15, with `go test -race ./internal/walops/` and `TEST_ETCD_BINARY` set; the whole package takes about four minutes per version.

### Acknowledged writes survive the takeover

`TestStandaloneKeepsAcknowledgedWrites`: a pair of members; a client writes keys through the leader and records every acknowledged key; the follower is killed with `SIGKILL` right after an acknowledgement; its WAL is inspected; then the follower is made a cluster of one, either with twok's `Standalone` and a restart with its usual flags, or, as the control, with etcd's own `--force-new-cluster` on a copy of the same data directory.
Three modes, three runs each, per etcd version:

| Mode | Persisted commit behind the last index | By how many entries | Lost with `Standalone` | Lost with `--force-new-cluster` |
| --- | --- | --- | --- | --- |
| immediate: killed right after the acknowledgement | 6 of 6 runs | 1 | 0 | 1 acknowledged entry, 6 of 6 runs |
| quiet: killed one second after the last write | 6 of 6 runs | 1 | 0 | 0 |
| concurrent: four writers, killed at a random moment | 6 of 6 runs | 2 to 4 | 0 | 1 to 4 acknowledged entries, 6 of 6 runs |

The quiet mode shows that the persisted commit index stays behind even after a second of silence: the commit-only hard state sits in etcd's write buffer.
The control loses nothing in that mode only because 3.5.34 and 3.6.15 take the larger of the commit index and the backend's consistent index, and the backend has been synced by then; 3.5.12, the version the operator runs today, does not do that.

After `Standalone` and a normal restart the member leads a cluster of one in 1.1 to 2.0 s, and its member list shows one member.
The other member, started with its usual flags, exits on its own as soon as it reaches the survivor: "the member has been permanently removed from the cluster".
A second `Standalone` on the same WAL changes nothing, byte for byte.

### Rejoin under a new member ID

`TestRejoinWithNewID`: the survivor runs `Standalone`, adds the other member as a learner under a new ID, the other member's WAL is rewritten with `Reidentify`, and it is started with its usual flags, data directory in place.
On both versions:

| Case | Result |
| --- | --- |
| incremental: a few entries behind | the member receives the missing entries, applies the removal of its old ID and its addition as a learner, becomes a learner, is promoted; 210 keys on both |
| snapshot: 5300 entries behind with `--snapshot-count=100` | the same through a snapshot; 5411 keys on both |
| divergent: the loser was the leader and holds 10 entries of its own below the cut | its tail is overwritten; none of its keys exists on either member |
| control: removals appended in the current term, as `--force-new-cluster` does | the loser fails with `panic: failed to update; member unknown` on both versions |

Until the removal of the old ID is applied, the rejoining member logs "failed to find remote peer in cluster": its old identity dials its own URL.
On 3.6.15 a member that caught up through a snapshot does not publish itself or serve reads until it applies one new entry, with thousands of "failed to publish ... too many requests" lines meanwhile; the control that rejoins from a wiped data directory behaves the same, so this is etcd's behaviour, and one write to the cluster ends it.

### Other checks

- `Standalone` and `Reidentify` refuse a WAL that a running etcd holds locked; `Inspect` reads it.
- With `--snapshot-count=10 --max-wals=2` the first WAL segment is gone; the operations open the WAL at the newest snapshot and the last key survives the restart.
- A promotion right after `AddLearner` fails with "learner not ready" and succeeds after two or three retries.

### Integration tests with real etcd

`internal/agent` runs two agents in one process against two real etcd members connected through TCP proxies, with the `memory://` arbiter, on both etcd versions (`go test -race ./internal/agent/` with `TEST_ETCD_BINARY`, about four minutes per version).
With the default settings and the link cut in both directions:

| | etcd 3.6.15 | etcd 3.5.34 |
| --- | --- | --- |
| First accepted write on the survivor after the cut | 29.4 to 30.6 s | 29.9 s |
| Pair restored after the heal | 5.4 to 9.9 s | 9.5 s |

The survivor was the last leader in every run, as the delays intend, and every acknowledged write was present on both members afterwards.
The same tests show that nobody serves writes while the arbiter is unreachable, that the loser stays stale when the survivor dies before the rejoin and rejoins when it is back, and that `dry-run` never touches the arbiter.

A cut of one proxy direction does not produce a one-way partition for Raft: each member dials the other, and etcd falls back from its streams to its pipeline connections, so messages still flow both ways through the connections the other member dialed, and no leader is lost.
A one-way partition needs packets dropped by direction, which the Kubernetes bench does with a Cilium egress policy; the proxy test only checks that nothing unsafe happens.

## End-to-end tests in containers

`test/e2e/`, behind the build tag `e2e`, runs the whole thing in docker: two etcd members, a twok agent next to each, and `twok arbiter serve` as the http arbiter in a third container, all on one docker network.
CI runs it for every etcd version in `build/etcd.sha256`, in the `e2e` job; the whole suite takes about two minutes, the five tests run in parallel.

```bash
make image IMAGE=twok:dev VERSION=dev
E2E_IMAGE=twok:dev ETCD_IMAGE=gcr.io/etcd-development/etcd:v3.6.15 go test -tags e2e -timeout 20m -v ./test/e2e/
```

### Layout

- Each member is laid out as a Pod with `shareProcessNamespace: true`: a pod container (Alpine with iptables, `--init`) holds the network and PID namespaces and the published ports, and the etcd and twok containers join both.
  The namespaces cannot belong to the etcd container itself: when etcd, the init process of its PID namespace, exits, the kernel kills every other process in the namespace, twok included, in the middle of the takeover it stopped etcd for.
- etcd runs with `--restart=always`, so docker starts it again after twok stopped it, as the kubelet does; the agent runs with `--restart=always` as well.
- etcd and twok run as 65532 without capabilities and with a read-only root, and share the member's data volume at `/var/lib/etcd`; twok finds etcd through `/proc` and stops it with `SIGTERM` as the same user.
- Client and peer mTLS with certificates from `internal/etcdtest`, default heartbeat and election timeout, default twok timers, mode `enforce` unless the test says otherwise.
- Writers: the test writes `w/<member>/<seq>` to each member separately through its published client port, a put every 0.2 s with a 1 s timeout, and records every outcome.
- Observation: the test reads each member's status and the arbiter record twice a second, the agents' metrics, and the agents' JSON logs; the containers share the host's clock, so the timeline needs no offsets.
- Cuts: iptables DROP rules in the members' pod containers, without resets.
  Both directions drop everything between the two member addresses; one direction drops the packets from the leader to the follower, in the leader's `OUTPUT` and the follower's `INPUT`; the arbiter cut drops everything between both members and the arbiter.

### Scenarios

| Test | What happens | Asserted |
| --- | --- | --- |
| `TestPartitionTwoWay` | partition in both directions, healed once the loser is stale | within 60 s exactly one member accepts writes, the last leader; the loser accepts none until it has rejoined; after the heal the loser is a voter again and every acknowledged key is on both |
| `TestPartitionOneWay` | the packets from the leader to the follower are dropped | the same, except which member survives, which is recorded |
| `TestArbiterUnreachable` | the arbiter is cut from both members, then the partition; the arbiter is restored after 60 s | nobody accepts writes for 60 s, both agents isolated with the arbiter unreachable and no record; after the restore one member takes over and holds every acknowledged key |
| `TestSurvivorLostBeforeRejoin` | partition, takeover, then the survivor's pod, etcd and twok containers are killed and the partition rules removed; the survivor is started again 45 s later | the loser stays stale and accepts no writes while the survivor is down, the record does not change; after the survivor is back the pair forms again with every acknowledged key |
| `TestDryRun` | partition in mode `dry-run` | nobody accepts writes for 60 s, the agents count the claim they would make, the arbiter has no record |

### Results

CI run of 2026-10-21 on GitHub-hosted runners, seconds from the cut; all ten runs passed.

| Test | etcd | Survivor | Survivor accepts writes | Loser accepts writes again | Acknowledged keys on both |
| --- | --- | --- | --- | --- | --- |
| two-way | v3.5.34 | a, the last leader | +30.2 s | +44.7 s, heal at +41.6 s | 98 of 98 |
| two-way | v3.6.15 | a, the last leader | +29.5 s | +44.5 s, heal at +42.2 s | 97 of 97 |
| one-way, leader b to a | v3.5.34 | b, the leader whose packets were dropped | +30.5 s | +47.0 s, heal at +42.6 s | 106 of 106 |
| one-way, leader a to b | v3.6.15 | a, the leader whose packets were dropped | +30.9 s | +44.5 s, heal at +42.2 s | 95 of 95 |
| arbiter unreachable | v3.5.34 | a | +65.7 s, arbiter back at +60.1 s | | 15 of 15 on a |
| arbiter unreachable | v3.6.15 | a | +63.0 s, arbiter back at +60.2 s | | 15 of 15 on a |
| survivor lost | v3.5.34 | a | +29.4 s; killed at +31.3 s, back at +76.7 s, writes at +78.7 s | +90.8 s | 95 of 95 |
| survivor lost | v3.6.15 | a | +29.6 s; killed at +31.3 s, back at +76.9 s, writes at +77.8 s | +84.9 s | 71 of 71 |
| dry-run | v3.5.34 | none | none for 60 s | none for 60 s | |
| dry-run | v3.6.15 | none | none for 60 s | none for 60 s | |

- No member accepted a write while the other one could, in any run, and no acknowledged write was lost.
- The last leader won every two-way partition, as `follower-extra-delay` intends. In the one-way partitions the leader whose packets to the follower were dropped won both times: both members lose the leader within two seconds, so the cut direction does not matter to the claim. Unlike a cut of one proxy direction above, dropping packets by direction does take the leader away from both members.
- The takeover takes about 30 s from the cut, as [design.md](design.md#expected-timeline-with-defaults) expects: about 20 s of `claim-after` after the leader is gone, the 3 s of `claim-delay`, then 3 to 5 s from the won claim to the first acknowledged write, for the next poll, the WAL edit and etcd's restart.
- The pair is restored 3 to 6 s after the heal. That is much faster than the 48 s the Kubernetes baseline needed only to re-elect a leader, because the rejoin restarts the loser's etcd, and with it the peer connections that sat through the partition.
- With the arbiter unreachable, both agents stay isolated and nothing is claimed; the claim follows within 2 s of the arbiter's return.
- While the survivor was down, the loser's agent retried the rejoin and failed on the survivor's client URL, and the loser stayed stale; 5 to 12 s after the survivor came back the loser was a learner, and 2 to 3 s later a voter.

The two-way partition on v3.6.15, as the test logged it, without the startup lines:

| T | Event |
| --- | --- |
| +0.0 s | partition applied; both writers' last acknowledged put returned at -0.2 s |
| +1.8 s | neither member has a leader |
| +21.8 s | b's agent: pair to isolated, claims after the delay of a follower |
| +22.5 s | a's agent: pair to isolated, claims after the delay of the last leader |
| +26.5 s | a's agent wins the claim; the record holds generation 1, survivor a |
| +27.5 s | a's agent stops etcd, edits the WAL on the first attempt, docker starts etcd again |
| +29.5 s | a leads a cluster of one and accepts writes |
| +30.8 s | b's agent loses its claim, +31.8 s goes stale |
| +42.2 s | partition removed |
| +43.0 s | b's agent adds b as a learner of a, stops etcd, rewrites the WAL under the new ID |
| +43.4 s | b runs as a learner of a |
| +44.5 s | a's agent promotes b; b accepts writes |
| +45.5 s | both agents report pair; docker restarted each etcd once |

## Kubernetes test bench

A two-member etcd on a real Kubernetes cluster, partitioned with Cilium network policies.
The bench is `test/kubernetes/`, a Go test behind the build tag `kubernetes` that drives the cluster through `kubectl` only.

### Setup

- Cluster: five Talos v1.12.0 nodes, Kubernetes v1.33.1, Cilium 1.19.5 with kube-proxy replacement and `enable-policy=default`, the NFS CSI driver as the default StorageClass.
- Members: the StatefulSet `etcd` of [deploy/kubernetes/base](../deploy/kubernetes/base), two replicas on different nodes, upstream etcd v3.6.15 pinned by digest, plain HTTP, default heartbeat (100 ms) and election timeout (1000 ms).
  The baseline removes the twok sidecar (`test/kubernetes/manifests/baseline`); a twok run uses the `arbiter-http` overlay with the image in `TWOK_IMAGE` and sets `mode` in the ConfigMap to `TWOK_MODE`, `enforce` by default, since the manifests ship with `off`.
- Writers: one Pod per member, on the member's node, writing `w/<member>/<seq>` with `etcdctl put` to its own member only, by the member's Pod DNS name, with 1 s dial and command timeouts.
  Each writer starts a put every 0.2 s plus the duration of the previous one and appends `seq,status,start_ms,end_ms` to a log in the Pod.
- Probe: one Pod that runs `etcdctl endpoint status` against each member on its own once a second and logs the leader each member reports.
  The harness also runs its one-off reads in it.
- Clocks: the harness measures each helper Pod's clock offset against its own through `kubectl exec date` and converts every timestamp to its own clock; the offsets were about +200 ms with a round trip of about 500 ms, so the timeline is accurate to a few hundred milliseconds.
- Partition: `test/kubernetes/manifests/partition.yaml`, one CiliumNetworkPolicy per member with `egressDeny` to the other member and `ingressDeny` from it, and `enableDefaultDeny` off in both directions so that nothing else is affected.
  `partition-one-way.yaml` cuts the connections from etcd-0 to etcd-1 only, and `cut-arbiter.yaml` cuts etcd-1 from the arbiter.

Two Cilium details shaped the policies:

- `statefulset.kubernetes.io/pod-name` is not part of a Cilium identity: the default label filter leaves it out, both members had the same identity, and a policy that selected on it was not enforced on any endpoint.
  The harness therefore labels each member Pod with `twok.aenix.io/member=<pod>` and waits until the member's CiliumEndpoint shows an identity with that label, which took 10 to 25 s.
- Cilium drops the packets of connections that were open when the policy arrived, not only new ones: the deny verdicts in the records are `tcp ACK` packets of the existing peer streams.

The harness proves the cut on two sides: `cilium-dbg endpoint list` in the Cilium agent on etcd-0's node shows enforcement for the member's endpoint, and `cilium-dbg monitor --type policy-verdict` shows the denied packets between the two member IPs on port 2380.
Both are read-only `kubectl exec` calls into the agent.

### Steps

1. Apply the member manifests and wait until both members are ready.
2. Label the members and wait for their Cilium identities.
3. Start the probe and the writers, measure the clock offsets, wait until both writers have 10 successful puts.
4. Apply the partition, wait 60 s, read each member's status in the middle.
5. Remove the partition, wait until both members report a leader and both writers succeed again, at most 5 minutes.
6. Collect the writer logs, the probe log, the policy verdicts and each member's log.
7. Read all keys with a linearizable read and check:
   - every key whose put returned success in either writer log exists;
   - at most one side had successful puts that started more than 3 s after the partition was applied (the 3 s cover puts in flight and the policy reaching the endpoints);
   - for the baseline, neither side had such puts.
8. Delete everything; the members' volumes are emptied by a Pod running as the members' user before the claims are deleted (see below).

### Commands

```bash
export K8S_KUBECONFIG=<kubeconfig of the test cluster>   # K8S_NAMESPACE defaults to twok-test
hack/kubernetes-bench.sh TestPartition                   # the baseline, plain etcd
hack/kubernetes-bench.sh TestPartitionOneWay
TWOK_IMAGE=ghcr.io/aenix-io/twok:main hack/kubernetes-bench.sh TestPartition   # TWOK_MODE defaults to enforce
hack/kubernetes-bench-clean.sh                           # lists what a failed run left; --yes deletes it
```

The script runs `go test -tags kubernetes -run '^TestPartition$' ./test/kubernetes/` and writes the raw record to `test/kubernetes/results/`.
The settings are documented at the top of `hack/kubernetes-bench.sh`.

### Baseline: plain etcd, two voters, no arbiter

Measured on 2026-10-21; the raw records are in [test/kubernetes/results/baseline-2026-10-21.md](../test/kubernetes/results/baseline-2026-10-21.md).

Partition in both directions for 60 s, seconds from the moment the policy was applied:

| T | Event |
| --- | --- |
| +0.0 s | partition applied |
| +0.7 s | both writers: last successful put returns |
| +0.9 s | both writers: first failed put starts |
| +2.4 s | etcd-0 (follower) logs `lost leader` and starts pre-vote elections, one a second, that never win |
| +3.0 s | etcd-1 (leader) steps down: `became follower at term 2`, `lost leader` |
| +3.0 s | the probe sees no leader on either member |
| +5.0 s | etcd-1 logs `failed to reach the peer URL ... dial tcp ...:2380: i/o timeout` |
| +60.0 s | partition removed |
| +107.8 s | etcd-0 has a leader again, after a new election at term 3 |
| +108.8 s | etcd-1 has a leader again |
| +110.0 s | both writers: first successful put after the heal returns |

- Neither side accepted a write during the partition: 0 successful puts on either side among those that started more than 3 s after the policy, 90 failed puts per side over the partition and the time until the pair recovered.
- No acknowledged write was lost: all 179 successful puts (90 and 89) were present after the heal.
- The one-way partition behaved the same: no leader on either member at +2.5 s and +3.5 s, no successful put during the partition, a leader again 48.6 s after the heal, no lost write.
- A repeat of the two-way run gave the same numbers within a second: no leader at +2.1 s, a leader again 47.9 s after the heal.

This is what Raft promises with two voters, and what twok is built to change: the first failure stops writes on both sides, safely.

One result was not expected: the pair needed about 48 s after the heal to elect a leader again, in all three runs.
The members' logs show no `lost TCP streaming connection` during the partition, so the peer streams were never closed; their packets were dropped.
After the heal, etcd-0 first logs `elected leader 64e4dbc145b29aea at term 2`, which is a message from etcd-1's leadership before the partition, and only then a new election.
That fits TCP retransmission backoff on the streams that stayed open through the partition: the next retransmission after 60 s of loss comes tens of seconds after the link is back, and Raft messages queued behind it wait as well.
It is an explanation, not a measurement; a run with partitions of different lengths would confirm it.
For twok it means that the time to restore the pair after a heal depends on the transport and not only on twok.

### Cleaning up on NFS

etcd creates its directories with mode 0700 as UID 65532.
The NFS CSI driver deletes a volume's directory as a different user, fails with `permission denied`, and leaves the PersistentVolume `Released`.
The first runs of the bench left six such volumes behind.
They were reclaimed by hand: each PersistentVolume was bound to a new claim in the test namespace through `spec.claimRef` and `volumeName`, emptied from a Pod running as UID 65532, and released again, after which the driver deleted it.
The harness and `hack/kubernetes-bench-clean.sh` now empty each volume from a Pod running as UID 65532 before deleting its claim, and the later runs left nothing.

## twok results

To be filled when the twok image exists: the same runs with `TWOK_IMAGE` set and each arbiter overlay, plus the arbiter cut (`cut-arbiter.yaml` together with the partition).
What to record for each run:

- the time from the partition to the survivor's first successful put, against the expected 30 s of [design.md](design.md#expected-timeline-with-defaults);
- which member won, and whether it was the last leader;
- that the loser accepted no write during the partition;
- the time from the heal to the pair being restored, against the 48 s the baseline needs only to re-elect;
- that no acknowledged write was lost.
