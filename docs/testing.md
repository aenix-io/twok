# Testing

What was measured, how, and with which result.
The design states what the tests must show in [design.md](design.md#failure-model); this file records what they did show.

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
The harness and `hack/kubernetes-bench-clean.sh` now empty each volume from a Pod running as UID 65532 before deleting its claim, and the later runs left nothing.

## twok results

To be filled when the twok image exists: the same runs with `TWOK_IMAGE` set and each arbiter overlay, plus the arbiter cut (`cut-arbiter.yaml` together with the partition).
What to record for each run:

- the time from the partition to the survivor's first successful put, against the expected 30 s of [design.md](design.md#expected-timeline-with-defaults);
- which member won, and whether it was the last leader;
- that the loser accepted no write during the partition;
- the time from the heal to the pair being restored, against the 48 s the baseline needs only to re-elect;
- that no acknowledged write was lost.
