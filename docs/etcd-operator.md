# twok and etcd-operator

twok does not run inside clusters managed by [cozystack/etcd-operator](https://github.com/cozystack/etcd-operator) today.
This note says why, what the operator would need, and how the operator is still used in twok's tests.
It is based on the operator at v0.5.4, API `etcd-operator.cozystack.io/v1alpha2`; the code references are in [research/etcd-operator-and-s3.md](research/etcd-operator-and-s3.md).

## Why not today

The operator builds every member Pod itself: the image is `<repository>:v<spec.version>`, the container command is `etcd` with the flags the operator computes, and the API has no field for extra arguments.
twok's image stands in for etcd, so the image part is one operator-wide flag away: `--etcd-image-repository=ghcr.io/aenix-io/twok-etcd` resolves to `ghcr.io/aenix-io/twok-etcd:v<spec.version>`, which is how the image is tagged.
That switches every cluster the operator manages at once, and it leaves no way to pass `--twok-arbiter` to the member, so it is not enough on its own.

## What the operator would need

Two changes were already written for threek and closed when threek moved to a sidecar, see [#379](https://github.com/cozystack/etcd-operator/pull/379) and [#380](https://github.com/cozystack/etcd-operator/pull/380).
They were not rejected on their merits, and they are exactly what twok needs:

1. `spec.extraArgs`: a map of etcd flags rendered after the operator's own, with a denylist of the flags the operator manages (`name`, `data-dir`, the URLs, `initial-cluster*`, `force-new-cluster`, TLS). twok's flags are `--twok-*`, which the denylist does not cover, and twok changes `initial-cluster*` and `force-new-cluster` inside the container, never through the operator.
2. A per-cluster image repository, so that one cluster can run the twok image while the others keep stock etcd.

Beyond those, the operator should learn to live with a cluster whose membership twok changes:

- `EtcdMember.status.memberID` and `isVoter` refreshed from the member list by peer URL on every reconcile, and a removal of a member that is already gone treated as done. Today a member re-added under a new ID wedges its own deletion and, with it, every later scale operation.
- Learner promotion left to twok while a cluster is in twok mode. The operator promotes any learner it sees; the race is benign but confusing in logs.
- Status and the PodDisruptionBudget aware of two members: one ready member out of two is reported as quorum lost, and the budget requires both to be available, which blocks every voluntary eviction.
- The restore init container stays inert on a rejoin, which it already does once `member` exists in the data directory.

Two replicas are accepted by the API as it is, and the bootstrap flow, one seed then one learner, is the flow twok expects.

## How the operator is used in the tests

The tests on a real Kubernetes cluster run the two-member etcd under twok in a StatefulSet and use an `EtcdCluster` created by the operator as the arbiter, through the `etcd://` backend.
That checks the backend against a cluster the operator runs, with its TLS and its client Service, and keeps the operator's own clusters untouched.
