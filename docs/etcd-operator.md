# twok and etcd-operator

twok does not run inside clusters managed by [cozystack/etcd-operator](https://github.com/cozystack/etcd-operator) today.
This note says why, what the operator would need, and how the operator is still used in twok's tests.
It is based on the operator at v0.5.4, API `etcd-operator.cozystack.io/v1alpha2`; the code references are in [research/etcd-operator-and-s3.md](research/etcd-operator-and-s3.md).

## Why not today

The operator builds every member Pod itself: the image is `<repository>:v<spec.version>`, the container command is `etcd` with the flags the operator computes, and the API has no field for a different command, extra arguments, environment, init containers or sidecars.
twok has to be etcd's parent process to restart it with `--force-new-cluster` and to set the data directory aside on a rejoin, so there is no place to put it.

threek had the same constraint and solved it with a mutating admission policy that adds a sidecar.
That does not work for twok: a sidecar can call the API, but cannot restart the etcd container with other flags.

## What the operator would need

From what the code does, the minimal set is:

1. A way to run the member under a supervisor: a `spec.twok` (or `spec.arbiter`) block that makes the operator start the container as `twok run --arbiter <url> -- etcd <the flags it computes today>`, with the twok binary either in the etcd image the operator uses or copied by an init container, and the arbiter credentials mounted from a Secret.
2. Membership read back from etcd instead of written once: `EtcdMember.status.memberID` and `isVoter` refreshed from the member list by peer URL on every reconcile, and a removal of a member that is already gone treated as done. Today a member re-added under a new ID wedges its own deletion and, with it, every later scale operation.
3. Learner promotion left to twok while a cluster is in twok mode. The operator promotes any learner it sees; the race is benign but confusing in logs.
4. Status and the PodDisruptionBudget aware of two members: one ready member out of two is reported as quorum lost, and the budget requires both to be available, which blocks every voluntary eviction.
5. The data directory handled by twok on a rejoin, which it does by renaming `member` inside the volume; the operator's restore init container must stay inert in that case, which it already is once `member` exists.

Two replicas are accepted by the API as it is, and the bootstrap flow, one seed then one learner, is the flow twok expects.

## How the operator is used in the tests

The tests on a real Kubernetes cluster run the two-member etcd under twok in a StatefulSet and use an `EtcdCluster` created by the operator as the arbiter, through the `etcd://` backend.
That checks the backend against a cluster the operator runs, with its TLS and its client Service, and keeps the operator's own clusters untouched.
