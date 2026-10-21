# twok

etcd on two members, with an external arbiter that breaks the tie.

> **Status: research.** twok is an experiment in running etcd across two sites without a third member. It is not released and not meant for production. The design, the safety argument and the failure model are in [docs/design.md](docs/design.md); what was measured on a test bench is in [docs/testing.md](docs/testing.md). Read both before running it anywhere that matters.

## The problem

Raft needs a majority. With two etcd members the majority is both of them, so the first failure stops the cluster: a dead node, a dead link, a dead site.
A third member in a third site fixes that, but it takes part in every write, so it needs to be as close as the other two. Not every pair of sites has such a third site.

## What twok does

twok runs the etcd process on each of the two members. While the cluster has a leader, twok does nothing.
When the cluster has had no leader for a while, each member tries to claim the right to go on alone in an **arbiter**: a small record in an external system that supports an atomic compare-and-swap.
Exactly one member can win. The winner restarts its etcd as a cluster of one and serves. The loser cannot commit anything, because Raft does not let a voter commit without the other, and when it meets the survivor again it discards its data and rejoins as a learner.

The arbiter is not on the write path, holds a few hundred bytes, and only matters when the cluster has lost its leader. It can be:

- an S3 bucket;
- another etcd cluster;
- a Kubernetes API server;
- any HTTP service that implements conditional `PUT` with `If-Match`.

A new backend is a new package that implements one interface, see [docs/design.md](docs/design.md#arbiter-backends).

```mermaid
sequenceDiagram
    participant A as etcd A (twok)
    participant R as arbiter
    participant B as etcd B (twok)

    Note over A,B: the link between A and B breaks
    Note over A: no leader for claim-after
    Note over B: no leader for claim-after
    A->>R: compare-and-swap generation 3 → 4, survivor A
    R-->>A: ok
    B->>R: compare-and-swap generation 3 → 4, survivor B
    R-->>B: conflict: generation 4, survivor A
    Note over A: force-new-cluster, serves alone
    Note over B: stops etcd, waits for A
    Note over A,B: the link heals
    B->>A: add me as a learner
    A-->>B: ok
    Note over B: rebuilds from A, promoted to voter
```

## What is guaranteed, and what is not

Guaranteed, for any sequence of link, node, site and arbiter failures, as long as the arbiter is a correct compare-and-swap register:

- no two clusters derived from the original accept writes at the same time;
- no write acknowledged to a client is lost by a takeover.

Not guaranteed:

- availability while the arbiter is unreachable at the moment of a failure: writes stop until it, or the peer, is back;
- availability after a double failure, when the survivor fails before the loser has rejoined;
- anything about clients that keep talking to the loser, or about workloads and storage on the losing node.

The full table, with what was measured and what is only assumed, is in [docs/design.md](docs/design.md#failure-model).

## How it runs

twok is one binary that starts etcd as a child process and restarts it when it has to:

```bash
twok run --arbiter s3://bucket/clusters/prod -- etcd --name a --data-dir /var/lib/etcd ...
```

- **Kubernetes:** a StatefulSet of two Pods, see [deploy/kubernetes/](deploy/kubernetes/).
- **etcd-operator:** not without changes to the operator, see [docs/etcd-operator.md](docs/etcd-operator.md).
- **Talos:** not supported; Talos runs etcd itself.

## Development

```bash
make test   # unit and integration tests; set TEST_ETCD_BINARY to an etcd binary to run the tests against real clusters
make lint   # golangci-lint
make build  # bin/twok
```

## Related

- [threek](https://github.com/aenix-io/threek): keeps the Raft leader of a three-member etcd on a member every other member can reach.
- [Two-node OpenShift with fencing](https://www.redhat.com/en/blog/two-node-openshift-fencing-improves-reliability-edge): the same reconfiguration, with a BMC instead of an arbiter.

## License

Apache License 2.0, see [LICENSE](LICENSE).
