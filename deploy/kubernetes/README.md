# twok on Kubernetes

A two-member etcd as a StatefulSet, each member running under twok, and three ways to provide the arbiter.
This is research code; read [docs/design.md](../../docs/design.md) first.

## Layout

| Directory | What it holds |
| --- | --- |
| `base/` | the StatefulSet `etcd` with two replicas, the headless Service `etcd`, the client Service `etcd-client`, the ConfigMap `twok` with the twok settings, and a PodDisruptionBudget; not deployable on its own, it has no arbiter |
| `arbiter-etcd/` | an `EtcdCluster` named `arbiter` with one member, run by [etcd-operator](https://github.com/cozystack/etcd-operator), and `TWOK_ARBITER=etcd://arbiter-client:2379/twok/etcd` |
| `arbiter-kubernetes/` | a ServiceAccount, Role and RoleBinding for the ConfigMap `twok-arbiter`, and `TWOK_ARBITER=kubernetes://<namespace>/twok-arbiter` |
| `arbiter-http/` | the reference arbiter `twok arbiter serve` as a Deployment behind the Service `arbiter`, and `TWOK_ARBITER=http://arbiter:8080/twok/etcd` |

```bash
kubectl apply --kustomize deploy/kubernetes/arbiter-http --namespace <namespace>
```

The arbiter overlays put the arbiter in the same namespace as the members, which is what a test needs.
In a real two-site deployment the arbiter belongs in a third place that both sites can reach, and only the URL in `TWOK_ARBITER` changes.

## Image

The member container runs `ghcr.io/aenix-io/twok-etcd:v<etcd version>`: the upstream etcd image with twok installed as `/usr/local/bin/etcd` and the real etcd next to it.
The container's command line is therefore a plain etcd command line, and twok reads its own settings from the `TWOK_*` variables of the ConfigMap `twok` and of the StatefulSet's `env`.
The same settings can be passed as `--twok-*` flags among the etcd arguments; a flag wins over the variable.

The image is set in one place, the `images` entry in `base/kustomization.yaml`, and the arbiter image of `arbiter-http` in `arbiter-http/kustomization.yaml`.
The development tags (`v3.6.15-main`, `main`) move with every push to `main`; pin a digest for anything that should stay put.

Replacing the image with the upstream etcd image runs the same StatefulSet as plain etcd, which is how the baseline in [docs/testing.md](../../docs/testing.md) is measured, see `test/kubernetes/manifests/baseline/`.

## Settings

| Variable | Default | Meaning |
| --- | --- | --- |
| `TWOK_ARBITER` | | the arbiter URL, set by the overlay |
| `TWOK_POLL_INTERVAL` | `1s` | how often the local member's status is read |
| `TWOK_CLAIM_AFTER` | `20s` | how long the cluster must be without a leader before a claim |
| `TWOK_CLAIM_DELAY` | `3s` | the last leader's wait before the claim |
| `TWOK_FOLLOWER_EXTRA_DELAY` | `5s` | the follower's extra wait |
| `TWOK_COMMIT_TAIL` | `true` | mark the whole log as committed before `--force-new-cluster` |
| `TWOK_METRICS_LISTEN` | `:2390` | Prometheus metrics |

The meaning of each setting is in [docs/design.md](../../docs/design.md#configuration).

## Notes

- The two Pods are kept on different nodes by a required anti-affinity; a cluster with one schedulable node cannot run them.
- The headless Service publishes not-ready addresses: a member without a leader is not ready, and the peers still need each other's DNS names to elect one.
- Readiness is etcd's `/readyz` on the metrics port, so a member without a leader leaves the `etcd-client` Service. There is no liveness probe; restarting etcd is twok's job.
- The data directory is `/var/lib/etcd/data` on the volume `data`, so that etcd creates it with its own permissions; twok's state file lives on the same volume.
- The PodDisruptionBudget allows one of the two members to be evicted at a time. With plain etcd that stops writes until the member is back; with twok it is a planned isolation, and the remaining member claims after `TWOK_CLAIM_AFTER`.
- The members run as UID 65532 with a read-only root filesystem and no capabilities.

## Follow-up: TLS

The manifests use plain HTTP between the members and for clients.
twok supports mTLS: it takes the TLS files from the etcd arguments it passes through.
Turning it on means a Secret with the server, peer and client certificates mounted into the member Pods, `https://` in every URL of the arguments, and the `--cert-file`, `--key-file`, `--trusted-ca-file`, `--client-cert-auth`, `--peer-cert-file`, `--peer-key-file`, `--peer-trusted-ca-file` and `--peer-client-cert-auth` flags.
For the `etcd` arbiter the operator's `spec.tls` and the client certificate in the arbiter URL's credentials are needed too.
