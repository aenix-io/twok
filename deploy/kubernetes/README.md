# twok on Kubernetes

A two-member etcd as a StatefulSet, each member with the twok agent as a native sidecar, and three ways to provide the arbiter.
This is research code; read [docs/design.md](../../docs/design.md) first.

## Layout

| Directory | What it holds |
| --- | --- |
| `base/` | the StatefulSet `etcd` with two replicas, the headless Service `etcd`, the client Service `etcd-client`, the ConfigMap `etcd-twok` with the twok settings, and a PodDisruptionBudget; not deployable on its own, it has no arbiter |
| `arbiter-etcd/` | an `EtcdCluster` named `arbiter` with one member, run by [etcd-operator](https://github.com/cozystack/etcd-operator), and `TWOK_ARBITER=etcd://arbiter-client:2379/twok/etcd` |
| `arbiter-kubernetes/` | a ServiceAccount, Role and RoleBinding for the ConfigMap `twok-arbiter`, and `TWOK_ARBITER=kubernetes://<namespace>/twok-arbiter` |
| `arbiter-http/` | the reference arbiter `twok arbiter serve` as a Deployment behind the Service `arbiter`, and `TWOK_ARBITER=http://arbiter:8080/twok/etcd` |

```bash
kubectl apply --kustomize deploy/kubernetes/arbiter-http --namespace <namespace>
```

The arbiter overlays put the arbiter in the same namespace as the members, which is what a test needs.
In a real two-site deployment the arbiter belongs in a third place that both sites can reach, and only `TWOK_ARBITER` changes.

For clusters run by etcd-operator, [deploy/twok-agent.yaml](../twok-agent.yaml) injects the same sidecar into the operator's member Pods; see the notes at the top of that file.

## The member Pod

- The container `etcd` runs the upstream etcd image with a plain etcd command line. Nothing in it knows about twok.
- The init container `twok` with `restartPolicy: Always` is a native sidecar: it starts before etcd and runs next to it. It runs `twok agent`, talks to etcd on `http://127.0.0.1:2379`, and mounts the same `data` volume at `/var/lib/etcd`, where it keeps its state file and sets the data directory aside on a rejoin.
- `shareProcessNamespace: true` lets the agent find etcd's process and stop it. The kubelet then starts the etcd container again with the same command, and etcd finds what the agent left on the volume. Both containers run as UID 65532, so no capability is needed for the signal.
- The Pod mirrors etcd-operator's member Pods: the label `etcd-operator.cozystack.io/cluster`, the container `etcd`, the volume `data` (and `tls-client`, `tls-peer` once TLS is on). That keeps one agent for both ways of running it.

The agent's flags are written in one place, the `twok` container of `base/statefulset.yaml`.
The other settings are files of the ConfigMap `etcd-twok`, mounted at `/etc/twok` and read again before every poll; a flag wins over a file.
The ConfigMap ships with `mode: "off"`, so a fresh deployment only observes; set `mode` to `enforce` to let twok act.

| Setting | Where | Default | Meaning |
| --- | --- | --- | --- |
| `--arbiter` | flag, from `TWOK_ARBITER` | | the arbiter URL, set by the overlay |
| `--endpoint` | flag | `http://127.0.0.1:2379` | the local member |
| `--data-dir` | flag | `/var/lib/etcd` | etcd's data directory, on the shared volume |
| `--metrics-listen` | flag | `:2390` | Prometheus metrics |
| `mode` | ConfigMap | `off` | `off` observes and logs, `dry-run` decides and logs, `enforce` acts; a missing ConfigMap means `off` |
| `poll-interval` | ConfigMap | `1s` | how often the local member's status is read |
| `claim-after` | ConfigMap | `20s` | how long the cluster must be without a leader before a claim |
| `claim-delay` | ConfigMap | `3s` | the last leader's wait before the claim |
| `follower-extra-delay` | ConfigMap | `5s` | the follower's extra wait |

The meaning of each setting is in [docs/design.md](../../docs/design.md#configuration).

## Images

Both images are set in one place, the `images` entries of `base/kustomization.yaml`: upstream etcd pinned by digest, and `ghcr.io/aenix-io/twok:main`, which moves with every push to `main`.
The HTTP arbiter takes its image from the sidecar through a kustomize replacement.
Pin a digest of the twok image for anything that should stay put.

## Notes

- The two Pods are kept on different nodes by a required anti-affinity; a cluster with one schedulable node cannot run them.
- The headless Service publishes not-ready addresses: a member without a leader is not ready, and the peers still need each other's DNS names to elect one.
- Readiness is etcd's `/readyz` on the metrics port, so a member without a leader leaves the `etcd-client` Service. There is no liveness probe; restarting etcd is the agent's job.
- The PodDisruptionBudget allows one of the two members to be evicted at a time. With plain etcd that stops writes until the member is back; with twok the remaining member claims after `claim-after`.
- etcd creates its directories with mode 0700 as UID 65532. A volume provisioner that deletes a volume's directory as another user, as the NFS CSI driver does, fails on such a volume and leaves the PersistentVolume `Released`. Empty the volume as UID 65532 before deleting the claim; `hack/kubernetes-bench-clean.sh` shows how.

## Follow-up: TLS

The manifests use plain HTTP between the members and for clients.
Turning TLS on means Secrets mounted as the volumes `tls-client` and `tls-peer` at `/etc/etcd/tls/client` and `/etc/etcd/tls/peer`, as etcd-operator does, `https://` in every URL of the etcd arguments, the etcd TLS flags, and the same volumes mounted read-only into the sidecar with its client certificate flags; `deploy/twok-agent.yaml` already does the sidecar part for operator Pods.
For the `etcd` arbiter the operator's `spec.tls` and a client certificate for the agent are needed too.
