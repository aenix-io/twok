# twok and etcd-operator

twok does not run inside clusters managed by [cozystack/etcd-operator](https://github.com/cozystack/etcd-operator) today.
This note says why, what the operator would need, and how the operator is still used in twok's tests.
It is based on the operator at v0.5.4, API `etcd-operator.cozystack.io/v1alpha2`; the code references are in [research/etcd-operator-and-s3.md](research/etcd-operator-and-s3.md).

## How twok gets into the member Pods

The same way threek does: a MutatingAdmissionPolicy in `deploy/twok-agent.yaml` adds a native sidecar to member Pods labelled `twok.aenix.io/inject: "true"`, through `spec.additionalMetadata.labels`.
Besides what threek's policy does, twok's mounts the member's `data` volume into the sidecar and sets `shareProcessNamespace: true` on the Pod, so the sidecar can find and stop the etcd process; both are Pod fields the policy may set, and the operator keeps building the Pod as it does.
The policy needs Kubernetes 1.36, or 1.35 with the `MutatingAdmissionPolicy` feature gate, which is why the test bench on a 1.33 cluster runs the same Pod layout as a StatefulSet instead.
The sidecar finds the survivor through the member's headless Service, `<subdomain>.<namespace>.svc`, which the policy passes as `--peer-endpoints`; with TLS that name has to be in the server certificate, and the operator issues it that way (checked on a v0.5.4 cluster: the SANs include `etcd.<namespace>.svc` and `*.etcd.<namespace>.svc`).

## What the operator does badly with a cluster twok reconfigures

The operator needs no change to run twok, but it was not written for a cluster whose membership changes behind its back:

- `EtcdMember.status.memberID` is written once and never refreshed. After a rejoin the member has a new ID; on deletion the operator tries to remove the old ID, gets "member not found", and requeues forever, which blocks every later scale operation. The fix is to look the member up by peer URL on every reconcile and to treat a removal of a member that is already gone as done.
- The operator promotes any learner it sees. twok's survivor promotes the rejoined learner too; the race is benign, because a promotion that is not ready fails and the other one succeeds.
- With two replicas the status reports quorum lost while the survivor serves, and the PodDisruptionBudget requires both members to be available, which blocks every voluntary eviction.
- The restore init container stays inert on a rejoin, which it already does once `member` exists in the data directory.

The two API changes once proposed for threek, `spec.extraArgs` ([#379](https://github.com/cozystack/etcd-operator/pull/379)) and a per-cluster image repository ([#380](https://github.com/cozystack/etcd-operator/pull/380)), are not needed by twok.

## How the operator is used in the tests

The tests on a real Kubernetes cluster run the two-member etcd under twok in a StatefulSet and use an `EtcdCluster` created by the operator as the arbiter, through the `etcd://` backend.
That checks the backend against a cluster the operator runs, with its TLS and its client Service, and keeps the operator's own clusters untouched.
