#!/usr/bin/env bash
# Lists, and with --yes deletes, everything the Kubernetes bench creates in
# its namespace: the objects labelled app.kubernetes.io/part-of=twok. The
# volumes are emptied first by a Pod running as the members' user, see
# test/kubernetes/manifests/wipe.yaml. The namespace itself stays.
#
#   K8S_KUBECONFIG=/path/to/kubeconfig hack/kubernetes-bench-clean.sh [--yes]
set -euo pipefail

: "${K8S_KUBECONFIG:?set K8S_KUBECONFIG to the kubeconfig of the test cluster}"
namespace=${K8S_NAMESPACE:-twok-test}
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
selector=app.kubernetes.io/part-of=twok
kinds=statefulsets,deployments,pods,services,configmaps,persistentvolumeclaims,poddisruptionbudgets,ciliumnetworkpolicies,serviceaccounts,roles,rolebindings
k=(kubectl --kubeconfig "${K8S_KUBECONFIG}" --namespace "${namespace}")
# The image of the wipe Pod, the same busybox the harness uses.
image=$(sed -n 's/^\tfetchImage = "\(.*\)"$/\1/p' "${root}/test/kubernetes/kubernetes_test.go")

"${k[@]}" get "${kinds}" --selector "${selector}" --ignore-not-found
if [[ "${1:-}" != "--yes" ]]; then
  echo "dry run; pass --yes to delete the objects above"
  exit 0
fi
"${k[@]}" delete statefulsets,deployments,pods --selector "${selector}" --ignore-not-found --wait=true
for claim in $("${k[@]}" get persistentvolumeclaims --selector "${selector}" --output name); do
  claim=${claim#persistentvolumeclaim/}
  pod=$(sed -e "s|{{ .Claim }}|${claim}|g" -e "s|{{ .Image }}|${image}|g" "${root}/test/kubernetes/manifests/wipe.yaml")
  "${k[@]}" apply --filename - <<<"${pod}"
  "${k[@]}" wait --for=jsonpath='{.status.phase}'=Succeeded --timeout=2m "pod/wipe-${claim}"
  "${k[@]}" delete --filename - --wait=true <<<"${pod}"
done
"${k[@]}" delete "${kinds}" --selector "${selector}" --ignore-not-found --wait=true
# The EtcdCluster of the arbiter-etcd overlay, when its CRD exists.
"${k[@]}" delete etcdclusters.etcd-operator.cozystack.io arbiter --ignore-not-found --wait=true 2>/dev/null || true
