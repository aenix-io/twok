#!/usr/bin/env bash
# Runs the Kubernetes partition bench of test/kubernetes against a real
# cluster and keeps the raw record under test/kubernetes/results/.
#
#   K8S_KUBECONFIG=/path/to/kubeconfig hack/kubernetes-bench.sh [TestPartition|TestPartitionOneWay]
#
# Environment:
#   K8S_KUBECONFIG        kubeconfig of the cluster (required)
#   K8S_NAMESPACE         namespace to work in, must exist (default twok-test)
#   TWOK_IMAGE            twok image for the sidecar and the HTTP arbiter;
#                         unset runs the plain etcd baseline without a sidecar
#   TWOK_MODE             the agent's mode in a twok run (default enforce)
#   TWOK_PARTITION_FOR    how long the partition lasts (default 60s)
#   TWOK_HEAL_FOR         how long to watch at least after the heal (default 10s)
#   TWOK_HEAL_TIMEOUT     how long to wait at most for the pair to recover (default 5m)
#   K8S_CILIUM_NAMESPACE  namespace of the Cilium agents, for the read-only
#                         policy verdict monitor; - to skip (default cozy-cilium)
#   K8S_KEEP              set to leave everything in place
set -euo pipefail

: "${K8S_KUBECONFIG:?set K8S_KUBECONFIG to the kubeconfig of the test cluster}"
test_name=${1:-TestPartition}
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
mode=baseline
if [[ -n "${TWOK_IMAGE:-}" ]]; then
  mode=twok
fi
results="${root}/test/kubernetes/results/${mode}-${test_name}-$(date -u +%Y%m%dT%H%M%SZ).log"
mkdir -p "$(dirname "${results}")"

export K8S_KUBECONFIG TWOK_RESULTS="${results}"
cd "${root}"
go test -tags kubernetes -count=1 -v -timeout 30m -run "^${test_name}\$" ./test/kubernetes/
echo "raw record: ${results}"
