#!/usr/bin/env bash
set -euo pipefail

if test "$(id -u)" -ne 0; then
  echo "run as root" >&2
  exit 2
fi

repo_root="${PROBERCA_REPO_ROOT:-/home/jyz/probeRCA}"
kubeconfig="${KUBECONFIG:-/home/jyz/.kube/config}"
output_root="${1:?usage: validate_dns_formal_aggregation.sh OUTPUT_ROOT}"
raw_dir="${output_root}.raw"
final_dir="${output_root}"
namespace=proberca-dns-formal-validation
pod=proberca-dns-formal-validation
validation_service=proberca-dns-formal-validation
pin_dir="/sys/fs/bpf/proberca-dns-formal-validation-$$"
loader_pid=

cleanup() {
  if test -n "${loader_pid}" && kill -0 "${loader_pid}" 2>/dev/null; then
    kill -TERM "${loader_pid}" 2>/dev/null || true
    wait "${loader_pid}" 2>/dev/null || true
  fi
  case "${pin_dir}" in
    /sys/fs/bpf/proberca-dns-formal-validation-*)
      rm -f -- "${pin_dir}"/* 2>/dev/null || true
      rmdir -- "${pin_dir}" 2>/dev/null || true
      ;;
  esac
  KUBECONFIG="${kubeconfig}" kubectl delete namespace "${namespace}" \
    --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT

test ! -e "${raw_dir}"
test ! -e "${final_dir}"
mkdir -p "${raw_dir}/build"
export KUBECONFIG="${kubeconfig}"

cd "${repo_root}"
make -f Makefile.final \
  BUILD_DIR="${raw_dir}/build" \
  BPFTOOL=/usr/lib/linux-tools-5.15.0-185/bpftool \
  all >"${raw_dir}/build.stdout" 2>"${raw_dir}/build.stderr"

"${raw_dir}/build/proberca-final-ebpf-loader" \
  --object "${raw_dir}/build/final_normal.bpf.o" \
  --cgroup /sys/fs/cgroup \
  --pin-dir "${pin_dir}" \
  >"${raw_dir}/loader.stdout" 2>"${raw_dir}/loader.stderr" &
loader_pid=$!

ready=false
for _attempt in $(seq 1 100); do
  if grep -q '"state":"ready"' "${raw_dir}/loader.stdout"; then
    ready=true
    break
  fi
  if ! kill -0 "${loader_pid}" 2>/dev/null; then
    echo "formal DNS BPF loader exited before readiness" >&2
    exit 3
  fi
  sleep 0.1
done
if test "${ready}" != true; then
  echo "formal DNS BPF loader readiness timeout" >&2
  exit 4
fi

kubectl delete namespace "${namespace}" \
  --ignore-not-found --wait=true >/dev/null
kubectl apply -f \
  "${repo_root}/deploy/final-dataplane/dns-formal-validation-pod.yaml" \
  >"${raw_dir}/kubectl-apply.txt"
kubectl -n "${namespace}" wait \
  --for=condition=Ready "pod/${pod}" --timeout=90s

cgroup_identity() {
  local item_namespace="$1"
  local item_pod="$2"
  local item_container="$3"
  local runtime_id
  local container_id
  local matches
  runtime_id="$(
    kubectl -n "${item_namespace}" get "pod/${item_pod}" \
      -o "jsonpath={.status.containerStatuses[?(@.name=='${item_container}')].containerID}"
  )"
  container_id="${runtime_id#containerd://}"
  if test -z "${container_id}" || test "${container_id}" = "${runtime_id}"; then
    return 1
  fi
  mapfile -t matches < <(
    find /sys/fs/cgroup -type d \
      -name "cri-containerd-${container_id}.scope" -print
  )
  if test "${#matches[@]}" -ne 1; then
    return 1
  fi
  stat -Lc '%i' "${matches[0]}"
}

validation_cgroup="$(
  cgroup_identity "${namespace}" "${pod}" server
)"
frontend_pod="$(
  kubectl -n online-boutique get pods -l app=frontend \
    -o jsonpath='{.items[0].metadata.name}'
)"
frontend_server_cgroup="$(
  cgroup_identity online-boutique "${frontend_pod}" server
)"
frontend_sidecar_cgroup="$(
  cgroup_identity online-boutique "${frontend_pod}" \
    proberca-healthy-dns-exposure
)"

python3 - \
  "${validation_cgroup}" \
  "${frontend_server_cgroup}" \
  "${frontend_sidecar_cgroup}" \
  >"${raw_dir}/identities.json" <<'PY'
import json
import sys

print(json.dumps([
    {
        "cgroup_id": int(sys.argv[1]),
        "namespace": "proberca-dns-formal-validation",
        "service": "proberca-dns-formal-validation",
        "container": "server",
    },
    {
        "cgroup_id": int(sys.argv[2]),
        "namespace": "online-boutique",
        "service": "frontend",
        "container": "server",
    },
    {
        "cgroup_id": int(sys.argv[3]),
        "namespace": "online-boutique",
        "service": "frontend",
        "container": "proberca-healthy-dns-exposure",
    },
], indent=2, sort_keys=True))
PY

kubectl -n "${namespace}" wait \
  --for=jsonpath='{.status.phase}'=Succeeded \
  "pod/${pod}" --timeout=180s
kubectl -n "${namespace}" logs "pod/${pod}" \
  -c server >"${raw_dir}/application.jsonl"

"${raw_dir}/build/proberca-final-ebpf-loader" \
  --snapshot "${pin_dir}" \
  --timeout-ms 5000 \
  --cgroup-id "${validation_cgroup}" \
  --cgroup-id "${frontend_server_cgroup}" \
  --cgroup-id "${frontend_sidecar_cgroup}" \
  >"${raw_dir}/bpf-snapshot.jsonl" \
  2>"${raw_dir}/snapshot.stderr"

dns_cluster_ip="$(
  kubectl -n kube-system get service kube-dns \
    -o jsonpath='{.spec.clusterIP}'
)"
sudo -u "${SUDO_USER:-jyz}" env PYTHONPATH="${repo_root}" \
  python3 scripts/validate_dns_formal_aggregation.py \
  --snapshot-jsonl "${raw_dir}/bpf-snapshot.jsonl" \
  --identity-json "${raw_dir}/identities.json" \
  --application-jsonl "${raw_dir}/application.jsonl" \
  --policy configs/final_dns_aggregation_policy.yaml \
  --dns-cluster-ip "${dns_cluster_ip}" \
  --validation-service "${validation_service}" \
  --output "${final_dir}" \
  >"${raw_dir}/validation.stdout"

cp "${raw_dir}/application.jsonl" "${final_dir}/"
cp "${raw_dir}/bpf-snapshot.jsonl" "${final_dir}/"
cp "${raw_dir}/identities.json" "${final_dir}/"
cp configs/final_dns_aggregation_policy.yaml "${final_dir}/"
mkdir "${final_dir}/validation-runtime"
find "${raw_dir}" -mindepth 1 -maxdepth 1 -type f \
  -exec mv -- {} "${final_dir}/validation-runtime/" \;
rm -rf -- "${raw_dir}/build"
rmdir -- "${raw_dir}"
(
  cd "${final_dir}"
  find . -type f ! -name SHA256SUMS -print0 \
    | sort -z \
    | xargs -0 sha256sum >SHA256SUMS
)
chown -R "${SUDO_USER:-jyz}:${SUDO_USER:-jyz}" "${final_dir}"
echo "${final_dir}"
