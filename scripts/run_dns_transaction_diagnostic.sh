#!/usr/bin/env bash
set -euo pipefail

if test "$(id -u)" -ne 0; then
  echo "run as root" >&2
  exit 2
fi

repo_root="${PROBERCA_REPO_ROOT:-/home/jyz/probeRCA}"
output_root="${1:?usage: run_dns_transaction_diagnostic.sh OUTPUT_ROOT}"
kubeconfig="${KUBECONFIG:-/home/jyz/.kube/config}"
pod_namespace=proberca-dns-diagnostic
pod_name=proberca-dns-transaction-diagnostic
service_name=proberca-dns-transaction-diagnostic
raw_dir="${output_root}.raw"
final_dir="${output_root}"
ebpf_pid=
tcpdump_pid=

cleanup() {
  if test -n "${tcpdump_pid}" && kill -0 "${tcpdump_pid}" 2>/dev/null; then
    kill -INT "${tcpdump_pid}" 2>/dev/null || true
    wait "${tcpdump_pid}" 2>/dev/null || true
  fi
  if test -n "${ebpf_pid}" && kill -0 "${ebpf_pid}" 2>/dev/null; then
    kill -TERM "${ebpf_pid}" 2>/dev/null || true
    wait "${ebpf_pid}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

test ! -e "${raw_dir}"
test ! -e "${final_dir}"
mkdir -p "${raw_dir}"
chown jyz:jyz "${raw_dir}"

export KUBECONFIG="${kubeconfig}"
kubectl delete namespace "${pod_namespace}" \
  --ignore-not-found --wait=true >/dev/null

kubectl apply -f \
  "${repo_root}/deploy/final-dataplane/dns-transaction-diagnostic-pod.yaml" \
  >"${raw_dir}/kubectl-apply.txt"
kubectl -n "${pod_namespace}" wait \
  --for=condition=Ready "pod/${pod_name}" --timeout=90s

pod_ip="$(
  kubectl -n "${pod_namespace}" get "pod/${pod_name}" \
    -o jsonpath='{.status.podIP}'
)"
pod_uid="$(
  kubectl -n "${pod_namespace}" get "pod/${pod_name}" \
    -o jsonpath='{.metadata.uid}'
)"
dns_cluster_ip="$(
  kubectl -n kube-system get service kube-dns \
    -o jsonpath='{.spec.clusterIP}'
)"
node_pid="$(docker inspect -f '{{.State.Pid}}' proberca-ob-control-plane)"
netns_inode="$(stat -Lc '%i' "/proc/${node_pid}/ns/net")"
runtime_id="$(
  kubectl -n "${pod_namespace}" get "pod/${pod_name}" \
    -o jsonpath='{.status.containerStatuses[0].containerID}'
)"
container_id="${runtime_id#containerd://}"
mapfile -t cgroup_matches < <(
  find /sys/fs/cgroup -type d \
    -name "cri-containerd-${container_id}.scope" -print
)
if test "${#cgroup_matches[@]}" -ne 1; then
  echo "dedicated Pod cgroup path is ambiguous" >&2
  exit 3
fi
pod_cgroup_path="${cgroup_matches[0]}"
test ! -L "${pod_cgroup_path}"
case "$(readlink -f "${pod_cgroup_path}")" in
  /sys/fs/cgroup/*) ;;
  *)
    echo "dedicated Pod cgroup escaped /sys/fs/cgroup" >&2
    exit 4
    ;;
esac
pod_cgroup_id="$(stat -Lc '%i' "${pod_cgroup_path}")"

cat >"${raw_dir}/metadata.json" <<EOF
{
  "pod_ip": "${pod_ip}",
  "pod_uid": "${pod_uid}",
  "dns_cluster_ip": "${dns_cluster_ip}",
  "node_pid": ${node_pid},
  "netns": "${netns_inode}",
  "container_id": "${container_id}",
  "cgroup_path": "${pod_cgroup_path}",
  "cgroup_id": ${pod_cgroup_id}
}
EOF

/usr/local/lib/proberca-final/proberca-final-burst-loader \
  --object /usr/local/lib/proberca-final/final_burst.bpf.o \
  --cgroup "${pod_cgroup_path}" \
  --output "${raw_dir}/ebpf-events.jsonl" \
  --timeout-ms 5000 \
  --sampling-profile full \
  --dns-only \
  >"${raw_dir}/ebpf-loader.stdout" \
  2>"${raw_dir}/ebpf-loader.stderr" &
ebpf_pid=$!

nsenter -t "${node_pid}" -n tcpdump \
  -i any -U -s 0 -w "${raw_dir}/dns.pcap" \
  "host ${pod_ip} and port 53" \
  >"${raw_dir}/tcpdump.stdout" \
  2>"${raw_dir}/tcpdump.stderr" &
tcpdump_pid=$!

kubectl -n "${pod_namespace}" wait \
  --for=jsonpath='{.status.phase}'=Succeeded \
  "pod/${pod_name}" --timeout=180s
sleep 7
cleanup
tcpdump_pid=
ebpf_pid=

kubectl -n "${pod_namespace}" logs "pod/${pod_name}" \
  -c resolver >"${raw_dir}/application.jsonl"
nsenter -t "${node_pid}" -n tcpdump \
  -tt -nn -vvv -r "${raw_dir}/dns.pcap" \
  >"${raw_dir}/tcpdump.txt" 2>"${raw_dir}/tcpdump-read.stderr"

cd "${repo_root}"
PYTHONPATH="${repo_root}" python3 scripts/reconcile_dns_transaction_diagnostic.py \
  --application "${raw_dir}/application.jsonl" \
  --tcpdump-text "${raw_dir}/tcpdump.txt" \
  --ebpf "${raw_dir}/ebpf-events.jsonl" \
  --output "${final_dir}" \
  --pod-ip "${pod_ip}" \
  --dns-cluster-ip "${dns_cluster_ip}" \
  --pod-uid "${pod_uid}" \
  --service "${service_name}" \
  --netns "${netns_inode}" \
  >"${raw_dir}/reconcile.stdout"

cp "${raw_dir}/application.jsonl" "${final_dir}/"
cp "${raw_dir}/dns.pcap" "${final_dir}/"
cp "${raw_dir}/tcpdump.txt" "${final_dir}/"
cp "${raw_dir}/metadata.json" "${final_dir}/"
mv "${raw_dir}/ebpf-events.jsonl" \
  "${final_dir}/raw-ebpf-events.jsonl"
mkdir "${final_dir}/diagnostic-runtime"
find "${raw_dir}" -mindepth 1 -maxdepth 1 -type f \
  -exec mv -- {} "${final_dir}/diagnostic-runtime/" \;
rmdir "${raw_dir}"
(
  cd "${final_dir}"
  sha256sum \
    application.jsonl \
    dns.pcap \
    dns-transactions.jsonl \
    enriched-ebpf-dns-events.jsonl \
    metadata.json \
    pcap-dns-packets.jsonl \
    raw-ebpf-events.jsonl \
    reconciliation.json \
    report.md \
    tcpdump.txt \
    transactions.csv \
    > SHA256SUMS
)
chown -R jyz:jyz "${final_dir}"

echo "${final_dir}"
