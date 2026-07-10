#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

python3 -m proberca.cli.check_p0_freeze --freeze-dir docs/p0_freeze_snapshot
python3 -m proberca.cli.check_p1_freeze --freeze-dir docs/p1_freeze_snapshot
kubectl get pods -n online-boutique
kubectl get deploy -n online-boutique
cadvisor_head="$(mktemp)"
kubectl get --raw /api/v1/nodes/proberca-ob-control-plane/proxy/metrics/cadvisor > "$cadvisor_head"
head -20 "$cadvisor_head"
rm -f "$cadvisor_head"
python3 -m proberca.cli.run_p2a1_cpu_fault --config configs/p2a1r_online_boutique_cpu_fault_cadvisor.yaml
echo "注意：当前是 P2A-1R real CPU metric collection repair，不运行 RCA pipeline，不输出准确率。"
