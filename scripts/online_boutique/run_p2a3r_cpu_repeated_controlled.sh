#!/usr/bin/env bash
set -euo pipefail
python3 -m proberca.cli.check_p0_freeze --freeze-dir docs/p0_freeze_snapshot
python3 -m proberca.cli.check_p1_freeze --freeze-dir docs/p1_freeze_snapshot
kubectl get pods -n online-boutique
kubectl get deploy -n online-boutique
python3 -m proberca.cli.diagnose_p2a3_cpu_repeated   --input data/p2_online_boutique/cpu_paymentservice_repeated
python3 -m proberca.cli.run_p2a3_cpu_repeated   --config configs/p2a3r_online_boutique_cpu_repeated_controlled.yaml
python3 -m proberca.cli.check_p2a3r_cpu_repeated   --input data/p2_online_boutique/cpu_paymentservice_repeated_controlled
