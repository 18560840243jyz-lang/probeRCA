#!/usr/bin/env bash
set -euo pipefail
python3 -m proberca.cli.check_p0_freeze --freeze-dir docs/p0_freeze_snapshot
python3 -m proberca.cli.check_p1_freeze --freeze-dir docs/p1_freeze_snapshot
kubectl get pods -n online-boutique
kubectl get deploy -n online-boutique
python3 -m proberca.cli.run_p2d1_lock_repeated --config configs/p2d1_online_boutique_lock_repeated.yaml
python3 -m proberca.cli.check_p2d1_lock_repeated --input data/p2_online_boutique/lock_cartservice_repeated
