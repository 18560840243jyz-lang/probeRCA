#!/usr/bin/env bash
set -euo pipefail
python3 -m proberca.cli.check_p0_freeze --freeze-dir docs/p0_freeze_snapshot
python3 -m proberca.cli.check_p1_freeze --freeze-dir docs/p1_freeze_snapshot
kubectl get pods -n online-boutique
kubectl get deploy -n online-boutique
python3 -m proberca.cli.run_p2c0_io_smoke --config configs/p2c0_online_boutique_io_smoke.yaml
python3 -m proberca.cli.check_p2c0_io_smoke --input data/p2_online_boutique/io_rediscart_smoke_001
