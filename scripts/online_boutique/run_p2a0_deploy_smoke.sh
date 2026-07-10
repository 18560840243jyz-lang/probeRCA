#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

scripts/online_boutique/00_check_env.sh
scripts/online_boutique/01_prepare_repo.sh
scripts/online_boutique/02_create_kind_cluster.sh
scripts/online_boutique/03_deploy_online_boutique.sh
scripts/online_boutique/04_smoke_test_frontend.sh
python3 -m proberca.cli.write_online_boutique_graph --output data/p2_online_boutique/deploy_smoke/service_graph.jsonl
scripts/online_boutique/05_status.sh
