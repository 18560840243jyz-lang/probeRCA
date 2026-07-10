#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

CLUSTER_NAME="proberca-ob"
echo "This script is intentionally dry-run only."
echo "To remove the P2A-0 kind cluster later, run explicitly:"
echo "kind delete cluster --name ${CLUSTER_NAME}"
echo "This does not run docker system prune and does not delete Docker volumes."
