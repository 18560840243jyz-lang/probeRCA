#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

CLUSTER_NAME="proberca-ob"
if kind get clusters | grep -Fx "$CLUSTER_NAME" >/dev/null 2>&1; then
  echo "kind cluster already exists: $CLUSTER_NAME"
else
  echo "Creating single-node kind cluster: $CLUSTER_NAME"
  kind create cluster --name "$CLUSTER_NAME"
fi
kubectl cluster-info --context "kind-${CLUSTER_NAME}"
kubectl config current-context
