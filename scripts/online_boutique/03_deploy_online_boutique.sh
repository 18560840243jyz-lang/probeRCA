#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

REPO_DIR="external/microservices-demo"
NAMESPACE="online-boutique"
OUT_DIR="data/p2_online_boutique/deploy_smoke"
mkdir -p "$OUT_DIR"

if [ -f "$REPO_DIR/release/kubernetes-manifests.yaml" ]; then
  MANIFEST="$REPO_DIR/release/kubernetes-manifests.yaml"
elif [ -d "$REPO_DIR/kubernetes-manifests" ]; then
  MANIFEST="$REPO_DIR/kubernetes-manifests"
else
  echo "ERROR: Online Boutique manifest not found under $REPO_DIR"
  exit 1
fi

echo "Applying manifest: $MANIFEST"
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -n "$NAMESPACE" -f "$MANIFEST"

echo "Waiting for deployments in namespace $NAMESPACE"
kubectl wait --for=condition=available --timeout=600s deployment --all -n "$NAMESPACE"

echo "Saving deployment status to $OUT_DIR"
kubectl get pods -n "$NAMESPACE" > "$OUT_DIR/kubectl_get_pods.txt"
kubectl get svc -n "$NAMESPACE" > "$OUT_DIR/kubectl_get_svc.txt"
kubectl get deploy -n "$NAMESPACE" > "$OUT_DIR/kubectl_get_deploy.txt"
kubectl get pods -n "$NAMESPACE" -o wide > "$OUT_DIR/kubectl_get_pods_wide.txt"
cat "$OUT_DIR/kubectl_get_pods.txt"
cat "$OUT_DIR/kubectl_get_svc.txt"
