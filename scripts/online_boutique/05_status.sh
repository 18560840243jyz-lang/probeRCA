#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

NAMESPACE="online-boutique"
echo "kubectl get pods -n $NAMESPACE"
kubectl get pods -n "$NAMESPACE"
echo "kubectl get svc -n $NAMESPACE"
kubectl get svc -n "$NAMESPACE"
echo "kubectl get deploy -n $NAMESPACE"
kubectl get deploy -n "$NAMESPACE"
echo "docker ps"
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}'
