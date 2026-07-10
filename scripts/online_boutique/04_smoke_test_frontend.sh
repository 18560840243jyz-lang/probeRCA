#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

NAMESPACE="online-boutique"
OUT_DIR="data/p2_online_boutique/deploy_smoke"
PORT="8080"
mkdir -p "$OUT_DIR"

SERVICE=""
for candidate in frontend-external frontend; do
  if kubectl get svc "$candidate" -n "$NAMESPACE" >/dev/null 2>&1; then
    SERVICE="$candidate"
    break
  fi
done

if [ -z "$SERVICE" ]; then
  echo "ERROR: frontend service not found in namespace $NAMESPACE" | tee "$OUT_DIR/frontend_smoke_test.txt"
  exit 1
fi

echo "Using frontend service: $SERVICE" | tee "$OUT_DIR/frontend_smoke_test.txt"
# Stop only this script's port-forward process after the smoke test.
kubectl port-forward -n "$NAMESPACE" "svc/${SERVICE}" "${PORT}:80" >> "$OUT_DIR/frontend_smoke_test.txt" 2>&1 &
PF_PID=$!
trap 'kill "$PF_PID" >/dev/null 2>&1 || true' EXIT
sleep 5

HTTP_CODE=$(curl -L -sS -o "$OUT_DIR/frontend_response_head.html" -w '%{http_code}' "http://127.0.0.1:${PORT}" || true)
echo "HTTP status: $HTTP_CODE" | tee -a "$OUT_DIR/frontend_smoke_test.txt"
if [ "$HTTP_CODE" != "200" ]; then
  echo "ERROR: frontend smoke test failed" | tee -a "$OUT_DIR/frontend_smoke_test.txt"
  exit 1
fi

echo "frontend smoke test passed: http://127.0.0.1:${PORT}" | tee -a "$OUT_DIR/frontend_smoke_test.txt"
