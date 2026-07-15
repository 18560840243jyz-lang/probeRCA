#!/usr/bin/env bash
set -euo pipefail
context="${PROBERCA_P11_CONTEXT:?set PROBERCA_P11_CONTEXT}"
namespace="${PROBERCA_P11_SMOKE_NAMESPACE:?set PROBERCA_P11_SMOKE_NAMESPACE}"
run_id="${PROBERCA_P11_RUN_ID:?set PROBERCA_P11_RUN_ID}"
[[ -n "$run_id" ]]
[[ "$(kubectl config current-context)" == "$context" ]]
case "$namespace" in default|kube-system|proberca-system) exit 2;; esac
kubectl create namespace "$namespace" --dry-run=client -o yaml |
  kubectl label --local -f - -o yaml \
    "proberca.io/smoke-run-id=$run_id" \
    "app.kubernetes.io/managed-by=proberca-p11-smoke" \
    "proberca.io/smoke-purpose=p11-final-gate" |
  kubectl apply -f - >/dev/null
kubectl auth can-i list pods --namespace "$namespace" >/dev/null
printf 'context_verified namespace_identity_applied\n'
