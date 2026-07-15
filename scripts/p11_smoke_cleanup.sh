#!/usr/bin/env bash
set -euo pipefail
namespace="${PROBERCA_P11_SMOKE_NAMESPACE:?set PROBERCA_P11_SMOKE_NAMESPACE}"
run_id="${PROBERCA_P11_RUN_ID:?set PROBERCA_P11_RUN_ID}"
context="${PROBERCA_P11_CONTEXT:?set PROBERCA_P11_CONTEXT}"
run_label="proberca.io/smoke-run-id"
managed_label="app.kubernetes.io/managed-by"
purpose_label="proberca.io/smoke-purpose"
[[ -n "$run_id" ]]
[[ "$(kubectl config current-context)" == "$context" ]]
case "$namespace" in default|kube-system|proberca-system) printf 'refusing protected namespace\n' >&2; exit 2;; esac
if ! kubectl get namespace "$namespace" >/dev/null 2>&1; then
  printf 'isolated_smoke_namespace_already_absent\n'
  exit 0
fi
actual="$(kubectl get namespace "$namespace" -o jsonpath='{.metadata.labels.proberca\.io/smoke-run-id}')"
managed="$(kubectl get namespace "$namespace" -o jsonpath='{.metadata.labels.app\.kubernetes\.io/managed-by}')"
purpose="$(kubectl get namespace "$namespace" -o jsonpath='{.metadata.labels.proberca\.io/smoke-purpose}')"
[[ "$actual" == "$run_id" ]] || { printf 'refusing cleanup: run label mismatch\n' >&2; exit 2; }
[[ "$managed" == "proberca-p11-smoke" ]] || { printf 'refusing cleanup: managed-by mismatch\n' >&2; exit 2; }
[[ "$purpose" == "p11-final-gate" ]] || { printf 'refusing cleanup: purpose mismatch\n' >&2; exit 2; }
for resource in clusterrole clusterrolebinding; do
  mapfile -t owned < <(kubectl get "$resource" -l "$run_label=$run_id,$managed_label=proberca-p11-smoke,$purpose_label=p11-final-gate" -o name)
  [[ "${#owned[@]}" -eq 1 ]] || { printf 'refusing cluster cleanup: expected one owned %s\n' "$resource" >&2; exit 2; }
  for item in "${owned[@]}"; do
    owner="$(kubectl get "$item" -o jsonpath='{.metadata.labels.proberca\.io/smoke-run-id}')"
    item_managed="$(kubectl get "$item" -o jsonpath='{.metadata.labels.app\.kubernetes\.io/managed-by}')"
    item_purpose="$(kubectl get "$item" -o jsonpath='{.metadata.labels.proberca\.io/smoke-purpose}')"
    [[ "$owner" == "$run_id" ]] || { printf 'refusing cluster cleanup: run label mismatch\n' >&2; exit 2; }
    [[ "$item_managed" == "proberca-p11-smoke" ]] || { printf 'refusing cluster cleanup: managed-by mismatch\n' >&2; exit 2; }
    [[ "$item_purpose" == "p11-final-gate" ]] || { printf 'refusing cluster cleanup: purpose mismatch\n' >&2; exit 2; }
    kubectl delete "$item" --wait=false >/dev/null
  done
done
kubectl delete namespace "$namespace" --wait=false >/dev/null
printf 'isolated_smoke_namespace_cleanup_requested\n'
