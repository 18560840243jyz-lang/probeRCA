from __future__ import annotations

from pathlib import Path

import yaml


REQUIRED_LABELS = {
    "app.kubernetes.io/managed-by": "proberca-p11-smoke",
    "proberca.io/smoke-purpose": "p11-final-gate",
    "proberca.io/smoke-run-id": "P11_SMOKE_RUN_ID",
}


def test_smoke_kustomization_applies_creation_time_identity_labels():
    payload = yaml.safe_load(Path(
        "deploy/kubernetes/test/p11-smoke/kustomization.yaml").read_text())
    pairs = payload["labels"][0]["pairs"]
    assert REQUIRED_LABELS.items() <= pairs.items()
    assert payload["labels"][0]["includeTemplates"] is True


def test_smoke_deployment_declares_safe_rolling_strategy_and_labels():
    deployment = yaml.safe_load(Path(
        "deploy/kubernetes/test/p11-smoke/proberca-live-deployment.yaml").read_text())
    assert deployment["spec"]["strategy"]["rollingUpdate"] == {
        "maxUnavailable": 0, "maxSurge": 1}
    assert deployment["spec"]["template"]["spec"]["terminationGracePeriodSeconds"] >= 30
    assert deployment["spec"]["template"]["spec"]["containers"][0]["readinessProbe"]["httpGet"]["path"] == "/podreadyz"


def test_cleanup_script_requires_all_identity_guards_and_protects_system_namespaces():
    source = Path("scripts/p11_smoke_cleanup.sh").read_text(encoding="utf-8")
    for required in (
        "proberca.io/smoke-run-id", "app.kubernetes.io/managed-by",
        "proberca.io/smoke-purpose", "kubectl config current-context",
        "default", "kube-system", "proberca-system",
    ):
        assert required in source
    assert "kubectl label" not in source


def test_cleanup_discovers_cluster_scoped_resources_by_full_ownership_labels():
    source = Path("scripts/p11_smoke_cleanup.sh").read_text(encoding="utf-8")
    assert '-l "$run_label=$run_id,$managed_label=proberca-p11-smoke,$purpose_label=p11-final-gate"' in source
    assert "proberca-p11-smoke-reader" not in source


def test_preflight_labels_namespace_before_any_smoke_workload_apply():
    source = Path("scripts/p11_smoke_preflight.sh").read_text(encoding="utf-8")
    assert "proberca.io/smoke-run-id" in source
    assert "app.kubernetes.io/managed-by" in source
    assert "proberca.io/smoke-purpose" in source


def test_smoke_run_rechecks_context_and_waits_by_selected_workload_role():
    source = Path("scripts/p11_smoke_run.sh").read_text(encoding="utf-8")
    assert "PROBERCA_P11_CONTEXT" in source
    assert "kubectl config current-context" in source
    assert '[[ "$stack_profile" == live ]]' in source
    assert "rollout status deployment -l proberca.io/workload-role=live-runner" in source
    assert "wait --for=condition=complete job -l proberca.io/workload-role=bounded-runner" in source
    assert "rollout status deployment/proberca-live" not in source
