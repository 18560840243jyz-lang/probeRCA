from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

import proberca.cli.live as live_cli
from proberca.cli.live import _parser
from proberca.config import KubernetesConfig, RetentionConfig
from proberca.k8s.client import KubernetesDiscoveryClient
from proberca.orchestration.checkpoint import apply_checkpoint_retention


REQUIRED = (
    "Pod", "Service", "EndpointSlice", "Node", "Deployment", "ReplicaSet",
    "StatefulSet", "DaemonSet", "Job", "PersistentVolumeClaim", "PersistentVolume",
)


def test_discovery_client_initial_lists_every_required_kind_without_secrets():
    called = []

    def adapter(kind):
        def load(namespaces):
            called.append((kind, namespaces))
            return [], f"rv-{kind}"
        return load

    config = KubernetesConfig(
        enabled=True, cluster_id="cluster-a", namespaces=("observability",))
    client = KubernetesDiscoveryClient(
        config, list_adapters={kind: adapter(kind) for kind in REQUIRED})
    inventory = client.discover_once(observed_at_ns=1)
    assert inventory.synchronized
    assert {kind for kind, _ in called} == set(REQUIRED)
    assert all(kind != "Secret" for kind, _ in called)


def generation(root, name, created):
    path = root / "generations" / name
    path.mkdir(parents=True)
    (path / "metadata.json").write_text(json.dumps({"created_at_ns": created}))
    return path


def test_checkpoint_retention_keeps_current_and_previous_and_never_reports(tmp_path):
    root = tmp_path / "checkpoint"
    old = generation(root, "001", 1)
    previous = generation(root, "002", 2)
    current = generation(root, "003", 3)
    (root / "CURRENT").write_text(json.dumps({"generation_id": "003"}))
    reports = tmp_path / "reports"
    reports.mkdir()
    report = reports / "report.json"
    report.write_text("{}")
    issues = apply_checkpoint_retention(
        root, RetentionConfig(checkpoint_generations=2, checkpoint_min_age_sec=0),
        now_ns=10_000_000_000)
    assert issues == []
    assert not old.exists() and previous.exists() and current.exists()
    assert report.exists()


def test_checkpoint_retention_cleanup_failure_is_structured_and_current_survives(
        tmp_path, monkeypatch):
    root = tmp_path / "checkpoint"
    generation(root, "001", 1)
    generation(root, "002", 2)
    generation(root, "003", 3)
    (root / "CURRENT").write_text(json.dumps({"generation_id": "003"}))
    monkeypatch.setattr("proberca.orchestration.checkpoint.shutil.rmtree",
                        lambda *_: (_ for _ in ()).throw(OSError("disk error")))
    issues = apply_checkpoint_retention(
        root, RetentionConfig(checkpoint_generations=2, checkpoint_min_age_sec=0),
        now_ns=10_000_000_000)
    assert issues[0]["reason_code"] == "checkpoint_retention_failed"
    assert (root / "generations" / "003").exists()


def test_kubernetes_manifests_are_least_privilege_and_non_privileged():
    base = Path("deploy/kubernetes/base")
    files = {path.name: yaml.safe_load(path.read_text()) for path in base.glob("*.yaml")
             if path.name not in {"kustomization.yaml"}}
    assert "secret.example.yaml" in files
    assert files["secret.example.yaml"].get("stringData", {}) == {}
    deployment = files["deployment.yaml"]
    pod_spec = deployment["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]
    security = container["securityContext"]
    assert pod_spec.get("hostPID", False) is False
    assert pod_spec.get("hostNetwork", False) is False
    assert security["allowPrivilegeEscalation"] is False
    assert security["readOnlyRootFilesystem"] is True
    assert security["capabilities"]["drop"] == ["ALL"]
    assert pod_spec["securityContext"]["runAsNonRoot"] is True
    assert pod_spec["securityContext"]["seccompProfile"]["type"] == "RuntimeDefault"
    rbac = "\n".join(path.read_text() for path in base.glob("*role*.yaml"))
    for forbidden in ("secrets", "pods/exec", "cluster-admin", "delete", "patch"):
        assert forbidden not in rbac


def test_live_cli_has_no_plaintext_token_label_or_stage_bypass_arguments():
    parser = _parser()
    destinations = {action.dest for action in parser._actions}
    assert {"config", "once", "max_windows", "resume", "dry_run_discovery",
            "dry_run_metrics"} <= destinations
    assert not {"token", "labels", "root_service", "skip_engine", "skip_stage"} & destinations


def test_live_cli_full_mode_enters_canonical_live_loop(monkeypatch):
    sentinel = object()
    monkeypatch.setattr(live_cli, "_effective_config", lambda args: sentinel)
    monkeypatch.setattr(live_cli, "_run_live", lambda config, args: 0)
    assert live_cli.main(["--config", "unused", "--max-windows", "1"]) == 0
