from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import stat
import subprocess

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts/p11_smoke_run.sh"
PROFILE_ROOT = ROOT / "deploy/kubernetes/test/p11-smoke/profiles"
ROLE = "proberca.io/workload-role"
DIGEST_IMAGE = "registry.invalid/proberca@sha256:" + "d" * 64


def _render_profile(profile: str) -> list[dict]:
    result = subprocess.run(
        [
            "kubectl",
            "kustomize",
            "--load-restrictor=LoadRestrictionsNone",
            str(PROFILE_ROOT / profile),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return [item for item in yaml.safe_load_all(result.stdout) if item]


def _role(document: dict) -> str | None:
    return document.get("metadata", {}).get("labels", {}).get(ROLE)


def _fake_kubectl(tmp_path: Path) -> tuple[Path, Path]:
    real = shutil.which("kubectl")
    assert real
    log = tmp_path / "kubectl.log"
    fake_dir = tmp_path / "bin"
    fake_dir.mkdir(parents=True)
    script = fake_dir / "kubectl"
    script.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$*\" >> \"$KUBECTL_LOG\"\n"
        "if [ \"$1\" = kustomize ]; then exec \"$REAL_KUBECTL\" \"$@\"; fi\n"
        "if [ \"$1 $2\" = 'config current-context' ]; then printf '%s\\n' \"$EXPECTED_CONTEXT\"; fi\n"
        "exit 0\n",
        encoding="utf-8",
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return fake_dir, log


def _runner(tmp_path: Path, *args: str, profile: str | None = None, extra_env: dict[str, str] | None = None):
    fake_dir, log = _fake_kubectl(tmp_path)
    render_dir = tmp_path / "render"
    env = os.environ.copy()
    for key in ("PROBERCA_P11_MAX_WINDOWS", "PROBERCA_P11_CONFIG_OVERRIDE"):
        env.pop(key, None)
    env.update(
        {
            "PATH": str(fake_dir) + os.pathsep + env["PATH"],
            "REAL_KUBECTL": shutil.which("kubectl") or "kubectl",
            "KUBECTL_LOG": str(log),
            "EXPECTED_CONTEXT": "kind-fixture",
            "PROBERCA_P11_CONTEXT": "kind-fixture",
            "PROBERCA_P11_SMOKE_NAMESPACE": "p11-profile-fixture",
            "PROBERCA_P11_RUN_ID": "profile-fixture-run",
            "PROBERCA_P11_IMAGE_SUPPLY_MODE": "registry_digest",
            "PROBERCA_P11_IMAGE": DIGEST_IMAGE,
            "PROBERCA_P11_RENDER_DIR": str(render_dir),
        }
    )
    if extra_env:
        env.update(extra_env)
    command = ["bash", str(RUNNER), *args]
    if profile is not None:
        command.extend(["--stack-profile", profile])
    result = subprocess.run(command, env=env, capture_output=True, text=True)
    return result, log, render_dir


def test_runner_requires_explicit_action_and_stack_profile(tmp_path):
    missing_action, _, _ = _runner(tmp_path / "a")
    assert missing_action.returncode == 2
    assert "action" in missing_action.stderr
    missing_profile, _, _ = _runner(tmp_path / "b", "--action", "render")
    assert missing_profile.returncode == 2
    assert "stack-profile" in missing_profile.stderr


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (("--action", "unknown", "--stack-profile", "bounded"), "unsupported action"),
        (("--action", "render", "--stack-profile", "unknown"), "unsupported stack profile"),
    ],
)
def test_runner_rejects_unknown_action_or_profile(tmp_path, args, expected):
    result, _, _ = _runner(tmp_path, *args)
    assert result.returncode == 2
    assert expected in result.stderr


@pytest.mark.parametrize(
    ("profile", "live_count", "bounded_count"),
    [("live", 1, 0), ("bounded", 0, 1)],
)
def test_profiles_are_structurally_mutually_exclusive(profile, live_count, bounded_count):
    documents = _render_profile(profile)
    assert sum(_role(item) == "live-runner" for item in documents) == live_count
    assert sum(_role(item) == "bounded-runner" for item in documents) == bounded_count
    assert sum(_role(item) == "shared-observability" for item in documents) == 3


def test_bounded_profile_has_exactly_one_non_restarting_job_and_shared_dependencies():
    documents = _render_profile("bounded")
    jobs = [item for item in documents if _role(item) == "bounded-runner"]
    assert len(jobs) == 1 and jobs[0]["kind"] == "Job"
    assert jobs[0]["spec"]["backoffLimit"] == 0
    assert jobs[0]["spec"]["template"]["spec"]["restartPolicy"] == "Never"
    assert not any(_role(item) == "live-runner" for item in documents)
    kinds = {item["kind"] for item in documents}
    assert {"Namespace", "ServiceAccount", "ClusterRole", "ClusterRoleBinding", "ConfigMap", "PersistentVolumeClaim"} <= kinds


def test_live_profile_has_one_live_deployment_and_no_bounded_job():
    documents = _render_profile("live")
    live = [item for item in documents if _role(item) == "live-runner"]
    assert len(live) == 1 and live[0]["kind"] == "Deployment"
    assert live[0]["spec"]["replicas"] == 2
    assert not any(_role(item) == "bounded-runner" for item in documents)


def test_render_action_has_no_cluster_write_or_wait(tmp_path):
    result, log, render_dir = _runner(
        tmp_path, "--action", "render", "--stack-profile", "bounded"
    )
    assert result.returncode == 0, result.stderr
    commands = log.read_text(encoding="utf-8").splitlines()
    forbidden = ("apply", "create", "delete", "patch", "scale", "rollout", "wait")
    assert not any(any(word in command.split() for word in forbidden) for command in commands)
    assert (render_dir / "final-render.yaml").is_file()
    assert str(render_dir) in result.stdout


def test_bounded_apply_never_waits_for_live_runner(tmp_path):
    result, log, _ = _runner(
        tmp_path, "--action", "apply", "--stack-profile", "bounded"
    )
    assert result.returncode == 0, result.stderr
    commands = log.read_text(encoding="utf-8")
    assert "apply -f" in commands
    assert "bounded-runner" in commands
    assert "live-runner" not in commands


def test_bounded_apply_waits_for_shared_dependencies_before_starting_job(tmp_path):
    result, log, _ = _runner(
        tmp_path, "--action", "apply", "--stack-profile", "bounded"
    )
    assert result.returncode == 0, result.stderr
    commands = log.read_text(encoding="utf-8").splitlines()
    apply_indexes = [
        index for index, command in enumerate(commands) if "apply -f" in command
    ]
    shared_ready = next(
        index for index, command in enumerate(commands)
        if "rollout status deployment" in command
        and "shared-observability" in command
    )
    bounded_wait = next(
        index for index, command in enumerate(commands)
        if "wait --for=condition=complete job" in command
        and "bounded-runner" in command
    )
    assert len(apply_indexes) == 2
    assert apply_indexes[0] < shared_ready < apply_indexes[1] < bounded_wait


def test_live_apply_waits_for_live_runner_and_never_waits_for_job(tmp_path):
    result, log, _ = _runner(
        tmp_path, "--action", "apply", "--stack-profile", "live"
    )
    assert result.returncode == 0, result.stderr
    commands = log.read_text(encoding="utf-8")
    assert "apply -f" in commands
    assert "live-runner" in commands
    assert "bounded-runner" not in commands


def test_transient_empty_reuses_bounded_profile_with_external_config_only(tmp_path):
    override = tmp_path / "transient-override.yaml"
    override.write_text(
        yaml.safe_dump(
            {
                "live_liveness": {
                    "controlled_collection_fault_enabled": True,
                    "controlled_transient_empty_attempts": 1,
                }
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    result, _, render_dir = _runner(
        tmp_path,
        "--action",
        "render",
        "--stack-profile",
        "bounded",
        extra_env={
            "PROBERCA_P11_CONFIG_OVERRIDE": str(override),
            "PROBERCA_P11_MAX_WINDOWS": "1",
        },
    )
    assert result.returncode == 0, result.stderr
    documents = [item for item in yaml.safe_load_all((render_dir / "final-render.yaml").read_text()) if item]
    assert sum(_role(item) == "bounded-runner" for item in documents) == 1
    assert not any(_role(item) == "live-runner" for item in documents)
    job = next(item for item in documents if _role(item) == "bounded-runner")
    args = job["spec"]["template"]["spec"]["containers"][0]["args"]
    assert args[args.index("--max-windows") + 1] == "1"
    config_map = next(item for item in documents if item.get("kind") == "ConfigMap" and "config.yaml" in item.get("data", {}))
    config = yaml.safe_load(config_map["data"]["config.yaml"])
    assert config["live_liveness"]["controlled_collection_fault_enabled"] is True
    assert config["live_liveness"]["controlled_transient_empty_attempts"] == 1


def test_runner_has_no_apply_then_scale_or_text_document_deletion():
    source = RUNNER.read_text(encoding="utf-8")
    assert "kubectl scale" not in source
    assert "kubectl delete deployment" not in source
    assert not any(token in source for token in ("grep -v", "sed -n", "awk '/kind:"))


def test_dynamic_run_identity_preserves_stable_managed_by_labels(tmp_path):
    run_id = "dynamic-run-identity"
    result, _, render_dir = _runner(
        tmp_path,
        "--action",
        "render",
        "--stack-profile",
        "bounded",
        extra_env={"PROBERCA_P11_RUN_ID": run_id},
    )
    assert result.returncode == 0, result.stderr
    documents = [
        item
        for item in yaml.safe_load_all(
            (render_dir / "final-render.yaml").read_text(encoding="utf-8")
        )
        if item
    ]
    owned_labels = []
    for document in documents:
        metadata = document.get("metadata", {})
        if metadata.get("labels"):
            owned_labels.append(metadata["labels"])
        template = document.get("spec", {}).get("template", {})
        if template.get("metadata", {}).get("labels"):
            owned_labels.append(template["metadata"]["labels"])

    assert owned_labels
    for labels in owned_labels:
        assert labels["app.kubernetes.io/managed-by"] == "proberca-p11-smoke"
        assert labels["proberca.io/smoke-run-id"] == run_id
