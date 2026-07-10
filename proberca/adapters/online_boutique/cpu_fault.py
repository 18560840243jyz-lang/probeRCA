"""CPU resource-limit fault injection helpers for Online Boutique P2A-1."""

from __future__ import annotations

import json
import time
from pathlib import Path

from proberca.adapters.online_boutique.metrics import run_cmd, write_json


def _deployment_json(namespace: str, deployment: str) -> dict:
    code, stdout, stderr = run_cmd(["kubectl", "get", "deployment", deployment, "-n", namespace, "-o", "json"], timeout=30)
    if code != 0:
        raise RuntimeError(f"failed to get deployment/{deployment}: {stderr}")
    return json.loads(stdout)


def get_deployment_resources(namespace: str, deployment: str, container: str) -> dict:
    """Save the original Kubernetes resources for one deployment container."""

    data = _deployment_json(namespace, deployment)
    containers = data.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
    for index, item in enumerate(containers):
        if item.get("name") == container:
            return {
                "namespace": namespace,
                "deployment": deployment,
                "container": container,
                "container_index": index,
                "resources": item.get("resources", {}) or {},
                "observed_generation": data.get("metadata", {}).get("generation"),
                "timestamp": time.time(),
            }
    raise ValueError(f"container {container!r} not found in deployment/{deployment}")


def wait_deployment_ready(namespace: str, deployment: str, timeout_sec: int = 180) -> dict:
    """Wait until a deployment rollout is complete."""

    code, stdout, stderr = run_cmd(
        ["kubectl", "rollout", "status", f"deployment/{deployment}", "-n", namespace, f"--timeout={timeout_sec}s"],
        timeout=timeout_sec + 10,
    )
    result = {"returncode": code, "stdout": stdout, "stderr": stderr, "ready": code == 0, "timestamp": time.time()}
    if code != 0:
        raise RuntimeError(f"deployment/{deployment} did not become ready: {stderr or stdout}")
    return result


def inject_cpu_limit_fault(namespace: str, deployment: str, container: str, cpu_limit: str, memory_limit: str) -> dict:
    """Apply a low CPU limit to create a real Kubernetes resource constraint fault."""

    cmd = [
        "kubectl",
        "set",
        "resources",
        f"deployment/{deployment}",
        "-n",
        namespace,
        f"--containers={container}",
        f"--limits=cpu={cpu_limit},memory={memory_limit}",
        f"--requests=cpu={cpu_limit}",
    ]
    code, stdout, stderr = run_cmd(cmd, timeout=60)
    record = {"action": "inject_cpu_limit_fault", "cmd": cmd, "returncode": code, "stdout": stdout, "stderr": stderr, "timestamp": time.time()}
    if code != 0:
        raise RuntimeError(f"CPU fault injection failed: {stderr or stdout}")
    record["rollout"] = wait_deployment_ready(namespace, deployment, timeout_sec=180)
    return record


def restore_deployment_resources(namespace: str, deployment: str, container: str, original_resources: dict) -> dict:
    """Restore the original resources for the target deployment container."""

    resources = original_resources.get("resources", {}) or {}
    index = int(original_resources.get("container_index", 0))
    patch = [{"op": "replace", "path": f"/spec/template/spec/containers/{index}/resources", "value": resources}]
    cmd = ["kubectl", "patch", f"deployment/{deployment}", "-n", namespace, "--type=json", "-p", json.dumps(patch)]
    code, stdout, stderr = run_cmd(cmd, timeout=60)
    record = {"action": "restore_deployment_resources", "cmd": cmd, "returncode": code, "stdout": stdout, "stderr": stderr, "timestamp": time.time()}
    if code != 0:
        undo_cmd = ["kubectl", "rollout", "undo", f"deployment/{deployment}", "-n", namespace]
        undo_code, undo_stdout, undo_stderr = run_cmd(undo_cmd, timeout=60)
        record["undo"] = {"cmd": undo_cmd, "returncode": undo_code, "stdout": undo_stdout, "stderr": undo_stderr}
        if undo_code != 0:
            raise RuntimeError(f"restore patch and rollout undo both failed: {stderr}; {undo_stderr}")
    record["rollout"] = wait_deployment_ready(namespace, deployment, timeout_sec=180)
    return record


def write_fault_logs(output_dir: str | Path, records: dict) -> None:
    """Write fault injection and restore logs."""

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    if "fault_injection" in records:
        write_json(out / "fault_injection_log.json", records["fault_injection"])
    if "restore" in records:
        write_json(out / "restore_log.json", records["restore"])
