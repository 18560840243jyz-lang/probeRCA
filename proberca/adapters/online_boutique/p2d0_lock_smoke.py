"""P2D-0 real lock contention feasibility smoke orchestration."""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from typing import Any

from proberca.adapters.online_boutique.lock_fault import (
    build_lockstress_python_command,
    curl_frontend,
    ensure_sidecar_image_loaded,
    evaluate_lock_fault_feasible,
    get_sidecar_logs,
    get_target_pod,
    parse_lockstress_logs,
    patch_cartservice_add_lockstress_sidecar,
    remove_lockstress_sidecar,
    run_cmd,
)
from proberca.adapters.online_boutique.p2a1_cpu_experiment import load_simple_yaml

LIMITATION = "sidecar_lock_contention_not_original_cartservice_code_bug"


def _write_json(path: str | Path, payload: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: str | Path, text: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def _deployment_ready(namespace: str, deployment: str) -> bool:
    code, stdout, _stderr = run_cmd(["kubectl", "get", "deploy", "-n", namespace, deployment, "-o", "json"], timeout=30)
    if code != 0:
        return False
    data = json.loads(stdout)
    desired = int((data.get("spec") or {}).get("replicas") or 1)
    status = data.get("status") or {}
    return int(status.get("readyReplicas") or 0) >= desired and int(status.get("availableReplicas") or 0) >= desired


def _kubectl_text(args: list[str]) -> str:
    code, stdout, stderr = run_cmd(["kubectl", *args], timeout=30)
    return stdout + ("\nSTDERR:\n" + stderr if stderr else "")


def _start_frontend_port_forward_if_needed(namespace: str, frontend_url: str, output_dir: Path) -> subprocess.Popen | None:
    if curl_frontend(frontend_url, 1, 3).get("http_ok"):
        return None
    log_path = output_dir / "frontend_port_forward.log"
    handle = log_path.open("a", encoding="utf-8")
    proc = subprocess.Popen(["kubectl", "port-forward", "-n", namespace, "svc/frontend", "8080:80"], stdout=handle, stderr=subprocess.STDOUT, text=True)
    handle.close()
    time.sleep(5)
    if not curl_frontend(frontend_url, 1, 3).get("http_ok"):
        proc.terminate()
        raise RuntimeError(f"frontend port-forward failed for {frontend_url}")
    return proc


def _stop_process(proc: subprocess.Popen | None) -> None:
    if proc is None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def _p99(frontend: dict[str, Any]) -> float:
    value = frontend.get("p99_latency_ms")
    return float(value) if value is not None else 0.0


def run_p2d0_lock_smoke(config_path: str | Path) -> dict[str, Any]:
    config = load_simple_yaml(config_path)
    namespace = str(config["kubernetes"]["namespace"])
    cluster_name = str(config["kubernetes"]["cluster_name"])
    exp = config["experiment"]
    fault = config["fault_injection"]
    output_dir = Path(str(exp["output_dir"]))
    output_dir.mkdir(parents=True, exist_ok=True)
    frontend_url = str(exp["frontend_url"])
    deployment = str(fault["target_deployment"])
    sidecar_name = str(fault["sidecar_name"])
    sidecar_image = str(fault["sidecar_image"])
    sidecar_pod_name = ""
    patch_result: dict[str, Any] = {"sidecar_injected": False}
    restore_result: dict[str, Any] = {"sidecar_removed": False}
    image_result: dict[str, Any] = {"image_loaded": False}
    pod_before: dict[str, Any] = {"name": ""}
    frontend_before: dict[str, Any] = {"http_ok": False, "p99_latency_ms": 0.0}
    frontend_during: dict[str, Any] = {"http_ok": False, "p99_latency_ms": 0.0}
    lock_metrics: dict[str, Any] = {
        "lock_wait_ms_sum_total": 0.0,
        "lock_wait_ms_mean_avg": 0.0,
        "lock_wait_ms_p95_max": 0.0,
        "lock_contention_count_total": 0,
        "records_count": 0,
        "lock_metrics_available": False,
    }
    port_forward_proc = _start_frontend_port_forward_if_needed(namespace, frontend_url, output_dir)
    try:
        for required in ["cartservice", "frontend"]:
            if not _deployment_ready(namespace, required):
                raise RuntimeError(f"deployment not ready: {required}")
        pod_before = get_target_pod(namespace, str(exp["target_service"]))
        if not pod_before.get("ready"):
            raise RuntimeError(f"target pod not ready: {pod_before}")
        _write_text(output_dir / "kubectl_get_pods_before.txt", _kubectl_text(["get", "pods", "-n", namespace]))
        _write_text(output_dir / "kubectl_get_deploy_before.txt", _kubectl_text(["get", "deploy", "-n", namespace]))
        frontend_before = curl_frontend(frontend_url, int(exp["requests_before"]), int(exp["request_timeout_sec"]))
        _write_json(output_dir / "frontend_lock_smoke_before.json", frontend_before)

        image_result = ensure_sidecar_image_loaded(sidecar_image, cluster_name)
        command = build_lockstress_python_command(int(fault["duration_sec"]), int(fault["workers"]), int(fault["lock_hold_ms"]))
        patch_result = patch_cartservice_add_lockstress_sidecar(namespace, deployment, sidecar_name, sidecar_image, command)
        time.sleep(5)
        pod_during = get_target_pod(namespace, str(exp["target_service"]))
        sidecar_pod_name = str(pod_during["name"])
        _write_text(output_dir / "kubectl_get_pods_during.txt", _kubectl_text(["get", "pods", "-n", namespace]))
        _write_text(output_dir / "kubectl_get_deploy_during.txt", _kubectl_text(["get", "deploy", "-n", namespace]))
        fault_log = {
            "image": image_result,
            "patch": patch_result,
            "pod_name_before": pod_before.get("name", ""),
            "pod_name_during": sidecar_pod_name,
            "sidecar_command": command,
            "limitation": LIMITATION,
        }
        _write_json(output_dir / "lock_fault_log.json", fault_log)

        frontend_during = curl_frontend(frontend_url, int(exp["requests_during"]), int(exp["request_timeout_sec"]))
        elapsed_wait = max(0, int(fault["duration_sec"]) + 5 - 5)
        if elapsed_wait > 0:
            time.sleep(elapsed_wait)
        logs = get_sidecar_logs(namespace, deployment, sidecar_name, sidecar_pod_name)
        _write_text(output_dir / "lockstress_logs.txt", logs)
        lock_metrics = parse_lockstress_logs(logs)
        _write_json(output_dir / "lock_metrics_during.json", lock_metrics)
        _write_json(output_dir / "frontend_lock_smoke_during.json", frontend_during)
    finally:
        try:
            restore_result = remove_lockstress_sidecar(namespace, deployment, sidecar_name)
        finally:
            _write_json(output_dir / "lock_restore_log.json", restore_result)
            _write_text(output_dir / "kubectl_get_pods_after.txt", _kubectl_text(["get", "pods", "-n", namespace]))
            _write_text(output_dir / "kubectl_get_deploy_after.txt", _kubectl_text(["get", "deploy", "-n", namespace]))

    if not _deployment_ready(namespace, "cartservice"):
        raise RuntimeError("cartservice not ready after lock sidecar restore")
    if not _deployment_ready(namespace, "frontend"):
        raise RuntimeError("frontend not ready after lock sidecar restore")
    frontend_after = curl_frontend(frontend_url, int(exp["requests_after"]), int(exp["request_timeout_sec"]))
    _write_json(output_dir / "frontend_lock_smoke_after.json", frontend_after)
    pod_after = get_target_pod(namespace, str(exp["target_service"]))
    summary = {
        "experiment_id": str(exp["experiment_id"]),
        "target_service": str(exp["target_service"]),
        "target_metric": str(exp["target_metric"]),
        "target_fault_type": str(exp["target_fault_type"]),
        "pod_name_before": str(pod_before.get("name", "")),
        "pod_name_during": sidecar_pod_name,
        "pod_name_after": str(pod_after.get("name", "")),
        "sidecar_injected": bool(patch_result.get("sidecar_injected")),
        "sidecar_removed": bool(restore_result.get("sidecar_removed")),
        "frontend_before_http_ok": bool(frontend_before.get("http_ok")),
        "frontend_during_http_ok": bool(frontend_during.get("http_ok")),
        "frontend_after_http_ok": bool(frontend_after.get("http_ok")),
        "frontend_p99_before_ms": _p99(frontend_before),
        "frontend_p99_during_ms": _p99(frontend_during),
        "frontend_p99_after_ms": _p99(frontend_after),
        "limitation": LIMITATION,
        **lock_metrics,
    }
    feasibility = evaluate_lock_fault_feasible(summary)
    summary.update(feasibility)
    _write_json(output_dir / "p2d0_lock_smoke_summary.json", summary)
    metadata = {
        "config_path": str(config_path),
        "output_dir": str(output_dir),
        "phase": "P2D-0",
        "image": sidecar_image,
        "image_loaded": image_result,
        "limitations": config.get("limitations", [LIMITATION]),
        "disabled_for_this_step": config.get("disabled_for_this_step", []),
        "note": "P2D-0 feasibility only; no RCA pipeline or lock accuracy. Lock contention is generated by a cartservice Pod sidecar, not by an original cartservice business-code bug.",
    }
    _write_json(output_dir / "p2d0_lock_smoke_metadata.json", metadata)
    _stop_process(port_forward_proc)
    return {"output_dir": str(output_dir), "summary": summary, "metadata": metadata}
