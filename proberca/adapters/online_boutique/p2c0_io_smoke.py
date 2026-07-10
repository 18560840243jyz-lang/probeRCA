"""P2C-0 real I/O fault feasibility smoke orchestration."""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from typing import Any

from proberca.adapters.online_boutique.io_fault import (
    check_pod_io_tools,
    cleanup_io_stress,
    collect_fs_snapshot,
    compute_fs_delta,
    curl_frontend,
    get_target_pod,
    run_cmd,
    start_io_stress,
    summarize_fs_metric,
)
from proberca.adapters.online_boutique.p2a1_cpu_experiment import load_simple_yaml


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


def _start_frontend_port_forward_if_needed(namespace: str, frontend_url: str, output_dir: Path) -> subprocess.Popen | None:
    if curl_frontend(frontend_url, 1, 3).get("error_rate") == 0.0:
        return None
    log_path = output_dir / "frontend_port_forward.log"
    handle = log_path.open("a", encoding="utf-8")
    proc = subprocess.Popen(["kubectl", "port-forward", "-n", namespace, "svc/frontend", "8080:80"], stdout=handle, stderr=subprocess.STDOUT, text=True)
    handle.close()
    time.sleep(5)
    if curl_frontend(frontend_url, 1, 3).get("error_rate") != 0.0:
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


def _df(namespace: str, deployment: str, container: str | None = None) -> str:
    cmd = ["kubectl", "exec", "-n", namespace, f"deploy/{deployment}"]
    if container:
        cmd.extend(["-c", container])
    cmd.extend(["--", "sh", "-c", "df -h /tmp /data 2>/dev/null || df -h /tmp"])
    code, stdout, stderr = run_cmd(cmd, timeout=30)
    return stdout + ("\nSTDERR:\n" + stderr if stderr else "")


def _http_ok(frontend: dict[str, Any]) -> bool:
    samples = frontend.get("samples") or []
    if samples:
        return float(frontend.get("error_rate", 1.0)) == 0.0 and any(int(item.get("http_code", 0)) // 100 == 2 for item in samples)
    return float(frontend.get("error_rate", 1.0)) == 0.0 and frontend.get("p99_latency_ms") is not None




def _snapshot_counter(snapshot: dict[str, Any], service: str, field: str) -> float:
    total = 0.0
    for entry in snapshot.values():
        if entry.get("service") == service and field in entry:
            total += float(entry[field])
    return total


def evaluate_io_fault_feasible(summary: dict[str, Any]) -> dict[str, Any]:
    failed: list[str] = []
    if summary.get("io_stress_started") is not True:
        failed.append("io_stress_started != true")
    if summary.get("io_stress_cleaned") is not True:
        failed.append("io_stress_cleaned != true")
    if summary.get("frontend_after_http_ok") is not True:
        failed.append("frontend_after_http_ok != true")
    if float(summary.get("write_bytes_delta_during", 0.0)) <= 0.0 and float(summary.get("write_ops_delta_during", 0.0)) <= 0.0:
        failed.append("write_bytes_delta_during/write_ops_delta_during not positive")
    return {"io_fault_feasible": not failed, "failed_checks": failed}


def run_p2c0_io_smoke(config_path: str | Path) -> dict[str, Any]:
    config = load_simple_yaml(config_path)
    namespace = str(config["kubernetes"]["namespace"])
    exp = config["experiment"]
    fault = config["fault_injection"]
    output_dir = Path(str(exp["output_dir"]))
    output_dir.mkdir(parents=True, exist_ok=True)
    frontend_url = str(exp["frontend_url"])
    port_forward_proc = _start_frontend_port_forward_if_needed(namespace, frontend_url, output_dir)
    stress = {"started": False, "completed": False}
    cleanup = {"cleaned": False}
    try:
        for deployment in ["redis-cart", "cartservice", "frontend"]:
            if not _deployment_ready(namespace, deployment):
                raise RuntimeError(f"deployment not ready: {deployment}")
        pod = get_target_pod(namespace, str(exp["target_service"]))
        if not pod.get("ready"):
            raise RuntimeError(f"target pod not ready: {pod}")
        tools = check_pod_io_tools(namespace, str(fault["target_deployment"]))
        if not tools.get("sh_available") or not tools.get("dd_available"):
            raise RuntimeError(f"redis-cart missing required IO tools: {tools}")
        _write_json(output_dir / "redis_cart_tool_check.json", tools)
        _write_text(output_dir / "redis_cart_df_before.txt", _df(namespace, str(fault["target_deployment"]), str(fault.get("target_container", ""))))
        before_snapshot = collect_fs_snapshot(namespace)
        frontend_before = curl_frontend(frontend_url, int(exp["requests_before"]), int(exp["request_timeout_sec"]))
        stress = start_io_stress(namespace, str(fault["target_deployment"]), str(fault["target_container"]), str(fault["temp_file"]), str(fault["block_size"]), int(fault["block_count"]), int(fault["duration_sec"]), output_dir / "io_stress_stdout_stderr.log")
        frontend_during = curl_frontend(frontend_url, int(exp["requests_during"]), int(exp["request_timeout_sec"]))
        proc = stress.pop("process")
        try:
            proc.wait(timeout=int(fault["duration_sec"]) + 30)
        except subprocess.TimeoutExpired:
            proc.terminate()
            proc.wait(timeout=10)
        stress["returncode"] = proc.returncode
        stress["completed"] = proc.returncode == 0
        during_snapshot = collect_fs_snapshot(namespace)
        cleanup = cleanup_io_stress(namespace, str(fault["target_deployment"]), str(fault["target_container"]), str(fault["temp_file"]))
        time.sleep(3)
        after_snapshot = collect_fs_snapshot(namespace)
        frontend_after = curl_frontend(frontend_url, int(exp["requests_after"]), int(exp["request_timeout_sec"]))
        _write_text(output_dir / "redis_cart_df_after.txt", _df(namespace, str(fault["target_deployment"]), str(fault.get("target_container", ""))))
        before_during_delta = compute_fs_delta(before_snapshot, during_snapshot, int(fault["duration_sec"]))
        during_after_delta = compute_fs_delta(during_snapshot, after_snapshot, 3)
        target_service = str(exp["target_service"])
        write_bytes_delta = summarize_fs_metric(before_during_delta, target_service, "io.write_bytes")
        write_ops_delta = summarize_fs_metric(before_during_delta, target_service, "io.write_ops")
        io_time_delta = summarize_fs_metric(before_during_delta, target_service, "io.io_time_ms")
        write_before = _snapshot_counter(before_snapshot, target_service, "fs_writes_bytes_total")
        write_during = _snapshot_counter(during_snapshot, target_service, "fs_writes_bytes_total")
        write_after = _snapshot_counter(after_snapshot, target_service, "fs_writes_bytes_total")
        summary = {
            "experiment_id": str(exp["experiment_id"]),
            "target_service": target_service,
            "target_metric": str(exp["target_metric"]),
            "target_fault_type": str(exp["target_fault_type"]),
            "pod_name": str(pod["name"]),
            "pod_ip": str(pod.get("pod_ip", "")),
            "io_stress_started": bool(stress.get("started")),
            "io_stress_completed": bool(stress.get("completed")),
            "io_stress_cleaned": bool(cleanup.get("cleaned")),
            "frontend_before_http_ok": _http_ok(frontend_before),
            "frontend_during_http_ok": _http_ok(frontend_during),
            "frontend_after_http_ok": _http_ok(frontend_after),
            "write_bytes_before": float(write_before),
            "write_bytes_during": float(write_during),
            "write_bytes_after": float(write_after),
            "write_bytes_delta_during": float(write_bytes_delta),
            "write_ops_delta_during": float(write_ops_delta),
            "io_time_delta_ms_during": float(io_time_delta),
            "frontend_p99_before_ms": frontend_before.get("p99_latency_ms"),
            "frontend_p99_during_ms": frontend_during.get("p99_latency_ms"),
            "frontend_p99_after_ms": frontend_after.get("p99_latency_ms"),
        }
        summary.update(evaluate_io_fault_feasible(summary))
        _write_json(output_dir / "io_fault_log.json", stress)
        _write_json(output_dir / "io_restore_log.json", cleanup)
        _write_json(output_dir / "io_metrics_before.json", {"snapshot": before_snapshot})
        _write_json(output_dir / "io_metrics_during.json", {"snapshot": during_snapshot, "delta_from_before": before_during_delta})
        _write_json(output_dir / "io_metrics_after.json", {"snapshot": after_snapshot, "delta_from_during": during_after_delta})
        _write_json(output_dir / "frontend_io_smoke_before.json", frontend_before)
        _write_json(output_dir / "frontend_io_smoke_during.json", frontend_during)
        _write_json(output_dir / "frontend_io_smoke_after.json", frontend_after)
        _write_json(output_dir / "p2c0_io_smoke_summary.json", summary)
        _write_json(output_dir / "p2c0_io_smoke_metadata.json", {"config_path": str(config_path), "output_dir": str(output_dir), "metrics": config.get("metrics", {}), "disabled_for_this_step": config.get("disabled_for_this_step", [])})
        return {"output_dir": str(output_dir), "summary": summary}
    finally:
        if not cleanup.get("cleaned"):
            try:
                cleanup_io_stress(namespace, str(fault["target_deployment"]), str(fault["target_container"]), str(fault["temp_file"]))
            except Exception:
                pass
        _stop_process(port_forward_proc)
