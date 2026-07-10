"""P2D-1 repeated real Online Boutique lock contention experiments."""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from typing import Any

import numpy as np

from proberca.adapters.online_boutique.lock_fault import (
    build_lockstress_python_command,
    curl_frontend,
    ensure_sidecar_image_loaded,
    get_sidecar_logs,
    get_target_pod,
    parse_lockstress_logs,
    patch_cartservice_add_lockstress_sidecar,
    remove_lockstress_sidecar,
    run_cmd,
)
from proberca.adapters.online_boutique.p1_bridge import build_real_observation_files, refresh_real_observation_from_normalized
from proberca.adapters.online_boutique.p2a1_cpu_experiment import load_simple_yaml
from proberca.adapters.online_boutique.topology import write_online_boutique_service_graph
from proberca.data.io import read_jsonl, write_jsonl
from proberca.evidence.ipw_semantic import IPWSemanticEvidenceConfig, score_ipw_semantic_evidence
from proberca.eval.p1_metrics import evaluate_p1_results
from proberca.eval.p1_result import build_p1_results
from proberca.explain.ipw_path import IPWPathExplanationConfig, explain_ipw_paths
from proberca.features.robust import normalize_dataset
from proberca.inference.ipw_sparse import IPWSparseInversionConfig, solve_ipw_sparse_inversion
from proberca.propagation.ipw import IPWPropagationConfig, train_ipw_masked_propagation

LIMITATION = "sidecar_lock_contention_not_original_cartservice_code_bug"
DISTRIBUTION = "distributed_from_sidecar_total"


def load_lock_repeat_config(config_path: str | Path) -> dict[str, Any]:
    return load_simple_yaml(config_path)


def _write_json(path: str | Path, payload: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: str | Path, text: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def _dump_scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if any(ch in text for ch in [":", "#", "\n"]) or text.strip() != text or text == "":
        return json.dumps(text, ensure_ascii=False)
    return text


def _dump_yaml(data: Any, indent: int = 0) -> str:
    prefix = " " * indent
    if isinstance(data, dict):
        lines: list[str] = []
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                lines.append(f"{prefix}{key}:")
                lines.append(_dump_yaml(value, indent + 2))
            else:
                lines.append(f"{prefix}{key}: {_dump_scalar(value)}")
        return "\n".join(lines)
    if isinstance(data, list):
        lines = []
        for value in data:
            if isinstance(value, (dict, list)):
                lines.append(f"{prefix}-")
                lines.append(_dump_yaml(value, indent + 2))
            else:
                lines.append(f"{prefix}- {_dump_scalar(value)}")
        return "\n".join(lines)
    return f"{prefix}{_dump_scalar(data)}"


def write_yaml(path: str | Path, data: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(_dump_yaml(data) + "\n", encoding="utf-8")


def _deployment_json(namespace: str, deployment: str) -> dict[str, Any]:
    code, stdout, stderr = run_cmd(["kubectl", "get", "deploy", "-n", namespace, deployment, "-o", "json"], timeout=30)
    if code != 0:
        raise RuntimeError(f"kubectl get deploy failed for {deployment}: {stderr}")
    return json.loads(stdout)


def _deployment_ready(namespace: str, deployment: str) -> bool:
    data = _deployment_json(namespace, deployment)
    desired = int((data.get("spec") or {}).get("replicas") or 1)
    status = data.get("status") or {}
    return int(status.get("readyReplicas") or 0) >= desired and int(status.get("availableReplicas") or 0) >= desired


def _container_names(namespace: str, deployment: str) -> list[str]:
    data = _deployment_json(namespace, deployment)
    return [str(row.get("name", "")) for row in data.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])]


def _kubectl_text(args: list[str]) -> str:
    code, stdout, stderr = run_cmd(["kubectl", *args], timeout=30)
    return stdout + ("\nSTDERR:\n" + stderr if stderr else "")


def _http_ok(frontend: dict[str, Any]) -> bool:
    return bool(frontend.get("http_ok")) or float(frontend.get("error_rate", 1.0)) == 0.0


def _start_frontend_port_forward_if_needed(namespace: str, frontend_url: str, output_dir: Path) -> subprocess.Popen | None:
    if _http_ok(curl_frontend(frontend_url, 1, 3)):
        return None
    log_path = output_dir / "frontend_port_forward.log"
    handle = log_path.open("a", encoding="utf-8")
    proc = subprocess.Popen(["kubectl", "port-forward", "-n", namespace, "svc/frontend", "8080:80"], stdout=handle, stderr=subprocess.STDOUT, text=True)
    handle.close()
    time.sleep(5)
    if not _http_ok(curl_frontend(frontend_url, 1, 3)):
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


def ensure_online_boutique_ready(namespace: str, frontend_url: str, sidecar_name: str = "proberca-lockstress") -> dict[str, Any]:
    removed = False
    if sidecar_name in _container_names(namespace, "cartservice"):
        remove_lockstress_sidecar(namespace, "cartservice", sidecar_name)
        removed = True
    required = ["frontend", "cartservice", "checkoutservice"]
    not_ready = [name for name in required if not _deployment_ready(namespace, name)]
    if not_ready:
        raise RuntimeError(f"Online Boutique deployments not ready: {not_ready}")
    smoke = curl_frontend(frontend_url, 1, 3)
    if not _http_ok(smoke):
        raise RuntimeError(f"frontend smoke failed: {smoke}")
    return {"checked_deployments": required, "removed_stale_sidecar": removed, "frontend_smoke": smoke}


def _frontend_records(frontend: dict[str, Any], timestamp: float, phase: str, incident_id: str) -> list[dict[str, Any]]:
    mapping = {
        "request.rps": frontend.get("rps"),
        "request.error_rate": frontend.get("error_rate"),
        "request.p50_latency_ms": frontend.get("p50_latency_ms"),
        "request.p95_latency_ms": frontend.get("p95_latency_ms"),
        "request.p99_latency_ms": frontend.get("p99_latency_ms"),
    }
    rows: list[dict[str, Any]] = []
    for metric, value in mapping.items():
        if value is None:
            continue
        rows.append({
            "incident_id": incident_id,
            "timestamp": float(timestamp),
            "service": "frontend",
            "instance": "frontend",
            "node": "frontend",
            "metric": metric,
            "value": float(value),
            "phase": phase,
            "source": "real_sidecar_lockstress_collection",
        })
    return rows


def collect_lock_window_metrics(config: dict[str, Any], phase: str, window_index: int, lock_metrics: dict | None = None) -> list[dict]:
    exp = config["experiment"]
    runtime = config["_runtime"]
    incident_id = str(runtime["incident_id"])
    started = time.time()
    frontend = curl_frontend(str(exp["frontend_url"]), int(exp["requests_per_window"]), int(exp["request_timeout_sec"]))
    elapsed = time.time() - started
    window_size = float(exp["window_size_sec"])
    if elapsed < window_size:
        time.sleep(window_size - elapsed)
    timestamp = time.time()
    rows = _frontend_records(frontend, timestamp, phase, incident_id)
    if lock_metrics and phase == "faulty":
        service = str(config["target"]["service"])
        pod_name = str(runtime.get("pod_name_during") or runtime.get("pod_name_before") or service)
        faulty_windows = max(int(exp["faulty_windows"]), 1)
        lock_sum = float(lock_metrics.get("lock_wait_ms_sum_total", 0.0)) / faulty_windows
        contention = float(lock_metrics.get("lock_contention_count_total", 0.0)) / faulty_windows
        mean_wait = float(lock_metrics.get("lock_wait_ms_mean_avg", 0.0))
        for metric, value in [
            ("lock.futex_wait_ms", lock_sum),
            ("lock.wait_ms", mean_wait),
            ("lock.contention_count", contention),
        ]:
            if value <= 0:
                continue
            rows.append({
                "incident_id": incident_id,
                "timestamp": float(timestamp),
                "service": service,
                "instance": pod_name,
                "node": service,
                "metric": metric,
                "value": float(value),
                "phase": phase,
                "source": "real_sidecar_lockstress_collection",
            })
    return rows


def _phase_values(metrics: list[dict], service: str, metric: str, phase: str) -> list[float]:
    return [float(row["value"]) for row in metrics if row.get("service") == service and row.get("metric") == metric and row.get("phase") == phase]


def _mean(values: list[float]) -> float:
    return float(np.mean(np.asarray(values, dtype=float))) if values else 0.0


def _lift(metrics: list[dict], service: str, metric: str) -> float:
    return _mean(_phase_values(metrics, service, metric, "faulty")) - _mean(_phase_values(metrics, service, metric, "baseline"))


def build_lock_evidence(metrics: list[dict], incident: dict) -> list[dict]:
    service = str(incident["root_service"])
    incident_id = str(incident["incident_id"])
    candidates = [
        ("lock.futex_wait_ms", sum(_phase_values(metrics, service, "lock.futex_wait_ms", "faulty"))),
        ("lock.contention_count", sum(_phase_values(metrics, service, "lock.contention_count", "faulty"))),
    ]
    positive = [(metric, value) for metric, value in candidates if value > 0]
    if not positive:
        return []
    max_value = max(value for _metric, value in positive)
    timestamp = float(incident["start_ts"])
    rows: list[dict[str, Any]] = []
    for metric, value in positive:
        score = min(1.0, float(value / max(max_value, 1e-9)))
        rows.append({
            "incident_id": incident_id,
            "timestamp": timestamp,
            "service": service,
            "instance": service,
            "node": service,
            "evidence_type": "Lock",
            "root_type_hint": "lock contention",
            "metric": metric,
            "value": score,
            "evidence_score": score,
            "source": "real_sidecar_lockstress_collection",
            "probe_id": "p2d1_sidecar_lockstress",
            "sampling_rate": 1.0,
        })
    return rows


def build_lock_incident(repeat_index: int, start_ts: float, end_ts: float) -> dict[str, Any]:
    suffix = f"{int(repeat_index):02d}"
    return {
        "incident_id": f"ob-lock-cartservice-repeat-{suffix}",
        "root_service": "cartservice",
        "root_metric": "lock.futex_wait_ms",
        "root_type": "lock contention",
        "symptom_service": "frontend",
        "start_ts": float(start_ts),
        "end_ts": float(end_ts),
        "injected_path": [
            "cartservice.lock.futex_wait_ms",
            "checkoutservice.request.p99_latency_ms",
            "frontend.request.p99_latency_ms",
        ],
    }


def _quality_report(metrics: list[dict], evidence: list[dict], config: dict[str, Any], patch: dict[str, Any], restore: dict[str, Any], lock_metrics: dict[str, Any], frontend_recovery: dict[str, Any]) -> dict[str, Any]:
    services_seen = sorted({str(row["service"]) for row in metrics})
    metrics_seen = sorted({str(row["metric"]) for row in metrics})
    report = {
        "metrics_count": len(metrics),
        "services_seen": services_seen,
        "metrics_seen": metrics_seen,
        "baseline_windows": int(config["experiment"]["baseline_windows"]),
        "faulty_windows": int(config["experiment"]["faulty_windows"]),
        "recovery_windows": int(config["experiment"]["recovery_windows"]),
        "sidecar_injected": bool(patch.get("sidecar_injected")),
        "sidecar_removed": bool(restore.get("sidecar_removed")),
        "lock_metrics_available": bool(lock_metrics.get("lock_metrics_available")),
        "lock_contention_count_total": int(lock_metrics.get("lock_contention_count_total", 0)),
        "lock_wait_ms_sum_total": float(lock_metrics.get("lock_wait_ms_sum_total", 0.0)),
        "lock_wait_ms_mean_avg": float(lock_metrics.get("lock_wait_ms_mean_avg", 0.0)),
        "lock_wait_ms_p95_max": float(lock_metrics.get("lock_wait_ms_p95_max", 0.0)),
        "p95_parse_warning": bool(lock_metrics.get("p95_parse_warning")),
        "cartservice_lock_metric_present": any(row.get("service") == "cartservice" and str(row.get("metric", "")).startswith("lock.") for row in metrics),
        "cartservice_futex_wait_metric_present": any(row.get("service") == "cartservice" and row.get("metric") == "lock.futex_wait_ms" for row in metrics),
        "frontend_latency_metric_present": any(row.get("service") == "frontend" and row.get("metric") == "request.p99_latency_ms" for row in metrics),
        "lock_evidence_present": bool(evidence),
        "fault_injection_succeeded": bool(patch.get("sidecar_injected")),
        "restore_succeeded": bool(restore.get("sidecar_removed")),
        "frontend_recovery_p99_ok": bool(frontend_recovery.get("p99_ok")),
        "frontend_latency_lift": float(_lift(metrics, "frontend", "request.p99_latency_ms")),
        "limitation": LIMITATION,
        "lock_metrics_window_distribution": DISTRIBUTION,
    }
    return report


def _quality_ok(quality: dict[str, Any], requirements: dict[str, Any]) -> bool:
    mapping = {
        "require_sidecar_injected": "sidecar_injected",
        "require_sidecar_removed": "sidecar_removed",
        "require_lock_metrics_available": "lock_metrics_available",
        "require_frontend_latency_metric_present": "frontend_latency_metric_present",
        "require_service_graph_present": "service_graph_present",
    }
    for req, field in mapping.items():
        if requirements.get(req) is True and quality.get(field) is not True:
            return False
    if requirements.get("require_lock_contention_count_positive") is True and float(quality.get("lock_contention_count_total", 0.0)) <= 0.0:
        return False
    if requirements.get("require_lock_wait_sum_positive") is True and float(quality.get("lock_wait_ms_sum_total", 0.0)) <= 0.0:
        return False
    return True


def _run_real_ob_rca(input_dir: str | Path, output_dir: str | Path, top_k: int = 5) -> dict[str, Any]:
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    build_real_observation_files(input_path, output_path, sampling_probability=1.0)
    normalize_dataset(output_path, output_path)
    refresh_real_observation_from_normalized(output_path, sampling_probability=1.0)
    train_ipw_masked_propagation(output_path, output_path, IPWPropagationConfig())
    solve_ipw_sparse_inversion(output_path, output_path, IPWSparseInversionConfig())
    score_ipw_semantic_evidence(output_path, output_path, IPWSemanticEvidenceConfig())
    explain_ipw_paths(output_path, output_path, IPWPathExplanationConfig(top_k_candidates=top_k))
    build_p1_results(output_path, output_path, top_k=top_k)
    incidents = read_jsonl(output_path / "incidents.jsonl")
    results = read_jsonl(output_path / "p1_results.jsonl")
    path_summary = json.loads((output_path / "ipw_path_explanation_summary.json").read_text(encoding="utf-8"))
    evaluation = evaluate_p1_results(results, incidents, path_summary=path_summary)
    _write_json(output_path / "p1_evaluation_summary.json", evaluation)
    quality = json.loads((output_path / "data_quality_report.json").read_text(encoding="utf-8"))
    result = results[0] if results else {}
    per = evaluation.get("per_incident", [{}])[0] if evaluation.get("per_incident") else {}
    top_metrics = result.get("top_metrics", [])
    top_services = result.get("top_services", [])
    summary = {
        "input_dir": str(input_path),
        "output_dir": str(output_path),
        "real_collection": True,
        "incident_count": int(evaluation["incidents_count"]),
        "service_hit_at_1": float(evaluation["service_hit_at_1"]),
        "service_hit_at_3": float(evaluation["service_hit_at_3"]),
        "metric_hit_at_1": float(evaluation["metric_hit_at_1"]),
        "metric_hit_at_3": float(evaluation["metric_hit_at_3"]),
        "metric_mrr": float(evaluation["metric_mrr"]),
        "root_type_accuracy": float(evaluation["root_type_accuracy"]),
        "path_fidelity": float(evaluation["path_fidelity"]),
        "observed_ratio": float(evaluation.get("observed_ratio", 0.0)),
        "predicted_top1_service": str(top_services[0].get("service", "")) if top_services else "",
        "predicted_top1_metric": str(top_metrics[0].get("node", "")) if top_metrics else "",
        "predicted_root_type": str(result.get("root_type", "unknown")),
        "true_root_service_debug": per.get("true_root_service_debug"),
        "true_root_metric_debug": per.get("true_root_metric_debug"),
        "metric_rank_debug": per.get("metric_rank_debug"),
        "path_services": result.get("path", {}).get("path_services", []),
        "lock_wait_ms_sum_debug": float(quality.get("lock_wait_ms_sum_total", 0.0)),
        "lock_contention_count_debug": float(quality.get("lock_contention_count_total", 0.0)),
        "frontend_latency_lift_debug": float(quality.get("frontend_latency_lift", 0.0)),
        "cartservice_lock_metric_present": bool(quality.get("cartservice_lock_metric_present")),
        "cartservice_futex_wait_metric_present": bool(quality.get("cartservice_futex_wait_metric_present")),
        "limitation": LIMITATION,
    }
    _write_json(output_path / "real_p1_rca_summary.json", summary)
    _write_json(output_path / "real_p1_rca_metadata.json", {"input_dir": str(input_path), "output_dir": str(output_path), "top_k": int(top_k), "real_collection": True, "note": "P2D-1 single real lock sidecar repeat case; not multi-fault accuracy."})
    return {"summary": summary, "evaluation": evaluation, "results": results}


def _frontend_recovery_check(frontend_url: str, requests: int, timeout_sec: int, threshold_ms: float, cooldown_sec: int) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    for attempt in range(1, 4):
        sample = curl_frontend(frontend_url, requests, timeout_sec)
        p99 = float(sample.get("p99_latency_ms") or 0.0)
        ok = bool(sample.get("http_ok")) and (threshold_ms <= 0 or p99 <= threshold_ms)
        attempts.append({"attempt": attempt, "p99_latency_ms": p99, "http_ok": bool(sample.get("http_ok")), "p99_ok": ok, "sample": sample})
        if ok:
            return {"p99_ok": True, "attempts": attempts, "final": sample}
        if attempt < 3 and cooldown_sec > 0:
            time.sleep(max(5, min(cooldown_sec, 30)))
    return {"p99_ok": False, "attempts": attempts, "final": attempts[-1]["sample"] if attempts else {}}


def run_single_lock_repeat(repeat_index: int, config: dict[str, Any]) -> dict[str, Any]:
    repeat_cfg = config["repeat_experiment"]
    base_dir = Path(str(repeat_cfg["base_output_dir"]))
    repeat_dir = base_dir / f"repeat_{int(repeat_index):02d}"
    raw_dir = repeat_dir / "raw"
    rca_dir = repeat_dir / "p1rca"
    raw_dir.mkdir(parents=True, exist_ok=True)
    rca_dir.mkdir(parents=True, exist_ok=True)
    runtime_config = json.loads(json.dumps(config))
    suffix = f"{int(repeat_index):02d}"
    incident_id = f"ob-lock-cartservice-repeat-{suffix}"
    namespace = str(config["kubernetes"]["namespace"])
    cluster_name = str(config["kubernetes"]["cluster_name"])
    exp = config["experiment"]
    fault = config["fault_injection"]
    frontend_url = str(exp["frontend_url"])
    deployment = str(fault["target_deployment"])
    sidecar_name = str(fault["sidecar_name"])
    sidecar_image = str(fault["sidecar_image"])
    port_forward_proc = _start_frontend_port_forward_if_needed(namespace, frontend_url, repeat_dir)
    patch_result: dict[str, Any] = {"sidecar_injected": False}
    restore_result: dict[str, Any] = {"sidecar_removed": False}
    lock_metrics: dict[str, Any] = {"lock_metrics_available": False, "lock_wait_ms_sum_total": 0.0, "lock_contention_count_total": 0, "p95_parse_warning": False}
    pod_before: dict[str, Any] = {"name": ""}
    pod_during: dict[str, Any] = {"name": ""}
    metrics: list[dict[str, Any]] = []
    baseline_states: list[dict[str, Any]] = []
    faulty_states: list[dict[str, Any]] = []
    recovery_states: list[dict[str, Any]] = []
    first_faulty_ts = 0.0
    last_faulty_ts = 0.0
    try:
        ready = ensure_online_boutique_ready(namespace, frontend_url, sidecar_name)
        pod_before = get_target_pod(namespace, str(config["target"]["service"]))
        runtime_config["_runtime"] = {"pod_name_before": pod_before.get("name", ""), "incident_id": incident_id}
        write_yaml(repeat_dir / "repeat_config.yaml", runtime_config)
        _write_text(raw_dir / "kubectl_get_pods_before.txt", _kubectl_text(["get", "pods", "-n", namespace]))
        _write_text(raw_dir / "kubectl_get_deploy_before.txt", _kubectl_text(["get", "deploy", "-n", namespace]))
        for idx in range(int(exp["baseline_windows"])):
            rows = collect_lock_window_metrics(runtime_config, "baseline", idx + 1)
            timestamp = max((float(row["timestamp"]) for row in rows), default=time.time())
            metrics.extend(rows)
            baseline_states.append({"phase": "baseline", "window_index": idx + 1, "timestamp": timestamp})

        image_result = ensure_sidecar_image_loaded(sidecar_image, cluster_name)
        command = build_lockstress_python_command(int(fault["duration_sec"]), int(fault["workers"]), int(fault["lock_hold_ms"]))
        patch_result = patch_cartservice_add_lockstress_sidecar(namespace, deployment, sidecar_name, sidecar_image, command)
        time.sleep(5)
        pod_during = get_target_pod(namespace, str(config["target"]["service"]))
        runtime_config["_runtime"]["pod_name_during"] = pod_during.get("name", "")
        _write_text(raw_dir / "kubectl_get_pods_during.txt", _kubectl_text(["get", "pods", "-n", namespace]))
        _write_text(raw_dir / "kubectl_get_deploy_during.txt", _kubectl_text(["get", "deploy", "-n", namespace]))
        _write_json(raw_dir / "lock_fault_log.json", {"image": image_result, "patch": patch_result, "pod_name_before": pod_before.get("name", ""), "pod_name_during": pod_during.get("name", ""), "sidecar_command": command, "limitation": LIMITATION})

        for idx in range(int(exp["faulty_windows"])):
            rows = collect_lock_window_metrics(runtime_config, "faulty", idx + 1)
            timestamp = max((float(row["timestamp"]) for row in rows), default=time.time())
            metrics.extend(rows)
            faulty_states.append({"phase": "faulty", "window_index": idx + 1, "timestamp": timestamp})
            if idx == 0:
                first_faulty_ts = timestamp
            last_faulty_ts = timestamp
        logs = get_sidecar_logs(namespace, deployment, sidecar_name, str(pod_during.get("name", "")))
        _write_text(raw_dir / "lockstress_logs.txt", logs)
        lock_metrics = parse_lockstress_logs(logs)
        _write_json(raw_dir / "lock_metrics_during.json", lock_metrics)
        lock_rows: list[dict[str, Any]] = []
        for idx, state in enumerate(faulty_states):
            lock_rows.extend(_lock_metric_records(runtime_config, state["timestamp"], lock_metrics, idx + 1))
        metrics.extend(lock_rows)
    finally:
        try:
            restore_result = remove_lockstress_sidecar(namespace, deployment, sidecar_name)
        finally:
            _write_json(raw_dir / "lock_restore_log.json", restore_result)
            _write_text(raw_dir / "kubectl_get_pods_after.txt", _kubectl_text(["get", "pods", "-n", namespace]))
            _write_text(raw_dir / "kubectl_get_deploy_after.txt", _kubectl_text(["get", "deploy", "-n", namespace]))

    if not _deployment_ready(namespace, "cartservice") or not _deployment_ready(namespace, "frontend"):
        raise RuntimeError("cartservice/frontend not ready after lock sidecar restore")
    cooldown = int(exp.get("post_restore_cooldown_sec", 0) or 0)
    if cooldown > 0:
        time.sleep(cooldown)
    if sidecar_name in _container_names(namespace, deployment):
        raise RuntimeError("lockstress sidecar still present after restore")
    frontend_recovery = _frontend_recovery_check(frontend_url, int(exp["requests_per_window"]), int(exp["request_timeout_sec"]), float(exp.get("require_frontend_recovery_p99_below_ms", 0) or 0), cooldown)
    _write_json(raw_dir / "frontend_recovery_check.json", frontend_recovery)
    for idx in range(int(exp["recovery_windows"])):
        rows = collect_lock_window_metrics(runtime_config, "recovery", idx + 1)
        timestamp = max((float(row["timestamp"]) for row in rows), default=time.time())
        metrics.extend(rows)
        recovery_states.append({"phase": "recovery", "window_index": idx + 1, "timestamp": timestamp})

    incident = build_lock_incident(repeat_index, first_faulty_ts, last_faulty_ts)
    evidence = build_lock_evidence(metrics, incident)
    write_jsonl(raw_dir / "metrics.jsonl", metrics)
    write_jsonl(raw_dir / "incidents.jsonl", [incident])
    write_jsonl(raw_dir / "evidence.jsonl", evidence)
    graph_result = write_online_boutique_service_graph(raw_dir / "service_graph.jsonl")
    quality = _quality_report(metrics, evidence, config, patch_result, restore_result, lock_metrics, frontend_recovery)
    quality["service_graph_present"] = Path(raw_dir / "service_graph.jsonl").exists()
    _write_json(raw_dir / "data_quality_report.json", quality)
    _write_json(raw_dir / "metadata.json", {
        "phase": "P2D-1",
        "repeat_index": int(repeat_index),
        "experiment_group_id": str(repeat_cfg["experiment_group_id"]),
        "raw_output_dir": str(raw_dir),
        "target_service": str(config["target"]["service"]),
        "target_metric": str(config["target"]["metric"]),
        "target_fault_type": str(config["target"]["fault_type"]),
        "pod_name_before": str(pod_before.get("name", "")),
        "pod_name_during": str(pod_during.get("name", "")),
        "ready_check": ready if 'ready' in locals() else {},
        "service_graph": graph_result,
        "baseline_states": baseline_states,
        "faulty_states": faulty_states,
        "recovery_states": recovery_states,
        "lock_metrics_window_distribution": DISTRIBUTION,
        "limitation": LIMITATION,
    })
    quality_ok = _quality_ok(quality, config.get("quality_requirements", {}))
    row: dict[str, Any] = {
        "repeat_index": int(repeat_index),
        "raw_output_dir": str(raw_dir),
        "rca_output_dir": str(rca_dir),
        "sidecar_injected": bool(patch_result.get("sidecar_injected")),
        "sidecar_removed": bool(restore_result.get("sidecar_removed")),
        "lock_metrics_available": bool(quality.get("lock_metrics_available")),
        "cartservice_lock_metric_present": bool(quality.get("cartservice_lock_metric_present")),
        "cartservice_futex_wait_metric_present": bool(quality.get("cartservice_futex_wait_metric_present")),
        "quality_ok": bool(quality_ok),
        "lock_wait_ms_sum_total": float(quality.get("lock_wait_ms_sum_total", 0.0)),
        "lock_contention_count_total": int(quality.get("lock_contention_count_total", 0)),
        "frontend_latency_lift": float(quality.get("frontend_latency_lift", 0.0)),
        "p95_parse_warning": bool(quality.get("p95_parse_warning")),
        "limitation": LIMITATION,
    }
    if quality_ok:
        rca = _run_real_ob_rca(raw_dir, rca_dir, top_k=5)
        summary = rca["summary"]
        row.update({
            "predicted_top1_service": summary.get("predicted_top1_service", ""),
            "predicted_top1_metric": summary.get("predicted_top1_metric", ""),
            "predicted_root_type": summary.get("predicted_root_type", ""),
            "metric_rank_debug": summary.get("metric_rank_debug"),
            "service_hit_at_1": float(summary.get("service_hit_at_1", 0.0)),
            "metric_hit_at_1": float(summary.get("metric_hit_at_1", 0.0)),
            "metric_hit_at_3": float(summary.get("metric_hit_at_3", 0.0)),
            "metric_mrr": float(summary.get("metric_mrr", 0.0)),
            "root_type_accuracy": float(summary.get("root_type_accuracy", 0.0)),
            "path_fidelity": float(summary.get("path_fidelity", 0.0)),
            "rca_ok": True,
        })
    else:
        row.update({"predicted_top1_service": "", "predicted_top1_metric": "", "predicted_root_type": "", "metric_rank_debug": None, "service_hit_at_1": 0.0, "metric_hit_at_1": 0.0, "metric_hit_at_3": 0.0, "metric_mrr": 0.0, "root_type_accuracy": 0.0, "path_fidelity": 0.0, "rca_ok": False})
    _write_json(repeat_dir / "repeat_summary.json", row)
    _stop_process(port_forward_proc)
    return row


def _lock_metric_records(config: dict[str, Any], timestamp: float, lock_metrics: dict[str, Any], window_index: int) -> list[dict[str, Any]]:
    exp = config["experiment"]
    runtime = config["_runtime"]
    service = str(config["target"]["service"])
    pod_name = str(runtime.get("pod_name_during") or runtime.get("pod_name_before") or service)
    incident_id = str(runtime["incident_id"])
    faulty_windows = max(int(exp["faulty_windows"]), 1)
    values = {
        "lock.futex_wait_ms": float(lock_metrics.get("lock_wait_ms_sum_total", 0.0)) / faulty_windows,
        "lock.wait_ms": float(lock_metrics.get("lock_wait_ms_mean_avg", 0.0)),
        "lock.contention_count": float(lock_metrics.get("lock_contention_count_total", 0.0)) / faulty_windows,
    }
    rows: list[dict[str, Any]] = []
    for metric, value in values.items():
        if value <= 0:
            continue
        rows.append({
            "incident_id": incident_id,
            "timestamp": float(timestamp),
            "service": service,
            "instance": pod_name,
            "node": service,
            "metric": metric,
            "value": float(value),
            "phase": "faulty",
            "source": "real_sidecar_lockstress_collection",
        })
    return rows


def _mean_metric(rows: list[dict[str, Any]], key: str) -> float:
    values = [float(row.get(key, 0.0)) for row in rows]
    return float(np.mean(np.asarray(values, dtype=float))) if values else 0.0


def _min_metric(rows: list[dict[str, Any]], key: str) -> float:
    values = [float(row.get(key, 0.0)) for row in rows]
    return float(np.min(np.asarray(values, dtype=float))) if values else 0.0


def aggregate_lock_repeat_summary(config: dict[str, Any], per_repeat: list[dict[str, Any]]) -> dict[str, Any]:
    repeat_cfg = config["repeat_experiment"]
    rca_rows = [row for row in per_repeat if row.get("rca_ok")]
    quality_rows = [row for row in per_repeat if row.get("quality_ok")]
    return {
        "experiment_group_id": str(repeat_cfg["experiment_group_id"]),
        "repeats_requested": int(repeat_cfg.get("repeats", 5)),
        "repeats_completed": len(per_repeat),
        "repeats_successful_quality": len(quality_rows),
        "repeats_successful_rca": len(rca_rows),
        "service_hit_at_1_mean": _mean_metric(rca_rows, "service_hit_at_1"),
        "service_hit_at_1_min": _min_metric(rca_rows, "service_hit_at_1"),
        "metric_hit_at_1_mean": _mean_metric(rca_rows, "metric_hit_at_1"),
        "metric_hit_at_1_min": _min_metric(rca_rows, "metric_hit_at_1"),
        "metric_hit_at_3_mean": _mean_metric(rca_rows, "metric_hit_at_3"),
        "metric_hit_at_3_min": _min_metric(rca_rows, "metric_hit_at_3"),
        "metric_mrr_mean": _mean_metric(rca_rows, "metric_mrr"),
        "metric_mrr_min": _min_metric(rca_rows, "metric_mrr"),
        "root_type_accuracy_mean": _mean_metric(rca_rows, "root_type_accuracy"),
        "root_type_accuracy_min": _min_metric(rca_rows, "root_type_accuracy"),
        "path_fidelity_mean": _mean_metric(rca_rows, "path_fidelity"),
        "path_fidelity_min": _min_metric(rca_rows, "path_fidelity"),
        "lock_wait_ms_sum_mean": _mean_metric(quality_rows, "lock_wait_ms_sum_total"),
        "lock_contention_count_mean": _mean_metric(quality_rows, "lock_contention_count_total"),
        "frontend_latency_lift_mean": _mean_metric(quality_rows, "frontend_latency_lift"),
        "limitation": LIMITATION,
        "per_repeat": per_repeat,
    }


def run_p2d1_lock_repeated_experiment(config_path: str | Path) -> dict[str, Any]:
    config = load_lock_repeat_config(config_path)
    repeat_cfg = config["repeat_experiment"]
    base_dir = Path(str(repeat_cfg["base_output_dir"]))
    base_dir.mkdir(parents=True, exist_ok=True)
    repeats = int(repeat_cfg.get("repeats", 5))
    sleep_between = int(repeat_cfg.get("sleep_between_repeats_sec", 0) or 0)
    per_repeat: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for index in range(1, repeats + 1):
        try:
            row = run_single_lock_repeat(index, config)
            per_repeat.append(row)
            if not row.get("quality_ok") or not row.get("rca_ok"):
                failures.append({"repeat_index": index, "row": row})
        except Exception as exc:
            namespace = str(config.get("kubernetes", {}).get("namespace", "online-boutique"))
            sidecar = str(config.get("fault_injection", {}).get("sidecar_name", "proberca-lockstress"))
            try:
                remove_lockstress_sidecar(namespace, "cartservice", sidecar)
            except Exception as restore_exc:
                failures.append({"repeat_index": index, "restore_error": str(restore_exc)})
            failure = {"repeat_index": index, "error": str(exc)}
            failures.append(failure)
            per_repeat.append({"repeat_index": index, "quality_ok": False, "rca_ok": False, "error": str(exc), "service_hit_at_1": 0.0, "metric_hit_at_1": 0.0, "metric_hit_at_3": 0.0, "metric_mrr": 0.0, "root_type_accuracy": 0.0, "path_fidelity": 0.0, "lock_wait_ms_sum_total": 0.0, "lock_contention_count_total": 0.0, "frontend_latency_lift": 0.0, "limitation": LIMITATION})
        if index < repeats and sleep_between > 0:
            time.sleep(sleep_between)
    summary = aggregate_lock_repeat_summary(config, per_repeat)
    metadata = {
        "config_path": str(config_path),
        "base_output_dir": str(base_dir),
        "target": config.get("target", {}),
        "fault_injection": config.get("fault_injection", {}),
        "experiment": config.get("experiment", {}),
        "limitation": LIMITATION,
        "note": "P2D-1 repeated real sidecar lock contention experiment; not multi-fault accuracy and not original cartservice business-code bug.",
    }
    _write_json(base_dir / "p2d1_lock_repeat_summary.json", summary)
    _write_json(base_dir / "p2d1_lock_repeat_metadata.json", metadata)
    _write_json(base_dir / "p2d1_lock_repeat_failures.json", {"failures": failures})
    return {"summary": summary, "metadata": metadata, "failures": failures, "output_dir": str(base_dir)}
