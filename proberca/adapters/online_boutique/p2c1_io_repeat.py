"""P2C-1 repeated real Online Boutique I/O fault injection experiments."""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from typing import Any

import numpy as np

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
from proberca.adapters.online_boutique.p1_bridge import (
    build_real_observation_files,
    refresh_real_observation_from_normalized,
)
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


def load_io_repeat_config(config_path: str | Path) -> dict[str, Any]:
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


def _deployment_ready(item: dict[str, Any]) -> bool:
    status = item.get("status", {})
    spec = item.get("spec", {})
    desired = int(spec.get("replicas", 1) or 1)
    return int(status.get("readyReplicas", 0) or 0) >= desired and int(status.get("availableReplicas", 0) or 0) >= desired


def ensure_online_boutique_ready(namespace: str, frontend_url: str = "http://127.0.0.1:8080") -> dict[str, Any]:
    code, stdout, stderr = run_cmd(["kubectl", "get", "deploy", "-n", namespace, "-o", "json"], timeout=30)
    if code != 0:
        raise RuntimeError(f"kubectl get deploy failed: {stderr}")
    data = json.loads(stdout)
    deployments = data.get("items", [])
    not_ready = [item.get("metadata", {}).get("name", "") for item in deployments if not _deployment_ready(item)]
    required = {"frontend", "cartservice", "redis-cart"}
    present = {item.get("metadata", {}).get("name", "") for item in deployments}
    missing = sorted(required - present)
    if missing or not_ready:
        raise RuntimeError(f"Online Boutique not ready; missing={missing}, not_ready={not_ready}")
    smoke = curl_frontend(frontend_url, 1, 3)
    if not _http_ok(smoke):
        proc = subprocess.Popen(
            ["kubectl", "port-forward", "-n", namespace, "svc/frontend", "8080:80"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        try:
            time.sleep(5)
            smoke = curl_frontend(frontend_url, 1, 3)
        finally:
            proc.terminate()
        if not _http_ok(smoke):
            raise RuntimeError(f"frontend smoke failed: {smoke}")
    return {"deployments_count": len(deployments), "frontend_smoke": smoke}


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


def _http_ok(frontend: dict[str, Any]) -> bool:
    samples = frontend.get("samples") or []
    if samples:
        return float(frontend.get("error_rate", 1.0)) == 0.0 and any(int(item.get("http_code", 0)) // 100 == 2 for item in samples)
    return float(frontend.get("error_rate", 1.0)) == 0.0 and frontend.get("p99_latency_ms") is not None


def _df(namespace: str, deployment: str, container: str | None = None) -> str:
    cmd = ["kubectl", "exec", "-n", namespace, f"deploy/{deployment}"]
    if container:
        cmd.extend(["-c", container])
    cmd.extend(["--", "sh", "-c", "df -h /tmp /data 2>/dev/null || df -h /tmp"])
    code, stdout, stderr = run_cmd(cmd, timeout=30)
    return stdout + ("\nSTDERR:\n" + stderr if stderr else "")


def choose_writable_temp_file(namespace: str, deployment: str, container: str, candidates: list[str]) -> str:
    for candidate in candidates:
        check_file = f"{candidate}.check"
        script = f"touch {check_file} 2>/dev/null && rm -f {check_file}"
        cmd = ["kubectl", "exec", "-n", namespace, f"deploy/{deployment}", "-c", container, "--", "sh", "-c", script]
        code, _stdout, _stderr = run_cmd(cmd, timeout=15)
        if code == 0:
            return candidate
    raise RuntimeError(f"no writable redis-cart temp file candidate: {candidates}")


def _frontend_records(frontend: dict[str, Any], timestamp: float, phase: str, incident_id: str) -> list[dict[str, Any]]:
    mapping = {
        "request.rps": frontend.get("rps"),
        "request.error_rate": frontend.get("error_rate"),
        "request.p50_latency_ms": frontend.get("p50_latency_ms"),
        "request.p95_latency_ms": frontend.get("p95_latency_ms"),
        "request.p99_latency_ms": frontend.get("p99_latency_ms"),
    }
    records: list[dict[str, Any]] = []
    for metric, value in mapping.items():
        if value is None:
            continue
        records.append({
            "incident_id": incident_id,
            "timestamp": float(timestamp),
            "service": "frontend",
            "instance": "frontend",
            "node": "frontend",
            "metric": metric,
            "value": float(value),
            "phase": phase,
            "source": "real_pod_io_collection",
        })
    return records


def _fs_delta_records(delta: dict[str, Any], service: str, pod_name: str, timestamp: float, phase: str, incident_id: str) -> list[dict[str, Any]]:
    metrics = ["io.write_bytes", "io.write_ops", "io.read_bytes", "io.read_ops", "io.io_time_ms"]
    records: list[dict[str, Any]] = []
    for metric in metrics:
        value = summarize_fs_metric(delta, service, metric)
        metric_present = any(entry.get("service") == service and metric in (entry.get("metrics") or {}) for entry in delta.values())
        if not metric_present:
            continue
        records.append({
            "incident_id": incident_id,
            "timestamp": float(timestamp),
            "service": service,
            "instance": pod_name,
            "node": service,
            "metric": metric,
            "value": float(value),
            "phase": phase,
            "source": "real_pod_io_collection",
        })
    return records


def collect_io_window_metrics(config: dict[str, Any], phase: str, window_index: int, prev_fs_snapshot: dict[str, Any] | None = None) -> tuple[list[dict], dict]:
    namespace = str(config["kubernetes"]["namespace"])
    exp = config["experiment"]
    target = config["target"]
    runtime = config["_runtime"]
    if prev_fs_snapshot is None:
        prev_fs_snapshot = collect_fs_snapshot(namespace)
    window_size = float(exp["window_size_sec"])
    started = time.time()
    frontend = curl_frontend(str(exp["frontend_url"]), int(exp["requests_per_window"]), int(exp["request_timeout_sec"]))
    elapsed = time.time() - started
    if elapsed < window_size:
        time.sleep(window_size - elapsed)
    curr_snapshot = collect_fs_snapshot(namespace)
    timestamp = time.time()
    delta = compute_fs_delta(prev_fs_snapshot, curr_snapshot, window_size)
    incident_id = str(runtime["incident_id"])
    records = _fs_delta_records(delta, str(target["service"]), str(runtime["pod_name"]), timestamp, phase, incident_id)
    records.extend(_frontend_records(frontend, timestamp, phase, incident_id))
    state = {"fs_snapshot": curr_snapshot, "fs_delta": delta, "frontend": frontend, "timestamp": timestamp, "window_index": int(window_index), "phase": phase}
    return records, state


def _phase_values(metrics: list[dict], service: str, metric: str, phase: str) -> list[float]:
    return [float(row["value"]) for row in metrics if row.get("service") == service and row.get("metric") == metric and row.get("phase") == phase]


def _mean(values: list[float]) -> float:
    return float(np.mean(np.asarray(values, dtype=float))) if values else 0.0


def _lift(metrics: list[dict], service: str, metric: str) -> float:
    return _mean(_phase_values(metrics, service, metric, "faulty")) - _mean(_phase_values(metrics, service, metric, "baseline"))


def build_io_evidence(metrics: list[dict], incident: dict) -> list[dict]:
    service = str(incident["root_service"])
    incident_id = str(incident["incident_id"])
    candidates = [
        ("io.write_bytes", _lift(metrics, service, "io.write_bytes")),
        ("io.write_ops", _lift(metrics, service, "io.write_ops")),
        ("io.io_time_ms", _lift(metrics, service, "io.io_time_ms")),
    ]
    positive = [(metric, lift) for metric, lift in candidates if lift > 0]
    if not positive:
        return []
    max_lift = max(lift for _metric, lift in positive)
    timestamp = float(incident["start_ts"])
    records: list[dict[str, Any]] = []
    for metric, lift in positive:
        score = min(1.0, float(lift / max(max_lift, 1e-9)))
        records.append({
            "incident_id": incident_id,
            "timestamp": timestamp,
            "service": service,
            "instance": service,
            "node": service,
            "evidence_type": "IO",
            "root_type_hint": "storage I/O",
            "metric": metric,
            "value": score,
            "evidence_score": score,
            "source": "real_pod_io_collection",
            "probe_id": "p2c1_pod_dd_write",
            "sampling_rate": 1.0,
        })
    return records


def build_io_incident(repeat_index: int, start_ts: float, end_ts: float) -> dict[str, Any]:
    suffix = f"{int(repeat_index):02d}"
    return {
        "incident_id": f"ob-io-rediscart-repeat-{suffix}",
        "root_service": "redis-cart",
        "root_metric": "io.write_bytes",
        "root_type": "storage I/O",
        "symptom_service": "frontend",
        "start_ts": float(start_ts),
        "end_ts": float(end_ts),
        "injected_path": [
            "redis-cart.io.write_bytes",
            "cartservice.request.p99_latency_ms",
            "frontend.request.p99_latency_ms",
        ],
    }


def _quality_report(metrics: list[dict], evidence: list[dict], config: dict[str, Any], stress: dict[str, Any], cleanup: dict[str, Any], temp_file_cleaned: bool) -> dict[str, Any]:
    services_seen = sorted({str(row["service"]) for row in metrics})
    metrics_seen = sorted({str(row["metric"]) for row in metrics})
    write_bytes_lift = _lift(metrics, "redis-cart", "io.write_bytes")
    write_ops_lift = _lift(metrics, "redis-cart", "io.write_ops")
    io_time_lift = _lift(metrics, "redis-cart", "io.io_time_ms")
    frontend_latency_lift = _lift(metrics, "frontend", "request.p99_latency_ms")
    report = {
        "metrics_count": len(metrics),
        "services_seen": services_seen,
        "metrics_seen": metrics_seen,
        "baseline_windows": int(config["experiment"]["baseline_windows"]),
        "faulty_windows": int(config["experiment"]["faulty_windows"]),
        "recovery_windows": int(config["experiment"]["recovery_windows"]),
        "io_stress_started": bool(stress.get("started")),
        "io_stress_completed": bool(stress.get("completed")),
        "io_stress_cleaned": bool(cleanup.get("cleaned")),
        "rediscart_io_metric_present": any(row.get("service") == "redis-cart" and str(row.get("metric", "")).startswith("io.") for row in metrics),
        "rediscart_write_bytes_metric_present": any(row.get("service") == "redis-cart" and row.get("metric") == "io.write_bytes" for row in metrics),
        "frontend_latency_metric_present": any(row.get("service") == "frontend" and row.get("metric") == "request.p99_latency_ms" for row in metrics),
        "io_evidence_present": bool(evidence),
        "fault_injection_succeeded": bool(stress.get("started")),
        "restore_succeeded": bool(cleanup.get("cleaned")) and bool(temp_file_cleaned),
        "write_bytes_lift": float(write_bytes_lift),
        "write_ops_lift": float(write_ops_lift),
        "io_time_lift": float(io_time_lift),
        "frontend_latency_lift": float(frontend_latency_lift),
        "temp_file_cleaned": bool(temp_file_cleaned),
    }
    return report


def _quality_ok(quality: dict[str, Any], requirements: dict[str, Any]) -> bool:
    mapping = {
        "require_io_stress_started": "io_stress_started",
        "require_io_stress_cleaned": "io_stress_cleaned",
        "require_rediscart_io_metric_present": "rediscart_io_metric_present",
        "require_rediscart_write_bytes_metric_present": "rediscart_write_bytes_metric_present",
        "require_frontend_latency_metric_present": "frontend_latency_metric_present",
        "require_service_graph_present": "service_graph_present",
    }
    for req, field in mapping.items():
        if requirements.get(req) is True and quality.get(field) is not True:
            return False
    return True


def _temp_file_absent(namespace: str, deployment: str, container: str, temp_files: list[str]) -> bool:
    tests = " && ".join(f"[ ! -e {path} ]" for path in temp_files)
    code, _stdout, _stderr = run_cmd(["kubectl", "exec", "-n", namespace, f"deploy/{deployment}", "-c", container, "--", "sh", "-c", tests], timeout=30)
    return code == 0


def run_real_ob_rca(input_dir: str | Path, output_dir: str | Path, top_k: int = 5) -> dict[str, Any]:
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
        "write_bytes_lift_debug": float(quality.get("write_bytes_lift", 0.0)),
        "write_ops_lift_debug": float(quality.get("write_ops_lift", 0.0)),
        "io_time_lift_debug": float(quality.get("io_time_lift", 0.0)),
        "frontend_latency_lift_debug": float(quality.get("frontend_latency_lift", 0.0)),
        "rediscart_io_metric_present": bool(quality.get("rediscart_io_metric_present")),
        "rediscart_write_bytes_metric_present": bool(quality.get("rediscart_write_bytes_metric_present")),
    }
    _write_json(output_path / "real_p1_rca_summary.json", summary)
    _write_json(output_path / "real_p1_rca_metadata.json", {"input_dir": str(input_path), "output_dir": str(output_path), "top_k": int(top_k), "real_collection": True, "note": "P2C-1 single real I/O repeat case; not multi-fault accuracy."})
    return {"summary": summary, "evaluation": evaluation, "results": results}


def run_single_io_repeat(repeat_index: int, config: dict[str, Any]) -> dict[str, Any]:
    repeat_cfg = config["repeat_experiment"]
    base_dir = Path(str(repeat_cfg["base_output_dir"]))
    repeat_dir = base_dir / f"repeat_{int(repeat_index):02d}"
    raw_dir = repeat_dir / "raw"
    rca_dir = repeat_dir / "p1rca"
    raw_dir.mkdir(parents=True, exist_ok=True)
    rca_dir.mkdir(parents=True, exist_ok=True)
    runtime_config = json.loads(json.dumps(config))
    suffix = f"{int(repeat_index):02d}"
    incident_id = f"ob-io-rediscart-repeat-{suffix}"
    namespace = str(config["kubernetes"]["namespace"])
    frontend_url = str(config["experiment"]["frontend_url"])
    fault = config["fault_injection"]
    ready = ensure_online_boutique_ready(namespace, frontend_url)
    port_forward_proc = _start_frontend_port_forward_if_needed(namespace, frontend_url, repeat_dir)
    stress: dict[str, Any] = {"started": False, "completed": False}
    cleanup: dict[str, Any] = {"cleaned": False}
    proc: subprocess.Popen | None = None
    temp_file = ""
    try:
        pod = get_target_pod(namespace, str(config["target"]["service"]))
        if not pod.get("ready"):
            raise RuntimeError(f"target pod not ready: {pod}")
        tools = check_pod_io_tools(namespace, str(fault["target_deployment"]))
        if not tools.get("sh_available") or not tools.get("dd_available"):
            raise RuntimeError(f"redis-cart missing required IO tools: {tools}")
        _write_json(raw_dir / "redis_cart_tool_check.json", tools)
        candidates = [str(item) for item in fault.get("temp_file_candidates", ["/data/proberca_io_stress.bin", "/tmp/proberca_io_stress.bin"])]
        temp_file = choose_writable_temp_file(namespace, str(fault["target_deployment"]), str(fault["target_container"]), candidates)
        runtime_config["_runtime"] = {"pod_name": pod["name"], "pod_ip": pod.get("pod_ip", ""), "incident_id": incident_id, "temp_file": temp_file}
        write_yaml(repeat_dir / "repeat_config.yaml", runtime_config)
        _write_text(raw_dir / "redis_cart_df_before.txt", _df(namespace, str(fault["target_deployment"]), str(fault.get("target_container", ""))))

        metrics: list[dict[str, Any]] = []
        window_states: list[dict[str, Any]] = []
        prev_snapshot = collect_fs_snapshot(namespace)
        first_faulty_ts = 0.0
        last_faulty_ts = 0.0
        for idx in range(int(config["experiment"]["baseline_windows"])):
            rows, state = collect_io_window_metrics(runtime_config, "baseline", idx + 1, prev_snapshot)
            metrics.extend(rows)
            window_states.append(state)
            prev_snapshot = state["fs_snapshot"]

        stress = start_io_stress(namespace, str(fault["target_deployment"]), str(fault["target_container"]), temp_file, str(fault["block_size"]), int(fault["block_count"]), int(fault["duration_sec"]), raw_dir / "io_stress_stdout_stderr.log")
        proc = stress.pop("process")
        for idx in range(int(config["experiment"]["faulty_windows"])):
            rows, state = collect_io_window_metrics(runtime_config, "faulty", idx + 1, prev_snapshot)
            metrics.extend(rows)
            window_states.append(state)
            prev_snapshot = state["fs_snapshot"]
            if idx == 0:
                first_faulty_ts = float(state["timestamp"])
            last_faulty_ts = float(state["timestamp"])
        try:
            proc.wait(timeout=int(fault["duration_sec"]) + 30)
        except subprocess.TimeoutExpired:
            proc.terminate()
            proc.wait(timeout=10)
        stress["returncode"] = proc.returncode
        stress["completed"] = proc.returncode == 0
    finally:
        if proc is not None and proc.poll() is None:
            proc.terminate()
        if temp_file:
            cleanup = cleanup_io_stress(namespace, str(fault["target_deployment"]), str(fault["target_container"]), temp_file)
        _write_json(raw_dir / "io_fault_log.json", {key: value for key, value in stress.items() if key != "process"})
        _write_json(raw_dir / "io_restore_log.json", cleanup)

    temp_files = [temp_file, "/tmp/proberca_io_stress.bin", "/data/proberca_io_stress.bin"]
    temp_file_cleaned = _temp_file_absent(namespace, str(fault["target_deployment"]), str(fault["target_container"]), sorted({path for path in temp_files if path}))
    _write_text(raw_dir / "redis_cart_df_after.txt", _df(namespace, str(fault["target_deployment"]), str(fault.get("target_container", ""))))
    for deployment in ["redis-cart", "cartservice", "frontend"]:
        if not _deployment_ready_json(namespace, deployment):
            raise RuntimeError(f"deployment not ready after IO stress cleanup: {deployment}")

    for idx in range(int(config["experiment"]["recovery_windows"])):
        rows, state = collect_io_window_metrics(runtime_config, "recovery", idx + 1, prev_snapshot)
        metrics.extend(rows)
        window_states.append(state)
        prev_snapshot = state["fs_snapshot"]

    incident = build_io_incident(repeat_index, first_faulty_ts, last_faulty_ts)
    evidence = build_io_evidence(metrics, incident)
    write_jsonl(raw_dir / "metrics.jsonl", metrics)
    write_jsonl(raw_dir / "incidents.jsonl", [incident])
    write_jsonl(raw_dir / "evidence.jsonl", evidence)
    graph_result = write_online_boutique_service_graph(raw_dir / "service_graph.jsonl")
    quality = _quality_report(metrics, evidence, config, stress, cleanup, temp_file_cleaned)
    quality["service_graph_present"] = Path(raw_dir / "service_graph.jsonl").exists()
    _write_json(raw_dir / "data_quality_report.json", quality)
    _write_json(raw_dir / "metadata.json", {
        "phase": "P2C-1",
        "repeat_index": int(repeat_index),
        "experiment_group_id": str(repeat_cfg["experiment_group_id"]),
        "raw_output_dir": str(raw_dir),
        "target_service": str(config["target"]["service"]),
        "target_metric": str(config["target"]["metric"]),
        "target_fault_type": str(config["target"]["fault_type"]),
        "pod_name": str(runtime_config["_runtime"]["pod_name"]),
        "pod_ip": str(runtime_config["_runtime"].get("pod_ip", "")),
        "temp_file": temp_file,
        "ready_check": ready,
        "service_graph": graph_result,
        "window_states_count": len(window_states),
    })

    requirements = config.get("quality_requirements", {})
    quality_ok = _quality_ok(quality, requirements)
    row: dict[str, Any] = {
        "repeat_index": int(repeat_index),
        "raw_output_dir": str(raw_dir),
        "rca_output_dir": str(rca_dir),
        "io_stress_started": bool(stress.get("started")),
        "io_stress_completed": bool(stress.get("completed")),
        "io_stress_cleaned": bool(cleanup.get("cleaned")) and bool(temp_file_cleaned),
        "rediscart_io_metric_present": bool(quality.get("rediscart_io_metric_present")),
        "rediscart_write_bytes_metric_present": bool(quality.get("rediscart_write_bytes_metric_present")),
        "quality_ok": bool(quality_ok),
        "write_bytes_lift": float(quality.get("write_bytes_lift", 0.0)),
        "write_ops_lift": float(quality.get("write_ops_lift", 0.0)),
        "io_time_lift": float(quality.get("io_time_lift", 0.0)),
        "frontend_latency_lift": float(quality.get("frontend_latency_lift", 0.0)),
    }
    if quality_ok:
        rca = run_real_ob_rca(raw_dir, rca_dir, top_k=5)
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


def _deployment_ready_json(namespace: str, deployment: str) -> bool:
    code, stdout, _stderr = run_cmd(["kubectl", "get", "deploy", "-n", namespace, deployment, "-o", "json"], timeout=30)
    if code != 0:
        return False
    return _deployment_ready(json.loads(stdout))


def _mean_metric(rows: list[dict[str, Any]], key: str) -> float:
    values = [float(row.get(key, 0.0)) for row in rows]
    return float(np.mean(np.asarray(values, dtype=float))) if values else 0.0


def _min_metric(rows: list[dict[str, Any]], key: str) -> float:
    values = [float(row.get(key, 0.0)) for row in rows]
    return float(np.min(np.asarray(values, dtype=float))) if values else 0.0


def aggregate_io_repeat_summary(config: dict[str, Any], per_repeat: list[dict[str, Any]]) -> dict[str, Any]:
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
        "write_bytes_lift_mean": _mean_metric(quality_rows, "write_bytes_lift"),
        "write_ops_lift_mean": _mean_metric(quality_rows, "write_ops_lift"),
        "io_time_lift_mean": _mean_metric(quality_rows, "io_time_lift"),
        "frontend_latency_lift_mean": _mean_metric(quality_rows, "frontend_latency_lift"),
        "per_repeat": per_repeat,
    }


def run_p2c1_io_repeated_experiment(config_path: str | Path) -> dict[str, Any]:
    config = load_io_repeat_config(config_path)
    repeat_cfg = config["repeat_experiment"]
    base_dir = Path(str(repeat_cfg["base_output_dir"]))
    base_dir.mkdir(parents=True, exist_ok=True)
    repeats = int(repeat_cfg.get("repeats", 5))
    sleep_between = int(repeat_cfg.get("sleep_between_repeats_sec", 0) or 0)
    per_repeat: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for index in range(1, repeats + 1):
        try:
            row = run_single_io_repeat(index, config)
            per_repeat.append(row)
            if not row.get("quality_ok") or not row.get("rca_ok"):
                failures.append({"repeat_index": index, "row": row})
        except Exception as exc:
            failure = {"repeat_index": index, "error": str(exc)}
            failures.append(failure)
            per_repeat.append({"repeat_index": index, "quality_ok": False, "rca_ok": False, "error": str(exc), "service_hit_at_1": 0.0, "metric_hit_at_1": 0.0, "metric_hit_at_3": 0.0, "metric_mrr": 0.0, "root_type_accuracy": 0.0, "path_fidelity": 0.0, "write_bytes_lift": 0.0, "write_ops_lift": 0.0, "io_time_lift": 0.0, "frontend_latency_lift": 0.0})
        if index < repeats and sleep_between > 0:
            time.sleep(sleep_between)
    summary = aggregate_io_repeat_summary(config, per_repeat)
    metadata = {
        "config_path": str(config_path),
        "base_output_dir": str(base_dir),
        "target": config.get("target", {}),
        "fault_injection": config.get("fault_injection", {}),
        "experiment": config.get("experiment", {}),
        "note": "P2C-1 repeated real I/O fault experiment; not multi-fault accuracy.",
    }
    _write_json(base_dir / "p2c1_io_repeat_summary.json", summary)
    _write_json(base_dir / "p2c1_io_repeat_metadata.json", metadata)
    _write_json(base_dir / "p2c1_io_repeat_failures.json", {"failures": failures})
    return {"summary": summary, "metadata": metadata, "failures": failures, "output_dir": str(base_dir)}
