"""P2D-1R phase-aware repeated real lock contention experiments."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import numpy as np

from proberca.adapters.online_boutique.lock_fault import (
    build_phaseaware_lockstress_python_command,
    curl_frontend,
    ensure_sidecar_image_loaded,
    get_sidecar_logs,
    get_target_pod,
    parse_lockstress_logs,
    patch_cartservice_add_lockstress_sidecar,
    remove_lockstress_sidecar,
)
from proberca.adapters.online_boutique.p2a1_cpu_experiment import load_simple_yaml
from proberca.adapters.online_boutique.p2d1_lock_repeat import (
    LIMITATION,
    _container_names,
    _deployment_ready,
    _frontend_records,
    _frontend_recovery_check,
    _http_ok,
    _kubectl_text,
    _mean_metric,
    _min_metric,
    _run_real_ob_rca,
    _start_frontend_port_forward_if_needed,
    _stop_process,
    _write_json,
    _write_text,
    ensure_online_boutique_ready,
    write_yaml,
)
from proberca.adapters.online_boutique.topology import write_online_boutique_service_graph
from proberca.data.io import write_jsonl

IDLE_LIMITATION = "baseline_lock_metrics_are_real_idle_sidecar_measurements"
SOURCE = "real_phaseaware_sidecar_lockstress"


def load_phaseaware_lock_repeat_config(config_path: str | Path) -> dict[str, Any]:
    return load_simple_yaml(config_path)


def _record_float(row: dict[str, Any], key: str) -> float:
    try:
        return float(row.get(key, 0.0) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def collect_phaseaware_lock_metrics_from_logs(parsed_logs: dict[str, Any], pod_name: str, service: str = "cartservice", incident_id: str = "") -> list[dict]:
    records: list[dict[str, Any]] = []
    for row in parsed_logs.get("records", []):
        phase = str(row.get("phase", ""))
        if phase not in {"baseline", "faulty", "recovery"}:
            continue
        timestamp = _record_float(row, "timestamp")
        lock_active = bool(row.get("lock_active"))
        window_index = int(row.get("window_index", 0) or 0)
        mapping = {
            "lock.futex_wait_ms": _record_float(row, "lock_wait_ms_sum"),
            "lock.wait_ms": _record_float(row, "lock_wait_ms_mean"),
            "lock.wait_p95_ms": _record_float(row, "lock_wait_ms_p95"),
            "lock.contention_count": _record_float(row, "lock_contention_count"),
        }
        for metric, value in mapping.items():
            records.append({
                "incident_id": incident_id,
                "timestamp": timestamp,
                "service": service,
                "instance": pod_name,
                "node": service,
                "metric": metric,
                "value": float(value),
                "phase": phase,
                "lock_active": lock_active,
                "window_index": window_index,
                "source": SOURCE,
            })
    return records


def collect_frontend_window_metrics(config: dict[str, Any], phase: str, window_index: int, incident_id: str) -> list[dict]:
    exp = config["experiment"]
    started = time.time()
    frontend = curl_frontend(str(exp["frontend_url"]), int(exp["requests_per_window"]), int(exp["request_timeout_sec"]))
    elapsed = time.time() - started
    window_size = float(exp["window_size_sec"])
    if elapsed < window_size:
        time.sleep(window_size - elapsed)
    timestamp = time.time()
    rows = _frontend_records(frontend, timestamp, phase, incident_id)
    for row in rows:
        row["source"] = SOURCE
        row["window_index"] = int(window_index)
    return rows


def _phase_values(metrics: list[dict], service: str, metric: str, phase: str) -> list[float]:
    return [float(row["value"]) for row in metrics if row.get("service") == service and row.get("metric") == metric and row.get("phase") == phase]


def _mean(values: list[float]) -> float:
    return float(np.mean(np.asarray(values, dtype=float))) if values else 0.0


def _lift(metrics: list[dict], service: str, metric: str) -> float:
    return _mean(_phase_values(metrics, service, metric, "faulty")) - _mean(_phase_values(metrics, service, metric, "baseline"))


def _sum_phase(metrics: list[dict], service: str, metric: str, phase: str) -> float:
    return float(sum(_phase_values(metrics, service, metric, phase)))


def build_lock_evidence(metrics: list[dict], incident: dict) -> list[dict]:
    service = str(incident["root_service"])
    incident_id = str(incident["incident_id"])
    lift = _lift(metrics, service, "lock.futex_wait_ms")
    contention = _sum_phase(metrics, service, "lock.contention_count", "faulty")
    if lift <= 0 and contention <= 0:
        return []
    timestamp = float(incident["start_ts"])
    rows: list[dict[str, Any]] = []
    if lift > 0:
        rows.append({
            "incident_id": incident_id,
            "timestamp": timestamp,
            "service": service,
            "instance": service,
            "node": service,
            "evidence_type": "Lock",
            "root_type_hint": "lock contention",
            "metric": "lock.futex_wait_ms",
            "value": 1.0,
            "evidence_score": 1.0,
            "source": SOURCE,
            "probe_id": "p2d1r_phaseaware_sidecar_lockstress",
            "sampling_rate": 1.0,
        })
    if contention > 0:
        rows.append({
            "incident_id": incident_id,
            "timestamp": timestamp,
            "service": service,
            "instance": service,
            "node": service,
            "evidence_type": "Lock",
            "root_type_hint": "lock contention",
            "metric": "lock.contention_count",
            "value": min(1.0, contention / max(contention, 1.0)),
            "evidence_score": min(1.0, contention / max(contention, 1.0)),
            "source": SOURCE,
            "probe_id": "p2d1r_phaseaware_sidecar_lockstress",
            "sampling_rate": 1.0,
        })
    return rows


def build_lock_incident(repeat_index: int, start_ts: float, end_ts: float) -> dict[str, Any]:
    suffix = f"{int(repeat_index):02d}"
    return {
        "incident_id": f"ob-lock-cartservice-phaseaware-repeat-{suffix}",
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


def _quality_report(metrics: list[dict], evidence: list[dict], config: dict[str, Any], patch: dict[str, Any], restore: dict[str, Any], parsed: dict[str, Any], frontend_recovery: dict[str, Any]) -> dict[str, Any]:
    services_seen = sorted({str(row["service"]) for row in metrics})
    metrics_seen = sorted({str(row["metric"]) for row in metrics})
    service = str(config["target"]["service"])
    report = {
        "metrics_count": len(metrics),
        "services_seen": services_seen,
        "metrics_seen": metrics_seen,
        "baseline_windows": int(config["experiment"]["baseline_windows"]),
        "faulty_windows": int(config["experiment"]["faulty_windows"]),
        "recovery_windows": int(config["experiment"]["recovery_windows"]),
        "sidecar_injected": bool(patch.get("sidecar_injected")),
        "sidecar_removed": bool(restore.get("sidecar_removed")),
        "phaseaware_metrics_available": bool(parsed.get("phaseaware_metrics_available")),
        "baseline_lock_metric_present": any(row.get("service") == service and row.get("metric") == "lock.futex_wait_ms" and row.get("phase") == "baseline" for row in metrics),
        "faulty_lock_metric_present": any(row.get("service") == service and row.get("metric") == "lock.futex_wait_ms" and row.get("phase") == "faulty" for row in metrics),
        "recovery_lock_metric_present": any(row.get("service") == service and row.get("metric") == "lock.futex_wait_ms" and row.get("phase") == "recovery" for row in metrics),
        "lock_metrics_available": bool(parsed.get("lock_metrics_available")),
        "faulty_lock_contention_count_total": int(parsed.get("faulty_lock_contention_count_total", 0)),
        "faulty_lock_wait_ms_sum_total": float(parsed.get("faulty_lock_wait_ms_sum_total", 0.0)),
        "baseline_lock_wait_ms_sum_total": float(parsed.get("baseline_lock_wait_ms_sum_total", 0.0)),
        "lock_wait_lift": float(_lift(metrics, service, "lock.futex_wait_ms")),
        "cartservice_lock_metric_present": any(row.get("service") == service and str(row.get("metric", "")).startswith("lock.") for row in metrics),
        "cartservice_futex_wait_metric_present": any(row.get("service") == service and row.get("metric") == "lock.futex_wait_ms" for row in metrics),
        "frontend_latency_metric_present": any(row.get("service") == "frontend" and row.get("metric") == "request.p99_latency_ms" for row in metrics),
        "frontend_latency_lift": float(_lift(metrics, "frontend", "request.p99_latency_ms")),
        "lock_evidence_present": bool(evidence),
        "fault_injection_succeeded": bool(patch.get("sidecar_injected")),
        "restore_succeeded": bool(restore.get("sidecar_removed")),
        "frontend_recovery_p99_ok": bool(frontend_recovery.get("p99_ok")),
        "p95_parse_warning": bool(parsed.get("p95_parse_warning")),
        "limitation": LIMITATION,
        "baseline_lock_metrics_are_real_idle_sidecar_measurements": True,
    }
    return report


def _quality_ok(quality: dict[str, Any], requirements: dict[str, Any]) -> bool:
    mapping = {
        "require_sidecar_injected": "sidecar_injected",
        "require_sidecar_removed": "sidecar_removed",
        "require_lock_metrics_available": "lock_metrics_available",
        "require_baseline_lock_metric_present": "baseline_lock_metric_present",
        "require_faulty_lock_metric_present": "faulty_lock_metric_present",
        "require_frontend_latency_metric_present": "frontend_latency_metric_present",
        "require_service_graph_present": "service_graph_present",
    }
    for req, field in mapping.items():
        if requirements.get(req) is True and quality.get(field) is not True:
            return False
    if requirements.get("require_lock_contention_count_positive") is True and float(quality.get("faulty_lock_contention_count_total", 0.0)) <= 0.0:
        return False
    if requirements.get("require_lock_wait_sum_positive") is True and float(quality.get("faulty_lock_wait_ms_sum_total", 0.0)) <= 0.0:
        return False
    return True


def run_single_phaseaware_lock_repeat(repeat_index: int, config: dict[str, Any]) -> dict[str, Any]:
    repeat_cfg = config["repeat_experiment"]
    base_dir = Path(str(repeat_cfg["base_output_dir"]))
    repeat_dir = base_dir / f"repeat_{int(repeat_index):02d}"
    raw_dir = repeat_dir / "raw"
    rca_dir = repeat_dir / "p1rca"
    raw_dir.mkdir(parents=True, exist_ok=True)
    rca_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"{int(repeat_index):02d}"
    incident_id = f"ob-lock-cartservice-phaseaware-repeat-{suffix}"
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
    parsed: dict[str, Any] = {"lock_metrics_available": False, "phaseaware_metrics_available": False, "records": []}
    pod_before: dict[str, Any] = {"name": ""}
    pod_during: dict[str, Any] = {"name": ""}
    frontend_metrics: list[dict[str, Any]] = []
    try:
        ready = ensure_online_boutique_ready(namespace, frontend_url, sidecar_name)
        pod_before = get_target_pod(namespace, str(config["target"]["service"]))
        repeat_config = json.loads(json.dumps(config))
        repeat_config["_runtime"] = {"incident_id": incident_id, "pod_name_before": pod_before.get("name", "")}
        write_yaml(repeat_dir / "repeat_config.yaml", repeat_config)
        _write_text(raw_dir / "kubectl_get_pods_before.txt", _kubectl_text(["get", "pods", "-n", namespace]))
        _write_text(raw_dir / "kubectl_get_deploy_before.txt", _kubectl_text(["get", "deploy", "-n", namespace]))
        ensure_sidecar_image_loaded(sidecar_image, cluster_name)
        command = build_phaseaware_lockstress_python_command(
            int(exp["window_size_sec"]),
            int(exp["baseline_windows"]),
            int(exp["faulty_windows"]),
            int(exp["recovery_windows"]),
            int(fault["workers"]),
            int(fault["lock_hold_ms"]),
        )
        patch_result = patch_cartservice_add_lockstress_sidecar(namespace, deployment, sidecar_name, sidecar_image, command)
        time.sleep(5)
        pod_during = get_target_pod(namespace, str(config["target"]["service"]))
        _write_text(raw_dir / "kubectl_get_pods_during.txt", _kubectl_text(["get", "pods", "-n", namespace]))
        _write_text(raw_dir / "kubectl_get_deploy_during.txt", _kubectl_text(["get", "deploy", "-n", namespace]))
        _write_json(raw_dir / "lock_fault_log.json", {"patch": patch_result, "pod_name_before": pod_before.get("name", ""), "pod_name_during": pod_during.get("name", ""), "sidecar_command": command, "limitation": LIMITATION, "baseline_lock_metrics_are_real_idle_sidecar_measurements": True})
        window_index = 0
        for phase, count in [("baseline", int(exp["baseline_windows"])), ("faulty", int(exp["faulty_windows"])), ("recovery", int(exp["recovery_windows"]))]:
            for _ in range(count):
                window_index += 1
                frontend_metrics.extend(collect_frontend_window_metrics(config, phase, window_index, incident_id))
        time.sleep(2)
        logs = get_sidecar_logs(namespace, deployment, sidecar_name, str(pod_during.get("name", "")))
        _write_text(raw_dir / "lockstress_logs.txt", logs)
        parsed = parse_lockstress_logs(logs)
        _write_json(raw_dir / "lock_metrics_during.json", {k: v for k, v in parsed.items() if k != "records"})
    finally:
        try:
            restore_result = remove_lockstress_sidecar(namespace, deployment, sidecar_name)
        finally:
            _write_json(raw_dir / "lock_restore_log.json", restore_result)
            _write_text(raw_dir / "kubectl_get_pods_after.txt", _kubectl_text(["get", "pods", "-n", namespace]))
            _write_text(raw_dir / "kubectl_get_deploy_after.txt", _kubectl_text(["get", "deploy", "-n", namespace]))

    if not _deployment_ready(namespace, "cartservice") or not _deployment_ready(namespace, "frontend"):
        raise RuntimeError("cartservice/frontend not ready after phase-aware lock restore")
    cooldown = int(exp.get("post_restore_cooldown_sec", 0) or 0)
    if cooldown > 0:
        time.sleep(cooldown)
    if sidecar_name in _container_names(namespace, deployment):
        raise RuntimeError("lockstress sidecar still present after phase-aware restore")
    frontend_recovery = _frontend_recovery_check(frontend_url, int(exp["requests_per_window"]), int(exp["request_timeout_sec"]), float(exp.get("require_frontend_recovery_p99_below_ms", 0) or 0), cooldown)
    _write_json(raw_dir / "frontend_recovery_check.json", frontend_recovery)

    lock_metrics = collect_phaseaware_lock_metrics_from_logs(parsed, str(pod_during.get("name", "")), service=str(config["target"]["service"]), incident_id=incident_id)
    metrics = sorted(frontend_metrics + lock_metrics, key=lambda row: float(row.get("timestamp", 0.0)))
    faulty_ts = [float(row["timestamp"]) for row in lock_metrics if row.get("phase") == "faulty"]
    if not faulty_ts:
        raise RuntimeError("phase-aware sidecar produced no faulty lock metrics")
    incident = build_lock_incident(repeat_index, min(faulty_ts), max(faulty_ts))
    evidence = build_lock_evidence(metrics, incident)
    write_jsonl(raw_dir / "metrics.jsonl", metrics)
    write_jsonl(raw_dir / "incidents.jsonl", [incident])
    write_jsonl(raw_dir / "evidence.jsonl", evidence)
    graph_result = write_online_boutique_service_graph(raw_dir / "service_graph.jsonl")
    quality = _quality_report(metrics, evidence, config, patch_result, restore_result, parsed, frontend_recovery)
    quality["service_graph_present"] = Path(raw_dir / "service_graph.jsonl").exists()
    _write_json(raw_dir / "data_quality_report.json", quality)
    _write_json(raw_dir / "metadata.json", {
        "phase": "P2D-1R",
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
        "limitation": LIMITATION,
        "baseline_lock_metrics_are_real_idle_sidecar_measurements": True,
    })
    quality_ok = _quality_ok(quality, config.get("quality_requirements", {}))
    row: dict[str, Any] = {
        "repeat_index": int(repeat_index),
        "raw_output_dir": str(raw_dir),
        "rca_output_dir": str(rca_dir),
        "sidecar_injected": bool(patch_result.get("sidecar_injected")),
        "sidecar_removed": bool(restore_result.get("sidecar_removed")),
        "phaseaware_metrics_available": bool(quality.get("phaseaware_metrics_available")),
        "baseline_lock_metric_present": bool(quality.get("baseline_lock_metric_present")),
        "faulty_lock_metric_present": bool(quality.get("faulty_lock_metric_present")),
        "cartservice_lock_metric_present": bool(quality.get("cartservice_lock_metric_present")),
        "cartservice_futex_wait_metric_present": bool(quality.get("cartservice_futex_wait_metric_present")),
        "quality_ok": bool(quality_ok),
        "faulty_lock_wait_ms_sum_total": float(quality.get("faulty_lock_wait_ms_sum_total", 0.0)),
        "baseline_lock_wait_ms_sum_total": float(quality.get("baseline_lock_wait_ms_sum_total", 0.0)),
        "lock_wait_lift": float(quality.get("lock_wait_lift", 0.0)),
        "faulty_lock_contention_count_total": int(quality.get("faulty_lock_contention_count_total", 0)),
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


def aggregate_phaseaware_lock_repeat_summary(config: dict[str, Any], per_repeat: list[dict[str, Any]]) -> dict[str, Any]:
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
        "faulty_lock_wait_ms_sum_mean": _mean_metric(quality_rows, "faulty_lock_wait_ms_sum_total"),
        "baseline_lock_wait_ms_sum_mean": _mean_metric(quality_rows, "baseline_lock_wait_ms_sum_total"),
        "lock_wait_lift_mean": _mean_metric(quality_rows, "lock_wait_lift"),
        "faulty_lock_contention_count_mean": _mean_metric(quality_rows, "faulty_lock_contention_count_total"),
        "frontend_latency_lift_mean": _mean_metric(quality_rows, "frontend_latency_lift"),
        "limitation": LIMITATION,
        "baseline_lock_metrics_are_real_idle_sidecar_measurements": True,
        "per_repeat": per_repeat,
    }


def run_p2d1r_lock_repeated_experiment(config_path: str | Path) -> dict[str, Any]:
    config = load_phaseaware_lock_repeat_config(config_path)
    repeat_cfg = config["repeat_experiment"]
    base_dir = Path(str(repeat_cfg["base_output_dir"]))
    base_dir.mkdir(parents=True, exist_ok=True)
    repeats = int(repeat_cfg.get("repeats", 5))
    sleep_between = int(repeat_cfg.get("sleep_between_repeats_sec", 0) or 0)
    per_repeat: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for index in range(1, repeats + 1):
        try:
            row = run_single_phaseaware_lock_repeat(index, config)
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
            failures.append({"repeat_index": index, "error": str(exc)})
            per_repeat.append({"repeat_index": index, "quality_ok": False, "rca_ok": False, "error": str(exc), "service_hit_at_1": 0.0, "metric_hit_at_1": 0.0, "metric_hit_at_3": 0.0, "metric_mrr": 0.0, "root_type_accuracy": 0.0, "path_fidelity": 0.0, "faulty_lock_wait_ms_sum_total": 0.0, "baseline_lock_wait_ms_sum_total": 0.0, "lock_wait_lift": 0.0, "faulty_lock_contention_count_total": 0.0, "frontend_latency_lift": 0.0, "limitation": LIMITATION})
        if index < repeats and sleep_between > 0:
            time.sleep(sleep_between)
    summary = aggregate_phaseaware_lock_repeat_summary(config, per_repeat)
    metadata = {
        "config_path": str(config_path),
        "base_output_dir": str(base_dir),
        "target": config.get("target", {}),
        "fault_injection": config.get("fault_injection", {}),
        "experiment": config.get("experiment", {}),
        "limitation": LIMITATION,
        "baseline_lock_metrics_are_real_idle_sidecar_measurements": True,
        "note": "P2D-1R phase-aware repeated real sidecar lock contention experiment; not multi-fault accuracy and not original cartservice business-code bug.",
    }
    _write_json(base_dir / "p2d1r_lock_repeat_summary.json", summary)
    _write_json(base_dir / "p2d1r_lock_repeat_metadata.json", metadata)
    _write_json(base_dir / "p2d1r_lock_repeat_failures.json", {"failures": failures})
    return {"summary": summary, "metadata": metadata, "failures": failures, "output_dir": str(base_dir)}
