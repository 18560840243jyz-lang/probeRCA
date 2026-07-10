"""P2B-1 repeated real Online Boutique network fault injection experiments."""

from __future__ import annotations

import json
import math
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

import numpy as np

from proberca.adapters.online_boutique.network_fault import (
    apply_netem_fault,
    clear_netem_fault,
    collect_proc_net_snmp,
    collect_ss_rtt,
    curl_frontend,
    get_pod_netns_pid,
    get_pod_sandbox_id,
    get_target_pod,
    get_tc_qdisc,
    run_cmd,
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


def load_network_repeat_config(config_path: str | Path) -> dict[str, Any]:
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
    required = {"frontend", "checkoutservice", "shippingservice"}
    present = {item.get("metadata", {}).get("name", "") for item in deployments}
    missing = sorted(required - present)
    if missing or not_ready:
        raise RuntimeError(f"Online Boutique not ready; missing={missing}, not_ready={not_ready}")
    smoke = curl_frontend(frontend_url, 1, 3)
    if not smoke.get("http_ok"):
        # P2A-0 usually leaves a local port-forward running. Start a short-lived one if needed.
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
        if not smoke.get("http_ok"):
            raise RuntimeError(f"frontend smoke failed: {smoke}")
    return {"deployments_count": len(deployments), "frontend_smoke": smoke}




def _start_frontend_port_forward_if_needed(namespace: str, frontend_url: str, output_dir: Path) -> subprocess.Popen | None:
    if curl_frontend(frontend_url, 1, 3).get("http_ok"):
        return None
    log_path = output_dir / "frontend_port_forward.log"
    handle = log_path.open("a", encoding="utf-8")
    proc = subprocess.Popen(
        ["kubectl", "port-forward", "-n", namespace, "svc/frontend", "8080:80"],
        stdout=handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    # Keep the file handle alive via the child process; close our copy after launch.
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
            "source": "real_tc_netem_collection",
        })
    return records


def _network_delta_records(prev: dict[str, Any], curr: dict[str, Any], target_pod: dict[str, Any], timestamp: float, phase: str, incident_id: str) -> list[dict[str, Any]]:
    prev_tcp = (prev or {}).get("tcp", {}) if prev else {}
    curr_tcp = (curr or {}).get("tcp", {})
    mapping = {
        "net.retrans": "RetransSegs",
        "net.out_segs": "OutSegs",
        "net.in_segs": "InSegs",
    }
    records: list[dict[str, Any]] = []
    for metric, key in mapping.items():
        if key not in curr_tcp or key not in prev_tcp:
            continue
        delta = int(curr_tcp[key]) - int(prev_tcp[key])
        if delta < 0:
            continue
        records.append({
            "incident_id": incident_id,
            "timestamp": float(timestamp),
            "service": str(target_pod["service"]),
            "instance": str(target_pod["pod_name"]),
            "node": str(target_pod["service"]),
            "metric": metric,
            "value": float(delta),
            "phase": phase,
            "source": "real_tc_netem_collection",
        })
    return records


def collect_network_window_metrics(config: dict[str, Any], phase: str, window_index: int, prev_snmp: dict[str, Any] | None = None) -> tuple[list[dict], dict]:
    namespace = str(config["kubernetes"]["namespace"])
    node = str(config["kind_node_container"])
    exp = config["experiment"]
    target = config["target"]
    runtime = config["_runtime"]
    pid = int(runtime["netns_pid"])
    target_pod = {
        "service": str(target["service"]),
        "pod_name": str(runtime["pod_name"]),
    }
    if prev_snmp is None:
        prev_snmp = collect_proc_net_snmp(node, pid)
    window_size = float(exp["window_size_sec"])
    started = time.time()
    frontend = curl_frontend(str(exp["frontend_url"]), int(exp["requests_per_window"]), int(exp["request_timeout_sec"]))
    elapsed = time.time() - started
    if elapsed < window_size:
        time.sleep(window_size - elapsed)
    curr_snmp = collect_proc_net_snmp(node, pid)
    rtt = collect_ss_rtt(node, pid) if config.get("metrics", {}).get("collect_ss_rtt", True) else {"available": False}
    timestamp = time.time()
    incident_id = str(runtime["incident_id"])
    records = _network_delta_records(prev_snmp, curr_snmp, target_pod, timestamp, phase, incident_id)
    if rtt.get("available") and rtt.get("rtt_ms") is not None:
        records.append({
            "incident_id": incident_id,
            "timestamp": float(timestamp),
            "service": str(target["service"]),
            "instance": str(runtime["pod_name"]),
            "node": str(target["service"]),
            "metric": "net.rtt_ms",
            "value": float(rtt["rtt_ms"]),
            "phase": phase,
            "source": "real_tc_netem_collection",
        })
    records.extend(_frontend_records(frontend, timestamp, phase, incident_id))
    state = {"snmp": curr_snmp, "frontend": frontend, "rtt": rtt, "timestamp": timestamp, "window_index": int(window_index), "phase": phase}
    return records, state


def _phase_values(metrics: list[dict], service: str, metric: str, phase: str) -> list[float]:
    return [float(row["value"]) for row in metrics if row.get("service") == service and row.get("metric") == metric and row.get("phase") == phase]


def _mean(values: list[float]) -> float:
    return float(np.mean(np.asarray(values, dtype=float))) if values else 0.0


def _lift(metrics: list[dict], service: str, metric: str) -> float:
    return _mean(_phase_values(metrics, service, metric, "faulty")) - _mean(_phase_values(metrics, service, metric, "baseline"))


def build_network_evidence(metrics: list[dict], incident: dict) -> list[dict]:
    service = str(incident["root_service"])
    incident_id = str(incident["incident_id"])
    candidates = [
        ("net.retrans", _lift(metrics, service, "net.retrans")),
        ("net.rtt_ms", _lift(metrics, service, "net.rtt_ms")),
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
            "evidence_type": "Net",
            "metric": metric,
            "value": score,
            "evidence_score": score,
            "source": "real_tc_netem_collection",
            "probe_id": "p2b1_tc_netem",
            "sampling_rate": 1.0,
        })
    return records


def build_network_incident(repeat_index: int, start_ts: float, end_ts: float) -> dict[str, Any]:
    suffix = f"{int(repeat_index):02d}"
    return {
        "incident_id": f"ob-network-shippingservice-repeat-{suffix}",
        "root_service": "shippingservice",
        "root_metric": "net.retrans",
        "root_type": "network instability",
        "symptom_service": "frontend",
        "start_ts": float(start_ts),
        "end_ts": float(end_ts),
        "injected_path": [
            "shippingservice.net.retrans",
            "checkoutservice.request.p99_latency_ms",
            "frontend.request.p99_latency_ms",
        ],
    }


def _quality_report(metrics: list[dict], evidence: list[dict], config: dict[str, Any], netem_applied: bool, netem_restored: bool) -> dict[str, Any]:
    services_seen = sorted({str(row["service"]) for row in metrics})
    metrics_seen = sorted({str(row["metric"]) for row in metrics})
    retrans_lift = _lift(metrics, "shippingservice", "net.retrans")
    rtt_lift = _lift(metrics, "shippingservice", "net.rtt_ms")
    frontend_latency_lift = _lift(metrics, "frontend", "request.p99_latency_ms")
    report = {
        "metrics_count": len(metrics),
        "services_seen": services_seen,
        "metrics_seen": metrics_seen,
        "baseline_windows": int(config["experiment"]["baseline_windows"]),
        "faulty_windows": int(config["experiment"]["faulty_windows"]),
        "recovery_windows": int(config["experiment"]["recovery_windows"]),
        "netem_applied": bool(netem_applied),
        "netem_restored": bool(netem_restored),
        "shippingservice_network_metric_present": any(row.get("service") == "shippingservice" and str(row.get("metric", "")).startswith("net.") for row in metrics),
        "shippingservice_retrans_metric_present": any(row.get("service") == "shippingservice" and row.get("metric") == "net.retrans" for row in metrics),
        "frontend_latency_metric_present": any(row.get("service") == "frontend" and row.get("metric") == "request.p99_latency_ms" for row in metrics),
        "network_evidence_present": bool(evidence),
        "fault_injection_succeeded": bool(netem_applied),
        "restore_succeeded": bool(netem_restored),
        "retrans_lift": float(retrans_lift),
        "rtt_lift": float(rtt_lift),
        "frontend_latency_lift": float(frontend_latency_lift),
    }
    return report


def _quality_ok(quality: dict[str, Any], requirements: dict[str, Any]) -> bool:
    mapping = {
        "require_netem_applied": "netem_applied",
        "require_netem_restored": "netem_restored",
        "require_shippingservice_network_metric_present": "shippingservice_network_metric_present",
        "require_frontend_latency_metric_present": "frontend_latency_metric_present",
        "require_service_graph_present": "service_graph_present",
    }
    for req, field in mapping.items():
        if requirements.get(req) is True and quality.get(field) is not True:
            return False
    return True


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
        "retrans_lift_debug": float(quality.get("retrans_lift", 0.0)),
        "rtt_lift_debug": float(quality.get("rtt_lift", 0.0)),
        "frontend_latency_lift_debug": float(quality.get("frontend_latency_lift", 0.0)),
        "shippingservice_network_metric_present": bool(quality.get("shippingservice_network_metric_present")),
        "shippingservice_retrans_metric_present": bool(quality.get("shippingservice_retrans_metric_present")),
    }
    _write_json(output_path / "real_p1_rca_summary.json", summary)
    _write_json(output_path / "real_p1_rca_metadata.json", {"input_dir": str(input_path), "output_dir": str(output_path), "top_k": int(top_k), "real_collection": True, "note": "P2B-1 single real network repeat case; not multi-fault accuracy."})
    return {"summary": summary, "evaluation": evaluation, "results": results}


def run_single_network_repeat(repeat_index: int, config: dict[str, Any]) -> dict[str, Any]:
    repeat_cfg = config["repeat_experiment"]
    base_dir = Path(str(repeat_cfg["base_output_dir"]))
    repeat_dir = base_dir / f"repeat_{int(repeat_index):02d}"
    raw_dir = repeat_dir / "raw"
    rca_dir = repeat_dir / "p1rca"
    raw_dir.mkdir(parents=True, exist_ok=True)
    rca_dir.mkdir(parents=True, exist_ok=True)
    runtime_config = json.loads(json.dumps(config))
    suffix = f"{int(repeat_index):02d}"
    incident_id = f"ob-network-shippingservice-repeat-{suffix}"
    namespace = str(config["kubernetes"]["namespace"])
    frontend_url = str(config["experiment"]["frontend_url"])
    ready = ensure_online_boutique_ready(namespace, frontend_url)
    port_forward_proc = _start_frontend_port_forward_if_needed(namespace, frontend_url, repeat_dir)
    pod = get_target_pod(namespace, str(config["target"]["service"]))
    if not pod.get("ready"):
        raise RuntimeError(f"target pod not ready: {pod}")
    sandbox_id = get_pod_sandbox_id(str(config["kind_node_container"]), namespace, str(pod["name"]))
    pid = get_pod_netns_pid(str(config["kind_node_container"]), sandbox_id)
    runtime_config["_runtime"] = {"pod_name": pod["name"], "pod_ip": pod.get("pod_ip", ""), "sandbox_id": sandbox_id, "netns_pid": pid, "incident_id": incident_id}
    write_yaml(repeat_dir / "repeat_config.yaml", runtime_config)

    metrics: list[dict[str, Any]] = []
    window_states: list[dict[str, Any]] = []
    fault_log: dict[str, Any] = {"applied": False}
    restore_log: dict[str, Any] = {"restored": False}
    tc_before = get_tc_qdisc(str(config["kind_node_container"]), pid, str(config["fault_injection"]["device"]))
    _write_text(raw_dir / "tc_qdisc_before.txt", tc_before)
    prev_snmp = collect_proc_net_snmp(str(config["kind_node_container"]), pid)
    first_faulty_ts = 0.0
    last_faulty_ts = 0.0
    try:
        for idx in range(int(config["experiment"]["baseline_windows"])):
            rows, state = collect_network_window_metrics(runtime_config, "baseline", idx + 1, prev_snmp)
            metrics.extend(rows)
            window_states.append(state)
            prev_snmp = state["snmp"]
        fault = config["fault_injection"]
        fault_log = apply_netem_fault(
            str(config["kind_node_container"]),
            pid,
            str(fault["device"]),
            int(fault["delay_ms"]),
            int(fault["jitter_ms"]),
            float(fault["loss_percent"]),
        )
        tc_during = get_tc_qdisc(str(config["kind_node_container"]), pid, str(fault["device"]))
        _write_text(raw_dir / "tc_qdisc_during.txt", tc_during)
        for idx in range(int(config["experiment"]["faulty_windows"])):
            rows, state = collect_network_window_metrics(runtime_config, "faulty", idx + 1, prev_snmp)
            metrics.extend(rows)
            window_states.append(state)
            prev_snmp = state["snmp"]
            if idx == 0:
                first_faulty_ts = float(state["timestamp"])
            last_faulty_ts = float(state["timestamp"])
    finally:
        restore_log = clear_netem_fault(str(config["kind_node_container"]), pid, str(config["fault_injection"]["device"]))
        tc_after = get_tc_qdisc(str(config["kind_node_container"]), pid, str(config["fault_injection"]["device"]))
        _write_text(raw_dir / "tc_qdisc_after.txt", tc_after)
        _write_json(raw_dir / "network_fault_log.json", fault_log)
        _write_json(raw_dir / "network_restore_log.json", restore_log)
    for idx in range(int(config["experiment"]["recovery_windows"])):
        rows, state = collect_network_window_metrics(runtime_config, "recovery", idx + 1, prev_snmp)
        metrics.extend(rows)
        window_states.append(state)
        prev_snmp = state["snmp"]

    incident = build_network_incident(repeat_index, first_faulty_ts, last_faulty_ts)
    evidence = build_network_evidence(metrics, incident)
    write_jsonl(raw_dir / "metrics.jsonl", metrics)
    write_jsonl(raw_dir / "incidents.jsonl", [incident])
    write_jsonl(raw_dir / "evidence.jsonl", evidence)
    graph_result = write_online_boutique_service_graph(raw_dir / "service_graph.jsonl")
    quality = _quality_report(metrics, evidence, config, bool(fault_log.get("applied")), bool(restore_log.get("restored")))
    quality["service_graph_present"] = Path(raw_dir / "service_graph.jsonl").exists()
    _write_json(raw_dir / "data_quality_report.json", quality)
    _write_json(raw_dir / "metadata.json", {
        "phase": "P2B-1",
        "repeat_index": int(repeat_index),
        "experiment_group_id": str(repeat_cfg["experiment_group_id"]),
        "raw_output_dir": str(raw_dir),
        "target_service": str(config["target"]["service"]),
        "target_metric": str(config["target"]["metric"]),
        "target_fault_type": str(config["target"]["fault_type"]),
        "pod_name": str(pod["name"]),
        "pod_ip": str(pod.get("pod_ip", "")),
        "netns_pid": int(pid),
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
        "netem_applied": bool(fault_log.get("applied")),
        "netem_restored": bool(restore_log.get("restored")),
        "shippingservice_network_metric_present": bool(quality.get("shippingservice_network_metric_present")),
        "shippingservice_retrans_metric_present": bool(quality.get("shippingservice_retrans_metric_present")),
        "quality_ok": bool(quality_ok),
        "retrans_lift": float(quality.get("retrans_lift", 0.0)),
        "rtt_lift": float(quality.get("rtt_lift", 0.0)),
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


def _mean_metric(rows: list[dict[str, Any]], key: str) -> float:
    values = [float(row.get(key, 0.0)) for row in rows]
    return float(np.mean(np.asarray(values, dtype=float))) if values else 0.0


def _min_metric(rows: list[dict[str, Any]], key: str) -> float:
    values = [float(row.get(key, 0.0)) for row in rows]
    return float(np.min(np.asarray(values, dtype=float))) if values else 0.0


def run_p2b1_network_repeated_experiment(config_path: str | Path) -> dict[str, Any]:
    config = load_network_repeat_config(config_path)
    repeat_cfg = config["repeat_experiment"]
    base_dir = Path(str(repeat_cfg["base_output_dir"]))
    base_dir.mkdir(parents=True, exist_ok=True)
    repeats = int(repeat_cfg.get("repeats", 5))
    sleep_between = int(repeat_cfg.get("sleep_between_repeats_sec", 0) or 0)
    per_repeat: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for index in range(1, repeats + 1):
        try:
            row = run_single_network_repeat(index, config)
            per_repeat.append(row)
            if not row.get("quality_ok") or not row.get("rca_ok"):
                failures.append({"repeat_index": index, "row": row})
        except Exception as exc:  # keep repeats auditable, but do not hide the failure.
            failure = {"repeat_index": index, "error": str(exc)}
            failures.append(failure)
            per_repeat.append({"repeat_index": index, "quality_ok": False, "rca_ok": False, "error": str(exc), "service_hit_at_1": 0.0, "metric_hit_at_1": 0.0, "metric_hit_at_3": 0.0, "metric_mrr": 0.0, "root_type_accuracy": 0.0, "path_fidelity": 0.0, "retrans_lift": 0.0, "rtt_lift": 0.0, "frontend_latency_lift": 0.0})
        if index < repeats and sleep_between > 0:
            time.sleep(sleep_between)
    rca_rows = [row for row in per_repeat if row.get("rca_ok")]
    quality_rows = [row for row in per_repeat if row.get("quality_ok")]
    summary = {
        "experiment_group_id": str(repeat_cfg["experiment_group_id"]),
        "repeats_requested": repeats,
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
        "retrans_lift_mean": _mean_metric(quality_rows, "retrans_lift"),
        "rtt_lift_mean": _mean_metric(quality_rows, "rtt_lift"),
        "frontend_latency_lift_mean": _mean_metric(quality_rows, "frontend_latency_lift"),
        "per_repeat": per_repeat,
    }
    metadata = {
        "config_path": str(config_path),
        "base_output_dir": str(base_dir),
        "target": config.get("target", {}),
        "fault_injection": config.get("fault_injection", {}),
        "experiment": config.get("experiment", {}),
        "note": "P2B-1 repeated real network fault experiment; not multi-fault accuracy.",
    }
    _write_json(base_dir / "p2b1_network_repeat_summary.json", summary)
    _write_json(base_dir / "p2b1_network_repeat_metadata.json", metadata)
    _write_json(base_dir / "p2b1_network_repeat_failures.json", {"failures": failures})
    return {"summary": summary, "metadata": metadata, "failures": failures, "output_dir": str(base_dir)}
