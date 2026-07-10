"""A5 adaptive probe policy preview for Online Boutique P2 artifacts.

This module builds probe plans from A3 alert windows, A4 candidate subgraphs,
and optional A1/A2 blind evidence. It never activates real probes and never uses
root labels or target configuration for policy selection.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np


@dataclass(frozen=True)
class ProbeSpec:
    probe_name: str
    evidence_type: str
    metric_patterns: list[str]
    cost: float
    min_sampling_probability: float
    max_sampling_probability: float
    default_sampling_probability: float
    layer: str


def default_probe_specs() -> list[ProbeSpec]:
    return [
        ProbeSpec("request_probe", "load", ["request.rps", "request.error_rate", "request.p50_latency_ms", "request.p95_latency_ms", "request.p99_latency_ms"], 1.0, 1.0, 1.0, 1.0, "always_on"),
        ProbeSpec("cpu_probe", "CPU", ["cpu.usage", "cpu.throttled_usec", "cpu.throttled_periods", "cpu.throttle_ratio"], 2.0, 0.1, 1.0, 0.3, "suspicious_burst"),
        ProbeSpec("memory_probe", "memory", ["memory.usage", "memory.events", "memory.oom", "memory.reclaim"], 2.0, 0.1, 1.0, 0.3, "suspicious_burst"),
        ProbeSpec("network_probe", "network", ["net.retrans", "net.rtt_ms", "net.in_segs", "net.out_segs"], 2.5, 0.1, 1.0, 0.3, "suspicious_burst"),
        ProbeSpec("io_probe", "storage I/O", ["io.write_bytes", "io.write_ops", "io.read_bytes", "io.read_ops", "io.io_time_ms"], 2.5, 0.1, 1.0, 0.3, "suspicious_burst"),
        ProbeSpec("lock_probe", "lock contention", ["lock.futex_wait_ms", "lock.wait_ms", "lock.wait_p95_ms", "lock.contention_count"], 3.0, 0.1, 1.0, 0.3, "suspicious_burst"),
    ]


def load_jsonl(path: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    file_path = Path(path)
    if not file_path.exists():
        return rows
    with file_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            item = json.loads(text)
            if isinstance(item, dict):
                rows.append(item)
    return rows


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        return {}


def metric_family(metric: str) -> str:
    if metric.startswith("cpu."):
        return "CPU"
    if metric.startswith("net."):
        return "network"
    if metric.startswith("io."):
        return "storage I/O"
    if metric.startswith("lock."):
        return "lock contention"
    if metric.startswith("memory."):
        return "memory"
    if metric.startswith("request."):
        return "load"
    return "unknown"


def load_candidate_graph(candidate_dir: str) -> dict[str, Any]:
    base = Path(candidate_dir)
    if (base / "repeat_candidate_summary.json").exists():
        summary = _read_json(base / "repeat_candidate_summary.json")
        window_dirs = [Path(item["window_output_dir"]) for item in summary.get("window_summaries", [])]
    else:
        summary = {}
        window_dirs = [base]
    services: dict[str, dict[str, Any]] = {}
    metric_nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    metadata: dict[str, Any] = {"window_count": len(window_dirs), "window_metadata": []}
    for window_dir in window_dirs:
        for row in load_jsonl(str(window_dir / "candidate_services.jsonl")):
            service = str(row.get("service", ""))
            if service:
                services[service] = row
        metric_nodes.extend(load_jsonl(str(window_dir / "candidate_metric_nodes.jsonl")))
        edges.extend(load_jsonl(str(window_dir / "candidate_edges.jsonl")))
        md = _read_json(window_dir / "candidate_subgraph_metadata.json")
        if md:
            metadata["window_metadata"].append(md)
    service_to_metrics: dict[str, set[str]] = defaultdict(set)
    for node in metric_nodes:
        service = node.get("service")
        metric = node.get("metric")
        if service and metric:
            service_to_metrics[str(service)].add(str(metric))
    centrality = compute_service_centrality(edges)
    return {
        "services": sorted(services),
        "metric_nodes": metric_nodes,
        "edges": edges,
        "metadata": metadata,
        "repeat_summary": summary,
        "service_to_metrics": {service: sorted(metrics) for service, metrics in service_to_metrics.items()},
        "service_degree": centrality,
    }


def load_alert_windows(alert_dir: str) -> list[dict[str, Any]]:
    rows = load_jsonl(str(Path(alert_dir) / "alert_windows.jsonl"))
    allowed = ("alert_window_id", "start_ts", "end_ts", "symptom_service", "trigger_metrics", "severity", "max_z_score")
    return [{key: row.get(key) for key in allowed if key in row} for row in rows]


def load_blind_evidence_optional(path_or_dir: str | None) -> dict[str, Any]:
    if not path_or_dir:
        rows: list[dict[str, Any]] = []
    else:
        base = Path(path_or_dir)
        candidates = [base / "blind_evidence.jsonl", base / "input" / "blind_evidence.jsonl", base / "evidence.jsonl"] if base.is_dir() else [base]
        rows = []
        for path in candidates:
            if path.exists():
                rows = load_jsonl(str(path))
                break
    by_service_type: dict[tuple[str, str], float] = defaultdict(float)
    by_service_metric: dict[tuple[str, str], float] = defaultdict(float)
    for row in rows:
        service = row.get("service")
        metric = row.get("metric")
        evidence_type = row.get("evidence_type") or (metric_family(str(metric)) if metric else None)
        try:
            score = float(row.get("evidence_score", 0.0))
        except (TypeError, ValueError):
            score = 0.0
        if service and evidence_type:
            key = (str(service), str(evidence_type))
            by_service_type[key] = max(by_service_type[key], score)
        if service and metric:
            key = (str(service), str(metric))
            by_service_metric[key] = max(by_service_metric[key], score)
    return {
        "rows": rows,
        "by_service_type": dict(by_service_type),
        "by_service_metric": dict(by_service_metric),
        "evidence_count": len(rows),
    }


def compute_service_centrality(candidate_edges: list[dict[str, Any]]) -> dict[str, float]:
    degree: dict[str, int] = defaultdict(int)
    for edge in candidate_edges:
        src = edge.get("src")
        dst = edge.get("dst")
        if src:
            degree[str(src)] += 1
        if dst:
            degree[str(dst)] += 1
    if not degree:
        return {}
    max_degree = max(degree.values()) or 1
    return {service: float(value) / float(max_degree) for service, value in degree.items()}


def _metric_family_available(metrics: list[str], evidence_type: str) -> bool:
    return any(metric_family(metric) == evidence_type for metric in metrics)


def compute_probe_context(
    probe: ProbeSpec,
    service: str,
    alert_window: dict[str, Any],
    candidate_graph: dict[str, Any],
    blind_evidence: dict[str, Any],
) -> dict[str, Any]:
    severity = str(alert_window.get("severity", "soft"))
    alert_severity_score = 1.0 if severity == "hard" else 0.5
    try:
        max_z = float(alert_window.get("max_z_score", 0.0))
    except (TypeError, ValueError):
        max_z = 0.0
    alert_intensity = min(max(max_z, 0.0) / 10.0, 1.0)
    service_centrality = float(candidate_graph.get("service_degree", {}).get(service, 0.0))
    service_metrics = candidate_graph.get("service_to_metrics", {}).get(service, [])
    family_available = _metric_family_available(service_metrics, probe.evidence_type)
    evidence_score = float(blind_evidence.get("by_service_type", {}).get((service, probe.evidence_type), 0.0))
    layer_bias = {"always_on": 1.0, "suspicious_burst": 0.7, "confirmation": 0.5}.get(probe.layer, 0.5)
    missingness = 0.0 if family_available else 1.0
    normalized_cost = min(float(probe.cost) / 3.0, 1.0)
    return {
        "alert_severity_score": alert_severity_score,
        "alert_intensity": alert_intensity,
        "service_centrality": service_centrality,
        "evidence_score": evidence_score,
        "metric_family_available": 1.0 if family_available else 0.0,
        "missingness": missingness,
        "cost": float(probe.cost),
        "normalized_cost": normalized_cost,
        "layer_bias": layer_bias,
        "last_gain": 0.0,
        "symptom_service_match": 1.0 if service == str(alert_window.get("symptom_service", "")) else 0.0,
    }


def estimate_probe_gain(context: dict[str, Any], alpha: float = 0.2, beta_cost: float = 0.15) -> dict[str, Any]:
    uncertainty_bonus = float(context["missingness"]) * 0.5
    breakdown = {
        "alert_severity": 0.30 * float(context["alert_severity_score"]),
        "alert_intensity": 0.25 * float(context["alert_intensity"]),
        "evidence": 0.20 * float(context["evidence_score"]),
        "service_centrality": 0.15 * float(context["service_centrality"]),
        "metric_family_available": 0.10 * float(context["metric_family_available"]),
        "uncertainty_bonus": alpha * uncertainty_bonus,
        "cost_penalty": -beta_cost * float(context["normalized_cost"]),
    }
    gain = float(sum(breakdown.values()))
    return {"gain": gain, "gain_breakdown": breakdown, "uncertainty_bonus": uncertainty_bonus}


def sampling_probability_from_gain(gain: float, probe_spec: ProbeSpec) -> float:
    if probe_spec.layer == "always_on":
        return 1.0
    sigmoid = 1.0 / (1.0 + math.exp(-float(gain)))
    return float(probe_spec.min_sampling_probability + (probe_spec.max_sampling_probability - probe_spec.min_sampling_probability) * sigmoid)


def _candidate_probe_rows(alert_window: dict[str, Any], candidate_graph: dict[str, Any], blind_evidence: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    specs = default_probe_specs()
    symptom_service = str(alert_window.get("symptom_service", ""))
    for service in candidate_graph.get("services", []):
        service_metrics = candidate_graph.get("service_to_metrics", {}).get(service, [])
        for probe in specs:
            family_available = _metric_family_available(service_metrics, probe.evidence_type)
            if probe.layer != "always_on" and not family_available:
                continue
            if probe.layer == "always_on" and service != symptom_service and not family_available:
                continue
            context = compute_probe_context(probe, service, alert_window, candidate_graph, blind_evidence)
            gain_result = estimate_probe_gain(context)
            sampling_probability = sampling_probability_from_gain(gain_result["gain"], probe)
            rows.append({
                "alert_window_id": alert_window.get("alert_window_id"),
                "service": service,
                "probe_name": probe.probe_name,
                "evidence_type": probe.evidence_type,
                "metric_patterns": list(probe.metric_patterns),
                "sampling_probability": sampling_probability,
                "gain": gain_result["gain"],
                "cost": probe.cost,
                "context": context,
                "gain_breakdown": gain_result["gain_breakdown"],
                "probe_spec": asdict(probe),
                "selected": False,
                "selection_reason": "not_selected",
            })
    return rows


def choose_probes_under_budget(candidates: list[dict[str, Any]], budget: float) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    spent = 0.0
    always_on = sorted([row for row in candidates if row["probe_spec"]["layer"] == "always_on"], key=lambda row: (-float(row["context"].get("symptom_service_match", 0.0)), -float(row["gain"]), row["service"]))
    burst = sorted([row for row in candidates if row["probe_spec"]["layer"] != "always_on"], key=lambda row: (-(float(row["gain"]) / max(float(row["cost"]), 1e-9)), -float(row["gain"]), row["service"], row["probe_name"]))
    for row in always_on:
        if spent + float(row["cost"]) <= budget or not selected:
            item = dict(row)
            item["selected"] = True
            item["selection_reason"] = "always_on"
            selected.append(item)
            spent += float(row["cost"])
    for row in burst:
        if spent + float(row["cost"]) <= budget:
            item = dict(row)
            item["selected"] = True
            item["selection_reason"] = "gain_cost_budget"
            selected.append(item)
            spent += float(row["cost"])
    if not selected and candidates:
        item = dict(sorted(candidates, key=lambda row: (row["probe_name"] != "request_probe", -float(row["gain"])))[0])
        item["selected"] = True
        item["selection_reason"] = "minimum_request_probe_fallback"
        selected.append(item)
    return selected


def build_probe_plan_for_window(
    alert_window: dict[str, Any],
    candidate_graph: dict[str, Any],
    blind_evidence: dict[str, Any],
    budget: float = 12.0,
) -> dict[str, Any]:
    candidates = _candidate_probe_rows(alert_window, candidate_graph, blind_evidence)
    selected = choose_probes_under_budget(candidates, budget)
    selected_keys = {(row["service"], row["probe_name"]) for row in selected}
    unselected = []
    for row in candidates:
        if (row["service"], row["probe_name"]) not in selected_keys:
            unselected.append(row)
    estimated_cost = float(sum(float(row["cost"]) for row in selected))
    return {
        "alert_window_id": alert_window.get("alert_window_id"),
        "symptom_service": alert_window.get("symptom_service"),
        "selected_probes": selected,
        "unselected_probes": unselected,
        "budget": float(budget),
        "estimated_cost": estimated_cost,
        "candidate_service_count": len(candidate_graph.get("services", [])),
        "candidate_metric_node_count": len(candidate_graph.get("metric_nodes", [])),
        "uses_root_labels": False,
        "uses_target_config": False,
        "uses_injected_path": False,
        "uses_incident_start_end": False,
    }


def _matching_metrics(metric_nodes: list[dict[str, Any]], service: str, evidence_type: str) -> list[str]:
    return sorted({str(node.get("metric")) for node in metric_nodes if node.get("service") == service and metric_family(str(node.get("metric", ""))) == evidence_type})


def write_probe_policy_outputs(
    alert_dir: str,
    candidate_dir: str,
    output_dir: str,
    blind_evidence_dir: str | None = None,
    budget: float = 12.0,
) -> dict[str, Any]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    alert_windows = load_alert_windows(alert_dir)
    candidate_graph = load_candidate_graph(candidate_dir)
    blind_evidence = load_blind_evidence_optional(blind_evidence_dir)
    plans: list[dict[str, Any]] = []
    sampling_rows: list[dict[str, Any]] = []
    mask_rows: list[dict[str, Any]] = []
    for window in alert_windows:
        plan = build_probe_plan_for_window(window, candidate_graph, blind_evidence, budget)
        plans.append(plan)
        selected = plan["selected_probes"]
        for probe_row in selected:
            metrics = _matching_metrics(candidate_graph.get("metric_nodes", []), probe_row["service"], probe_row["evidence_type"])
            if not metrics:
                metrics = list(probe_row["metric_patterns"])
            for metric in metrics:
                sampling_rows.append({
                    "alert_window_id": plan["alert_window_id"],
                    "service": probe_row["service"],
                    "probe_name": probe_row["probe_name"],
                    "evidence_type": probe_row["evidence_type"],
                    "metric": metric,
                    "sampling_probability": probe_row["sampling_probability"],
                    "selected": True,
                    "cost": probe_row["cost"],
                    "source": "adaptive_probe_policy",
                })
        for node in candidate_graph.get("metric_nodes", []):
            service = str(node.get("service"))
            metric = str(node.get("metric"))
            best = 0.0
            by_probe = None
            for probe_row in selected:
                if service == probe_row["service"] and metric_family(metric) == probe_row["evidence_type"]:
                    if float(probe_row["sampling_probability"]) > best:
                        best = float(probe_row["sampling_probability"])
                        by_probe = probe_row["probe_name"]
            mask_rows.append({
                "alert_window_id": plan["alert_window_id"],
                "service": service,
                "metric": metric,
                "observed_probability": best,
                "observed_by_probe": by_probe,
                "mask_policy": "probabilistic_policy_preview",
            })
    _write_jsonl(out / "probe_plan.jsonl", plans)
    _write_jsonl(out / "sampling_log.jsonl", sampling_rows)
    _write_jsonl(out / "observation_mask.jsonl", mask_rows)
    metadata = {
        "alert_dir": alert_dir,
        "candidate_dir": candidate_dir,
        "output_dir": str(out),
        "blind_evidence_dir": blind_evidence_dir,
        "budget": float(budget),
        "alert_windows_count": len(alert_windows),
        "probe_plan_count": len(plans),
        "sampling_log_count": len(sampling_rows),
        "observation_mask_count": len(mask_rows),
        "average_selected_probe_count": float(np.mean([len(plan["selected_probes"]) for plan in plans])) if plans else 0.0,
        "average_estimated_cost": float(np.mean([plan["estimated_cost"] for plan in plans])) if plans else 0.0,
        "uses_root_labels": False,
        "uses_target_config": False,
        "uses_injected_path": False,
        "uses_incident_start_end": False,
        "actual_probe_activation": False,
        "source": "a5_adaptive_probe_policy",
    }
    _write_json(out / "adaptive_probe_metadata.json", metadata)
    return {"plans": plans, "sampling_log": sampling_rows, "observation_mask": mask_rows, "metadata": metadata}


def evaluate_probe_policy_for_debug(output_dir: str, incidents_path: str) -> dict[str, Any]:
    plans = load_jsonl(str(Path(output_dir) / "probe_plan.jsonl"))
    incidents = load_jsonl(incidents_path)
    selected_pairs: set[tuple[str, str]] = set()
    selected_services: set[str] = set()
    for plan in plans:
        for probe in plan.get("selected_probes", []):
            selected_services.add(str(probe.get("service")))
            selected_pairs.add((str(probe.get("service")), str(probe.get("evidence_type"))))
    family_hits: list[bool] = []
    service_hits: list[bool] = []
    for incident in incidents:
        service = incident.get("root_service")
        metric = incident.get("root_metric")
        family = metric_family(str(metric)) if metric else "unknown"
        service_hits.append(bool(service in selected_services) if service else False)
        family_hits.append(bool((str(service), family) in selected_pairs) if service else False)
    return {
        "debug_only": True,
        "ground_truth_incidents": len(incidents),
        "debug_root_metric_family_selected_rate": float(np.mean(np.asarray(family_hits, dtype=float))) if family_hits else 0.0,
        "debug_root_service_has_selected_probe_rate": float(np.mean(np.asarray(service_hits, dtype=float))) if service_hits else 0.0,
        "debug_notes": "Root labels are read only after policy generation for coverage diagnostics.",
    }
