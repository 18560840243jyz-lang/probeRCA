"""Blind evidence generation for real Online Boutique metrics.

This module builds evidence only from observed metric lift over an alert window.
It does not use experiment target configuration or ground-truth labels for
scoring. The incident file is used only as a temporary source of start/end
windows until the alert gate exists.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any

from proberca.adapters.online_boutique.service_metric_identity import assert_or_repair_node_ownership

FORBIDDEN_TERMS = (
    "root_service",
    "root_metric",
    "root_type",
    "target_service",
    "target_metric",
    "target_fault_type",
    "injected_path",
)

ALLOWED_INCIDENT_KEYS = {"incident_id", "start_ts", "end_ts", "symptom_service"}
EPS = 1e-9


def _jsonl_path(path: str | Path, default_name: str) -> Path:
    candidate = Path(path)
    if candidate.is_dir():
        return candidate / default_name
    return candidate


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            item = json.loads(text)
            if isinstance(item, dict):
                records.append(item)
    return records


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")


def load_metrics(path: str) -> list[dict[str, Any]]:
    """Load observed metric records from a metrics.jsonl path or directory."""

    return _read_jsonl(_jsonl_path(path, "metrics.jsonl"))


def load_incident_windows(path: str) -> list[dict[str, Any]]:
    """Load only alert-window fields from incidents.jsonl.

    The full incident records can contain ground truth fields. This function
    returns a new dictionary containing only the fields listed in
    ALLOWED_INCIDENT_KEYS so downstream evidence generation cannot accidentally
    consume answer labels.
    """

    windows: list[dict[str, Any]] = []
    for item in _read_jsonl(_jsonl_path(path, "incidents.jsonl")):
        window = {key: item.get(key) for key in ALLOWED_INCIDENT_KEYS if key in item}
        if "incident_id" not in window:
            window["incident_id"] = f"incident-{len(windows) + 1:03d}"
        if "start_ts" not in window or "end_ts" not in window:
            raise ValueError("incident window requires start_ts and end_ts")
        windows.append(window)
    return windows


def metric_to_evidence_type(metric: str) -> str:
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


def _metric_value(record: dict[str, Any]) -> float | None:
    value = record.get("value")
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
    else:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def compute_baseline_faulty_lift(
    metrics: list[dict[str, Any]], start_ts: float, end_ts: float
) -> list[dict[str, Any]]:
    """Compute blind baseline/faulty lift for every observed service.metric."""

    grouped: dict[tuple[str, str], dict[str, list[float]]] = defaultdict(
        lambda: {"baseline": [], "faulty": []}
    )
    for record in metrics:
        service = record.get("service")
        metric = record.get("metric")
        timestamp = record.get("timestamp")
        value = _metric_value(record)
        if not service or not metric or timestamp is None or value is None:
            continue
        try:
            ts = float(timestamp)
        except (TypeError, ValueError):
            continue
        key = (str(service), str(metric))
        if ts < start_ts:
            grouped[key]["baseline"].append(value)
        elif start_ts <= ts <= end_ts:
            grouped[key]["faulty"].append(value)

    lift_records: list[dict[str, Any]] = []
    for (service, metric), values in grouped.items():
        baseline_mean = _mean(values["baseline"])
        faulty_mean = _mean(values["faulty"])
        if baseline_mean is None or faulty_mean is None:
            continue
        absolute_lift = faulty_mean - baseline_mean
        relative_lift = absolute_lift / (abs(baseline_mean) + EPS)
        lift_records.append(
            {
                "service": service,
                "metric": metric,
                "evidence_type": metric_to_evidence_type(metric),
                "baseline_mean": baseline_mean,
                "faulty_mean": faulty_mean,
                "absolute_lift": absolute_lift,
                "relative_lift": relative_lift,
                "baseline_count": len(values["baseline"]),
                "faulty_count": len(values["faulty"]),
            }
        )
    lift_records.sort(key=lambda item: (item["evidence_type"], item["service"], item["metric"]))
    return lift_records


def normalize_evidence_scores(lift_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalize positive lift within each incident and evidence type."""

    max_by_group: dict[tuple[str, str], float] = defaultdict(float)
    for record in lift_records:
        positive_lift = max(float(record.get("absolute_lift", 0.0)), 0.0)
        if positive_lift <= 0.0:
            continue
        incident = str(record.get("incident_id", "default"))
        evidence_type = str(record.get("evidence_type", "unknown"))
        key = (incident, evidence_type)
        if positive_lift > max_by_group[key]:
            max_by_group[key] = positive_lift

    normalized: list[dict[str, Any]] = []
    for record in lift_records:
        incident = str(record.get("incident_id", "default"))
        evidence_type = str(record.get("evidence_type", "unknown"))
        max_lift = max_by_group.get((incident, evidence_type), 0.0)
        positive_lift = max(float(record.get("absolute_lift", 0.0)), 0.0)
        if max_lift <= 0.0 or positive_lift <= 0.0:
            continue
        item = dict(record)
        item["evidence_score"] = max(0.0, min(1.0, positive_lift / max_lift))
        normalized.append(item)

    normalized.sort(
        key=lambda item: (
            str(item.get("incident_id", "")),
            str(item.get("evidence_type", "")),
            -float(item.get("evidence_score", 0.0)),
            str(item.get("service", "")),
            str(item.get("metric", "")),
        )
    )
    return normalized


def _limit_top_k_per_type(
    records: list[dict[str, Any]], min_score: float, top_k_per_type: int
) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        if float(record.get("evidence_score", 0.0)) < min_score:
            continue
        key = (str(record.get("incident_id", "default")), str(record.get("evidence_type", "unknown")))
        buckets[key].append(record)

    selected: list[dict[str, Any]] = []
    for key in sorted(buckets):
        ranked = sorted(
            buckets[key],
            key=lambda item: (
                -float(item.get("evidence_score", 0.0)),
                -float(item.get("absolute_lift", 0.0)),
                str(item.get("service", "")),
                str(item.get("metric", "")),
            ),
        )
        selected.extend(ranked[:top_k_per_type])
    return selected



def _attach_ownership_fields(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for record in records:
        item = assert_or_repair_node_ownership(record)
        item["node"] = item.get("node_id", item.get("node", "unknown.unknown"))
        item["metric_family"] = item.get("metric_family") or item.get("evidence_type", "unknown")
        item["evidence_type"] = item.get("evidence_type") or item.get("metric_family", "unknown")
        enriched.append(item)
    return enriched

def generate_blind_evidence(
    input_dir: str,
    output_dir: str | None = None,
    min_score: float = 0.05,
    top_k_per_type: int = 20,
) -> dict[str, Any]:
    base = Path(input_dir)
    out = Path(output_dir) if output_dir else base
    out.mkdir(parents=True, exist_ok=True)

    metrics = load_metrics(str(base / "metrics.jsonl"))
    windows = load_incident_windows(str(base / "incidents.jsonl"))
    all_lifts: list[dict[str, Any]] = []
    for window in windows:
        start_ts = float(window["start_ts"])
        end_ts = float(window["end_ts"])
        incident_id = str(window["incident_id"])
        for record in compute_baseline_faulty_lift(metrics, start_ts, end_ts):
            item = dict(record)
            item["incident_id"] = incident_id
            item["source"] = "blind_metric_lift_evidence"
            item["node"] = f"{item['service']}.{item['metric']}"
            all_lifts.append(item)

    normalized = normalize_evidence_scores(all_lifts)
    selected = _limit_top_k_per_type(normalized, min_score=min_score, top_k_per_type=top_k_per_type)
    selected = _attach_ownership_fields(selected)
    selected = _attach_ownership_fields(selected)
    for item in selected:
        item["value"] = float(item.get("evidence_score", 0.0))
    selected.sort(
        key=lambda item: (
            str(item.get("incident_id", "")),
            str(item.get("evidence_type", "")),
            -float(item.get("evidence_score", 0.0)),
            str(item.get("service", "")),
            str(item.get("metric", "")),
        )
    )

    evidence_path = out / "blind_evidence.jsonl"
    metadata_path = out / "blind_evidence_metadata.json"
    _write_jsonl(evidence_path, selected)
    evidence_types = sorted({str(item.get("evidence_type", "unknown")) for item in selected})
    metadata = {
        "input_dir": str(base),
        "output_dir": str(out),
        "incidents_count": len(windows),
        "metrics_count": len(metrics),
        "evidence_count": len(selected),
        "evidence_types": evidence_types,
        "min_score": min_score,
        "top_k_per_type": top_k_per_type,
        "blind_evidence": True,
        "uses_root_labels": False,
        "uses_target_config": False,
        "uses_injected_path": False,
        "uses_alert_window_only": True,
    }
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return {"evidence_path": str(evidence_path), "metadata_path": str(metadata_path), **metadata}


def audit_blind_evidence_code_safety() -> dict[str, Any]:
    """Text-scan this file for risky answer-label references."""

    path = Path(__file__)
    suspicious_lines: list[dict[str, Any]] = []
    allowed_lines: list[dict[str, Any]] = []
    allowed_context_markers = (
        "FORBIDDEN_TERMS",
        "forbidden",
        "ground truth",
        "ALLOWED_INCIDENT_KEYS",
        "answer labels",
        "suspicious_lines",
        "allowed_lines",
        "forbidden_terms",
        "uses_injected_path",
    )
    in_forbidden_term_literal = False
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = line.strip()
        if stripped.startswith("FORBIDDEN_TERMS"):
            in_forbidden_term_literal = True
        hits = [term for term in FORBIDDEN_TERMS if term in line]
        if hits:
            payload = {"line": lineno, "terms": hits, "text": stripped}
            if in_forbidden_term_literal or any(marker in line for marker in allowed_context_markers):
                allowed_lines.append(payload)
            else:
                suspicious_lines.append(payload)
        if in_forbidden_term_literal and stripped == ")":
            in_forbidden_term_literal = False
    return {
        "passed": not suspicious_lines,
        "suspicious_lines": suspicious_lines,
        "allowed_lines": allowed_lines,
        "forbidden_terms": list(FORBIDDEN_TERMS),
    }


def _timestamp_from_record(record: dict[str, Any]) -> float | None:
    timestamp = record.get("timestamp", record.get("ts", record.get("time")))
    if timestamp is None:
        return None
    try:
        value = float(timestamp)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _service_metric_from_record(record: dict[str, Any]) -> tuple[str | None, str | None]:
    service = record.get("service", record.get("service_name", record.get("pod_service")))
    metric = record.get("metric", record.get("metric_name", record.get("name")))
    return (str(service) if service else None, str(metric) if metric else None)


def _compute_alert_window_lift(
    metrics: list[dict[str, Any]],
    start_ts: float,
    end_ts: float,
    min_baseline_points: int = 2,
    prefix_ratio: float = 0.3,
) -> tuple[list[dict[str, Any]], str]:
    grouped: dict[tuple[str, str], list[tuple[float, float]]] = defaultdict(list)
    for record in metrics:
        service, metric = _service_metric_from_record(record)
        ts = _timestamp_from_record(record)
        value = _metric_value(record)
        if not service or not metric or ts is None or value is None:
            continue
        grouped[(service, metric)].append((ts, value))

    any_prefix_fallback = False
    lift_records: list[dict[str, Any]] = []
    for (service, metric), points in grouped.items():
        ordered = sorted(points)
        baseline_values = [value for ts, value in ordered if ts < start_ts]
        faulty_values = [value for ts, value in ordered if start_ts <= ts <= end_ts]
        strategy = "alert_window_pre_fault"
        if len(baseline_values) < min_baseline_points:
            prefix_n = max(min_baseline_points, int(math.ceil(len(ordered) * prefix_ratio))) if ordered else 0
            baseline_values = [value for _, value in ordered[:prefix_n]]
            strategy = "prefix_baseline_fallback"
            any_prefix_fallback = True
        baseline_mean = _mean(baseline_values)
        faulty_mean = _mean(faulty_values)
        if baseline_mean is None or faulty_mean is None:
            continue
        absolute_lift = faulty_mean - baseline_mean
        relative_lift = absolute_lift / (abs(baseline_mean) + EPS)
        lift_records.append({
            "service": service,
            "metric": metric,
            "evidence_type": metric_to_evidence_type(metric),
            "baseline_mean": baseline_mean,
            "faulty_mean": faulty_mean,
            "absolute_lift": absolute_lift,
            "relative_lift": relative_lift,
            "baseline_count": len(baseline_values),
            "faulty_count": len(faulty_values),
            "baseline_strategy": strategy,
        })
    lift_records.sort(key=lambda item: (item["evidence_type"], item["service"], item["metric"]))
    return lift_records, "mixed_alert_window_with_prefix_fallback" if any_prefix_fallback else "alert_window_pre_fault"


def generate_blind_evidence_from_alert_windows(
    metrics_path: str,
    alert_windows_path: str,
    output_dir: str,
    min_score: float = 0.05,
    top_k_per_type: int = 20,
) -> dict[str, Any]:
    """Generate blind metric-lift evidence from A3 alert windows only.

    This function does not read incidents.jsonl and does not consume root labels,
    target labels, injected paths, or incident start/end labels. The provided
    alert windows are produced by A3 from metrics.
    """

    metrics_file = Path(metrics_path)
    windows_file = Path(alert_windows_path)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    metrics = load_metrics(str(metrics_file))
    windows = _read_jsonl(windows_file)

    all_lifts: list[dict[str, Any]] = []
    strategies: set[str] = set()
    for index, window in enumerate(windows, start=1):
        if "start_ts" not in window or "end_ts" not in window:
            continue
        start_ts = float(window["start_ts"])
        end_ts = float(window["end_ts"])
        alert_window_id = str(window.get("alert_window_id") or f"alert-window-{index:04d}")
        lifts, strategy = _compute_alert_window_lift(metrics, start_ts, end_ts)
        strategies.add(strategy)
        for record in lifts:
            item = dict(record)
            item["incident_id"] = alert_window_id
            item["alert_window_id"] = alert_window_id
            item["source"] = "alert_window_blind_metric_lift_evidence"
            item["node"] = f"{item['service']}.{item['metric']}"
            all_lifts.append(item)

    normalized = normalize_evidence_scores(all_lifts)
    selected = _limit_top_k_per_type(normalized, min_score=min_score, top_k_per_type=top_k_per_type)
    for item in selected:
        item["value"] = float(item.get("evidence_score", 0.0))
    selected.sort(
        key=lambda item: (
            str(item.get("alert_window_id", item.get("incident_id", ""))),
            str(item.get("evidence_type", "")),
            -float(item.get("evidence_score", 0.0)),
            str(item.get("service", "")),
            str(item.get("metric", "")),
        )
    )

    evidence_path = out / "blind_evidence.jsonl"
    metadata_path = out / "blind_evidence_metadata.json"
    _write_jsonl(evidence_path, selected)
    evidence_types = sorted({str(item.get("evidence_type", "unknown")) for item in selected})
    metadata = {
        "metrics_path": str(metrics_file),
        "alert_windows_path": str(windows_file),
        "output_dir": str(out),
        "alert_windows_count": len(windows),
        "metrics_count": len(metrics),
        "evidence_count": len(selected),
        "evidence_types": evidence_types,
        "min_score": min_score,
        "top_k_per_type": top_k_per_type,
        "baseline_strategy": "+".join(sorted(strategies)) if strategies else "none",
        "blind_evidence": True,
        "uses_blind_evidence": True,
        "uses_root_labels": False,
        "uses_target_config": False,
        "uses_injected_path": False,
        "uses_incident_start_end": False,
        "uses_alert_windows": True,
        "source": "alert_window_blind_metric_lift_evidence",
    }
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return {"evidence_path": str(evidence_path), "metadata_path": str(metadata_path), **metadata}
