"""Metrics-driven alert gate for Online Boutique real P2 datasets.

A3 builds alert events and alert windows from metrics only. Incident labels are
available solely for optional post-detection debug overlap evaluation.
"""

from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import numpy as np

DEFAULT_CONFIG = {
    "soft_threshold": 3.0,
    "hard_threshold": 6.0,
    "consecutive_windows": 2,
    "baseline_ratio": 0.3,
    "pre_window_sec": 30.0,
    "post_window_sec": 60.0,
    "min_points": 3,
}

STRONG_SYMPTOM_METRICS = {"request.p95_latency_ms", "request.p99_latency_ms", "request.error_rate"}
MEDIUM_SYMPTOM_METRICS = {"request.p50_latency_ms", "request.rps"}
RESOURCE_AUX_METRICS = {
    "cpu.throttled_usec",
    "cpu.throttle_ratio",
    "net.retrans",
    "io.write_bytes",
    "io.write_ops",
    "lock.futex_wait_ms",
    "lock.contention_count",
    "memory.usage",
}
EPS = 1e-9


def _jsonl_path(path: str | Path, default_name: str) -> Path:
    candidate = Path(path)
    return candidate / default_name if candidate.is_dir() else candidate


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        raise FileNotFoundError(f"missing JSONL file: {path}")
    with path.open("r", encoding="utf-8") as handle:
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


def _as_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        result = float(value)
    else:
        try:
            result = float(value)
        except (TypeError, ValueError):
            return None
    return result if math.isfinite(result) else None


def load_metrics(path: str) -> list[dict[str, Any]]:
    return _read_jsonl(_jsonl_path(path, "metrics.jsonl"))


def normalize_metric_records(metrics: list[dict[str, Any]]) -> dict[str, dict[str, list[tuple[float, float]]]]:
    organized: dict[str, dict[str, list[tuple[float, float]]]] = defaultdict(lambda: defaultdict(list))
    for row in metrics:
        service = row.get("service", row.get("service_name", row.get("svc")))
        metric = row.get("metric", row.get("metric_name", row.get("name")))
        timestamp = row.get("timestamp", row.get("ts", row.get("time")))
        value = row.get("value", row.get("metric_value"))
        ts = _as_float(timestamp)
        numeric = _as_float(value)
        if service is None or metric is None or ts is None or numeric is None:
            continue
        organized[str(service)][str(metric)].append((ts, numeric))
    return {
        service: {metric: sorted(values, key=lambda item: item[0]) for metric, values in metric_map.items()}
        for service, metric_map in organized.items()
    }


def robust_zscore_series(values: list[float], baseline_values: list[float]) -> list[float]:
    if not values or not baseline_values:
        return []
    baseline = np.asarray(baseline_values, dtype=float)
    median = float(np.median(baseline))
    mad = float(np.median(np.abs(baseline - median)))
    scale = 1.4826 * mad + EPS
    return [float((float(value) - median) / scale) for value in values]


def infer_baseline_prefix(records: list[tuple[float, float]], baseline_ratio: float = 0.3, min_points: int = 3) -> list[float]:
    if len(records) < min_points:
        return []
    count = max(min_points, int(math.ceil(len(records) * float(baseline_ratio))))
    count = min(count, len(records))
    return [float(value) for _timestamp, value in records[:count]]


def metric_alert_score(metric_name: str, z: float, value: float) -> float:
    metric = str(metric_name)
    if metric in STRONG_SYMPTOM_METRICS:
        return max(0.0, float(z)) * 2.0
    if metric == "request.rps":
        return abs(float(z)) * 1.2
    if metric in MEDIUM_SYMPTOM_METRICS:
        return max(0.0, float(z)) * 1.2
    if metric in RESOURCE_AUX_METRICS:
        return max(0.0, float(z)) * 0.8
    return max(0.0, float(z))


def _typical_step(records_by_metric: dict[str, dict[str, list[tuple[float, float]]]]) -> float:
    steps: list[float] = []
    for metric_map in records_by_metric.values():
        for points in metric_map.values():
            for (left, _), (right, _) in zip(points, points[1:]):
                delta = right - left
                if delta > 0:
                    steps.append(delta)
    if not steps:
        return 10.0
    return max(5.0, float(np.median(np.asarray(steps, dtype=float))) * 1.5)


def detect_alert_events(input_dir: str, config: dict | None = None) -> list[dict[str, Any]]:
    cfg = {**DEFAULT_CONFIG, **(config or {})}
    metrics = load_metrics(str(Path(input_dir) / "metrics.jsonl"))
    series = normalize_metric_records(metrics)
    nearby_sec = _typical_step(series)
    resource_anomalies: list[dict[str, Any]] = []
    request_candidates: list[dict[str, Any]] = []

    for service, metric_map in series.items():
        for metric, points in metric_map.items():
            baseline = infer_baseline_prefix(points, float(cfg["baseline_ratio"]), int(cfg["min_points"]))
            values = [value for _timestamp, value in points]
            zscores = robust_zscore_series(values, baseline)
            if not zscores:
                continue
            high_streak = 0
            for index, ((timestamp, value), z_score) in enumerate(zip(points, zscores)):
                score = metric_alert_score(metric, z_score, value)
                if metric in RESOURCE_AUX_METRICS and z_score >= float(cfg["soft_threshold"]):
                    resource_anomalies.append({
                        "timestamp": timestamp,
                        "service": service,
                        "metric": metric,
                        "z_score": z_score,
                        "value": value,
                        "alert_score": score,
                    })
                is_latency = metric in {"request.p95_latency_ms", "request.p99_latency_ms"}
                is_strong_symptom = metric in STRONG_SYMPTOM_METRICS
                if is_latency and z_score >= float(cfg["soft_threshold"]):
                    high_streak += 1
                elif is_latency:
                    high_streak = 0
                if is_strong_symptom and z_score >= float(cfg["soft_threshold"]):
                    request_candidates.append({
                        "timestamp": timestamp,
                        "service": service,
                        "metric": metric,
                        "z_score": z_score,
                        "value": value,
                        "alert_score": score,
                        "consecutive_high": high_streak,
                    })

    events: list[dict[str, Any]] = []
    for candidate in sorted(request_candidates, key=lambda item: (item["timestamp"], item["service"], item["metric"])):
        near_resource = [
            item for item in resource_anomalies
            if abs(float(item["timestamp"]) - float(candidate["timestamp"])) <= nearby_sec
        ]
        hard_reasons: list[str] = []
        if float(candidate["z_score"]) >= float(cfg["hard_threshold"]):
            hard_reasons.append("request_latency_z_ge_hard_threshold")
        if int(candidate.get("consecutive_high", 0)) >= int(cfg["consecutive_windows"]):
            hard_reasons.append("consecutive_request_latency_soft_alerts")
        if near_resource:
            hard_reasons.append("request_latency_with_resource_auxiliary_resonance")
        severity = "hard" if hard_reasons else "soft"
        reason = ";".join(hard_reasons) if hard_reasons else "request_latency_soft_alert"
        event = {
            "alert_id": f"alert-{len(events) + 1:04d}",
            "timestamp": float(candidate["timestamp"]),
            "service": str(candidate["service"]),
            "metric": str(candidate["metric"]),
            "severity": severity,
            "z_score": float(candidate["z_score"]),
            "value": float(candidate["value"]),
            "alert_score": float(candidate["alert_score"]),
            "reason": reason,
            "source": "metrics_robust_alert_gate",
        }
        if near_resource:
            event["resource_auxiliary_metrics"] = sorted({f"{item['service']}.{item['metric']}" for item in near_resource})
        events.append(event)
    return events


def _window_symptom_service(events: list[dict[str, Any]]) -> str:
    request_events = [event for event in events if str(event.get("metric", "")).startswith("request.")]
    candidates = request_events or events
    counts = Counter(str(event.get("service", "")) for event in candidates)
    max_z_by_service: dict[str, float] = defaultdict(float)
    max_score_by_service: dict[str, float] = defaultdict(float)
    for event in candidates:
        service = str(event.get("service", ""))
        max_z_by_service[service] = max(max_z_by_service[service], float(event.get("z_score", 0.0)))
        max_score_by_service[service] = max(max_score_by_service[service], float(event.get("alert_score", 0.0)))
    return sorted(counts, key=lambda svc: (-counts[svc], -max_z_by_service[svc], -max_score_by_service[svc], svc))[0] if counts else "unknown"


def build_alert_windows(alert_events: list[dict[str, Any]], pre_window_sec: float = 30.0, post_window_sec: float = 60.0) -> list[dict[str, Any]]:
    if not alert_events:
        return []
    sorted_events = sorted(alert_events, key=lambda item: float(item["timestamp"]))
    clusters: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_end = -math.inf
    for event in sorted_events:
        ts = float(event["timestamp"])
        start = ts - float(pre_window_sec)
        end = ts + float(post_window_sec)
        if not current or start <= current_end:
            current.append(event)
            current_end = max(current_end, end)
        else:
            clusters.append(current)
            current = [event]
            current_end = end
    if current:
        clusters.append(current)

    windows: list[dict[str, Any]] = []
    for index, events in enumerate(clusters, start=1):
        start_ts = min(float(event["timestamp"]) for event in events) - float(pre_window_sec)
        end_ts = max(float(event["timestamp"]) for event in events) + float(post_window_sec)
        severity = "hard" if any(event.get("severity") == "hard" for event in events) else "soft"
        windows.append({
            "alert_window_id": f"alert-window-{index:04d}",
            "start_ts": start_ts,
            "end_ts": end_ts,
            "symptom_service": _window_symptom_service(events),
            "trigger_metrics": sorted({f"{event['service']}.{event['metric']}" for event in events}),
            "max_z_score": max(float(event.get("z_score", 0.0)) for event in events),
            "severity": severity,
            "event_count": len(events),
            "source": "alert_gate",
            "uses_root_labels": False,
            "uses_target_config": False,
            "uses_injected_path": False,
        })
    return windows


def write_alert_outputs(input_dir: str, output_dir: str, config: dict | None = None) -> dict[str, Any]:
    cfg = {**DEFAULT_CONFIG, **(config or {})}
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    metrics = load_metrics(str(Path(input_dir) / "metrics.jsonl"))
    events = detect_alert_events(input_dir, cfg)
    windows = build_alert_windows(events, float(cfg["pre_window_sec"]), float(cfg["post_window_sec"]))
    _write_jsonl(out / "alert_events.jsonl", events)
    _write_jsonl(out / "alert_windows.jsonl", windows)
    metadata = {
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "metrics_count": len(metrics),
        "alert_events_count": len(events),
        "alert_windows_count": len(windows),
        "soft_alert_count": sum(1 for event in events if event.get("severity") == "soft"),
        "hard_alert_count": sum(1 for event in events if event.get("severity") == "hard"),
        "uses_root_labels": False,
        "uses_target_config": False,
        "uses_injected_path": False,
        "uses_incident_start_end_for_detection": False,
        "baseline_strategy": "prefix_baseline",
        "config": cfg,
    }
    _write_json(out / "alert_gate_metadata.json", metadata)
    return {"events": events, "windows": windows, "metadata": metadata}


def _overlaps(left_start: float, left_end: float, right_start: float, right_end: float) -> bool:
    return max(left_start, right_start) <= min(left_end, right_end)


def evaluate_alert_windows_for_debug(alert_windows_path: str, incidents_path: str) -> dict[str, Any]:
    windows = _read_jsonl(Path(alert_windows_path))
    incidents = _read_jsonl(Path(incidents_path))
    overlap_count = 0
    deltas: list[float] = []
    for incident in incidents:
        istart = float(incident["start_ts"])
        iend = float(incident["end_ts"])
        matches = [window for window in windows if _overlaps(float(window["start_ts"]), float(window["end_ts"]), istart, iend)]
        if matches:
            overlap_count += 1
            first = sorted(matches, key=lambda item: abs(float(item["start_ts"]) - istart))[0]
            deltas.append(float(first["start_ts"]) - istart)
    result = {
        "detected_windows": len(windows),
        "ground_truth_incidents": len(incidents),
        "overlap_count": overlap_count,
        "incident_window_recall": float(overlap_count / len(incidents)) if incidents else 0.0,
        "average_start_delta_sec": float(np.mean(np.asarray(deltas, dtype=float))) if deltas else None,
        "notes": "Debug-only overlap evaluation. Incidents are not used by alert detection.",
    }
    return result
