"""Robust normalization for probeRCA P0 Step 3."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np

from proberca.data.io import read_jsonl, write_jsonl


@dataclass
class RobustStats:
    """Baseline robust statistics for one incident-service-instance-metric group."""

    incident_id: str
    service: str
    instance: str
    metric: str
    median: float
    mad: float
    scale: float
    baseline_count: int


@dataclass
class NormalizedMetricRecord:
    """Metric record with a robust deviation score added as z_value."""

    timestamp: float
    service: str
    instance: str
    node: str
    metric: str
    value: float
    z_value: float
    source: str
    incident_id: str | None = None


def _incident_id(incident: dict[str, Any]) -> str:
    value = incident.get("incident_id")
    if value is None:
        raise ValueError("incident record missing incident_id")
    return str(value)


def _metric_group(record: dict[str, Any]) -> tuple[str, str, str]:
    return (str(record["service"]), str(record["instance"]), str(record["metric"]))


def _is_clean_baseline(record: dict[str, Any], start_ts: float) -> bool:
    return float(record["timestamp"]) < start_ts and record.get("incident_id") is None


def _is_real_baseline(record: dict[str, Any], start_ts: float) -> bool:
    return float(record["timestamp"]) < start_ts


def compute_robust_stats(
    metric_records: list[dict],
    incident_records: list[dict],
    eps: float = 1e-6,
) -> dict[tuple[str, str, str, str], RobustStats]:
    """Compute median/MAD baseline statistics for each incident-service-instance-metric."""

    if not incident_records:
        raise ValueError("incident_records is empty; cannot compute robust stats")
    if not metric_records:
        raise ValueError("metric_records is empty; cannot compute robust stats")

    expected_groups = {_metric_group(record) for record in metric_records}
    stats: dict[tuple[str, str, str, str], RobustStats] = {}

    for incident in incident_records:
        incident_id = _incident_id(incident)
        start_ts = float(incident["start_ts"])
        clean_grouped: dict[tuple[str, str, str], list[float]] = {group: [] for group in expected_groups}
        real_grouped: dict[tuple[str, str, str], list[float]] = {group: [] for group in expected_groups}
        clean_service_metric: dict[tuple[str, str], list[float]] = {}
        real_service_metric: dict[tuple[str, str], list[float]] = {}

        for record in metric_records:
            group = _metric_group(record)
            service, _instance, metric = group
            if _is_clean_baseline(record, start_ts):
                value = float(record["value"])
                clean_grouped.setdefault(group, []).append(value)
                clean_service_metric.setdefault((service, metric), []).append(value)
            if _is_real_baseline(record, start_ts):
                value = float(record["value"])
                real_grouped.setdefault(group, []).append(value)
                real_service_metric.setdefault((service, metric), []).append(value)

        for service, instance, metric in sorted(expected_groups):
            values = clean_grouped.get((service, instance, metric), [])
            if not values:
                values = real_grouped.get((service, instance, metric), [])
            if not values:
                values = clean_service_metric.get((service, metric), [])
            if not values:
                values = real_service_metric.get((service, metric), [])
            if not values:
                continue
            array = np.asarray(values, dtype=float)
            median = float(np.median(array))
            mad = float(np.median(np.abs(array - median)))
            scale = float(1.4826 * mad + eps)
            stats[(incident_id, service, instance, metric)] = RobustStats(
                incident_id=incident_id,
                service=service,
                instance=instance,
                metric=metric,
                median=median,
                mad=mad,
                scale=scale,
                baseline_count=len(values),
            )

    return stats


def _incident_ranges(metric_records: list[dict], incident_records: list[dict]) -> list[tuple[float, float, dict]]:
    incidents = sorted(incident_records, key=lambda item: float(item["start_ts"]))
    min_timestamp = min(float(record["timestamp"]) for record in metric_records)
    ranges: list[tuple[float, float, dict]] = []
    previous_end = min_timestamp
    for incident in incidents:
        end_ts = float(incident["end_ts"])
        ranges.append((previous_end, end_ts, incident))
        previous_end = end_ts
    return ranges


def _clip_value(value: float, clip: float | None) -> float:
    if clip is None:
        return value
    return float(np.clip(value, -clip, clip))


def normalize_metrics(
    metric_records: list[dict],
    incident_records: list[dict],
    eps: float = 1e-6,
    clip: float | None = 20.0,
) -> tuple[list[dict], list[dict]]:
    """Normalize metric records into robust deviation scores."""

    stats = compute_robust_stats(metric_records, incident_records, eps=eps)
    normalized_records: list[dict] = []

    for start_ts, end_ts, incident in _incident_ranges(metric_records, incident_records):
        incident_id = _incident_id(incident)
        for record in metric_records:
            timestamp = float(record["timestamp"])
            if not (start_ts <= timestamp < end_ts):
                continue
            key = (incident_id, str(record["service"]), str(record["instance"]), str(record["metric"]))
            if key not in stats:
                continue
            robust_stats = stats[key]
            z_value = (float(record["value"]) - robust_stats.median) / robust_stats.scale
            normalized_records.append(
                asdict(
                    NormalizedMetricRecord(
                        timestamp=timestamp,
                        service=str(record["service"]),
                        instance=str(record["instance"]),
                        node=str(record["node"]),
                        metric=str(record["metric"]),
                        value=float(record["value"]),
                        z_value=_clip_value(float(z_value), clip),
                        source=str(record["source"]),
                        incident_id=record.get("incident_id"),
                    )
                )
            )

    stats_records = [asdict(item) for item in stats.values()]
    return normalized_records, stats_records


def _clip_for_metadata(clip: float | None) -> float | None:
    return None if clip is None else float(clip)


def normalize_dataset(
    input_dir: str | Path,
    output_dir: str | Path | None = None,
    eps: float = 1e-6,
    clip: float | None = 20.0,
) -> dict:
    """Normalize a generated synthetic dataset directory."""

    input_path = Path(input_dir)
    output_path = Path(output_dir) if output_dir is not None else input_path
    metrics_path = input_path / "metrics.jsonl"
    incidents_path = input_path / "incidents.jsonl"

    if not metrics_path.exists():
        raise FileNotFoundError(f"missing required input file: {metrics_path}")
    if not incidents_path.exists():
        raise FileNotFoundError(f"missing required input file: {incidents_path}")

    metric_records = read_jsonl(metrics_path)
    incident_records = read_jsonl(incidents_path)
    normalized_records, stats_records = normalize_metrics(metric_records, incident_records, eps=eps, clip=clip)

    output_path.mkdir(parents=True, exist_ok=True)
    normalized_path = output_path / "normalized_metrics.jsonl"
    stats_path = output_path / "robust_stats.jsonl"
    metadata_path = output_path / "normalization_metadata.json"

    write_jsonl(normalized_path, normalized_records)
    write_jsonl(stats_path, stats_records)

    metadata = {
        "input_dir": str(input_path),
        "output_dir": str(output_path),
        "normalized_count": len(normalized_records),
        "stats_count": len(stats_records),
        "eps": float(eps),
        "clip": _clip_for_metadata(clip),
    }
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    return {
        "normalized_metrics_path": str(normalized_path),
        "robust_stats_path": str(stats_path),
        "normalization_metadata_path": str(metadata_path),
        "metadata": metadata,
    }
