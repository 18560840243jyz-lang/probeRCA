"""Bridge real Online Boutique metric collection into P1 full-observation inputs."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from proberca.data.io import read_jsonl, write_jsonl

_REQUIRED_FILES = [
    "metrics.jsonl",
    "evidence.jsonl",
    "incidents.jsonl",
    "service_graph.jsonl",
    "metadata.json",
    "data_quality_report.json",
]


def _read_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"JSON file is not an object: {path}")
    return data


def load_real_ob_dataset(input_dir: str | Path) -> dict[str, Any]:
    """Load the real Online Boutique P2A-1R dataset files."""

    input_path = Path(input_dir)
    missing = [name for name in _REQUIRED_FILES if not (input_path / name).exists()]
    if missing:
        raise FileNotFoundError(f"missing required real Online Boutique files in {input_path}: {missing}")
    return {
        "input_dir": str(input_path),
        "metrics": read_jsonl(input_path / "metrics.jsonl"),
        "evidence": read_jsonl(input_path / "evidence.jsonl"),
        "incidents": read_jsonl(input_path / "incidents.jsonl"),
        "service_graph": read_jsonl(input_path / "service_graph.jsonl"),
        "metadata": _read_json(input_path / "metadata.json"),
        "data_quality_report": _read_json(input_path / "data_quality_report.json"),
    }


def _incident_for_timestamp(timestamp: float, incidents: list[dict]) -> tuple[str | None, bool]:
    for incident in incidents:
        start_ts = float(incident["start_ts"])
        end_ts = float(incident["end_ts"])
        if timestamp < start_ts:
            return None, True
        if start_ts <= timestamp <= end_ts:
            return str(incident["incident_id"]), False
    return None, False


def _with_observation_fields(record: dict, incidents: list[dict], sampling_probability: float) -> dict:
    timestamp = float(record["timestamp"])
    incident_id, is_baseline = _incident_for_timestamp(timestamp, incidents)
    row = dict(record)
    row["observed"] = True
    row["sampling_probability"] = float(sampling_probability)
    row["observation_mode"] = "real_observed"
    row["reason"] = "real_metric_collection"
    if is_baseline:
        row["incident_id"] = None
    elif incident_id is not None:
        row["incident_id"] = incident_id
    return row


def _sampling_record(record: dict, incidents: list[dict], sampling_probability: float) -> dict:
    row = _with_observation_fields(record, incidents, sampling_probability)
    return {
        "incident_id": row.get("incident_id"),
        "timestamp": float(row["timestamp"]),
        "service": str(row["service"]),
        "metric": str(row["metric"]),
        "sampling_probability": float(sampling_probability),
        "observed": True,
        "observation_mode": "real_observed",
        "reason": "real_metric_collection",
        "source": "real_online_boutique_collection",
    }


def _mask_record(record: dict, incidents: list[dict], sampling_probability: float) -> dict:
    row = _with_observation_fields(record, incidents, sampling_probability)
    return {
        "incident_id": row.get("incident_id"),
        "timestamp": float(row["timestamp"]),
        "service": str(row["service"]),
        "metric": str(row["metric"]),
        "observed": True,
        "sampling_probability": float(sampling_probability),
        "source": "real_online_boutique_collection",
    }


def _write_observation_files(records: list[dict], incidents: list[dict], output_path: Path, sampling_probability: float) -> dict:
    observed = [_with_observation_fields(record, incidents, sampling_probability) for record in records]
    sampling_log = [_sampling_record(record, incidents, sampling_probability) for record in records]
    mask = [_mask_record(record, incidents, sampling_probability) for record in records]
    write_jsonl(output_path / "observed_metrics.jsonl", observed)
    write_jsonl(output_path / "sampling_log.jsonl", sampling_log)
    write_jsonl(output_path / "observation_mask.jsonl", mask)
    metadata = {
        "source": "real_online_boutique_collection",
        "total_records": len(records),
        "observed_records": len(observed),
        "observed_ratio": 1.0 if records else 0.0,
        "mean_sampling_probability": float(sampling_probability),
        "real_collection": True,
        "partial_observation": False,
    }
    (output_path / "adaptive_observation_metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return metadata


def refresh_real_observation_from_normalized(output_dir: str | Path, sampling_probability: float = 1.0) -> dict:
    """Rewrite P1 observation files from normalized_metrics.jsonl after normalization."""

    output_path = Path(output_dir)
    normalized_path = output_path / "normalized_metrics.jsonl"
    incidents_path = output_path / "incidents.jsonl"
    if not normalized_path.exists():
        raise FileNotFoundError(f"missing normalized metrics for real bridge: {normalized_path}")
    if not incidents_path.exists():
        raise FileNotFoundError(f"missing incidents for real bridge: {incidents_path}")
    return _write_observation_files(read_jsonl(normalized_path), read_jsonl(incidents_path), output_path, sampling_probability)


def build_real_observation_files(input_dir: str | Path, output_dir: str | Path, sampling_probability: float = 1.0) -> dict:
    """Build full-observation P1 input files from real collected metrics.

    The first bridge pass preserves raw records. After robust normalization, callers should
    run refresh_real_observation_from_normalized so observed_metrics carries z_value.
    """

    input_path = Path(input_dir)
    output_path = Path(output_dir)
    dataset = load_real_ob_dataset(input_path)
    output_path.mkdir(parents=True, exist_ok=True)
    for name in _REQUIRED_FILES:
        src = input_path / name
        dst = output_path / name
        if src.resolve() != dst.resolve():
            shutil.copy2(src, dst)
    metadata = _write_observation_files(dataset["metrics"], dataset["incidents"], output_path, sampling_probability)
    return {
        "input_dir": str(input_path),
        "output_dir": str(output_path),
        "metadata": metadata,
        "total_records": len(dataset["metrics"]),
        "observed_records": len(dataset["metrics"]),
        "observed_ratio": metadata["observed_ratio"],
    }
