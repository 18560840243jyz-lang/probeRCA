"""Build final P0 RCAResult records from Step 6 and Step 7 outputs."""

from __future__ import annotations

import json
from pathlib import Path

from proberca.data.io import read_jsonl, write_jsonl


def load_required_dataset(input_dir: str | Path) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """Load required files for building P0 RCAResult records."""

    input_path = Path(input_dir)
    required = {
        "semantic_interventions": input_path / "semantic_interventions.jsonl",
        "semantic_type_scores": input_path / "semantic_type_scores.jsonl",
        "path_explanations": input_path / "path_explanations.jsonl",
        "incidents": input_path / "incidents.jsonl",
    }
    for name, required_path in required.items():
        if not required_path.exists():
            raise FileNotFoundError(f"missing required {name} file: {required_path}")
    return (
        read_jsonl(required["semantic_interventions"]),
        read_jsonl(required["semantic_type_scores"]),
        read_jsonl(required["path_explanations"]),
        read_jsonl(required["incidents"]),
    )


def canonical_type(type_name: str) -> str:
    """Normalize root-cause type strings for comparison."""

    value = str(type_name or "").strip().lower()
    padded = f" {value.replace('-', ' ').replace('_', ' ')} "
    if "cpu" in value:
        return "CPU"
    if "network" in value or "net" in value:
        return "network"
    if "lock" in value or "futex" in value:
        return "lock contention"
    if "storage" in value or "i/o" in value or padded == " io " or " io " in padded:
        return "storage I/O"
    if "memory" in value or "mem" in value:
        return "memory"
    if "load" in value:
        return "load"
    return value


def _group_top_services(semantic_records: list[dict], top_k: int) -> list[dict]:
    best_by_service: dict[str, dict] = {}
    for record in semantic_records:
        service = str(record["service"])
        score = float(record["semantic_score"])
        current = best_by_service.get(service)
        if current is None or score > float(current["score"]) or (score == float(current["score"]) and str(record["metric"]) < str(current["best_metric"])):
            best_by_service[service] = {"service": service, "score": score, "best_metric": str(record["metric"])}
    return sorted(best_by_service.values(), key=lambda item: (-float(item["score"]), item["service"]))[:top_k]


def _top_metrics(semantic_records: list[dict], top_k: int) -> list[dict]:
    records = sorted(semantic_records, key=lambda item: (int(item["semantic_rank"]), str(item["node"])))[:top_k]
    return [
        {
            "service": str(record["service"]),
            "metric": str(record["metric"]),
            "score": float(record["semantic_score"]),
            "semantic_rank": int(record["semantic_rank"]),
            "evidence_type": str(record.get("evidence_type", "Unknown")),
            "evidence_score": float(record.get("evidence_score", 0.0)),
        }
        for record in records
    ]


def _evidence_summary(top_record: dict | None) -> list[str]:
    if top_record is None:
        return []
    evidence_type = str(top_record.get("evidence_type", "Unknown"))
    metrics = ",".join(str(item) for item in top_record.get("evidence_metrics", []))
    evidence = [f"semantic evidence type: {evidence_type}"]
    if metrics:
        evidence.append(f"supporting metrics: {metrics}")
    else:
        evidence.append("supporting metrics: none")
    evidence.append(f"evidence score: {float(top_record.get('evidence_score', 0.0))}")
    return evidence


def build_rca_result_for_incident(
    semantic_records: list[dict],
    type_records: list[dict],
    path_records: list[dict],
    incident: dict,
    top_k: int = 5,
) -> dict:
    """Build one final P0 RCAResult without using root labels for ranking."""

    incident_id = str(incident["incident_id"])
    current_semantic = [row for row in semantic_records if row.get("incident_id") == incident_id]
    current_types = [row for row in type_records if row.get("incident_id") == incident_id]
    current_paths = [row for row in path_records if row.get("incident_id") == incident_id]
    if not current_semantic:
        raise ValueError(f"no semantic intervention records found for incident_id={incident_id}")

    semantic_sorted = sorted(current_semantic, key=lambda item: (int(item["semantic_rank"]), str(item["node"])))
    top_record = semantic_sorted[0]
    top_metrics = _top_metrics(current_semantic, top_k)
    top_services = _group_top_services(current_semantic, top_k)
    type_sorted = sorted(current_types, key=lambda item: (int(item["rank"]), str(item["root_type_candidate"])))
    root_type = str(type_sorted[0]["root_type_candidate"]) if type_sorted else "unknown"
    path_sorted = sorted(current_paths, key=lambda item: (-float(item["path_score"]), int(item.get("path_length", 0)), "->".join(item.get("path", []))))
    if path_sorted:
        path = [str(item) for item in path_sorted[0].get("path", [])]
    else:
        path = [str(top_record["service"])]

    return {
        "incident_id": incident_id,
        "symptom_service": str(incident["symptom_service"]),
        "top_services": top_services,
        "top_metrics": top_metrics,
        "root_type": root_type,
        "evidence": _evidence_summary(top_record),
        "path": path,
        "latency_ms": None,
    }


def build_p0_results(input_dir: str | Path, output_dir: str | Path | None = None, top_k: int = 5) -> dict:
    """Build final P0 RCAResult records for all incidents."""

    input_path = Path(input_dir)
    output_path = Path(output_dir) if output_dir is not None else input_path
    output_path.mkdir(parents=True, exist_ok=True)
    semantic_records, type_records, path_records, incidents = load_required_dataset(input_path)

    results = [build_rca_result_for_incident(semantic_records, type_records, path_records, incident, top_k) for incident in incidents]
    metadata = {
        "input_dir": str(input_path),
        "output_dir": str(output_path),
        "incidents_count": len(incidents),
        "results_count": len(results),
        "top_k": top_k,
    }
    results_path = output_path / "p0_results.jsonl"
    metadata_path = output_path / "p0_results_metadata.json"
    write_jsonl(results_path, results)
    with metadata_path.open("w", encoding="utf-8") as handle:
        json.dump(metadata, handle, ensure_ascii=False, indent=2)
    return {
        "p0_results_path": str(results_path),
        "p0_results_metadata_path": str(metadata_path),
        "results": results,
        "metadata": metadata,
    }
