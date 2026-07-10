"""A2 blind rerun for real Online Boutique P2 experiments.

The rerun uses existing raw metrics, replaces legacy target-aware evidence with
A1 blind metric-lift evidence, and then calls the frozen P1 pipeline functions.
No fault injection code is invoked here.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import numpy as np

from proberca.adapters.online_boutique.blind_evidence import generate_blind_evidence
from proberca.adapters.online_boutique.p1_bridge import (
    build_real_observation_files,
    refresh_real_observation_from_normalized,
)
from proberca.data.io import read_jsonl, write_jsonl
from proberca.evidence.ipw_semantic import IPWSemanticEvidenceConfig, score_ipw_semantic_evidence
from proberca.eval.p1_metrics import evaluate_p1_results
from proberca.eval.p1_result import build_p1_results
from proberca.explain.ipw_path import IPWPathExplanationConfig, explain_ipw_paths
from proberca.features.robust import normalize_dataset
from proberca.inference.ipw_sparse import IPWSparseInversionConfig, solve_ipw_sparse_inversion
from proberca.propagation.ipw import IPWPropagationConfig, train_ipw_masked_propagation

COPY_INPUT_FILES = [
    "metrics.jsonl",
    "incidents.jsonl",
    "service_graph.jsonl",
    "metadata.json",
    "data_quality_report.json",
]

FAULT_TYPE_CONFIG = {
    "cpu": {
        "fault_type": "CPU",
        "source_root": "cpu_paymentservice_repeated_controlled",
        "summary_name": "p2a3_cpu_repeat_summary.json",
    },
    "network": {
        "fault_type": "Network",
        "source_root": "network_shippingservice_repeated",
        "summary_name": "p2b1_network_repeat_summary.json",
    },
    "io": {
        "fault_type": "I/O",
        "source_root": "io_rediscart_repeated",
        "summary_name": "p2c1_io_repeat_summary.json",
    },
    "lock": {
        "fault_type": "Lock",
        "source_root": "lock_cartservice_repeated_phaseaware",
        "summary_name": "p2d1r_lock_repeat_summary.json",
    },
}


def _read_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"JSON file is not an object: {path}")
    return data


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _mean(values: list[float]) -> float:
    return float(np.mean(np.asarray(values, dtype=float))) if values else 0.0


def _min(values: list[float]) -> float:
    return float(np.min(np.asarray(values, dtype=float))) if values else 0.0


def fault_type_sources(base_dir: str = "data/p2_online_boutique") -> dict[str, list[str]]:
    base = Path(base_dir)
    sources: dict[str, list[str]] = {}
    for key, config in FAULT_TYPE_CONFIG.items():
        root = base / str(config["source_root"])
        sources[key] = [str(root / f"repeat_{index:02d}" / "raw") for index in range(1, 6)]
    return sources


def _p1_evidence_type(metric: str) -> str:
    if metric.startswith("cpu."):
        return "CPU"
    if metric.startswith("net."):
        return "Net"
    if metric.startswith("io."):
        return "IO"
    if metric.startswith("lock."):
        return "Lock"
    if metric.startswith("memory."):
        return "Mem"
    if metric.startswith("request."):
        return "Load"
    return "Unknown"


def _write_p1_compatible_evidence(blind_evidence_path: Path, evidence_path: Path) -> dict[str, Any]:
    records = read_jsonl(blind_evidence_path)
    converted: list[dict[str, Any]] = []
    for record in records:
        metric = str(record.get("metric", ""))
        item = dict(record)
        item["blind_evidence_type"] = str(record.get("evidence_type", "unknown"))
        item["evidence_type"] = _p1_evidence_type(metric)
        item["value"] = float(record.get("evidence_score", record.get("value", 0.0)))
        item["source"] = "blind_metric_lift_evidence"
        converted.append(item)
    write_jsonl(evidence_path, converted)
    return {
        "blind_records": len(records),
        "p1_evidence_records": len(converted),
        "p1_evidence_types": sorted({str(item.get("evidence_type")) for item in converted}),
        "bridge_note": "evidence.jsonl is derived from blind_evidence.jsonl; type names are metric-prefix compatibility labels for frozen P1 semantic scoring.",
    }


def prepare_blind_rca_input(raw_input_dir: str, blind_output_dir: str) -> dict[str, Any]:
    raw_path = Path(raw_input_dir)
    blind_path = Path(blind_output_dir)
    if not raw_path.exists():
        raise FileNotFoundError(f"raw input directory does not exist: {raw_path}")
    if not (raw_path / "metrics.jsonl").exists():
        raise FileNotFoundError(f"missing raw metrics.jsonl: {raw_path / 'metrics.jsonl'}")
    if not (raw_path / "incidents.jsonl").exists():
        raise FileNotFoundError(f"missing raw incidents.jsonl: {raw_path / 'incidents.jsonl'}")
    if not (raw_path / "service_graph.jsonl").exists():
        raise FileNotFoundError(f"missing raw service_graph.jsonl: {raw_path / 'service_graph.jsonl'}")

    blind_path.mkdir(parents=True, exist_ok=True)
    copied_files: list[str] = []
    for name in COPY_INPUT_FILES:
        src = raw_path / name
        if src.exists():
            shutil.copy2(src, blind_path / name)
            copied_files.append(name)

    blind_result = generate_blind_evidence(str(raw_path), str(blind_path))
    compatibility = _write_p1_compatible_evidence(blind_path / "blind_evidence.jsonl", blind_path / "evidence.jsonl")
    metadata = {
        "raw_input_dir": str(raw_path),
        "blind_output_dir": str(blind_path),
        "copied_files": copied_files,
        "blind_evidence_path": str(blind_path / "blind_evidence.jsonl"),
        "p1_evidence_path": str(blind_path / "evidence.jsonl"),
        "uses_legacy_evidence": False,
        "uses_blind_evidence": True,
        "uses_root_labels_for_evidence": False,
        "uses_target_config_for_evidence": False,
        "uses_injected_path_for_evidence": False,
        "uses_incident_window": True,
        "alert_window_note": "A2 still uses incident start_ts/end_ts as the alert window; A3 implements Alert Gate.",
        "blind_evidence": blind_result,
        "p1_compatibility": compatibility,
    }
    _write_json(blind_path / "blind_input_metadata.json", metadata)
    return metadata


def _first_result_summary(results: list[dict], evaluation: dict[str, Any]) -> dict[str, Any]:
    result = results[0] if results else {}
    per = evaluation.get("per_incident", [{}])[0] if evaluation.get("per_incident") else {}
    top_metrics = result.get("top_metrics", [])
    top_services = result.get("top_services", [])
    return {
        "predicted_top1_service": str(top_services[0].get("service", "")) if top_services else "",
        "predicted_top1_metric": str(top_metrics[0].get("node", "")) if top_metrics else "",
        "predicted_root_type": str(result.get("root_type", "unknown")),
        "metric_rank_debug": per.get("metric_rank_debug"),
        "service_rank_debug": per.get("service_rank_debug"),
        "path_services": result.get("path", {}).get("path_services", []),
    }


def run_real_ob_rca(input_dir: str, output_dir: str, top_k: int = 5) -> dict[str, Any]:
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    bridge_result = build_real_observation_files(input_path, output_path, sampling_probability=1.0)
    normalize_result = normalize_dataset(output_path, output_path)
    refreshed_observation = refresh_real_observation_from_normalized(output_path, sampling_probability=1.0)
    propagation_result = train_ipw_masked_propagation(output_path, output_path, IPWPropagationConfig())
    sparse_result = solve_ipw_sparse_inversion(output_path, output_path, IPWSparseInversionConfig())
    semantic_result = score_ipw_semantic_evidence(output_path, output_path, IPWSemanticEvidenceConfig())
    path_result = explain_ipw_paths(output_path, output_path, IPWPathExplanationConfig(top_k_candidates=top_k))
    p1_result = build_p1_results(output_path, output_path, top_k=top_k)

    incidents = read_jsonl(output_path / "incidents.jsonl")
    results = read_jsonl(output_path / "p1_results.jsonl")
    path_summary = _read_json(output_path / "ipw_path_explanation_summary.json")
    evaluation = evaluate_p1_results(results, incidents, path_summary=path_summary)
    _write_json(output_path / "p1_evaluation_summary.json", evaluation)
    first = _first_result_summary(results, evaluation)
    blind_metadata = _read_json(input_path / "blind_input_metadata.json") if (input_path / "blind_input_metadata.json").exists() else {}
    summary = {
        "input_dir": str(input_path),
        "output_dir": str(output_path),
        "real_collection": True,
        "blind_rerun": True,
        "incident_count": int(evaluation["incidents_count"]),
        "service_hit_at_1": float(evaluation["service_hit_at_1"]),
        "service_hit_at_3": float(evaluation["service_hit_at_3"]),
        "metric_hit_at_1": float(evaluation["metric_hit_at_1"]),
        "metric_hit_at_3": float(evaluation["metric_hit_at_3"]),
        "metric_mrr": float(evaluation["metric_mrr"]),
        "root_type_accuracy": float(evaluation["root_type_accuracy"]),
        "path_fidelity": float(evaluation["path_fidelity"]),
        "observed_ratio": float(evaluation.get("observed_ratio", 0.0)),
        "uses_blind_evidence": bool(blind_metadata.get("uses_blind_evidence", True)),
        "uses_legacy_evidence": bool(blind_metadata.get("uses_legacy_evidence", False)),
        **first,
    }
    _write_json(output_path / "real_p1_rca_summary.json", summary)
    metadata = {
        "input_dir": str(input_path),
        "output_dir": str(output_path),
        "top_k": int(top_k),
        "blind_rerun": True,
        "uses_blind_evidence": summary["uses_blind_evidence"],
        "uses_legacy_evidence": summary["uses_legacy_evidence"],
        "pipeline_steps": [
            "build_real_observation_files",
            "normalize_dataset",
            "refresh_real_observation_from_normalized",
            "train_ipw_masked_propagation",
            "solve_ipw_sparse_inversion",
            "score_ipw_semantic_evidence",
            "explain_ipw_paths",
            "build_p1_results",
            "evaluate_p1_results",
        ],
    }
    _write_json(output_path / "real_p1_rca_metadata.json", metadata)
    return {
        "summary": summary,
        "evaluation": evaluation,
        "results": results,
        "step_outputs": {
            "build_real_observation_files": bridge_result,
            "normalize_dataset": normalize_result.get("metadata", {}),
            "refresh_real_observation_from_normalized": refreshed_observation,
            "train_ipw_masked_propagation": propagation_result.get("metadata", {}),
            "solve_ipw_sparse_inversion": sparse_result.get("metadata", {}),
            "score_ipw_semantic_evidence": semantic_result.get("metadata", {}),
            "explain_ipw_paths": path_result.get("metadata", {}),
            "build_p1_results": p1_result.get("metadata", {}),
        },
    }


def run_single_blind_repeat(raw_input_dir: str, blind_output_dir: str, top_k: int = 5) -> dict[str, Any]:
    repeat_root = Path(blind_output_dir)
    blind_input_dir = repeat_root / "input"
    rca_output_dir = repeat_root / "p1rca"
    input_metadata = prepare_blind_rca_input(raw_input_dir, str(blind_input_dir))
    rca_result = run_real_ob_rca(str(blind_input_dir), str(rca_output_dir), top_k=top_k)
    summary = rca_result["summary"]
    repeat_summary = {
        "raw_input_dir": str(Path(raw_input_dir)),
        "blind_output_dir": str(repeat_root),
        "blind_input_dir": str(blind_input_dir),
        "rca_output_dir": str(rca_output_dir),
        "predicted_top1_service": summary.get("predicted_top1_service"),
        "predicted_top1_metric": summary.get("predicted_top1_metric"),
        "predicted_root_type": summary.get("predicted_root_type"),
        "metric_rank_debug": summary.get("metric_rank_debug"),
        "service_hit_at_1": float(summary.get("service_hit_at_1", 0.0)),
        "metric_hit_at_1": float(summary.get("metric_hit_at_1", 0.0)),
        "metric_hit_at_3": float(summary.get("metric_hit_at_3", 0.0)),
        "metric_mrr": float(summary.get("metric_mrr", 0.0)),
        "root_type_accuracy": float(summary.get("root_type_accuracy", 0.0)),
        "path_fidelity": float(summary.get("path_fidelity", 0.0)),
        "uses_blind_evidence": bool(input_metadata.get("uses_blind_evidence")),
        "uses_legacy_evidence": bool(input_metadata.get("uses_legacy_evidence")),
        "rca_succeeded": True,
    }
    _write_json(repeat_root / "blind_repeat_summary.json", repeat_summary)
    return repeat_summary


def _fault_summary(fault_type: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "fault_type": fault_type,
        "repeats": len(rows),
        "service_hit_at_1_mean": _mean([float(row.get("service_hit_at_1", 0.0)) for row in rows]),
        "metric_hit_at_3_mean": _mean([float(row.get("metric_hit_at_3", 0.0)) for row in rows]),
        "root_type_accuracy_mean": _mean([float(row.get("root_type_accuracy", 0.0)) for row in rows]),
        "path_fidelity_mean": _mean([float(row.get("path_fidelity", 0.0)) for row in rows]),
        "auxiliary_metric_hit_at_1_mean": _mean([float(row.get("metric_hit_at_1", 0.0)) for row in rows]),
        "auxiliary_metric_mrr_mean": _mean([float(row.get("metric_mrr", 0.0)) for row in rows]),
        "service_hit_at_1_min": _min([float(row.get("service_hit_at_1", 0.0)) for row in rows]),
        "metric_hit_at_3_min": _min([float(row.get("metric_hit_at_3", 0.0)) for row in rows]),
    }


def _overall(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "total_repeats": 20,
        "total_completed": len(rows),
        "total_successful_rca": sum(1 for row in rows if row.get("rca_succeeded")),
        "service_hit_at_1_overall": _mean([float(row.get("service_hit_at_1", 0.0)) for row in rows]),
        "metric_hit_at_3_overall": _mean([float(row.get("metric_hit_at_3", 0.0)) for row in rows]),
        "root_type_accuracy_overall": _mean([float(row.get("root_type_accuracy", 0.0)) for row in rows]),
        "path_fidelity_overall": _mean([float(row.get("path_fidelity", 0.0)) for row in rows]),
        "auxiliary_metric_hit_at_1_overall": _mean([float(row.get("metric_hit_at_1", 0.0)) for row in rows]),
        "auxiliary_metric_mrr_overall": _mean([float(row.get("metric_mrr", 0.0)) for row in rows]),
    }


def run_p2_blind_rerun(output_dir: str = "data/p2_online_boutique/blind_rerun", top_k: int = 5) -> dict[str, Any]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    sources = fault_type_sources()
    per_repeat: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for key, raw_dirs in sources.items():
        fault_type = str(FAULT_TYPE_CONFIG[key]["fault_type"])
        for index, raw_dir in enumerate(raw_dirs, start=1):
            repeat_root = output_path / key / f"repeat_{index:02d}"
            try:
                raw_path = Path(raw_dir)
                for required in ["metrics.jsonl", "incidents.jsonl", "service_graph.jsonl"]:
                    if not (raw_path / required).exists():
                        raise FileNotFoundError(f"missing required raw file: {raw_path / required}")
                row = run_single_blind_repeat(str(raw_path), str(repeat_root), top_k=top_k)
                row["fault_type"] = fault_type
                row["fault_type_key"] = key
                row["repeat_index"] = index
                per_repeat.append(row)
            except Exception as exc:  # preserve failure without pretending success
                failures.append({
                    "fault_type": fault_type,
                    "fault_type_key": key,
                    "repeat_index": index,
                    "raw_input_dir": raw_dir,
                    "blind_output_dir": str(repeat_root),
                    "error": str(exc),
                })

    per_fault_type = {}
    for key, config in FAULT_TYPE_CONFIG.items():
        rows = [row for row in per_repeat if row.get("fault_type_key") == key]
        per_fault_type[key] = _fault_summary(str(config["fault_type"]), rows)

    overall = _overall(per_repeat)
    summary = {
        **overall,
        "per_fault_type": per_fault_type,
        "per_repeat": per_repeat,
        "uses_blind_evidence": all(row.get("uses_blind_evidence") is True for row in per_repeat) if per_repeat else False,
        "uses_legacy_evidence": any(row.get("uses_legacy_evidence") is True for row in per_repeat),
    }
    metadata = {
        "output_dir": str(output_path),
        "top_k": int(top_k),
        "sources": sources,
        "blind_rerun": True,
        "uses_existing_raw_metrics": True,
        "reinjects_faults": False,
        "modifies_p1_scoring": False,
        "uses_incident_window": True,
        "alert_window_note": "A2 is alert-window-aware blind rerun; A3 implements Alert Gate.",
    }
    _write_json(output_path / "p2_blind_rerun_summary.json", summary)
    _write_json(output_path / "p2_blind_rerun_metadata.json", metadata)
    _write_json(output_path / "p2_blind_rerun_failures.json", {"failures": failures, "failed_count": len(failures)})
    return {"summary": summary, "metadata": metadata, "failures": failures}
