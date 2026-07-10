"""B2 integrated replay over existing Online Boutique raw metrics.

This module orchestrates the B1R integrated blind RCA pipeline over existing raw
metrics only. Incident labels are read only after final integrated results are
written, for post-hoc evaluation.
"""

from __future__ import annotations

import json
from pathlib import Path
from statistics import mean
from typing import Any

from proberca.adapters.online_boutique.integrated_pipeline import run_integrated_blind_rca


FAULT_SOURCES = {
    "cpu": "data/p2_online_boutique/cpu_paymentservice_repeated_controlled",
    "network": "data/p2_online_boutique/network_shippingservice_repeated",
    "io": "data/p2_online_boutique/io_rediscart_repeated",
    "lock": "data/p2_online_boutique/lock_cartservice_repeated_phaseaware",
}


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            item = json.loads(text)
            if isinstance(item, dict):
                rows.append(item)
    return rows


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _metric_node(service: str, metric: str) -> str:
    if not metric:
        return ""
    return metric if "." in metric and metric.startswith(f"{service}.") else f"{service}.{metric}"


def _normalize_root_type(value: Any) -> str:
    text = str(value or "").strip().lower()
    if "cpu" in text:
        return "CPU"
    if "net" in text or "network" in text:
        return "network"
    if "io" in text or "i/o" in text or "storage" in text:
        return "storage I/O"
    if "lock" in text or "contention" in text:
        return "lock contention"
    if "memory" in text:
        return "memory"
    if "load" in text or "request" in text or "latency" in text:
        return "load"
    return text or "unknown"


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _mean_present(values: list[float | None]) -> float | None:
    present = [float(v) for v in values if v is not None]
    return mean(present) if present else None


def fault_type_sources() -> dict[str, list[str]]:
    return {
        fault_type: [str(Path(root) / f"repeat_{idx:02d}" / "raw") for idx in range(1, 6)]
        for fault_type, root in FAULT_SOURCES.items()
    }


def _select_result(result_dir: Path) -> tuple[dict[str, Any], str]:
    aggregate_path = result_dir / "09_final_result" / "integrated_rca_aggregate.json"
    results_path = result_dir / "09_final_result" / "integrated_rca_results.jsonl"
    aggregate = _read_json(aggregate_path)
    result = aggregate.get("result") if isinstance(aggregate.get("result"), dict) else None
    if result:
        return result, "aggregate:highest_confidence_window"
    rows = _read_jsonl(results_path)
    if not rows:
        return {}, "missing_result"
    selected = max(rows, key=lambda item: (float(item.get("confidence") or 0.0), str(item.get("alert_window_id") or "")))
    return selected, "per_window_highest_confidence"


def _metric_hit_metrics(metrics: list[str], root_node: str, root_metric: str, top_k: int) -> tuple[float, float, float, str]:
    metric_parts = [metric.split(".", 1)[1] if "." in metric else metric for metric in metrics]
    metric_hit_1 = 0.0
    metric_hit_k = 0.0
    metric_mrr = 0.0
    match_mode = "service_metric_node"
    for rank, metric in enumerate(metrics, start=1):
        if metric == root_node:
            if rank == 1:
                metric_hit_1 = 1.0
            if rank <= top_k:
                metric_hit_k = 1.0
            metric_mrr = 1.0 / rank
            match_mode = "service.metric"
            break
    if metric_mrr == 0.0 and root_metric:
        for rank, metric_part in enumerate(metric_parts, start=1):
            if metric_part == root_metric:
                if rank == 1:
                    metric_hit_1 = 1.0
                if rank <= top_k:
                    metric_hit_k = 1.0
                metric_mrr = 1.0 / rank
                match_mode = "metric_only"
                break
    return metric_hit_1, metric_hit_k, metric_mrr, match_mode


def evaluate_integrated_result(result_dir: str, incidents_path: str) -> dict[str, Any]:
    """Post-hoc evaluation only; labels are read after final results exist."""
    result_root = Path(result_dir)
    selected, source = _select_result(result_root)
    incidents = _read_jsonl(Path(incidents_path))
    if not selected or not incidents:
        return {
            "service_hit_at_1": None,
            "metric_hit_at_1": None,
            "metric_hit_at_3": None,
            "service_conditioned_metric_hit_at_1": None,
            "service_conditioned_metric_hit_at_3": None,
            "global_metric_hit_at_3_auxiliary": None,
            "service_metric_pair_hit_at_1": None,
            "metric_mrr": None,
            "root_type_accuracy": None,
            "path_fidelity": None,
            "selected_result_source": source,
            "match_mode": "unavailable",
            "evaluation_uses_labels": True,
            "labels_used_only_after_result": True,
            "evaluation_notes": "missing selected result or incidents",
        }

    incident = incidents[0]
    root_service = str(incident.get("root_service") or "")
    root_metric = str(incident.get("root_metric") or "")
    root_node = _metric_node(root_service, root_metric)
    top_service = str(selected.get("predicted_top1_service") or "")
    primary_top_metrics = [str(row.get("node_id") or row.get("metric") or "") for row in selected.get("top_metrics", []) if isinstance(row, dict)]
    global_top_metrics = [str(row.get("node_id") or row.get("metric") or "") for row in selected.get("global_top_metrics_auxiliary", []) if isinstance(row, dict)]

    service_hit = 1.0 if top_service == root_service else 0.0
    primary_hit_1, primary_hit_3, primary_mrr, primary_match_mode = _metric_hit_metrics(primary_top_metrics, root_node, root_metric, 3)
    if service_hit != 1.0:
        service_conditioned_metric_hit_1 = 0.0
        service_conditioned_metric_hit_3 = 0.0
        service_conditioned_mrr = 0.0
        match_mode = "service_miss_metric_not_counted"
    else:
        service_conditioned_metric_hit_1 = primary_hit_1
        service_conditioned_metric_hit_3 = primary_hit_3
        service_conditioned_mrr = primary_mrr
        match_mode = primary_match_mode
    global_metric_hit_1, global_metric_hit_3, global_metric_mrr, global_match_mode = _metric_hit_metrics(global_top_metrics, root_node, root_metric, 3)
    service_metric_pair_hit_at_1 = 1.0 if service_hit == 1.0 and service_conditioned_metric_hit_1 == 1.0 else 0.0

    predicted_type = _normalize_root_type(selected.get("predicted_root_type"))
    root_type = _normalize_root_type(incident.get("root_type"))
    root_type_accuracy = 1.0 if predicted_type == root_type else 0.0

    selected_path = [str(item) for item in selected.get("path", [])]
    injected_path_raw = incident.get("injected_path")
    injected_path: list[str]
    if isinstance(injected_path_raw, list):
        injected_path = [str(item) for item in injected_path_raw]
    elif isinstance(injected_path_raw, str) and injected_path_raw:
        injected_path = [part.strip() for part in injected_path_raw.split("->") if part.strip()]
    else:
        injected_path = []
    if injected_path:
        injected_services = [item.split(".", 1)[0] for item in injected_path]
        path_fidelity = 1.0 if (selected_path == injected_services or root_service in selected_path or bool(set(selected_path) & set(injected_services))) else 0.0
    else:
        path_fidelity = None

    return {
        "service_hit_at_1": service_hit,
        "metric_hit_at_1": service_conditioned_metric_hit_1,
        "metric_hit_at_3": service_conditioned_metric_hit_3,
        "primary_metric_hit_at_3": service_conditioned_metric_hit_3,
        "service_conditioned_metric_hit_at_1": service_conditioned_metric_hit_1,
        "service_conditioned_metric_hit_at_3": service_conditioned_metric_hit_3,
        "global_metric_hit_at_1_auxiliary": global_metric_hit_1,
        "global_metric_hit_at_3_auxiliary": global_metric_hit_3,
        "global_metric_mrr_auxiliary": global_metric_mrr,
        "service_metric_pair_hit_at_1": service_metric_pair_hit_at_1,
        "metric_mrr": service_conditioned_mrr,
        "root_type_accuracy": root_type_accuracy,
        "path_fidelity": path_fidelity,
        "selected_result_source": source,
        "match_mode": match_mode,
        "global_match_mode_auxiliary": global_match_mode,
        "evaluation_uses_labels": True,
        "labels_used_only_after_result": True,
        "root_service_debug": root_service,
        "root_metric_debug": root_metric,
        "root_type_debug": root_type,
        "selected_top1_service": top_service,
        "selected_top1_metric": primary_top_metrics[0] if primary_top_metrics else selected.get("predicted_top1_metric"),
        "predicted_root_type": predicted_type,
    }

def run_single_integrated_replay(
    fault_type: str,
    repeat_index: int,
    raw_input_dir: str,
    output_root: str,
    debug_evaluate_incidents: bool = True,
) -> dict[str, Any]:
    repeat_output = Path(output_root) / fault_type / f"repeat_{repeat_index:02d}"
    result = run_integrated_blind_rca(raw_input_dir, str(repeat_output), debug_evaluate_incidents=debug_evaluate_incidents)
    final_dir = repeat_output / "09_final_result"
    integrated_md = _read_json(final_dir / "integrated_rca_metadata.json")
    evaluation = evaluate_integrated_result(str(repeat_output), str(Path(raw_input_dir) / "incidents.jsonl"))
    _write_json(final_dir / "integrated_replay_evaluation.json", evaluation)
    return {
        "fault_type": fault_type,
        "repeat_index": repeat_index,
        "raw_input_dir": raw_input_dir,
        "output_dir": str(repeat_output),
        "alert_windows_count": integrated_md.get("alert_windows_count"),
        "final_results_count": integrated_md.get("final_results_count"),
        "selected_top1_service": evaluation.get("selected_top1_service"),
        "selected_top1_metric": evaluation.get("selected_top1_metric"),
        "predicted_root_type": evaluation.get("predicted_root_type"),
        "service_hit_at_1": evaluation.get("service_hit_at_1"),
        "metric_hit_at_1": evaluation.get("metric_hit_at_1"),
        "metric_hit_at_3": evaluation.get("metric_hit_at_3"),
        "primary_metric_hit_at_3": evaluation.get("primary_metric_hit_at_3"),
        "service_conditioned_metric_hit_at_1": evaluation.get("service_conditioned_metric_hit_at_1"),
        "service_conditioned_metric_hit_at_3": evaluation.get("service_conditioned_metric_hit_at_3"),
        "global_metric_hit_at_3_auxiliary": evaluation.get("global_metric_hit_at_3_auxiliary"),
        "service_metric_pair_hit_at_1": evaluation.get("service_metric_pair_hit_at_1"),
        "metric_mrr": evaluation.get("metric_mrr"),
        "root_type_accuracy": evaluation.get("root_type_accuracy"),
        "path_fidelity": evaluation.get("path_fidelity"),
        "labels_used_only_after_result": evaluation.get("labels_used_only_after_result"),
        "uses_root_labels_for_inference": False,
        "uses_target_config_for_inference": False,
        "uses_injected_path_for_inference": False,
        "uses_incident_start_end_for_inference": False,
        "uses_legacy_evidence": False,
        "runs_old_p1_rca": False,
        "reinjects_faults": False,
        "actual_probe_activation": False,
        "integrated_metadata": integrated_md,
        "evaluation": evaluation,
    }


def _summarize_fault_type(fault_type: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "fault_type": fault_type,
        "repeats": len(rows),
        "repeats_completed": len(rows),
        "service_hit_at_1_mean": _mean_present([_safe_float(row.get("service_hit_at_1")) for row in rows]),
        "metric_hit_at_3_mean": _mean_present([_safe_float(row.get("metric_hit_at_3")) for row in rows]),
        "primary_metric_hit_at_3_mean": _mean_present([_safe_float(row.get("primary_metric_hit_at_3")) for row in rows]),
        "service_conditioned_metric_hit_at_1_mean": _mean_present([_safe_float(row.get("service_conditioned_metric_hit_at_1")) for row in rows]),
        "service_conditioned_metric_hit_at_3_mean": _mean_present([_safe_float(row.get("service_conditioned_metric_hit_at_3")) for row in rows]),
        "global_metric_hit_at_3_auxiliary_mean": _mean_present([_safe_float(row.get("global_metric_hit_at_3_auxiliary")) for row in rows]),
        "service_metric_pair_hit_at_1_mean": _mean_present([_safe_float(row.get("service_metric_pair_hit_at_1")) for row in rows]),
        "root_type_accuracy_mean": _mean_present([_safe_float(row.get("root_type_accuracy")) for row in rows]),
        "path_fidelity_mean": _mean_present([_safe_float(row.get("path_fidelity")) for row in rows]),
        "auxiliary_metric_hit_at_1_mean": _mean_present([_safe_float(row.get("metric_hit_at_1")) for row in rows]),
        "auxiliary_metric_mrr_mean": _mean_present([_safe_float(row.get("metric_mrr")) for row in rows]),
    }

def run_p2_integrated_replay(
    output_dir: str = "data/p2_online_boutique/b2_integrated_replay",
    debug_evaluate_incidents: bool = True,
) -> dict[str, Any]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    per_repeat: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    sources = fault_type_sources()
    for fault_type, raw_dirs in sources.items():
        for repeat_index, raw_dir in enumerate(raw_dirs, start=1):
            try:
                for required in ("metrics.jsonl", "service_graph.jsonl", "incidents.jsonl"):
                    path = Path(raw_dir) / required
                    if not path.exists():
                        raise FileNotFoundError(f"missing {path}")
                summary = run_single_integrated_replay(fault_type, repeat_index, raw_dir, str(out), debug_evaluate_incidents=debug_evaluate_incidents)
                per_repeat.append(summary)
            except Exception as exc:  # keep replay running and record failure.
                failures.append({"fault_type": fault_type, "repeat_index": repeat_index, "raw_input_dir": raw_dir, "error": repr(exc)})

    per_fault_type = [_summarize_fault_type(ft, [row for row in per_repeat if row.get("fault_type") == ft]) for ft in sources]
    summary = {
        "total_repeats": sum(len(items) for items in sources.values()),
        "repeats_completed": len(per_repeat),
        "repeats_failed": len(failures),
        "service_hit_at_1_overall": _mean_present([_safe_float(row.get("service_hit_at_1")) for row in per_repeat]),
        "metric_hit_at_3_overall": _mean_present([_safe_float(row.get("metric_hit_at_3")) for row in per_repeat]),
        "primary_metric_hit_at_3_overall": _mean_present([_safe_float(row.get("primary_metric_hit_at_3")) for row in per_repeat]),
        "service_conditioned_metric_hit_at_1_overall": _mean_present([_safe_float(row.get("service_conditioned_metric_hit_at_1")) for row in per_repeat]),
        "service_conditioned_metric_hit_at_3_overall": _mean_present([_safe_float(row.get("service_conditioned_metric_hit_at_3")) for row in per_repeat]),
        "global_metric_hit_at_3_auxiliary_overall": _mean_present([_safe_float(row.get("global_metric_hit_at_3_auxiliary")) for row in per_repeat]),
        "service_metric_pair_hit_at_1_overall": _mean_present([_safe_float(row.get("service_metric_pair_hit_at_1")) for row in per_repeat]),
        "root_type_accuracy_overall": _mean_present([_safe_float(row.get("root_type_accuracy")) for row in per_repeat]),
        "path_fidelity_overall": _mean_present([_safe_float(row.get("path_fidelity")) for row in per_repeat]),
        "auxiliary_metric_hit_at_1_overall": _mean_present([_safe_float(row.get("metric_hit_at_1")) for row in per_repeat]),
        "auxiliary_metric_mrr_overall": _mean_present([_safe_float(row.get("metric_mrr")) for row in per_repeat]),
        "per_fault_type": per_fault_type,
        "per_repeat": per_repeat,
        "uses_root_labels_for_inference": False,
        "uses_target_config_for_inference": False,
        "uses_injected_path_for_inference": False,
        "uses_incident_start_end_for_inference": False,
        "uses_legacy_evidence": False,
        "runs_old_p1_rca": False,
        "reinjects_faults": False,
        "actual_probe_activation": False,
        "evaluation_uses_labels_posthoc": True,
        "source": "b2_integrated_replay_existing_raw_metrics",
    }
    metadata = {
        "output_dir": str(out),
        "debug_evaluate_incidents": debug_evaluate_incidents,
        "total_repeats": summary["total_repeats"],
        "repeats_completed": summary["repeats_completed"],
        "repeats_failed": summary["repeats_failed"],
        "uses_root_labels_for_inference": False,
        "uses_target_config_for_inference": False,
        "uses_injected_path_for_inference": False,
        "uses_incident_start_end_for_inference": False,
        "uses_legacy_evidence": False,
        "runs_old_p1_rca": False,
        "reinjects_faults": False,
        "actual_probe_activation": False,
        "evaluation_uses_labels_posthoc": True,
        "labels_used_only_after_result": True,
        "source": "b2_integrated_replay_existing_raw_metrics",
    }
    _write_json(out / "p2_integrated_replay_summary.json", summary)
    _write_json(out / "p2_integrated_replay_failures.json", {"failures": failures})
    _write_json(out / "p2_integrated_replay_metadata.json", metadata)
    return {"summary": summary, "failures": failures, "metadata": metadata}
