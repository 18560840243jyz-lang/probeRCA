from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from proberca.adapters.online_boutique.service_metric_identity import split_node_id


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if text:
                item = json.loads(text)
                if isinstance(item, dict):
                    rows.append(item)
    return rows


def _service_from_node(node_id: str) -> str:
    return split_node_id(node_id)[0]


def _family_to_root_type(family: str) -> str:
    return {"CPU": "CPU", "network": "network", "storage I/O": "storage I/O", "lock contention": "lock contention", "memory": "memory", "load": "load"}.get(family, "unknown")


def check_b1_integrated_pipeline(input_dir: str) -> dict[str, Any]:
    root = Path(input_dir)
    failed: list[str] = []
    required = [
        root / "01_alert_gate" / "alert_windows.jsonl",
        root / "02_blind_evidence" / "blind_evidence.jsonl",
        root / "02_blind_evidence" / "blind_evidence_metadata.json",
        root / "03_candidate_subgraph" / "repeat_candidate_summary.json",
        root / "04_probe_policy" / "sampling_log.jsonl",
        root / "04_probe_policy" / "observation_mask.jsonl",
        root / "05_ipw_rls" / "ipw_rls_metadata.json",
        root / "05b_structured_propagation" / "structured_propagation_metadata.json",
        root / "06_evidence_channel" / "calibrated_residuals.jsonl",
        root / "07_graph_sparse" / "metric_scores.jsonl",
        root / "07_graph_sparse" / "service_scores.jsonl",
        root / "08_counterfactual" / "counterfactual_metadata.json",
        root / "09_final_result" / "metric_candidate_table.jsonl",
        root / "09_final_result" / "top_services.jsonl",
        root / "09_final_result" / "integrated_rca_results.jsonl",
        root / "09_final_result" / "integrated_rca_metadata.json",
    ]
    for path in required:
        if not path.exists():
            failed.append(f"missing required artifact: {path}")

    ev_md = _read_json(root / "02_blind_evidence" / "blind_evidence_metadata.json")
    if ev_md.get("uses_alert_windows") is not True:
        failed.append("blind evidence must use alert windows")
    if ev_md.get("uses_incident_start_end") is not False:
        failed.append("blind evidence must not use incident start/end")

    prop_md = _read_json(root / "05b_structured_propagation" / "structured_propagation_metadata.json")
    if prop_md.get("structured_propagation_model") != "structured_multilag_ridge" and prop_md.get("propagation_model") != "structured_multilag_ridge":
        failed.append("structured propagation model must be structured_multilag_ridge")
    if prop_md.get("stable_only") is not True:
        failed.append("structured propagation stable_only must be true")
    if prop_md.get("propagation_drift_used") is not False:
        failed.append("structured propagation_drift_used must be false")
    if prop_md.get("uses_root_labels") is not False:
        failed.append("structured propagation uses_root_labels must be false")
    if prop_md.get("uses_incident_start_end") is not False:
        failed.append("structured propagation uses_incident_start_end must be false")

    md = _read_json(root / "09_final_result" / "integrated_rca_metadata.json")
    expected_false = ["uses_root_labels", "uses_target_config", "uses_injected_path", "uses_incident_start_end", "uses_legacy_evidence", "runs_old_p1_rca"]
    for key in expected_false:
        if md.get(key) is not False:
            failed.append(f"integrated metadata {key} must be false")
    if md.get("uses_alert_windows") is not True:
        failed.append("integrated metadata uses_alert_windows must be true")
    if md.get("primary_candidate_source") != "service_candidate_table":
        failed.append("primary_candidate_source must be service_candidate_table")
    if md.get("top_service_metric_consistent") is not True:
        failed.append("top_service_metric_consistent must be true")
    if md.get("per_window_results_match_alert_windows") is not True:
        failed.append("per_window_results_match_alert_windows must be true")
    if md.get("service_first_enabled") is not True:
        failed.append("service_first_enabled must be true")
    if md.get("primary_service_source") != "service_candidate_table":
        failed.append("primary_service_source must be service_candidate_table")
    if md.get("primary_metric_source") != "metric_candidates_within_root_service":
        failed.append("primary_metric_source must be metric_candidates_within_root_service")
    if md.get("global_top_metrics_primary") is not False:
        failed.append("global_top_metrics_primary must be false")
    if md.get("structured_propagation_enabled") is not True:
        failed.append("structured_propagation_enabled must be true")
    if md.get("structured_propagation_uses_labels") is not False:
        failed.append("structured_propagation_uses_labels must be false")
    if md.get("structured_propagation_uses_injected_path") is not False:
        failed.append("structured_propagation_uses_injected_path must be false")
    if md.get("propagation_drift_used") is not False:
        failed.append("integrated propagation_drift_used must be false")
    if md.get("service_local_support_used") is not True:
        failed.append("service_local_support_used must be true")
    if md.get("global_family_support_weight_limited") is not True:
        failed.append("global_family_support_weight_limited must be true")
    if "ownership_invalid_count" not in md:
        failed.append("ownership_invalid_count must be recorded")
    if md.get("primary_candidate_ownership_valid") is not True:
        failed.append("primary_candidate_ownership_valid must be true")

    windows = _read_jsonl(root / "01_alert_gate" / "alert_windows.jsonl")
    results = _read_jsonl(root / "09_final_result" / "integrated_rca_results.jsonl")
    if len(results) != len(windows):
        failed.append("per_window_results_count must equal alert_windows_count")
    if md.get("per_window_results_count") != md.get("alert_windows_count"):
        failed.append("metadata per_window_results_count must equal alert_windows_count")

    for idx, result in enumerate(results, start=1):
        top1_service = str(result.get("predicted_top1_service", ""))
        top1_metric = str(result.get("predicted_top1_metric", ""))
        if top1_service != _service_from_node(top1_metric):
            failed.append(f"result {idx} top1_service and top1_metric service mismatch")
        primary = result.get("primary_candidate") or {}
        expected_type = _family_to_root_type(str(primary.get("metric_family", "unknown")))
        if result.get("predicted_root_type") != expected_type:
            failed.append(f"result {idx} predicted_root_type mismatch")
        if result.get("root_type_source") != "primary_metric_family":
            failed.append(f"result {idx} root_type_source must be primary_metric_family")
        if result.get("root_type_uses_labels") is not False:
            failed.append(f"result {idx} root_type_uses_labels must be false")
        if result.get("service_first") is not True:
            failed.append(f"result {idx} service_first must be true")
        if result.get("primary_metric_conditioned_on_service") is not True:
            failed.append(f"result {idx} primary_metric_conditioned_on_service must be true")
        if result.get("global_top_metrics_primary") is not False:
            failed.append(f"result {idx} global_top_metrics_primary must be false")
        components = primary.get("score_components") if isinstance(primary.get("score_components"), dict) else {}
        if primary.get("ownership_valid") is not True:
            failed.append(f"result {idx} primary_candidate ownership_valid must be true")
        if primary.get("service_matches_node_id") is not True:
            failed.append(f"result {idx} primary_candidate service_matches_node_id must be true")
        if "diagnostic_specificity" not in components:
            failed.append(f"result {idx} primary score_components must include diagnostic_specificity")
        for component_key in ("structured_propagation_support", "path_edge_support", "lag_support"):
            if component_key not in components:
                failed.append(f"result {idx} primary score_components must include {component_key}")
        for component_key in ("node_evidence_support", "service_family_evidence_support", "family_global_evidence_support"):
            if component_key not in components:
                failed.append(f"result {idx} primary score_components must include {component_key}")
        if float(components.get("family_global_evidence_weight", 1.0) or 1.0) > 0.20:
            failed.append(f"result {idx} family_global_evidence_weight must be <= 0.20")
        if str(primary.get("metric")) == "memory.usage" and "weak_memory_usage_penalty_applied" not in components:
            failed.append(f"result {idx} memory.usage primary must record weak_memory_usage_penalty_applied")
        top_metrics = result.get("top_metrics") if isinstance(result.get("top_metrics"), list) else []
        top_services = result.get("top_services") if isinstance(result.get("top_services"), list) else []
        if top_metrics and top_services and top_metrics[0].get("service") != top_services[0].get("service"):
            failed.append(f"result {idx} top_metrics[0].service must equal top_services[0].service")
        for metric_idx, metric_row in enumerate(top_metrics, start=1):
            if metric_row.get("service") != top1_service:
                failed.append(f"result {idx} top_metrics[{metric_idx}].service must equal top1_service")
        aux = result.get("global_top_metrics_auxiliary") if isinstance(result.get("global_top_metrics_auxiliary"), list) else []
        for aux_idx, aux_row in enumerate(aux, start=1):
            if aux_row.get("auxiliary") is not True:
                failed.append(f"result {idx} global_top_metrics_auxiliary[{aux_idx}] must be auxiliary")
        path = result.get("path_explanation") or {}
        if path.get("path_root_service") != top1_service:
            failed.append(f"result {idx} path root service must equal top1_service")
        if path.get("path_uses_injected_path") is not False:
            failed.append(f"result {idx} path must not use injected path")
        safety = result.get("label_safety") or {}
        for key in ("uses_root_labels", "uses_target_config", "uses_injected_path", "uses_incident_start_end", "uses_legacy_evidence", "runs_old_p1_rca"):
            if safety.get(key) is not False:
                failed.append(f"result {idx} label_safety {key} must be false")
    return {"passed": not failed, "failed_checks": failed}


def main() -> int:
    parser = argparse.ArgumentParser(description="Check B1/B1R integrated blind RCA smoke artifacts.")
    parser.add_argument("--input", required=True)
    args = parser.parse_args()
    result = check_b1_integrated_pipeline(args.input)
    if result["passed"]:
        print("B1 integrated blind RCA structural check passed.")
        return 0
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
