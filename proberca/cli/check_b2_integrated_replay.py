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


def check_b2_integrated_replay(input_dir: str) -> dict[str, Any]:
    root = Path(input_dir)
    failed: list[str] = []
    summary_path = root / "p2_integrated_replay_summary.json"
    metadata_path = root / "p2_integrated_replay_metadata.json"
    if not summary_path.exists():
        failed.append("missing p2_integrated_replay_summary.json")
    if not metadata_path.exists():
        failed.append("missing p2_integrated_replay_metadata.json")
    summary = _read_json(summary_path)
    if int(summary.get("total_repeats") or 0) < 20:
        failed.append("total_repeats must be >= 20")
    if int(summary.get("repeats_completed") or 0) < 20:
        failed.append("repeats_completed must be >= 20")
    for key in [
        "uses_root_labels_for_inference",
        "uses_target_config_for_inference",
        "uses_injected_path_for_inference",
        "uses_incident_start_end_for_inference",
        "uses_legacy_evidence",
        "runs_old_p1_rca",
        "reinjects_faults",
    ]:
        if summary.get(key) is not False:
            failed.append(f"summary {key} must be false")
    if summary.get("evaluation_uses_labels_posthoc") is not True:
        failed.append("summary evaluation_uses_labels_posthoc must be true")

    per_repeat = summary.get("per_repeat") if isinstance(summary.get("per_repeat"), list) else []
    for row in per_repeat:
        repeat_dir = Path(str(row.get("output_dir") or ""))
        if not (repeat_dir / "09_final_result" / "integrated_rca_results.jsonl").exists():
            failed.append(f"missing integrated_rca_results.jsonl for {repeat_dir}")
        md = _read_json(repeat_dir / "09_final_result" / "integrated_rca_metadata.json")
        for key in ["uses_root_labels", "uses_incident_start_end", "uses_legacy_evidence", "runs_old_p1_rca"]:
            if md.get(key) is not False:
                failed.append(f"{repeat_dir} metadata {key} must be false")
        if md.get("top_service_metric_consistent") is not True:
            failed.append(f"{repeat_dir} top_service_metric_consistent must be true")
        if md.get("per_window_results_match_alert_windows") is not True:
            failed.append(f"{repeat_dir} per_window_results_match_alert_windows must be true")
        if md.get("root_type_source") != "primary_metric_family":
            failed.append(f"{repeat_dir} root_type_source must be primary_metric_family")
        if md.get("root_type_uses_labels") is not False:
            failed.append(f"{repeat_dir} root_type_uses_labels must be false")
        if md.get("service_first_enabled") is not True:
            failed.append(f"{repeat_dir} service_first_enabled must be true")
        if md.get("primary_service_source") != "service_candidate_table":
            failed.append(f"{repeat_dir} primary_service_source must be service_candidate_table")
        if md.get("primary_metric_source") != "metric_candidates_within_root_service":
            failed.append(f"{repeat_dir} primary_metric_source must be metric_candidates_within_root_service")
        prop_md = _read_json(repeat_dir / "05b_structured_propagation" / "structured_propagation_metadata.json")
        if prop_md.get("structured_propagation_model") != "structured_multilag_ridge" and prop_md.get("propagation_model") != "structured_multilag_ridge":
            failed.append(f"{repeat_dir} structured propagation model must be structured_multilag_ridge")
        if prop_md.get("stable_only") is not True:
            failed.append(f"{repeat_dir} structured propagation stable_only must be true")
        if prop_md.get("propagation_drift_used") is not False:
            failed.append(f"{repeat_dir} structured propagation_drift_used must be false")
        if prop_md.get("uses_root_labels") is not False:
            failed.append(f"{repeat_dir} structured propagation uses_root_labels must be false")
        if prop_md.get("uses_incident_start_end") is not False:
            failed.append(f"{repeat_dir} structured propagation uses_incident_start_end must be false")
        if md.get("global_top_metrics_primary") is not False:
            failed.append(f"{repeat_dir} global_top_metrics_primary must be false")
        if md.get("structured_propagation_enabled") is not True:
            failed.append(f"{repeat_dir} structured_propagation_enabled must be true")
        if md.get("structured_propagation_uses_labels") is not False:
            failed.append(f"{repeat_dir} structured_propagation_uses_labels must be false")
        if md.get("structured_propagation_uses_injected_path") is not False:
            failed.append(f"{repeat_dir} structured_propagation_uses_injected_path must be false")
        if md.get("propagation_drift_used") is not False:
            failed.append(f"{repeat_dir} propagation_drift_used must be false")
        if md.get("service_local_support_used") is not True:
            failed.append(f"{repeat_dir} service_local_support_used must be true")
        if md.get("global_family_support_weight_limited") is not True:
            failed.append(f"{repeat_dir} global_family_support_weight_limited must be true")
        if "ownership_invalid_count" not in md:
            failed.append(f"{repeat_dir} ownership_invalid_count must be recorded")
        if md.get("primary_candidate_ownership_valid") is not True:
            failed.append(f"{repeat_dir} primary_candidate_ownership_valid must be true")
        results_path = repeat_dir / "09_final_result" / "integrated_rca_results.jsonl"
        if results_path.exists():
            for line_no, line in enumerate(results_path.read_text(encoding="utf-8").splitlines(), start=1):
                if not line.strip():
                    continue
                result = json.loads(line)
                primary = result.get("primary_candidate") if isinstance(result.get("primary_candidate"), dict) else {}
                components = primary.get("score_components") if isinstance(primary.get("score_components"), dict) else {}
                if primary.get("ownership_valid") is not True:
                    failed.append(f"{repeat_dir} result {line_no} primary_candidate ownership_valid must be true")
                if primary.get("service_matches_node_id") is not True:
                    failed.append(f"{repeat_dir} result {line_no} primary_candidate service_matches_node_id must be true")
                if "diagnostic_specificity" not in components:
                    failed.append(f"{repeat_dir} result {line_no} primary score_components must include diagnostic_specificity")
                for component_key in ("structured_propagation_support", "path_edge_support", "lag_support"):
                    if component_key not in components:
                        failed.append(f"{repeat_dir} result {line_no} primary score_components must include {component_key}")
                for component_key in ("node_evidence_support", "service_family_evidence_support", "family_global_evidence_support"):
                    if component_key not in components:
                        failed.append(f"{repeat_dir} result {line_no} primary score_components must include {component_key}")
                if float(components.get("family_global_evidence_weight", 1.0) or 1.0) > 0.20:
                    failed.append(f"{repeat_dir} result {line_no} family_global_evidence_weight must be <= 0.20")
                if str(primary.get("metric")) == "memory.usage" and "weak_memory_usage_penalty_applied" not in components:
                    failed.append(f"{repeat_dir} result {line_no} memory.usage primary must record weak_memory_usage_penalty_applied")
                top_metrics = result.get("top_metrics") if isinstance(result.get("top_metrics"), list) else []
                top_services = result.get("top_services") if isinstance(result.get("top_services"), list) else []
                if top_metrics and top_services and top_metrics[0].get("service") != top_services[0].get("service"):
                    failed.append(f"{repeat_dir} result {line_no} top_metrics[0].service must equal top_services[0].service")
                top1_service = str(result.get("predicted_top1_service", ""))
                top1_metric = str(result.get("predicted_top1_metric", ""))
                if top1_service != split_node_id(top1_metric)[0]:
                    failed.append(f"{repeat_dir} result {line_no} top1_service and top1_metric service mismatch")
                for metric_idx, metric_row in enumerate(top_metrics, start=1):
                    if metric_row.get("service") != top1_service:
                        failed.append(f"{repeat_dir} result {line_no} top_metrics[{metric_idx}].service must equal top1_service")
                aux = result.get("global_top_metrics_auxiliary") if isinstance(result.get("global_top_metrics_auxiliary"), list) else []
                for aux_idx, aux_row in enumerate(aux, start=1):
                    if aux_row.get("auxiliary") is not True:
                        failed.append(f"{repeat_dir} result {line_no} global_top_metrics_auxiliary[{aux_idx}] must be auxiliary")
                if result.get("root_type_source") != "primary_metric_family":
                    failed.append(f"{repeat_dir} result {line_no} root_type_source must be primary_metric_family")
                if result.get("root_type_uses_labels") is not False:
                    failed.append(f"{repeat_dir} result {line_no} root_type_uses_labels must be false")
                if result.get("service_first") is not True:
                    failed.append(f"{repeat_dir} result {line_no} service_first must be true")
                if result.get("primary_metric_conditioned_on_service") is not True:
                    failed.append(f"{repeat_dir} result {line_no} primary_metric_conditioned_on_service must be true")
                if result.get("global_top_metrics_primary") is not False:
                    failed.append(f"{repeat_dir} result {line_no} global_top_metrics_primary must be false")
    return {"passed": not failed, "failed_checks": failed}


def main() -> int:
    parser = argparse.ArgumentParser(description="Check B2 integrated replay artifacts.")
    parser.add_argument("--input", required=True)
    args = parser.parse_args()
    result = check_b2_integrated_replay(args.input)
    if result["passed"]:
        print("B2 integrated replay structural check passed.")
        return 0
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
