"""Run A5 probe policy preview over all P2 repeats."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np

from proberca.adapters.online_boutique.adaptive_probe_policy import evaluate_probe_policy_for_debug, write_probe_policy_outputs

FAULT_SOURCES = {
    "cpu": ("CPU", "cpu_paymentservice_repeated_controlled"),
    "network": ("Network", "network_shippingservice_repeated"),
    "io": ("I/O", "io_rediscart_repeated"),
    "lock": ("Lock", "lock_cartservice_repeated_phaseaware"),
}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _mean(values: list[float]) -> float:
    return float(np.mean(np.asarray(values, dtype=float))) if values else 0.0


def run_p2_probe_policy_preview(
    alert_root: str = "data/p2_online_boutique/a3_alert_preview",
    candidate_root: str = "data/p2_online_boutique/a4_candidate_preview",
    blind_root: str = "data/p2_online_boutique/blind_rerun",
    output_dir: str = "data/p2_online_boutique/a5_probe_policy_preview",
    budget: float = 12.0,
    debug_evaluate_incidents: bool = False,
) -> dict[str, Any]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    per_repeat: list[dict[str, Any]] = []
    per_fault_type: dict[str, dict[str, Any]] = {}
    for key, (fault_type, raw_root) in FAULT_SOURCES.items():
        rows: list[dict[str, Any]] = []
        for index in range(1, 6):
            repeat = f"repeat_{index:02d}"
            alert_dir = Path(alert_root) / key / repeat
            candidate_dir = Path(candidate_root) / key / repeat
            blind_dir = Path(blind_root) / key / repeat / "input"
            repeat_out = output / key / repeat
            if not (alert_dir / "alert_windows.jsonl").exists():
                raise FileNotFoundError(f"missing alert windows: {alert_dir / 'alert_windows.jsonl'}")
            if not (candidate_dir / "repeat_candidate_summary.json").exists():
                raise FileNotFoundError(f"missing candidate summary: {candidate_dir / 'repeat_candidate_summary.json'}")
            result = write_probe_policy_outputs(str(alert_dir), str(candidate_dir), str(repeat_out), str(blind_dir) if blind_dir.exists() else None, budget)
            metadata = result["metadata"]
            debug = None
            incidents = Path("data/p2_online_boutique") / raw_root / repeat / "raw" / "incidents.jsonl"
            if debug_evaluate_incidents and incidents.exists():
                debug = evaluate_probe_policy_for_debug(str(repeat_out), str(incidents))
                _write_json(repeat_out / "probe_policy_debug_evaluation.json", debug)
            selected_count = sum(len(plan.get("selected_probes", [])) for plan in result["plans"])
            row = {
                "fault_type": fault_type,
                "fault_type_key": key,
                "repeat_index": index,
                "alert_dir": str(alert_dir),
                "candidate_dir": str(candidate_dir),
                "blind_evidence_dir": str(blind_dir) if blind_dir.exists() else None,
                "output_dir": str(repeat_out),
                "probe_plan_count": metadata["probe_plan_count"],
                "selected_probe_count": selected_count,
                "sampling_log_count": metadata["sampling_log_count"],
                "observation_mask_count": metadata["observation_mask_count"],
                "estimated_cost": float(metadata.get("average_estimated_cost", 0.0)),
                "has_probe_plan": metadata["probe_plan_count"] > 0,
                "debug_root_metric_family_selected_rate": float(debug.get("debug_root_metric_family_selected_rate", 0.0)) if debug else None,
                "debug_root_service_has_selected_probe_rate": float(debug.get("debug_root_service_has_selected_probe_rate", 0.0)) if debug else None,
            }
            rows.append(row)
            per_repeat.append(row)
        per_fault_type[key] = {
            "fault_type": fault_type,
            "repeats": len(rows),
            "repeats_with_probe_plan": sum(1 for row in rows if row["has_probe_plan"]),
            "average_selected_probe_count": _mean([float(row["selected_probe_count"]) for row in rows]),
            "average_sampling_log_count": _mean([float(row["sampling_log_count"]) for row in rows]),
            "average_observation_mask_count": _mean([float(row["observation_mask_count"]) for row in rows]),
            "average_estimated_cost": _mean([float(row["estimated_cost"]) for row in rows]),
            "debug_root_metric_family_selected_rate": _mean([float(row.get("debug_root_metric_family_selected_rate") or 0.0) for row in rows]) if debug_evaluate_incidents else None,
            "debug_root_service_has_selected_probe_rate": _mean([float(row.get("debug_root_service_has_selected_probe_rate") or 0.0) for row in rows]) if debug_evaluate_incidents else None,
        }
    summary = {
        "total_repeats": len(per_repeat),
        "repeats_with_probe_plan": sum(1 for row in per_repeat if row["has_probe_plan"]),
        "average_selected_probe_count": _mean([float(row["selected_probe_count"]) for row in per_repeat]),
        "average_sampling_log_count": _mean([float(row["sampling_log_count"]) for row in per_repeat]),
        "average_observation_mask_count": _mean([float(row["observation_mask_count"]) for row in per_repeat]),
        "average_estimated_cost": _mean([float(row["estimated_cost"]) for row in per_repeat]),
        "per_fault_type": per_fault_type,
        "per_repeat": per_repeat,
        "uses_root_labels_for_policy": False,
        "uses_target_config_for_policy": False,
        "uses_injected_path_for_policy": False,
        "uses_incident_start_end_for_policy": False,
        "actual_probe_activation": False,
        "runs_rca_pipeline": False,
        "reinjects_faults": False,
        "calls_kubectl_tc_docker": False,
    }
    if debug_evaluate_incidents:
        summary["debug_only"] = True
        summary["debug_root_metric_family_selected_rate"] = _mean([float(row.get("debug_root_metric_family_selected_rate") or 0.0) for row in per_repeat])
        summary["debug_root_service_has_selected_probe_rate"] = _mean([float(row.get("debug_root_service_has_selected_probe_rate") or 0.0) for row in per_repeat])
    _write_json(output / "p2_probe_policy_preview_summary.json", summary)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Run A5 probe policy preview over all P2 repeats.")
    parser.add_argument("--alert-root", default="data/p2_online_boutique/a3_alert_preview")
    parser.add_argument("--candidate-root", default="data/p2_online_boutique/a4_candidate_preview")
    parser.add_argument("--blind-root", default="data/p2_online_boutique/blind_rerun")
    parser.add_argument("--output", default="data/p2_online_boutique/a5_probe_policy_preview")
    parser.add_argument("--budget", type=float, default=12.0)
    parser.add_argument("--debug-evaluate-incidents", action="store_true")
    args = parser.parse_args()
    summary = run_p2_probe_policy_preview(args.alert_root, args.candidate_root, args.blind_root, args.output, args.budget, args.debug_evaluate_incidents)
    print("probeRCA A5 P2 probe policy preview 摘要")
    print(f"total_repeats：{summary['total_repeats']}")
    print(f"repeats_with_probe_plan：{summary['repeats_with_probe_plan']}")
    print(f"average_selected_probe_count：{summary['average_selected_probe_count']}")
    print(f"average_sampling_log_count：{summary['average_sampling_log_count']}")
    print(f"average_observation_mask_count：{summary['average_observation_mask_count']}")
    print(f"average_estimated_cost：{summary['average_estimated_cost']}")
    if "debug_root_metric_family_selected_rate" in summary:
        print(f"debug_root_metric_family_selected_rate：{summary['debug_root_metric_family_selected_rate']}")
        print(f"debug_root_service_has_selected_probe_rate：{summary['debug_root_service_has_selected_probe_rate']}")
    print("actual_probe_activation：False")
    print("注意：当前是 A5 Adaptive Probe Policy preview，只生成 probe policy 和 sampling log，不真实开启 probe，不运行 RCA pipeline，不重新注入故障。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
