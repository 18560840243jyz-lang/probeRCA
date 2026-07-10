"""Run A4 candidate subgraph preview over all existing P2 repeats."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np

from proberca.adapters.online_boutique.candidate_subgraph import (
    build_candidate_subgraphs_for_repeat,
    evaluate_candidate_subgraph_for_debug,
)

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


def run_p2_candidate_preview(
    alert_root: str = "data/p2_online_boutique/a3_alert_preview",
    output_dir: str = "data/p2_online_boutique/a4_candidate_preview",
    debug_evaluate_incidents: bool = False,
    reverse_hops: int = 2,
    forward_hops: int = 1,
) -> dict[str, Any]:
    alert_base = Path(alert_root)
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    per_repeat: list[dict[str, Any]] = []
    per_fault_type: dict[str, dict[str, Any]] = {}

    for key, (fault_type, root_dir) in FAULT_SOURCES.items():
        rows: list[dict[str, Any]] = []
        for index in range(1, 6):
            raw_dir = Path("data/p2_online_boutique") / root_dir / f"repeat_{index:02d}" / "raw"
            alert_dir = alert_base / key / f"repeat_{index:02d}"
            repeat_out = output / key / f"repeat_{index:02d}"
            if not (raw_dir / "metrics.jsonl").exists():
                raise FileNotFoundError(f"missing raw metrics: {raw_dir / 'metrics.jsonl'}")
            if not (alert_dir / "alert_windows.jsonl").exists():
                raise FileNotFoundError(f"missing A3 alert windows: {alert_dir / 'alert_windows.jsonl'}")
            summary = build_candidate_subgraphs_for_repeat(str(raw_dir), str(alert_dir), str(repeat_out), reverse_hops, forward_hops)
            debug = None
            incidents = raw_dir / "incidents.jsonl"
            if debug_evaluate_incidents and incidents.exists():
                debug = evaluate_candidate_subgraph_for_debug(str(repeat_out / "repeat_candidate_summary.json"), str(incidents))
                _write_json(repeat_out / "candidate_debug_evaluation.json", debug)
            row = {
                "fault_type": fault_type,
                "fault_type_key": key,
                "repeat_index": index,
                "raw_input_dir": str(raw_dir),
                "alert_output_dir": str(alert_dir),
                "candidate_output_dir": str(repeat_out),
                "alert_windows_count": summary["alert_windows_count"],
                "candidate_service_count": summary["candidate_service_count"],
                "candidate_metric_node_count": summary["candidate_metric_node_count"],
                "has_candidate_graph": bool(summary["alert_windows_count"] > 0 and summary["candidate_service_count"] > 0),
                "debug_root_service_candidate_hit_rate": float(debug.get("root_service_hit_rate_debug", 0.0)) if debug else None,
                "debug_root_metric_candidate_hit_rate": float(debug.get("root_metric_hit_rate_debug", 0.0)) if debug else None,
            }
            rows.append(row)
            per_repeat.append(row)
        per_fault_type[key] = {
            "fault_type": fault_type,
            "repeats": len(rows),
            "repeats_with_candidate_graph": sum(1 for row in rows if row["has_candidate_graph"]),
            "average_candidate_service_count": _mean([float(row["candidate_service_count"]) for row in rows]),
            "average_candidate_metric_node_count": _mean([float(row["candidate_metric_node_count"]) for row in rows]),
            "debug_root_service_candidate_hit_rate": _mean([float(row.get("debug_root_service_candidate_hit_rate") or 0.0) for row in rows]) if debug_evaluate_incidents else None,
            "debug_root_metric_candidate_hit_rate": _mean([float(row.get("debug_root_metric_candidate_hit_rate") or 0.0) for row in rows]) if debug_evaluate_incidents else None,
        }

    repeats_with_candidate = sum(1 for row in per_repeat if row["has_candidate_graph"])
    summary = {
        "total_repeats": len(per_repeat),
        "repeats_with_candidate_graph": repeats_with_candidate,
        "average_candidate_service_count": _mean([float(row["candidate_service_count"]) for row in per_repeat]),
        "average_candidate_metric_node_count": _mean([float(row["candidate_metric_node_count"]) for row in per_repeat]),
        "per_fault_type": per_fault_type,
        "per_repeat": per_repeat,
        "uses_root_labels_for_building": False,
        "uses_target_config_for_building": False,
        "uses_injected_path_for_building": False,
        "uses_incident_start_end_for_building": False,
        "runs_rca_pipeline": False,
        "reinjects_faults": False,
    }
    if debug_evaluate_incidents:
        summary["debug_only"] = True
        summary["debug_root_service_candidate_hit_rate"] = _mean([float(row.get("debug_root_service_candidate_hit_rate") or 0.0) for row in per_repeat])
        summary["debug_root_metric_candidate_hit_rate"] = _mean([float(row.get("debug_root_metric_candidate_hit_rate") or 0.0) for row in per_repeat])
    _write_json(output / "p2_candidate_preview_summary.json", summary)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Run A4 candidate subgraph preview over all P2 repeats.")
    parser.add_argument("--alert-root", default="data/p2_online_boutique/a3_alert_preview")
    parser.add_argument("--output", default="data/p2_online_boutique/a4_candidate_preview")
    parser.add_argument("--reverse-hops", type=int, default=2)
    parser.add_argument("--forward-hops", type=int, default=1)
    parser.add_argument("--debug-evaluate-incidents", action="store_true")
    args = parser.parse_args()
    summary = run_p2_candidate_preview(args.alert_root, args.output, args.debug_evaluate_incidents, args.reverse_hops, args.forward_hops)
    print("probeRCA A4 P2 candidate preview 摘要")
    print(f"output_dir：{args.output}")
    print(f"total_repeats：{summary['total_repeats']}")
    print(f"repeats_with_candidate_graph：{summary['repeats_with_candidate_graph']}")
    print(f"average_candidate_service_count：{summary['average_candidate_service_count']}")
    print(f"average_candidate_metric_node_count：{summary['average_candidate_metric_node_count']}")
    if "debug_root_service_candidate_hit_rate" in summary:
        print(f"debug_root_service_candidate_hit_rate：{summary['debug_root_service_candidate_hit_rate']}")
        print(f"debug_root_metric_candidate_hit_rate：{summary['debug_root_metric_candidate_hit_rate']}")
    print("uses_root_labels_for_building：False")
    print("uses_incident_start_end_for_building：False")
    print("注意：当前是 A4 Candidate Subgraph Builder preview，只构建候选子图，不运行 RCA pipeline，不重新注入故障。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
