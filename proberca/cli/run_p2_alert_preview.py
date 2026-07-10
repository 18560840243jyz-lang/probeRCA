"""Run A3 alert preview over all existing P2 real raw metrics."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np

from proberca.adapters.online_boutique.alert_gate import evaluate_alert_windows_for_debug, write_alert_outputs

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


def run_p2_alert_preview(output_dir: str = "data/p2_online_boutique/a3_alert_preview") -> dict[str, Any]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    per_repeat: list[dict[str, Any]] = []
    per_fault_type: dict[str, dict[str, Any]] = {}

    for key, (fault_type, root_dir) in FAULT_SOURCES.items():
        rows: list[dict[str, Any]] = []
        for index in range(1, 6):
            input_dir = Path("data/p2_online_boutique") / root_dir / f"repeat_{index:02d}" / "raw"
            repeat_out = output / key / f"repeat_{index:02d}"
            if not (input_dir / "metrics.jsonl").exists():
                raise FileNotFoundError(f"missing raw metrics for alert preview: {input_dir / 'metrics.jsonl'}")
            result = write_alert_outputs(str(input_dir), str(repeat_out))
            debug = None
            incidents = input_dir / "incidents.jsonl"
            if incidents.exists():
                debug = evaluate_alert_windows_for_debug(str(repeat_out / "alert_windows.jsonl"), str(incidents))
                _write_json(repeat_out / "alert_debug_evaluation.json", debug)
            windows = result["windows"]
            metadata = result["metadata"]
            row = {
                "fault_type": fault_type,
                "fault_type_key": key,
                "repeat_index": index,
                "input_dir": str(input_dir),
                "output_dir": str(repeat_out),
                "alert_events_count": metadata["alert_events_count"],
                "alert_windows_count": metadata["alert_windows_count"],
                "first_alert_window": windows[0] if windows else None,
                "debug_overlap_with_incident": bool(debug and debug.get("overlap_count", 0) > 0),
                "debug_incident_window_recall": float(debug.get("incident_window_recall", 0.0)) if debug else None,
            }
            rows.append(row)
            per_repeat.append(row)
        per_fault_type[key] = {
            "fault_type": fault_type,
            "repeats": len(rows),
            "repeats_with_alert_window": sum(1 for row in rows if int(row["alert_windows_count"]) > 0),
            "detection_rate": sum(1 for row in rows if int(row["alert_windows_count"]) > 0) / len(rows) if rows else 0.0,
            "average_alert_windows": _mean([float(row["alert_windows_count"]) for row in rows]),
            "debug_incident_window_recall": _mean([float(row.get("debug_incident_window_recall") or 0.0) for row in rows]),
        }

    repeats_with_alert = sum(1 for row in per_repeat if int(row["alert_windows_count"]) > 0)
    summary = {
        "total_repeats": len(per_repeat),
        "repeats_with_alert_window": repeats_with_alert,
        "alert_window_detection_rate": repeats_with_alert / len(per_repeat) if per_repeat else 0.0,
        "per_fault_type": per_fault_type,
        "per_repeat": per_repeat,
        "uses_root_labels_for_detection": False,
        "uses_target_config_for_detection": False,
        "uses_injected_path_for_detection": False,
        "uses_incident_start_end_for_detection": False,
        "runs_rca_pipeline": False,
        "reinjects_faults": False,
    }
    _write_json(output / "p2_alert_preview_summary.json", summary)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Run A3 alert preview over all P2 repeats.")
    parser.add_argument("--output", default="data/p2_online_boutique/a3_alert_preview")
    args = parser.parse_args()
    summary = run_p2_alert_preview(args.output)
    print("probeRCA A3 P2 alert preview 摘要")
    print(f"output_dir：{args.output}")
    print(f"total_repeats：{summary['total_repeats']}")
    print(f"repeats_with_alert_window：{summary['repeats_with_alert_window']}")
    print(f"alert_window_detection_rate：{summary['alert_window_detection_rate']}")
    print("uses_root_labels_for_detection：False")
    print("uses_incident_start_end_for_detection：False")
    print("注意：当前是 A3 Alert Gate preview，只检测告警窗口，不运行 RCA pipeline，不重新注入故障。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
