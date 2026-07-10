"""Build A5 adaptive probe policy for one repeat."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from proberca.adapters.online_boutique.adaptive_probe_policy import evaluate_probe_policy_for_debug, write_probe_policy_outputs


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _guess_incidents_path(alert_input: str) -> Path | None:
    marker = "data/p2_online_boutique/a3_alert_preview/"
    if marker not in alert_input:
        return None
    suffix = alert_input.split(marker, 1)[1]
    parts = Path(suffix).parts
    if len(parts) < 2:
        return None
    fault, repeat = parts[0], parts[1]
    roots = {
        "cpu": "cpu_paymentservice_repeated_controlled",
        "network": "network_shippingservice_repeated",
        "io": "io_rediscart_repeated",
        "lock": "lock_cartservice_repeated_phaseaware",
    }
    root = roots.get(fault)
    if not root:
        return None
    path = Path("data/p2_online_boutique") / root / repeat / "raw" / "incidents.jsonl"
    return path if path.exists() else None


def main() -> int:
    parser = argparse.ArgumentParser(description="Build A5 adaptive probe policy for one repeat.")
    parser.add_argument("--alert-input", required=True)
    parser.add_argument("--candidate-input", required=True)
    parser.add_argument("--blind-evidence-input")
    parser.add_argument("--output", required=True)
    parser.add_argument("--budget", type=float, default=12.0)
    parser.add_argument("--debug-evaluate-incidents", action="store_true")
    args = parser.parse_args()

    result = write_probe_policy_outputs(args.alert_input, args.candidate_input, args.output, args.blind_evidence_input, args.budget)
    debug = None
    incidents = _guess_incidents_path(args.alert_input)
    if args.debug_evaluate_incidents and incidents is not None:
        debug = evaluate_probe_policy_for_debug(args.output, str(incidents))
        _write_json(Path(args.output) / "probe_policy_debug_evaluation.json", debug)
    metadata = result["metadata"]
    print("probeRCA A5 adaptive probe policy 摘要")
    print(f"alert_windows_count：{metadata['alert_windows_count']}")
    print(f"probe_plan_count：{metadata['probe_plan_count']}")
    print(f"sampling_log_count：{metadata['sampling_log_count']}")
    print(f"observation_mask_count：{metadata['observation_mask_count']}")
    print(f"budget：{metadata['budget']}")
    if debug is not None:
        print(f"debug_root_metric_family_selected_rate：{debug['debug_root_metric_family_selected_rate']}")
        print(f"debug_root_service_has_selected_probe_rate：{debug['debug_root_service_has_selected_probe_rate']}")
    print("uses_root_labels=false")
    print("actual_probe_activation=false")
    print("注意：当前是 A5 Adaptive Probe Policy，只生成 probe policy 和 sampling log，不真实开启 probe，不运行 RCA pipeline，不重新注入故障。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
