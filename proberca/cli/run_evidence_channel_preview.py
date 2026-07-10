"""Run A7 evidence channel preview for one repeat."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from proberca.evidence.evidence_channel import EvidenceChannelConfig, build_evidence_channel, evaluate_evidence_channel_debug


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _guess_incidents_path(blind_input: str) -> Path | None:
    path = Path(blind_input)
    parts = path.parts
    if "blind_rerun" not in parts:
        return None
    try:
        idx = parts.index("blind_rerun")
        key = parts[idx + 1]
        repeat = parts[idx + 2]
    except (ValueError, IndexError):
        return None
    raw_roots = {
        "cpu": "cpu_paymentservice_repeated_controlled",
        "network": "network_shippingservice_repeated",
        "io": "io_rediscart_repeated",
        "lock": "lock_cartservice_repeated_phaseaware",
    }
    raw_root = raw_roots.get(key)
    if not raw_root:
        return None
    candidate = Path("data/p2_online_boutique") / raw_root / repeat / "raw" / "incidents.jsonl"
    return candidate if candidate.exists() else None


def main() -> int:
    parser = argparse.ArgumentParser(description="Run A7 C h_t evidence channel preview.")
    parser.add_argument("--blind-evidence-input", required=True)
    parser.add_argument("--probe-policy-input", required=True)
    parser.add_argument("--ipw-rls-input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--residual-clip-value", type=float, default=10.0)
    parser.add_argument("--max-evidence-effect", type=float, default=5.0)
    parser.add_argument("--debug-evaluate-incidents", action="store_true")
    args = parser.parse_args()
    cfg = EvidenceChannelConfig(residual_clip_value=args.residual_clip_value, max_evidence_effect=args.max_evidence_effect)
    result = build_evidence_channel(args.blind_evidence_input, args.probe_policy_input, args.ipw_rls_input, args.output, cfg)
    debug = None
    incidents = _guess_incidents_path(args.blind_evidence_input)
    if args.debug_evaluate_incidents and incidents:
        debug = evaluate_evidence_channel_debug(args.output, str(incidents))
        _write_json(Path(args.output) / "evidence_channel_debug_evaluation.json", debug)
    metadata = result["metadata"]
    print("probeRCA A7 C h_t evidence channel preview 摘要")
    print(f"residual_count：{metadata['residual_count']}")
    print(f"average_abs_raw_residual：{metadata['average_abs_raw_residual']}")
    print(f"average_abs_calibrated_residual：{metadata['average_abs_calibrated_residual']}")
    print(f"max_abs_raw_residual：{metadata['max_abs_raw_residual']}")
    print(f"max_abs_calibrated_residual：{metadata['max_abs_calibrated_residual']}")
    if debug:
        print(f"debug_root_metric_calibrated_residual_rank_mean：{debug.get('root_metric_calibrated_residual_rank_mean')}")
        print(f"debug_root_service_calibrated_residual_rank_mean：{debug.get('root_service_calibrated_residual_rank_mean')}")
    print("consumes_blind_evidence=true")
    print("produces_calibrated_residuals=true")
    print("raw_residual_directly_used_for_sparse_inversion=false")
    print("注意：当前是 A7 C h_t Evidence Channel，只生成 evidence channel 和 calibrated residual，不运行 RCA pipeline，不重新注入故障。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
