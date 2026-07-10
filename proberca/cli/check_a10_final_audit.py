from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


VALID_RISKS = {"LOW", "MEDIUM", "HIGH", "CRITICAL"}


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return data


def check_a10_final_audit(input_dir: str) -> dict[str, Any]:
    root = Path(input_dir)
    failed_checks: list[str] = []

    required_files = [
        root / "final_blind_audit_summary.json",
        root / "final_claims_table.md",
        root / "final_metrics_table.md",
        root / "module_status_table.md",
        Path("docs/audits/A10_FINAL_BLIND_AUDIT.md"),
    ]
    for path in required_files:
        if not path.exists():
            failed_checks.append(f"missing required file: {path}")

    summary: dict[str, Any] = {}
    summary_path = root / "final_blind_audit_summary.json"
    if summary_path.exists():
        try:
            summary = _load_json(summary_path)
        except Exception as exc:  # pragma: no cover - defensive CLI reporting
            failed_checks.append(f"failed to read summary JSON: {exc}")

    if "a10_final_passed" not in summary:
        failed_checks.append("a10_final_passed field missing")
    if summary.get("official_blind_result_source") != "A2 Blind P2 Rerun":
        failed_checks.append("official_blind_result_source must be A2 Blind P2 Rerun")
    if summary.get("production_readiness") != "NOT_PRODUCTION_READY":
        failed_checks.append("production_readiness must be NOT_PRODUCTION_READY")
    if not summary.get("claims_forbidden"):
        failed_checks.append("claims_forbidden must be non-empty")
    if not summary.get("remaining_risks"):
        failed_checks.append("remaining_risks must be non-empty")

    risk = summary.get("final_label_leakage_risk_new_pipeline")
    if risk not in VALID_RISKS:
        failed_checks.append("final_label_leakage_risk_new_pipeline has invalid value")
    if summary.get("a10_final_passed") is True and risk not in {"LOW", "MEDIUM"}:
        failed_checks.append("a10_final_passed=true requires LOW or MEDIUM leakage risk")

    legacy = summary.get("legacy_target_aware_metrics") or {}
    if isinstance(legacy, dict) and legacy.get("available"):
        protocol = str(legacy.get("protocol", "")).lower()
        if "not official blind" not in protocol:
            failed_checks.append("legacy target-aware metrics must be marked as not official blind")

    return {"passed": not failed_checks, "failed_checks": failed_checks}


def main() -> int:
    parser = argparse.ArgumentParser(description="Check A10 final blind audit artifacts.")
    parser.add_argument("--input", required=True, help="final_blind_audit output directory")
    args = parser.parse_args()
    result = check_a10_final_audit(args.input)
    if result["passed"]:
        print("A10 final blind audit structural check passed.")
        return 0
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
