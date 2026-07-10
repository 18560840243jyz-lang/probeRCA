"""Safely clean large P1 artifacts while preserving freeze-worthy results."""

from __future__ import annotations

import argparse
from pathlib import Path

DELETABLE_FILENAMES = {
    "metrics.jsonl",
    "normalized_metrics.jsonl",
    "observed_metrics.jsonl",
    "sampling_log.jsonl",
    "observation_mask.jsonl",
    "ipw_stable_residuals.jsonl",
    "ipw_stable_propagation_model.json",
    "ipw_sparse_interventions.jsonl",
    "ipw_semantic_interventions.jsonl",
    "ipw_path_explanations.jsonl",
    "robust_stats.jsonl",
    "evidence.jsonl",
}

PRESERVED_FILENAMES = {
    "p1_results.jsonl",
    "p1_results_metadata.json",
    "p1_evaluation_summary.json",
    "p1_experiment_metadata.json",
    "p1_audit_summary.json",
    "p1_audit_metadata.json",
    "p1_failure_analysis.json",
    "p1_gate_decision.json",
    "adaptive_observation_metadata.json",
    "ipw_propagation_metadata.json",
    "ipw_sparse_inversion_summary.json",
    "ipw_semantic_evidence_summary.json",
    "ipw_path_explanation_summary.json",
    "incidents.jsonl",
    "metadata.json",
    "service_graph.jsonl",
}

CLEANABLE_TOP_LEVELS = {"audit_quick", "audit_full", "demo"}


def _is_cleanable_relative_path(path: Path) -> bool:
    if not path.parts:
        return False
    first = path.parts[0]
    return first in CLEANABLE_TOP_LEVELS or first.startswith("demo_")


def find_cleanup_candidates(base: str | Path = "data/p1_single_vm") -> list[Path]:
    """Return large P1 files that are safe to delete."""

    base_path = Path(base)
    if not base_path.exists():
        return []

    resolved_base = base_path.resolve()
    candidates: list[Path] = []
    for file_path in base_path.rglob("*"):
        if not file_path.is_file():
            continue
        if file_path.name in PRESERVED_FILENAMES:
            continue
        if file_path.name not in DELETABLE_FILENAMES:
            continue

        resolved_file = file_path.resolve()
        if resolved_base not in resolved_file.parents and resolved_file != resolved_base:
            continue

        relative = file_path.relative_to(base_path)
        if _is_cleanable_relative_path(relative):
            candidates.append(file_path)

    return sorted(candidates, key=lambda item: str(item))


def cleanup_p1_artifacts(base: str | Path = "data/p1_single_vm", apply: bool = False) -> dict:
    """Dry-run or apply cleanup for large P1 artifacts."""

    candidates = find_cleanup_candidates(base)
    total_bytes = sum(path.stat().st_size for path in candidates if path.exists())
    deleted: list[str] = []

    if apply:
        for path in candidates:
            if path.exists():
                path.unlink()
                deleted.append(str(path))

    return {
        "base": str(base),
        "apply": apply,
        "candidate_files": [str(path) for path in candidates],
        "total_bytes": total_bytes,
        "deleted_files": deleted,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Clean large probeRCA P1 artifacts safely.")
    parser.add_argument("--base", default="data/p1_single_vm")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)

    base_path = Path(args.base)
    if not base_path.exists():
        print(f"清理基目录不存在：{base_path}")
        print("nothing to clean")
        return 0

    result = cleanup_p1_artifacts(args.base, apply=args.apply)
    mode = "apply" if args.apply else "dry-run"
    print(f"probeRCA P1 artifact cleanup ({mode})")
    print(f"base：{result['base']}")
    print("candidate files:")
    if result["candidate_files"]:
        for file_name in result["candidate_files"]:
            print(f"- {file_name}")
    else:
        print("- none")
        print("nothing to clean")
    print(f"total bytes that would be deleted：{result['total_bytes']}")
    if args.apply:
        print("actually deleted files:")
        if result["deleted_files"]:
            for file_name in result["deleted_files"]:
                print(f"- {file_name}")
        else:
            print("- none")
    else:
        print("dry-run：未删除任何文件。加 --apply 才会真正删除。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
