"""Safely clean large P0 audit artifacts."""

from __future__ import annotations

import argparse
from pathlib import Path

DELETABLE_FILENAMES = {
    "metrics.jsonl",
    "normalized_metrics.jsonl",
    "stable_residuals.jsonl",
    "stable_propagation_model.json",
    "robust_stats.jsonl",
    "sparse_interventions.jsonl",
    "semantic_interventions.jsonl",
    "path_explanations.jsonl",
    "evidence.jsonl",
}

PRESERVED_FILENAMES = {
    "p0_audit_summary.json",
    "p0_audit_metadata.json",
    "g1_decision.json",
    "p0_failure_analysis.json",
    "p0_evaluation_summary.json",
    "p0_results.jsonl",
    "p0_results_metadata.json",
    "p0_experiment_metadata.json",
    "metadata.json",
}


def _is_audit_relative_path(path: Path) -> bool:
    return any(part.startswith("audit") for part in path.parts)


def _is_demo_relative_path(path: Path) -> bool:
    return bool(path.parts) and path.parts[0] == "demo"


def find_cleanup_candidates(base: str | Path = "data/p0_single_vm", include_demo: bool = False) -> list[Path]:
    """Return large P0 files that are safe to delete."""

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
        if _is_audit_relative_path(relative) or (include_demo and _is_demo_relative_path(relative)):
            candidates.append(file_path)

    return sorted(candidates, key=lambda item: str(item))


def cleanup_p0_artifacts(
    base: str | Path = "data/p0_single_vm",
    apply: bool = False,
    include_demo: bool = False,
) -> dict:
    """Dry-run or apply cleanup for large P0 artifacts."""

    candidates = find_cleanup_candidates(base, include_demo=include_demo)
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
        "include_demo": include_demo,
        "candidate_files": [str(path) for path in candidates],
        "total_bytes": total_bytes,
        "deleted_files": deleted,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Clean large probeRCA P0 artifacts safely.")
    parser.add_argument("--base", default="data/p0_single_vm")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--include-demo", action="store_true")
    args = parser.parse_args(argv)

    base_path = Path(args.base)
    if not base_path.exists():
        print(f"清理基目录不存在：{base_path}")
        print("nothing to clean")
        return 0

    result = cleanup_p0_artifacts(args.base, apply=args.apply, include_demo=args.include_demo)
    mode = "apply" if args.apply else "dry-run"
    print(f"probeRCA P0 artifact cleanup ({mode})")
    print(f"base：{result['base']}")
    print(f"include_demo：{result['include_demo']}")
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
