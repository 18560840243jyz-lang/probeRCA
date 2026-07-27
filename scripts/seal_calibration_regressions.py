#!/usr/bin/env python3
"""Seal the two pre-repair calibration failures as immutable references."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from collections import Counter
from pathlib import Path
from typing import Any

from proberca.dataplane import CollectionArchive
from proberca.dataplane.contracts import canonical_json, fingerprint


CASES = {
    "smoke_sparse_edge_coverage_failure": "01",
    "smoke_zero_mad_scale_failure": "03",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _file_manifest(root: Path, files: list[Path]) -> dict[str, dict[str, Any]]:
    return {
        str(path.relative_to(root)): {
            "bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for path in sorted(files)
    }


def _git_output(repository: Path, *arguments: str) -> bytes:
    return subprocess.run(
        ["git", *arguments],
        cwd=repository,
        check=True,
        stdout=subprocess.PIPE,
    ).stdout


def _write_once(path: Path, content: bytes) -> None:
    if path.exists():
        if path.read_bytes() != content:
            raise RuntimeError(f"sealed file changed: {path}")
        return
    path.write_bytes(content)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", type=Path, default=Path.cwd())
    parser.add_argument(
        "--smoke-root",
        type=Path,
        default=Path(
            "artifacts/final-blind-rca-smoke-20260727T085851Z"
        ),
    )
    parser.add_argument(
        "--dataset-root",
        type=Path,
        default=Path(
            "artifacts/final-single-vm-pilot-v2-20260726T150030Z"
        ),
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("artifacts/calibration-regressions"),
    )
    parser.add_argument("--pre-repair-commit")
    arguments = parser.parse_args(argv)

    repository = arguments.repository.resolve()
    smoke_root = (repository / arguments.smoke_root).resolve()
    dataset_root = (repository / arguments.dataset_root).resolve()
    output_root = (repository / arguments.output_root).resolve()
    sealed_commits = {
        json.loads(path.read_text(encoding="utf-8"))["pre_repair_commit"]
        for path in (
            output_root / name / "regression-manifest.json"
            for name in CASES
        )
        if path.is_file()
    }
    if len(sealed_commits) > 1:
        raise RuntimeError("regression manifests disagree on pre-repair commit")
    commit = (
        arguments.pre_repair_commit
        or (next(iter(sealed_commits)) if sealed_commits else None)
        or _git_output(repository, "rev-parse", "HEAD").decode().strip()
    )
    pre_repair_config = _git_output(
        repository, "show", f"{commit}:configs/final_control.yaml",
    )
    dataset_files = [
        dataset_root / "dataset-manifest.json",
        dataset_root / "dataset-manifest.sha256",
        dataset_root / "run-events.jsonl",
    ]
    if not all(path.is_file() for path in dataset_files):
        raise RuntimeError("source dataset provenance is incomplete")

    for name, case_number in CASES.items():
        case_root = smoke_root / case_number
        replay_root = case_root / "replay-archive"
        replay = CollectionArchive.load(replay_root)
        source_files = [
            case_root / "failure.json",
            case_root / "inference-summary.json",
            case_root / "replay-audit.json",
            replay_root / "collection-manifest.json",
            replay_root / "collected-windows.jsonl",
        ]
        if not all(path.is_file() for path in source_files):
            raise RuntimeError(f"regression source is incomplete: {name}")
        audit = json.loads(
            (case_root / "replay-audit.json").read_text(encoding="utf-8")
        )
        if audit.get("truth_manifest_read_during_inference") is not False \
                or audit.get("fault_injection_code_imported") is not False \
                or audit.get(
                    "experiment_phase_present_in_replay_windows"
                ) is not False:
            raise RuntimeError(f"regression replay is label-unsafe: {name}")

        regression_root = output_root / name
        replay_output = regression_root / f"replay-case{case_number}"
        control_path = replay_output / "control-run.json"
        readiness_path = replay_output / "calibration-readiness.json"
        results_path = replay_output / "rca-results.jsonl"
        if not all(
            path.is_file()
            for path in (control_path, readiness_path, results_path)
        ):
            raise RuntimeError(
                f"post-repair replay output is incomplete: {name}"
            )
        control = json.loads(control_path.read_text(encoding="utf-8"))
        readiness = json.loads(readiness_path.read_text(encoding="utf-8"))
        states = Counter(
            item["state"] for item in control["state_timeline"]
        )
        maximum = max(
            (
                float(item.get("maximum_symptom_score", 0.0))
                for item in control["state_timeline"]
            ),
            default=0.0,
        )
        if (
            readiness.get("ready") is not False
            or states != {"calibrating": replay.window_count}
            or control.get("results")
            or control.get("rca_not_ready_events")
            or maximum != 0.0
        ):
            raise RuntimeError(
                f"post-repair replay did not fail closed: {name}"
            )
        invalid_counts = readiness.get("invalid_observation_counts", {})
        if name == "smoke_sparse_edge_coverage_failure" and not (
            invalid_counts.get("zero_coverage", 0) > 0
            and invalid_counts.get("insufficient_sample_count", 0) > 0
        ):
            raise RuntimeError(
                "sparse-edge regression did not retain missing observations"
            )

        regression_root.mkdir(parents=True, exist_ok=True)
        config_path = regression_root / "pre-repair-final-control.yaml"
        _write_once(config_path, pre_repair_config)
        payload = {
            "schema_version": "probeRCA-calibration-regression-v1",
            "regression_name": name,
            "source_case_number": case_number,
            "source_smoke_root": str(smoke_root.relative_to(repository)),
            "source_dataset_root": str(dataset_root.relative_to(repository)),
            "source_archive_fingerprint": replay.manifest_fingerprint,
            "source_window_count": replay.window_count,
            "source_files": _file_manifest(repository, source_files),
            "source_dataset_provenance": _file_manifest(
                repository, dataset_files,
            ),
            "pre_repair_commit": commit,
            "pre_repair_config": {
                "path": str(config_path.relative_to(repository)),
                "sha256": _sha256(config_path),
            },
            "blind_replay_audit": audit,
            "post_repair_expectation": {
                "state": "calibrating",
                "calibration_ready": False,
                "result_count": 0,
                "rca_not_ready_event_count": 0,
                "maximum_symptom_score": 0.0,
                "invalid_observation_counts": invalid_counts,
                "reason": (
                    "missing_observation_or_calibration_readiness"
                ),
            },
            "post_repair_files": _file_manifest(
                repository,
                [control_path, readiness_path, results_path],
            ),
            "manifest_fingerprint": "",
        }
        payload["manifest_fingerprint"] = fingerprint(payload)
        _write_once(
            regression_root / "regression-manifest.json",
            (canonical_json(payload) + "\n").encode("utf-8"),
        )
        print(canonical_json({
            "manifest_fingerprint": payload["manifest_fingerprint"],
            "regression": name,
            "status": "sealed",
        }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
