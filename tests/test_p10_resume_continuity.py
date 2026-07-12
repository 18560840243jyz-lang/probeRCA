from __future__ import annotations

import json

import pytest

from proberca.orchestration.checkpoint import save_engine_checkpoint
from proberca.replay import ReplayRunner
from proberca.replay.output import ReplayOutputError
from test_p10_runner_cli import dataset, replay_engine


def canonical_files(path):
    reports = {
        item.name: json.loads(item.read_text(encoding="utf-8"))
        for item in (path / "reports").glob("*.json")
    }
    for report in reports.values():
        report.pop("runtime", None)
    manifest = json.loads((path / "run_manifest.json").read_text(encoding="utf-8"))
    manifest.pop("runtime_summary", None)
    return {
        "alerts": (path / "alerts.jsonl").read_text(encoding="utf-8"),
        "failures": (path / "failures.jsonl").read_text(encoding="utf-8"),
        "reports": reports,
        "manifest": manifest,
    }


def partial_run_with_checkpoint(root, output, checkpoint):
    runner = ReplayRunner(root, output)
    _, results = runner.run(stop_after_window=8)
    save_engine_checkpoint(
        runner.engine, checkpoint,
        manifest_hash=runner.manifest.manifest_sha256, replay_sequence=8)
    assert any(item.state == "soft" for result in results for item in result.alerts)


def test_resume_to_new_output_rebuilds_complete_history(tmp_path):
    root = dataset(tmp_path / "dataset")
    full_output = tmp_path / "full"
    ReplayRunner(root, full_output).run()
    checkpoint = tmp_path / "checkpoint"
    partial_run_with_checkpoint(root, tmp_path / "partial", checkpoint)
    resumed_output = tmp_path / "resumed-new"
    ReplayRunner(root, resumed_output, resume_from=checkpoint).run()
    assert canonical_files(resumed_output) == canonical_files(full_output)


def test_resume_to_original_output_is_idempotent_and_complete(tmp_path):
    root = dataset(tmp_path / "dataset")
    full_output = tmp_path / "full"
    ReplayRunner(root, full_output).run()
    original = tmp_path / "partial"
    checkpoint = tmp_path / "checkpoint"
    partial_run_with_checkpoint(root, original, checkpoint)
    ReplayRunner(root, original, resume_from=checkpoint).run()
    assert canonical_files(original) == canonical_files(full_output)
    alerts = [line for line in (original / "alerts.jsonl").read_text().splitlines() if line]
    assert len(alerts) == len({json.loads(line)["alert_id"] for line in alerts})


def test_resume_rejects_conflicting_existing_report(tmp_path):
    root = dataset(tmp_path / "dataset")
    original = tmp_path / "partial"
    checkpoint = tmp_path / "checkpoint"
    partial_run_with_checkpoint(root, original, checkpoint)
    reports = original / "reports"
    (reports / "conflict.json").write_text('{"report_fingerprint":"conflict"}')
    with pytest.raises(ReplayOutputError):
        ReplayRunner(root, original, resume_from=checkpoint)


def test_resume_rejects_truncated_alert_ledger(tmp_path):
    root = dataset(tmp_path / "dataset")
    original = tmp_path / "partial"
    checkpoint = tmp_path / "checkpoint"
    partial_run_with_checkpoint(root, original, checkpoint)
    alerts = original / "alerts.jsonl"
    alerts.write_text(alerts.read_text(encoding="utf-8")[:10], encoding="utf-8")
    with pytest.raises(ReplayOutputError):
        ReplayRunner(root, original, resume_from=checkpoint)
