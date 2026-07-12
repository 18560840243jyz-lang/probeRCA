from __future__ import annotations

import json
import os

import pytest

import proberca.orchestration.checkpoint as checkpoint_module
from proberca.orchestration.checkpoint import (
    ReplayCheckpointError, restore_engine_checkpoint, save_engine_checkpoint,
)
from test_p10_engine import engine, window


def warmed_engine(last_window=4):
    target = engine()
    for index in range(1, last_window + 1):
        target.process_window(window(index, 1.0, include_topology=index == 1))
    return target


def test_checkpoint_uses_generation_and_atomic_current_pointer(tmp_path):
    path = tmp_path / "checkpoint"
    save_engine_checkpoint(
        warmed_engine(), path, manifest_hash="manifest-a", replay_sequence=4)
    current = json.loads((path / "CURRENT").read_text(encoding="utf-8"))
    generation = path / "generations" / current["generation_id"]
    assert generation.is_dir()
    assert current["checkpoint_fingerprint"]
    assert (generation / "metadata.json").is_file()


def test_replace_failure_never_removes_previous_checkpoint(tmp_path, monkeypatch):
    path = tmp_path / "checkpoint"
    first = warmed_engine(4)
    save_engine_checkpoint(first, path, manifest_hash="manifest-a", replay_sequence=4)
    original_replace = checkpoint_module.os.replace

    def fail_replace(source, destination):
        raise OSError("injected generation publish failure")

    monkeypatch.setattr(checkpoint_module.os, "replace", fail_replace)
    with pytest.raises(OSError, match="injected"):
        save_engine_checkpoint(
            warmed_engine(5), path, manifest_hash="manifest-a", replay_sequence=5)
    monkeypatch.setattr(checkpoint_module.os, "replace", original_replace)
    assert restore_engine_checkpoint(
        engine(), path, manifest_hash="manifest-a") == 4


def test_current_replace_failure_keeps_old_generation_selected(tmp_path, monkeypatch):
    path = tmp_path / "checkpoint"
    save_engine_checkpoint(
        warmed_engine(4), path, manifest_hash="manifest-a", replay_sequence=4)
    original_replace = checkpoint_module.os.replace

    def fail_current(source, destination):
        if os.fspath(destination).endswith("CURRENT"):
            raise OSError("injected CURRENT replace failure")
        return original_replace(source, destination)

    monkeypatch.setattr(checkpoint_module.os, "replace", fail_current)
    with pytest.raises(OSError, match="CURRENT"):
        save_engine_checkpoint(
            warmed_engine(5), path, manifest_hash="manifest-a", replay_sequence=5)
    monkeypatch.setattr(checkpoint_module.os, "replace", original_replace)
    assert restore_engine_checkpoint(engine(), path, manifest_hash="manifest-a") == 4


def test_loader_ignores_orphans_and_unreferenced_generations(tmp_path):
    path = tmp_path / "checkpoint"
    save_engine_checkpoint(
        warmed_engine(), path, manifest_hash="manifest-a", replay_sequence=4)
    (path / "generations" / ".orphan.tmp").mkdir()
    (path / "generations" / "unreferenced").mkdir()
    assert restore_engine_checkpoint(engine(), path, manifest_hash="manifest-a") == 4


def test_corrupt_current_or_selected_generation_fails_fast(tmp_path):
    path = tmp_path / "checkpoint"
    save_engine_checkpoint(
        warmed_engine(), path, manifest_hash="manifest-a", replay_sequence=4)
    current_path = path / "CURRENT"
    current = json.loads(current_path.read_text(encoding="utf-8"))
    current_path.write_text("not-json", encoding="utf-8")
    with pytest.raises(ReplayCheckpointError):
        restore_engine_checkpoint(engine(), path, manifest_hash="manifest-a")
    current_path.write_text(json.dumps(current), encoding="utf-8")
    selected = path / "generations" / current["generation_id"] / "metadata.json"
    selected.write_text("{}", encoding="utf-8")
    with pytest.raises(ReplayCheckpointError):
        restore_engine_checkpoint(engine(), path, manifest_hash="manifest-a")


def test_three_successive_saves_keep_current_valid(tmp_path):
    path = tmp_path / "checkpoint"
    for sequence in (4, 5, 6):
        save_engine_checkpoint(
            warmed_engine(sequence), path,
            manifest_hash="manifest-a", replay_sequence=sequence)
        assert restore_engine_checkpoint(
            engine(), path, manifest_hash="manifest-a") == sequence
    assert len([item for item in (path / "generations").iterdir()
                if not item.name.startswith(".")]) >= 3


@pytest.mark.parametrize("failure_stage", [
    "component_snapshot",
    "metadata_write",
    "file_fsync",
    "generation_validation",
    "current_temp_write",
])
def test_crash_before_current_commit_preserves_previous_generation(
        tmp_path, monkeypatch, failure_stage):
    path = tmp_path / "checkpoint"
    save_engine_checkpoint(
        warmed_engine(4), path, manifest_hash="manifest-a", replay_sequence=4)
    replacement = warmed_engine(5)

    if failure_stage == "component_snapshot":
        monkeypatch.setattr(
            replacement.metric_learner.runtime_history, "snapshot",
            lambda *_: (_ for _ in ()).throw(OSError("component snapshot")))
    elif failure_stage == "metadata_write":
        original = checkpoint_module._write_json_fsync

        def fail_metadata(target, payload):
            if target.name == "metadata.json":
                raise OSError("metadata write")
            return original(target, payload)

        monkeypatch.setattr(checkpoint_module, "_write_json_fsync", fail_metadata)
    elif failure_stage == "file_fsync":
        monkeypatch.setattr(
            checkpoint_module, "_fsync_file",
            lambda *_: (_ for _ in ()).throw(OSError("file fsync")))
    elif failure_stage == "generation_validation":
        original = checkpoint_module._validate_generation

        def fail_temporary_generation(target, expected_fingerprint=None):
            if target.name.startswith("."):
                raise ReplayCheckpointError("generation validation")
            return original(target, expected_fingerprint)

        monkeypatch.setattr(
            checkpoint_module, "_validate_generation", fail_temporary_generation)
    elif failure_stage == "current_temp_write":
        original = checkpoint_module._write_json_fsync

        def fail_current_temp(target, payload):
            if target.name == "CURRENT.tmp":
                raise OSError("CURRENT temporary write")
            return original(target, payload)

        monkeypatch.setattr(checkpoint_module, "_write_json_fsync", fail_current_temp)

    with pytest.raises((OSError, ReplayCheckpointError)):
        save_engine_checkpoint(
            replacement, path, manifest_hash="manifest-a", replay_sequence=5)
    assert restore_engine_checkpoint(engine(), path, manifest_hash="manifest-a") == 4


def test_published_unselected_generation_does_not_replace_old_checkpoint(
        tmp_path, monkeypatch):
    path = tmp_path / "checkpoint"
    save_engine_checkpoint(
        warmed_engine(4), path, manifest_hash="manifest-a", replay_sequence=4)
    original = checkpoint_module._write_json_fsync

    def fail_current_temp(target, payload):
        if target.name == "CURRENT.tmp":
            raise OSError("CURRENT write after generation publish")
        return original(target, payload)

    monkeypatch.setattr(checkpoint_module, "_write_json_fsync", fail_current_temp)
    with pytest.raises(OSError, match="after generation publish"):
        save_engine_checkpoint(
            warmed_engine(5), path, manifest_hash="manifest-a", replay_sequence=5)
    current = json.loads((path / "CURRENT").read_text(encoding="utf-8"))
    published = [item.name for item in (path / "generations").iterdir()
                 if not item.name.startswith(".")]
    assert current["generation_id"] in published
    assert len(published) == 2
    assert restore_engine_checkpoint(engine(), path, manifest_hash="manifest-a") == 4


def test_cleanup_failure_after_current_commit_keeps_new_checkpoint(
        tmp_path, monkeypatch):
    path = tmp_path / "checkpoint"
    first = warmed_engine(4)
    save_engine_checkpoint(first, path, manifest_hash="manifest-a", replay_sequence=4)
    (path / "generations" / ".abandoned.tmp").mkdir()
    replacement = warmed_engine(5)
    monkeypatch.setattr(
        checkpoint_module, "_cleanup_orphan_temporaries",
        lambda *_: (_ for _ in ()).throw(OSError("cleanup failure")))
    save_engine_checkpoint(
        replacement, path, manifest_hash="manifest-a", replay_sequence=5)
    assert restore_engine_checkpoint(engine(), path, manifest_hash="manifest-a") == 5
    assert replacement._checkpoint_issues == [{
        "reason_code": "checkpoint_cleanup_failed",
        "detail": "cleanup failure",
    }]


def test_missing_current_never_falls_back_to_generation_scan(tmp_path):
    path = tmp_path / "checkpoint"
    save_engine_checkpoint(
        warmed_engine(4), path, manifest_hash="manifest-a", replay_sequence=4)
    (path / "CURRENT").unlink()
    with pytest.raises(ReplayCheckpointError, match="CURRENT"):
        restore_engine_checkpoint(engine(), path, manifest_hash="manifest-a")
