from __future__ import annotations

import json

import pytest


def test_checkpoint_publish_revalidates_fence_at_each_durable_boundary(tmp_path):
    from proberca.orchestration.checkpoint import save_engine_checkpoint
    from test_p10_checkpoint_atomicity import warmed_engine

    operations = []
    token = object()

    def validate(operation, actual):
        assert actual is token
        operations.append(operation)

    save_engine_checkpoint(
        warmed_engine(), tmp_path / "checkpoint", manifest_hash="manifest-a",
        replay_sequence=4, fence_token=token, fence_validator=validate,
        instance_fingerprint="instance-a", transaction_id="transaction-a")
    assert operations == [
        "generation_prepare", "generation_publish", "current_replace"]


def test_lost_fence_before_current_keeps_previous_generation_selected(
        tmp_path):
    from proberca.orchestration.checkpoint import save_engine_checkpoint
    from test_p10_checkpoint_atomicity import warmed_engine

    root = tmp_path / "checkpoint"
    save_engine_checkpoint(
        warmed_engine(4), root, manifest_hash="manifest-a", replay_sequence=4)
    previous = json.loads((root / "CURRENT").read_text())["generation_id"]

    def validate(operation, _token):
        if operation == "current_replace":
            raise RuntimeError("fence lost")

    with pytest.raises(RuntimeError, match="fence lost"):
        save_engine_checkpoint(
            warmed_engine(5), root, manifest_hash="manifest-a", replay_sequence=5,
            fence_token=object(), fence_validator=validate,
            instance_fingerprint="instance-b", transaction_id="transaction-b")
    assert json.loads((root / "CURRENT").read_text())["generation_id"] == previous
import json


def test_two_instances_never_share_temporary_generation_path(tmp_path):
    from proberca.orchestration.checkpoint import temporary_generation_path

    generations = tmp_path / "generations"
    generations.mkdir()
    first = temporary_generation_path(
        generations, "generation-7", "instance-a", "transaction-a")
    second = temporary_generation_path(
        generations, "generation-7", "instance-b", "transaction-b")
    assert first != second
    assert first.name.endswith(".tmp") and second.name.endswith(".tmp")


def test_owned_orphan_cleanup_never_deletes_another_instance_tmp(tmp_path):
    from proberca.orchestration.checkpoint import cleanup_owned_temporaries

    generations = tmp_path / "generations"
    generations.mkdir()
    own = generations / ".generation.instance-a.tx-a.tmp"
    other = generations / ".generation.instance-b.tx-b.tmp"
    own.mkdir(); other.mkdir()
    cleanup_owned_temporaries(generations, "instance-a", active_transaction_ids=())
    assert not own.exists()
    assert other.exists()


def test_current_pointer_temporary_name_is_transaction_unique(tmp_path):
    from proberca.orchestration.checkpoint import temporary_current_path

    assert temporary_current_path(tmp_path, "instance-a", "tx-a") != \
        temporary_current_path(tmp_path, "instance-b", "tx-b")


def test_checkpoint_diagnostics_have_monotonic_sanitized_events():
    from proberca.live.transaction import DiagnosticRecorder

    recorder = DiagnosticRecorder("instance-fingerprint")
    recorder.record("checkpoint", "prepare", sequence=7, generation_id="g7")
    recorder.record("checkpoint", "publish", sequence=7, generation_id="g7")
    payload = recorder.to_list()
    assert [item["event_index"] for item in payload] == [1, 2]
    assert all(item["instance_fingerprint"] == "instance-fingerprint" for item in payload)
    assert "uid" not in json.dumps(payload).lower()


def test_retention_never_deletes_another_owner_or_active_transaction_tmp(tmp_path):
    from types import SimpleNamespace
    from proberca.orchestration.checkpoint import (
        apply_checkpoint_retention, save_engine_checkpoint,
        temporary_generation_path,
    )
    from test_p10_checkpoint_atomicity import warmed_engine

    root = tmp_path / "checkpoint"
    save_engine_checkpoint(
        warmed_engine(), root, manifest_hash="manifest-a", replay_sequence=4)
    generations = root / "generations"
    active = temporary_generation_path(
        generations, "generation-a", "instance-a", "active-tx")
    other = temporary_generation_path(
        generations, "generation-b", "instance-b", "other-tx")
    active.mkdir()
    other.mkdir()
    apply_checkpoint_retention(
        root, SimpleNamespace(checkpoint_generations=2,
                              checkpoint_min_age_sec=0), now_ns=10**20,
        instance_fingerprint="instance-a",
        active_transaction_ids=("active-tx",))
    assert active.is_dir()
    assert other.is_dir()


def test_concurrent_identical_generation_publish_is_idempotent(tmp_path, monkeypatch):
    import threading

    from proberca.live.generation import ImmutableGenerationStore

    store = ImmutableGenerationStore(tmp_path / "live-generations")
    barrier = threading.Barrier(2)
    from proberca.live import generation as generation_module
    original_replace = generation_module.os.replace

    def synchronized_replace(source, target):
        if generation_module.Path(source).is_dir() and generation_module.Path(source).name.endswith(".tmp"):
            barrier.wait(timeout=2)
        return original_replace(source, target)

    monkeypatch.setattr(generation_module.os, "replace", synchronized_replace)
    results = []
    errors = []

    def engine_writer(path):
        (path / "component.json").write_text("{}", encoding="utf-8")


    def publish(instance):
        try:
            results.append(store.prepare(
                previous_generation_id=None,
                proposed_sequence=1,
                window_start_ns=0,
                window_end_ns=1,
                leadership_epoch=1,
                holder_fingerprint="h" * 64,
                engine_state=engine_writer,
                output_ledger={"ledger": 1},
                output_bundle={
                    "alerts.jsonl": "", "failures.jsonl": "", "reports": {},
                },
                config_fingerprint="c" * 64,
                code_schema_version="generation_v5",
                transaction_id="transaction-" + instance,
                instance_fingerprint=instance,
            ))
        except Exception as error:
            errors.append(error)

    threads = [
        threading.Thread(target=publish, args=("instance-a",)),
        threading.Thread(target=publish, args=("instance-b",)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert errors == []
    assert len(results) == 2
    assert results[0].generation_id == results[1].generation_id
    assert not list((tmp_path / "live-generations").glob(".*.tmp"))
