from __future__ import annotations

import json

import pytest


def test_generation_v5_is_complete_immutable_and_content_addressed(tmp_path):
    from proberca.live.generation import ImmutableGenerationStore

    store = ImmutableGenerationStore(tmp_path / "generation-store")
    generation = store.prepare(
        previous_generation_id=None, proposed_sequence=1, window_start_ns=0,
        window_end_ns=1, leadership_epoch=1, holder_fingerprint="h" * 64,
        engine_state={"baseline": "state"}, output_ledger={"ledger": "value"},
        output_bundle={"alerts.jsonl": "", "failures.jsonl": "", "reports": {}},
        config_fingerprint="c" * 64, code_schema_version="p11-live-v5",
    )
    manifest = json.loads((generation.path / "manifest.json").read_text())
    assert manifest["schema_version"] == "generation_v5"
    assert generation.path.name == generation.generation_id
    assert (generation.path / "engine_state" / "state.json").is_file()
    assert (generation.path / "output_bundle" / "alerts.jsonl").is_file()
    assert store.load(generation.generation_id).generation_fingerprint == generation.generation_fingerprint


def test_generation_same_content_is_idempotent_but_corruption_fails_fast(tmp_path):
    from proberca.live.generation import GenerationIntegrityError, ImmutableGenerationStore

    store = ImmutableGenerationStore(tmp_path / "generation-store")
    kwargs = dict(
        previous_generation_id=None, proposed_sequence=1, window_start_ns=0,
        window_end_ns=1, leadership_epoch=1, holder_fingerprint="h" * 64,
        engine_state={"state": 1}, output_ledger={"ledger": 1},
        output_bundle={"alerts.jsonl": "", "failures.jsonl": "", "reports": {}},
        config_fingerprint="c" * 64, code_schema_version="p11-live-v5",
    )
    first = store.prepare(**kwargs)
    assert store.prepare(**kwargs).generation_id == first.generation_id
    (first.path / "output_ledger.json").write_text("{}")
    with pytest.raises(GenerationIntegrityError):
        store.load(first.generation_id)


def test_retention_keeps_current_and_previous_and_projector_survives_truncated_chain(
    tmp_path,
):
    from proberca.live.generation import ImmutableGenerationStore
    from proberca.live.output_projector import OutputProjector

    store = ImmutableGenerationStore(tmp_path / "generation-store")
    previous = None
    values = []
    for sequence in range(1, 4):
        generation = store.prepare(
            previous_generation_id=previous,
            proposed_sequence=sequence,
            window_start_ns=sequence - 1,
            window_end_ns=sequence,
            leadership_epoch=1,
            holder_fingerprint="h" * 64,
            engine_state={"sequence": sequence},
            output_ledger={"ledger": sequence},
            output_bundle={
                "alerts.jsonl": f"{sequence}\n",
                "failures.jsonl": "",
                "reports": {},
            },
            config_fingerprint="c" * 64,
            code_schema_version="generation_v5",
        )
        values.append(generation)
        previous = generation.generation_id
    projector = OutputProjector(tmp_path / "output", store)
    projector.project(values[1].generation_id)

    issues = store.apply_retention(
        values[2].generation_id,
        keep_generations=2,
        minimum_age_sec=0,
        now=10**20,
    )
    assert issues == []
    assert not values[0].path.exists()
    assert values[1].path.is_dir() and values[2].path.is_dir()
    marker = projector.project(values[2].generation_id)
    assert marker.materialized_sequence == 3
