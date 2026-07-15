from __future__ import annotations

import json

import pytest


def _generation(store, sequence, previous=None, alerts=""):
    return store.prepare(
        previous_generation_id=previous,
        proposed_sequence=sequence,
        window_start_ns=sequence - 1,
        window_end_ns=sequence,
        leadership_epoch=1,
        holder_fingerprint="h" * 24,
        engine_state={"state": sequence},
        output_ledger={"ledger": sequence},
        output_bundle={
            "alerts.jsonl": alerts,
            "failures.jsonl": "",
            "reports": {},
        },
        config_fingerprint="c" * 64,
        code_schema_version="p11-live-v5",
    )


def test_missing_marker_with_nonempty_output_fails_fast(tmp_path):
    from proberca.live.generation import ImmutableGenerationStore
    from proberca.live.output_projector import (
        OutputProjectionError,
        OutputProjector,
    )

    store = ImmutableGenerationStore(tmp_path / "generations")
    generation = _generation(store, 1, alerts="one\n")
    output = tmp_path / "output"
    output.mkdir()
    (output / "alerts.jsonl").write_text("unknown\n")

    with pytest.raises(OutputProjectionError, match="marker"):
        OutputProjector(output, store).project(generation.generation_id)


def test_ahead_marker_fails_instead_of_rolling_output_back(tmp_path):
    from proberca.live.generation import ImmutableGenerationStore
    from proberca.live.output_projector import (
        OutputProjectionError,
        OutputProjector,
    )

    store = ImmutableGenerationStore(tmp_path / "generations")
    first = _generation(store, 1, alerts="one\n")
    second = _generation(
        store, 2, previous=first.generation_id, alerts="two\n",
    )
    projector = OutputProjector(tmp_path / "output", store)
    projector.project(second.generation_id)

    with pytest.raises(OutputProjectionError, match="ahead"):
        projector.project(first.generation_id)


def test_unknown_report_is_never_deleted_or_overwritten(tmp_path):
    from proberca.live.generation import ImmutableGenerationStore
    from proberca.live.output_projector import (
        OutputProjectionError,
        OutputProjector,
    )

    store = ImmutableGenerationStore(tmp_path / "generations")
    generation = _generation(store, 1)
    output = tmp_path / "output"
    projector = OutputProjector(output, store)
    projector.project(generation.generation_id)
    unknown = output / "reports" / "unknown.json"
    unknown.parent.mkdir(exist_ok=True)
    unknown.write_text(json.dumps({"foreign": True}))

    with pytest.raises(OutputProjectionError, match="unknown"):
        projector.project(generation.generation_id)
    assert unknown.is_file()


def test_partial_current_view_is_rebuilt_from_generation_bundle(tmp_path):
    from proberca.live.generation import ImmutableGenerationStore
    from proberca.live.output_projector import OutputProjector

    store = ImmutableGenerationStore(tmp_path / "generations")
    generation = _generation(store, 1, alerts="one\n")
    output = tmp_path / "output"
    projector = OutputProjector(output, store)
    projector.project(generation.generation_id)
    (output / "alerts.jsonl").unlink()

    marker = projector.project(generation.generation_id)
    assert marker.materialized_generation_id == generation.generation_id
    assert (output / "alerts.jsonl").read_text() == "one\n"


def test_uncommitted_run_state_rejects_any_existing_output_view(tmp_path):
    from proberca.live.generation import ImmutableGenerationStore
    from proberca.live.output_projector import OutputProjectionError, OutputProjector

    output = tmp_path / "output"
    output.mkdir()
    (output / "alerts.jsonl").write_text("external\n", encoding="utf-8")
    projector = OutputProjector(
        output,
        ImmutableGenerationStore(tmp_path / "generations"),
    )
    with pytest.raises(OutputProjectionError, match="empty"):
        projector.validate_initial_empty()
