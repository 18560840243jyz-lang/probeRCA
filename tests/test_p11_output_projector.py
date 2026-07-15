from __future__ import annotations


def test_projector_recovers_view_lagging_multiple_generations(tmp_path):
    from proberca.live.generation import ImmutableGenerationStore
    from proberca.live.output_projector import OutputProjector

    store = ImmutableGenerationStore(tmp_path / "generations")
    first = store.prepare(previous_generation_id=None, proposed_sequence=1,
        window_start_ns=0, window_end_ns=1, leadership_epoch=1,
        holder_fingerprint="h" * 24, engine_state={"state": 1}, output_ledger={"ledger": 1},
        output_bundle={"alerts.jsonl": "one\n", "failures.jsonl": "", "reports": {}},
        config_fingerprint="c" * 64, code_schema_version="p11-live-v5")
    latest = store.prepare(previous_generation_id=first.generation_id, proposed_sequence=2,
        window_start_ns=1, window_end_ns=2, leadership_epoch=1,
        holder_fingerprint="h" * 24, engine_state={"state": 2}, output_ledger={"ledger": 2},
        output_bundle={"alerts.jsonl": "two\n", "failures.jsonl": "", "reports": {}},
        config_fingerprint="c" * 64, code_schema_version="p11-live-v5")
    output = tmp_path / "output"
    projector = OutputProjector(output, store)
    projector.project(first.generation_id)
    marker = projector.project(latest.generation_id)
    assert marker.materialized_generation_id == latest.generation_id
    assert (output / "alerts.jsonl").read_text() == "two\n"
