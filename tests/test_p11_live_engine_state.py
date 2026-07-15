from __future__ import annotations


def test_live_engine_state_component_bundle_round_trip(tmp_path):
    from proberca.live.engine_state import (
        restore_live_engine_state,
        write_live_engine_state,
    )
    from test_p10_engine import engine

    source = engine()
    directory = tmp_path / "engine-state"
    write_live_engine_state(source, directory)

    restored = restore_live_engine_state(engine(), directory)
    assert restored.config_fingerprint == source.config_fingerprint
    assert restored.aggregator.to_dict() == source.aggregator.to_dict()
    assert restored.baseline.to_dict() == source.baseline.to_dict()
    assert restored.alert_machine.to_dict() == source.alert_machine.to_dict()
    assert restored.topology_store.to_dict() == source.topology_store.to_dict()
    assert restored._last_timestamp == source._last_timestamp
    assert restored._alerts == source._alerts
    assert restored._reports == source._reports
    assert restored._failures == source._failures


def test_generation_accepts_full_engine_bundle_without_nested_current(tmp_path):
    from proberca.live.engine_state import write_live_engine_state
    from proberca.live.generation import ImmutableGenerationStore
    from test_p10_engine import engine

    source = engine()
    generation = ImmutableGenerationStore(tmp_path / "generations").prepare(
        previous_generation_id=None,
        proposed_sequence=1,
        window_start_ns=0,
        window_end_ns=1,
        leadership_epoch=1,
        holder_fingerprint="h" * 64,
        engine_state=lambda path: write_live_engine_state(source, path),
        output_ledger={"ledger": 1},
        output_bundle={
            "alerts.jsonl": "",
            "failures.jsonl": "",
            "reports": {},
        },
        config_fingerprint="c" * 64,
        code_schema_version="generation_v5",
    )
    assert (generation.path / "engine_state" / "metadata.json").is_file()
    assert not list(generation.path.rglob("CURRENT"))
    assert not list(generation.path.rglob("sequence_commits.jsonl"))
