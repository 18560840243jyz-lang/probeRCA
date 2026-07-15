from __future__ import annotations

from types import SimpleNamespace

import pytest

from proberca.config import LeaderElectionConfig


def _config():
    return LeaderElectionConfig(
        enabled=True,
        lease_namespace="test",
        lease_name="live",
        lease_duration_sec=10,
        renew_deadline_sec=6,
        retry_period_sec=1,
    )


def test_lease_fence_is_stable_across_renew_and_changes_after_handoff():
    from proberca.live.leader import InMemoryLeaseAPI, LeaseCoordinator

    now = [100.0]
    api = InMemoryLeaseAPI()
    first = LeaseCoordinator(api, _config(), "instance-one", clock=lambda: now[0])
    token = first.acquire()
    assert token is not None
    first.activate(token)
    first.renew()
    assert first.fence_token == token

    now[0] = 111.0
    second = LeaseCoordinator(api, _config(), "instance-two", clock=lambda: now[0])
    replacement = second.acquire()
    assert replacement is not None
    second.activate(replacement)
    assert replacement.token_fingerprint != token.token_fingerprint
    assert replacement.lease_transition == token.lease_transition + 1
    assert not first.validate_fence(token)


@pytest.mark.parametrize("operation", [
    "engine_begin",
    "engine_complete",
    "output_publish",
    "generation_publish",
    "current_replace",
    "sequence_commit",
    "retention_cleanup",
])
def test_standby_and_old_fence_cannot_authorize_durable_write(operation):
    from proberca.live.leader import (
        InMemoryLeaseAPI,
        LeaseCoordinator,
        LeadershipFenceError,
    )

    coordinator = LeaseCoordinator(
        InMemoryLeaseAPI(),
        _config(),
        "standby",
        clock=lambda: 10.0,
    )
    with pytest.raises(LeadershipFenceError):
        coordinator.authorize(operation, None)

    token = coordinator.acquire()
    assert token is not None
    with pytest.raises(LeadershipFenceError):
        coordinator.authorize(operation, token)
    coordinator.activate(token)
    coordinator.authorize(operation, token)
    coordinator.lose("lease_replaced")
    with pytest.raises(LeadershipFenceError):
        coordinator.authorize(operation, token)


def test_follower_does_not_call_engine_or_prepare_generation():
    from proberca.live.coordinator import LiveCoordinatorState
    from proberca.live.runner import ProbeRCALiveRunner

    calls = []
    coordinator = SimpleNamespace(state=LiveCoordinatorState.STANDBY)
    runner = ProbeRCALiveRunner(
        coordinator=coordinator,
        inventory=SimpleNamespace(ready=True, freeze=lambda *_: "revision"),
        topology_builder=lambda *_: calls.append("topology"),
        metric_collector=lambda *_: calls.append("metrics"),
        window_adapter=lambda *_: calls.append("engine"),
        commit_payload_builder=lambda *_: calls.append("prepare"),
    )
    with pytest.raises(Exception, match="leadership"):
        runner.process_window(
            SimpleNamespace(sequence=99, start_ns=0, end_ns=1),
        )
    assert calls == []


def test_runner_sequence_and_commit_are_owned_by_transactional_coordinator():
    from proberca.live.coordinator import LiveCoordinatorState
    from proberca.live.runner import ProbeRCALiveRunner

    calls = []

    class Coordinator:
        state = LiveCoordinatorState.LEADER_ACTIVE

        def begin_window(self, start_ns, end_ns, attempt_index=1):
            calls.append(("begin", start_ns, end_ns))
            return SimpleNamespace(
                sequence=5,
                working_engine=SimpleNamespace(),
                engine_result=None,
            )

        def run_engine(self, context, value):
            calls.append(("engine", context.sequence, value))
            context.engine_result = value
            return value

        def prepare_generation(self, context, **payload):
            calls.append(("prepare", context.sequence))
            return payload

        def commit(self, context, generation):
            calls.append(("commit", context.sequence))

    runner = ProbeRCALiveRunner(
        coordinator=Coordinator(),
        inventory=SimpleNamespace(ready=True, freeze=lambda *_: "revision"),
        topology_builder=lambda *_: "topology",
        metric_collector=lambda *_: (["node"], ["edge"]),
        window_adapter=lambda window, *_: (
            calls.append(("adapter_sequence", window.sequence))
            or "engine-input"
        ),
        commit_payload_builder=lambda *_: {
            "engine_state": {},
            "output_ledger": {},
            "output_bundle": {},
            "config_fingerprint": "c" * 64,
            "code_schema_version": "generation_v5",
        },
    )
    result = runner.process_window(
        SimpleNamespace(sequence=999, start_ns=4, end_ns=5),
    )
    assert result == "engine-input"
    assert calls == [
        ("begin", 4, 5),
        ("adapter_sequence", 5),
        ("engine", 5, "engine-input"),
        ("prepare", 5),
        ("commit", 5),
    ]


def test_leader_recovery_uses_run_state_before_active_window(tmp_path):
    from proberca.live.coordinator import LiveCommitCoordinator
    from proberca.live.generation import ImmutableGenerationStore
    from proberca.live.run_state import (
        InMemoryLeaseRunStateStore,
        LeaseRunStateRecord,
    )

    record = LeaseRunStateRecord.initial(
        run_id="run",
        cluster_id="cluster",
        namespace_scope=("ns",),
        config_fingerprint="c" * 64,
        code_schema_version="generation_v5",
    )
    coordinator = LiveCommitCoordinator(
        InMemoryLeaseRunStateStore(record),
        ImmutableGenerationStore(tmp_path / "generations"),
        "instance",
    )
    coordinator.acquire_and_recover(active_engine=SimpleNamespace())
    recovered = coordinator.recover_current(
        engine_loader=lambda value: value,
    )
    assert recovered is coordinator.active_engine
    assert coordinator.begin_window(0, 1).sequence == 1


def test_output_materialization_revalidates_fence_before_atomic_replace(tmp_path):
    from proberca.orchestration.state import OutputLedger
    from proberca.replay.output import ReplayOutputWriter

    writer = ReplayOutputWriter(tmp_path / "output")
    ledger = OutputLedger.create(
        alerts=[],
        reports=[],
        failures=[],
        processed_window_count=1,
        last_processed_timestamp=1,
        pending_incident=None,
        dataset_fingerprint="dataset",
        config_fingerprint="config",
    )

    def reject(operation, token):
        assert operation == "output_publish"
        assert token == "old-token"
        raise RuntimeError("output fence changed")

    with pytest.raises(RuntimeError, match="output fence changed"):
        writer.materialize_ledger(
            ledger,
            fence_token="old-token",
            fence_validator=reject,
        )
    assert not (tmp_path / "output" / "alerts.jsonl").exists()


def test_output_publish_accepts_exact_previous_ledger_but_rejects_unknown_content(
    tmp_path,
):
    from proberca.orchestration.state import OutputLedger
    from proberca.replay.output import ReplayOutputError, ReplayOutputWriter

    class Alert:
        alert_id = "alert-1"

        @staticmethod
        def to_dict():
            return {"alert_id": "alert-1", "state": "healthy"}

    first = OutputLedger.create(
        alerts=[],
        reports=[],
        failures=[],
        processed_window_count=1,
        last_processed_timestamp=1,
        pending_incident=None,
        dataset_fingerprint="dataset",
        config_fingerprint="config",
    )
    second = OutputLedger.create(
        alerts=[Alert()],
        reports=[],
        failures=[],
        processed_window_count=2,
        last_processed_timestamp=2,
        pending_incident=None,
        dataset_fingerprint="dataset",
        config_fingerprint="config",
    )
    writer = ReplayOutputWriter(tmp_path / "output")
    writer.materialize_ledger(first)
    writer.materialize_ledger(second, previous_ledger=first)
    assert '"alert_id":"alert-1"' in (
        tmp_path / "output" / "alerts.jsonl"
    ).read_text()

    (tmp_path / "output" / "alerts.jsonl").write_text("unknown\n")
    with pytest.raises(ReplayOutputError, match="conflict"):
        writer.materialize_ledger(second, previous_ledger=first)
