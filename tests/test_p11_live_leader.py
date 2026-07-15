from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from proberca.config import LeaderElectionConfig, LiveConfig
from proberca.live.health import LiveHealthState
from proberca.live.leader import InMemoryLeaseAPI, LeaseCoordinator
from proberca.live.runner import ProbeRCALiveRunner
from proberca.live.scheduler import LiveWindowScheduler, MissedWindowError


def test_epoch_scheduler_never_repeats_or_zero_fills_missed_windows():
    scheduler = LiveWindowScheduler(LiveConfig(
        window_sec=1, collection_delay_sec=0, maximum_catchup_windows=1,
        fail_on_missed_window=True))
    first = scheduler.eligible_windows(now_ns=2_000_000_000)
    assert [(item.start_ns, item.end_ns, item.sequence) for item in first] == [
        (1_000_000_000, 2_000_000_000, 1)]
    scheduler.commit(first[0])
    assert scheduler.eligible_windows(now_ns=2_500_000_000) == ()
    with pytest.raises(MissedWindowError):
        scheduler.eligible_windows(now_ns=5_000_000_000)


def test_scheduler_snapshot_resume_continues_next_sequence():
    config = LiveConfig(window_sec=1, collection_delay_sec=0)
    scheduler = LiveWindowScheduler(config)
    window = scheduler.eligible_windows(2_000_000_000)[0]
    scheduler.commit(window)
    restored = LiveWindowScheduler.restore(config, scheduler.to_dict())
    assert restored.eligible_windows(3_000_000_000)[0].sequence == 2


def test_two_lease_contenders_have_at_most_one_leader_and_loss_stops_commit():
    api = InMemoryLeaseAPI()
    config = LeaderElectionConfig(
        enabled=True, lease_namespace="observability", lease_name="proberca",
        lease_duration_sec=10, renew_deadline_sec=6, retry_period_sec=2)
    one = LeaseCoordinator(api, config, "pod-uid-1", clock=lambda: 100)
    two = LeaseCoordinator(api, config, "pod-uid-2", clock=lambda: 100)
    assert one.try_acquire()
    assert not two.try_acquire()
    one.lose()
    assert not one.can_commit


@dataclass
class Result:
    alerts: list
    reports: list
    failures: list


class Engine:
    def __init__(self): self.calls = []
    def process_window(self, value):
        self.calls.append(value)
        return Result([], [], [])


def test_live_runner_only_calls_canonical_engine_after_topology_and_metrics():
    from proberca.live.coordinator import LiveCoordinatorState

    engine = Engine()
    order = []

    class Coordinator:
        state = LiveCoordinatorState.LEADER_ACTIVE

        def begin_window(self, start_ns, end_ns, attempt_index=1):
            return SimpleNamespace(
                sequence=7, working_engine=engine, engine_result=None,
            )

        def run_engine(self, context, value):
            context.engine_result = engine.process_window(value)
            return context.engine_result

        def prepare_generation(self, context, **payload):
            order.append("prepare")
            return payload

        def commit(self, context, generation):
            order.append("commit")

    runner = ProbeRCALiveRunner(
        coordinator=Coordinator(),
        inventory=type("Inventory", (), {
            "ready": True, "freeze": lambda self, now: "revision",
        })(),
        topology_builder=lambda window, revision: order.append("topology") or "topology",
        metric_collector=lambda window, revision: order.append("metrics") or ([], []),
        window_adapter=lambda window, topology, node, edge: "engine-input",
        commit_payload_builder=lambda *args: {
            "engine_state": {}, "output_ledger": {}, "output_bundle": {},
            "config_fingerprint": "c" * 64,
            "code_schema_version": "generation_v5",
        },
    )
    runner.process_window(SimpleNamespace(start_ns=1, end_ns=2, sequence=99))
    assert order == ["topology", "metrics", "prepare", "commit"]
    assert engine.calls == ["engine-input"]


def test_readiness_requires_sync_prometheus_leader_and_writable_state():
    state = LiveHealthState()
    assert not state.ready
    state.update(kubernetes_connected=True, watchers_synchronized=True,
                 inventory_stale=False, prometheus_healthy=True, leader=True,
                 checkpoint_writable=True, output_writable=True,
                 engine_available=True, catchup_exceeded=False)
    assert state.ready
    status = state.status()
    assert "token" not in str(status).lower()



def test_degraded_output_still_counts_durable_checkpoint_commit():
    from proberca.live.coordinator import (
        CommittedOutputDegradedError,
        LiveCoordinatorState,
    )

    engine = Engine()

    class Coordinator:
        state = LiveCoordinatorState.LEADER_ACTIVE

        def begin_window(self, start_ns, end_ns, attempt_index=1):
            return SimpleNamespace(sequence=1, working_engine=engine,
                                   engine_result=None)

        def run_engine(self, context, value):
            context.engine_result = engine.process_window(value)
            return context.engine_result

        def prepare_generation(self, context, **payload):
            return payload

        def commit(self, context, generation):
            raise CommittedOutputDegradedError("derived output unavailable")

    health = LiveHealthState()
    health.update(leader=True)
    runner = ProbeRCALiveRunner(
        coordinator=Coordinator(),
        inventory=type("Inventory", (), {
            "ready": True, "freeze": lambda self, now: "revision",
        })(),
        topology_builder=lambda *_: "topology",
        metric_collector=lambda *_: ([], []),
        window_adapter=lambda *_: "engine-input",
        commit_payload_builder=lambda *args: {
            "engine_state": {}, "output_ledger": {}, "output_bundle": {},
            "config_fingerprint": "c" * 64,
            "code_schema_version": "generation_v5",
        },
        health=health,
    )
    with pytest.raises(CommittedOutputDegradedError):
        runner.process_window(SimpleNamespace(start_ns=1, end_ns=2, sequence=9))
    assert health.counter("processed_windows_total") == 1
    assert health.counter("checkpoint_saves_total") == 1


def test_scheduler_from_initial_run_state_starts_at_latest_complete_window():
    config = LiveConfig(
        window_sec=1, collection_delay_sec=0,
        maximum_catchup_windows=2, fail_on_missed_window=True,
    )
    record = SimpleNamespace(committed_sequence=0, last_window_end_ns=0)

    scheduler = LiveWindowScheduler.from_run_state(config, record)
    windows = scheduler.eligible_windows(1_207_000_000_000)

    assert [(item.start_ns, item.end_ns, item.sequence) for item in windows] == [
        (1_206_000_000_000, 1_207_000_000_000, 1),
    ]


def test_scheduler_from_committed_run_state_continues_durable_cursor():
    config = LiveConfig(window_sec=1, collection_delay_sec=0)
    record = SimpleNamespace(
        committed_sequence=402,
        last_window_end_ns=1_207_000_000_000,
    )

    scheduler = LiveWindowScheduler.from_run_state(config, record)
    window = scheduler.eligible_windows(1_208_000_000_000)[0]

    assert (window.start_ns, window.end_ns, window.sequence) == (
        1_207_000_000_000, 1_208_000_000_000, 403,
    )
