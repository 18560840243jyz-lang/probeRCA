from __future__ import annotations

from dataclasses import replace

import pytest

from proberca.collectors.ebpf.controller import (
    BurstProbeConfig,
    BurstProbeController,
    ProbeControllerError,
    ProbeState,
)
from proberca.collectors.ebpf.contracts import (
    EVENT_SCHEMA_VERSION,
    EventClass,
    EventQuality,
    EventType,
    KernelEvent,
)
from proberca.collectors.ebpf.filters import CandidateSnapshot
from proberca.collectors.ebpf.loss import EventLossExceeded, LossTracker


def kernel_event(sequence=1, cpu=0):
    return KernelEvent(
        EVENT_SCHEMA_VERSION,
        EventType.PROCESS_EXEC,
        EventClass.NODE,
        EventQuality.EXACT,
        10_000 + sequence,
        1_000,
        101,
        101,
        0,
        1,
        0,
        7,
        sequence,
        cpu,
        123,
        123,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        "worker",
    )


def candidates(version=1):
    return CandidateSnapshot(
        version=version,
        cgroup_ids=(101,),
        service_pairs=((101, 0),),
        ttl_sec=0.01,
        max_candidates=16,
    )


class ScriptedBackend:
    def __init__(self, *, records=None, failure=None, lost=0):
        self.records = list(records or [kernel_event()])
        self.failure = failure
        self.lost = lost
        self.attach_count = 0
        self.detach_count = 0
        self.cleanup_count = 0
        self.update_count = 0
        self.attached = False

    def preflight(self):
        if self.failure == "preflight":
            return {"status": "permission_missing", "reason_code": "probe_unavailable"}
        return {"status": "supported"}

    def cleanup_orphans(self):
        self.cleanup_count += 1
        return 0

    def attach(self, snapshot, attach_epoch):
        if self.failure == "attach":
            raise RuntimeError("verifier rejected")
        self.attach_count += 1
        self.attached = True

    def update_candidates(self, snapshot):
        if not self.attached:
            raise RuntimeError("not attached")
        self.update_count += 1

    def read_until(self, deadline_monotonic):
        if self.failure == "read":
            raise RuntimeError("ring read failed")
        return self.records, {
            "received_events": len(self.records),
            "ring_buffer_drops": self.lost,
            "filtered_events": 2,
            "mapping_failures": 0,
        }

    def detach(self, timeout_sec):
        self.detach_count += 1
        self.attached = False

    def close(self):
        self.attached = False


def config(**changes):
    values = {
        "enabled": True,
        "ttl_sec": 0.01,
        "detach_timeout_sec": 1.0,
        "event_loss_threshold": 0.01,
        "max_candidates": 16,
        "probe_names": ("process",),
    }
    values.update(changes)
    return BurstProbeConfig(**values)


def test_controller_follows_complete_state_machine_and_closes():
    backend = ScriptedBackend()
    controller = BurstProbeController(config(), backend)
    result = controller.run(candidates())
    assert result.state is ProbeState.CLOSED
    assert result.state_history == (
        ProbeState.DISABLED,
        ProbeState.PREFLIGHT,
        ProbeState.LOADING,
        ProbeState.ATTACHED,
        ProbeState.ACTIVE,
        ProbeState.DRAINING,
        ProbeState.DETACHING,
        ProbeState.CLOSED,
    )
    assert backend.attach_count == backend.detach_count == 1
    assert result.events == (kernel_event(),)


def test_attach_and_shutdown_are_idempotent():
    backend = ScriptedBackend()
    controller = BurstProbeController(config(), backend)
    controller.run(candidates())
    controller.shutdown()
    controller.shutdown()
    assert backend.attach_count == 1
    assert backend.detach_count == 1


def test_candidate_update_does_not_reload_programs():
    backend = ScriptedBackend()
    controller = BurstProbeController(config(), backend)
    controller.prepare(candidates())
    controller.update_candidates(candidates(version=2))
    controller.finish()
    assert backend.attach_count == 1
    assert backend.update_count == 1
    assert backend.detach_count == 1


@pytest.mark.parametrize(
    ("failure", "state", "reason"),
    [
        ("preflight", ProbeState.UNAVAILABLE, "probe_unavailable"),
        ("attach", ProbeState.FAILED, "attach_failed"),
        ("read", ProbeState.FAILED, "read_failed"),
    ],
)
def test_failures_are_explicit_and_never_report_empty_success(failure, state, reason):
    controller = BurstProbeController(config(), ScriptedBackend(failure=failure))
    with pytest.raises(ProbeControllerError, match=reason):
        controller.run(candidates())
    assert controller.state is state
    assert controller.status().last_error == reason


def test_event_loss_over_threshold_is_failure():
    controller = BurstProbeController(
        config(), ScriptedBackend(records=[kernel_event()], lost=1)
    )
    with pytest.raises(ProbeControllerError, match="event_loss_exceeded"):
        controller.run(candidates())
    assert controller.state is ProbeState.FAILED


def test_default_ttl_is_thirty_seconds_and_test_ttl_is_explicit():
    assert BurstProbeConfig().ttl_sec == 30.0
    assert config().ttl_sec == 0.01
    with pytest.raises(ValueError):
        replace(config(), ttl_sec=0)


def test_disabled_controller_never_attaches_and_reports_disabled():
    backend = ScriptedBackend()
    controller = BurstProbeController(config(enabled=False), backend)
    result = controller.run(candidates())
    assert result.state is ProbeState.DISABLED
    assert backend.attach_count == 0


def test_snapshot_restore_cleans_orphans_before_attach():
    backend = ScriptedBackend()
    controller = BurstProbeController(config(), backend)
    snapshot = controller.snapshot()
    restored = BurstProbeController.restore(snapshot, backend)
    restored.run(candidates())
    assert backend.cleanup_count >= 1


def test_status_and_metrics_report_probe_lifecycle_without_algorithm_identity():
    controller = BurstProbeController(config(), ScriptedBackend())
    result = controller.run(candidates())
    status = controller.status().to_dict()
    metrics = controller.prometheus_metrics()
    assert status["probe_state"] == "CLOSED"
    assert status["events_received"] == 1
    for name in (
        "probe_attach_total",
        "probe_detach_total",
        "probe_events_total",
        "probe_filtered_total",
        "probe_lost_total",
        "probe_active",
        "probe_ttl_seconds",
    ):
        assert f"proberca_{name}" in metrics
    assert "algorithm" not in str(status).lower()
    assert result.loss_rate == 0.0


def test_loss_tracker_uses_kernel_drops_and_per_cpu_sequence_gaps():
    tracker = LossTracker(threshold=0.5)
    tracker.observe(kernel_event(sequence=1, cpu=0))
    tracker.observe(kernel_event(sequence=3, cpu=0))
    tracker.add_kernel_drops(1)
    report = tracker.report()
    assert report.received_events == 2
    assert report.sequence_gaps == 1
    assert report.kernel_drops == 1
    assert report.lost_events == 2
    assert report.loss_rate == pytest.approx(0.5)


def test_loss_threshold_is_not_silently_relaxed():
    tracker = LossTracker(threshold=0.01)
    tracker.observe(kernel_event())
    tracker.add_kernel_drops(1)
    with pytest.raises(EventLossExceeded):
        tracker.assert_within_limit()


def test_shutdown_after_partial_attach_detaches_once():
    backend = ScriptedBackend()
    controller = BurstProbeController(config(), backend)
    controller.prepare(candidates())
    controller.shutdown()
    controller.shutdown()
    assert backend.detach_count == 1
    assert controller.state is ProbeState.CLOSED


def test_same_probe_cannot_attach_twice():
    backend = ScriptedBackend()
    controller = BurstProbeController(config(), backend)
    controller.prepare(candidates())
    with pytest.raises(ProbeControllerError, match="already_attached"):
        controller.prepare(candidates())
    controller.finish()
