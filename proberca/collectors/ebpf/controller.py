"""Single fail-closed lifecycle controller for bounded P12 burst probes."""
from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from enum import Enum

from .filters import CandidateSnapshot
from .loss import EventLossExceeded, LossTracker
from .status import ProbeStatusSnapshot


class ProbeState(str, Enum):
    DISABLED = "DISABLED"
    PREFLIGHT = "PREFLIGHT"
    LOADING = "LOADING"
    ATTACHED = "ATTACHED"
    ACTIVE = "ACTIVE"
    DRAINING = "DRAINING"
    DETACHING = "DETACHING"
    CLOSED = "CLOSED"
    UNAVAILABLE = "UNAVAILABLE"
    FAILED = "FAILED"


class ProbeControllerError(RuntimeError):
    pass


@dataclass(frozen=True)
class BurstProbeConfig:
    enabled: bool = False
    ttl_sec: float = 30.0
    detach_timeout_sec: float = 5.0
    event_loss_threshold: float = 0.01
    max_candidates: int = 1024
    probe_names: tuple[str, ...] = (
        "tcp", "sched", "block", "futex", "process", "dns",
    )

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise TypeError("enabled must be boolean")
        if self.ttl_sec <= 0 or self.detach_timeout_sec <= 0:
            raise ValueError("probe TTL and detach timeout must be positive")
        if not 0 < self.event_loss_threshold < 1:
            raise ValueError("event loss threshold must be in (0,1)")
        if self.max_candidates < 1:
            raise ValueError("max_candidates must be positive")
        allowed = {"tcp", "sched", "block", "futex", "process", "dns"}
        if not self.probe_names or set(self.probe_names) - allowed:
            raise ValueError("unknown or empty probe set")


@dataclass(frozen=True)
class ProbeRunResult:
    state: ProbeState
    state_history: tuple[ProbeState, ...]
    events: tuple
    loss_rate: float
    statistics: dict


class BurstProbeController:
    def __init__(self, config: BurstProbeConfig, backend, *, monotonic=time.monotonic):
        if not isinstance(config, BurstProbeConfig):
            raise TypeError("config must be BurstProbeConfig")
        self.config = config
        self.backend = backend
        self._clock = monotonic
        self.state = ProbeState.DISABLED
        self._history = [self.state]
        self._attached = False
        self._attach_epoch = 0
        self._candidate_count = 0
        self._deadline = 0.0
        self._last_error = None
        self._capability_status = "unchecked"
        self._stats = {
            "probe_attach_total": 0, "probe_attach_failed_total": 0,
            "probe_detach_total": 0, "probe_events_total": 0,
            "probe_filtered_total": 0, "probe_lost_total": 0,
            "probe_mapping_failed_total": 0,
        }

    def _transition(self, state: ProbeState) -> None:
        if self.state is not state:
            self.state = state
            self._history.append(state)

    def _fail(self, state: ProbeState, reason: str, error=None):
        self._last_error = reason
        self._transition(state)
        if self._attached:
            try:
                self.backend.detach(self.config.detach_timeout_sec)
                self._stats["probe_detach_total"] += 1
            finally:
                self._attached = False
        self.backend.close()
        detail = f": {type(error).__name__}" if error is not None else ""
        raise ProbeControllerError(f"{reason}{detail}") from error

    def prepare(self, snapshot: CandidateSnapshot) -> None:
        if self._attached or self.state in {
            ProbeState.LOADING, ProbeState.ATTACHED, ProbeState.ACTIVE,
        }:
            raise ProbeControllerError("already_attached")
        if not self.config.enabled:
            return
        if snapshot.max_candidates > self.config.max_candidates:
            raise ProbeControllerError("candidate_limit_exceeded")
        if snapshot.ttl_sec != self.config.ttl_sec:
            raise ProbeControllerError("candidate_ttl_mismatch")
        self._transition(ProbeState.PREFLIGHT)
        capability = self.backend.preflight()
        self._capability_status = str(capability.get("status", "runtime_failed"))
        if self._capability_status != "supported":
            self._fail(
                ProbeState.UNAVAILABLE,
                str(capability.get("reason_code", "probe_unavailable")),
            )
        self.backend.cleanup_orphans()
        self._transition(ProbeState.LOADING)
        self._attach_epoch += 1
        try:
            self.backend.attach(snapshot, self._attach_epoch)
        except Exception as error:
            self._stats["probe_attach_failed_total"] += 1
            self._fail(ProbeState.FAILED, "attach_failed", error)
        self._attached = True
        self._stats["probe_attach_total"] += 1
        self._candidate_count = len(snapshot.cgroup_ids) + len(snapshot.service_pairs)
        self._transition(ProbeState.ATTACHED)
        self._deadline = self._clock() + self.config.ttl_sec
        self._transition(ProbeState.ACTIVE)

    def update_candidates(self, snapshot: CandidateSnapshot) -> None:
        if not self._attached or self.state is not ProbeState.ACTIVE:
            raise ProbeControllerError("probe_not_active")
        if len(snapshot.cgroup_ids) + len(snapshot.service_pairs) > self.config.max_candidates:
            raise ProbeControllerError("candidate_limit_exceeded")
        self.backend.update_candidates(snapshot)
        self._candidate_count = len(snapshot.cgroup_ids) + len(snapshot.service_pairs)

    def finish(self) -> None:
        if not self._attached:
            if self.state not in {ProbeState.DISABLED, ProbeState.CLOSED}:
                self._transition(ProbeState.CLOSED)
            return
        self._transition(ProbeState.DRAINING)
        self._transition(ProbeState.DETACHING)
        try:
            self.backend.detach(self.config.detach_timeout_sec)
        except Exception as error:
            self._fail(ProbeState.FAILED, "detach_failed", error)
        self._attached = False
        self._stats["probe_detach_total"] += 1
        self.backend.close()
        self._transition(ProbeState.CLOSED)

    def run(self, snapshot: CandidateSnapshot) -> ProbeRunResult:
        if not self.config.enabled:
            return ProbeRunResult(self.state, tuple(self._history), (), 0.0, dict(self._stats))
        self.prepare(snapshot)
        try:
            events, stats = self.backend.read_until(self._deadline)
        except Exception as error:
            self._fail(ProbeState.FAILED, "read_failed", error)
        tracker = LossTracker(self.config.event_loss_threshold)
        for event in events:
            tracker.observe(event)
        tracker.add_kernel_drops(int(stats.get("ring_buffer_drops", 0)))
        try:
            loss = tracker.assert_within_limit()
        except EventLossExceeded as error:
            self._stats["probe_lost_total"] += tracker.report().lost_events
            self._fail(ProbeState.FAILED, "event_loss_exceeded", error)
        self._stats["probe_events_total"] += len(events)
        self._stats["probe_filtered_total"] += int(stats.get("filtered_events", 0))
        self._stats["probe_lost_total"] += loss.lost_events
        self._stats["probe_mapping_failed_total"] += int(stats.get("mapping_failures", 0))
        self.finish()
        return ProbeRunResult(
            self.state, tuple(self._history), tuple(events), loss.loss_rate,
            {**dict(stats), **dict(self._stats)},
        )

    def shutdown(self) -> None:
        self.finish()

    def snapshot(self) -> dict:
        return {
            "schema_version": "p12-burst-controller-v1",
            "config": asdict(self.config),
            "state": self.state.value,
            "attach_epoch": self._attach_epoch,
        }

    @classmethod
    def restore(cls, snapshot: dict, backend):
        if snapshot.get("schema_version") != "p12-burst-controller-v1":
            raise ValueError("unsupported controller snapshot")
        values = dict(snapshot["config"])
        values["probe_names"] = tuple(values["probe_names"])
        controller = cls(BurstProbeConfig(**values), backend)
        controller._attach_epoch = int(snapshot.get("attach_epoch", 0))
        backend.cleanup_orphans()
        return controller

    def status(self) -> ProbeStatusSnapshot:
        return ProbeStatusSnapshot(
            probe_state=self.state.value, probe_types=tuple(self.config.probe_names),
            attach_epoch=self._attach_epoch,
            active_candidate_count=self._candidate_count,
            ttl_remaining_sec=max(0.0, self._deadline - self._clock()) if self._attached else 0.0,
            events_received=self._stats["probe_events_total"],
            events_emitted=self._stats["probe_events_total"],
            events_filtered=self._stats["probe_filtered_total"],
            ring_buffer_drops=self._stats["probe_lost_total"],
            mapping_failures=self._stats["probe_mapping_failed_total"],
            last_error=self._last_error, capability_status=self._capability_status,
        )

    def prometheus_metrics(self) -> str:
        values = {
            **self._stats,
            "probe_active": 1 if self.state is ProbeState.ACTIVE else 0,
            "probe_ttl_seconds": self.config.ttl_sec,
        }
        return "\n".join(
            f"proberca_{name} {value}" for name, value in sorted(values.items())
        ) + "\n"


__all__ = [
    "BurstProbeConfig", "BurstProbeController", "ProbeControllerError",
    "ProbeRunResult", "ProbeState",
]
