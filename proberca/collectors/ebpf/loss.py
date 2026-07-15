"""Kernel drop and per-CPU sequence-gap accounting for burst probes."""
from __future__ import annotations

from dataclasses import dataclass

from .contracts import KernelEvent


class EventLossExceeded(RuntimeError):
    pass


@dataclass(frozen=True)
class LossReport:
    received_events: int
    kernel_drops: int
    sequence_gaps: int
    lost_events: int
    loss_rate: float
    threshold: float


class LossTracker:
    def __init__(self, threshold: float = 0.01):
        if not 0 <= threshold < 1:
            raise ValueError("event loss threshold must be in [0,1)")
        self.threshold = float(threshold)
        self._last_by_cpu: dict[int, int] = {}
        self._received = 0
        self._kernel_drops = 0
        self._gaps = 0

    def observe(self, event: KernelEvent) -> None:
        previous = self._last_by_cpu.get(event.cpu)
        if previous is not None and event.event_sequence > previous + 1:
            self._gaps += event.event_sequence - previous - 1
        if previous is None or event.event_sequence > previous:
            self._last_by_cpu[event.cpu] = event.event_sequence
        self._received += 1

    def add_kernel_drops(self, count: int) -> None:
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ValueError("kernel drop count must be a non-negative integer")
        self._kernel_drops += count

    def report(self) -> LossReport:
        lost = self._kernel_drops + self._gaps
        denominator = self._received + lost
        rate = float(lost / denominator) if denominator else 0.0
        return LossReport(
            self._received, self._kernel_drops, self._gaps, lost, rate,
            self.threshold,
        )

    def assert_within_limit(self) -> LossReport:
        report = self.report()
        if report.loss_rate >= self.threshold and report.lost_events:
            raise EventLossExceeded(
                f"event_loss_exceeded rate={report.loss_rate:.6f} "
                f"threshold={self.threshold:.6f}"
            )
        return report


__all__ = ["EventLossExceeded", "LossReport", "LossTracker"]
