"""Actual-time phase labels for one fault episode."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FaultLifecycle:
    planned_start_ns: int
    apply_command_start_ns: int
    effect_confirmed_ns: int
    planned_end_ns: int
    cleanup_command_start_ns: int
    cleanup_confirmed_ns: int

    def __post_init__(self) -> None:
        values = (
            self.planned_start_ns,
            self.apply_command_start_ns,
            self.effect_confirmed_ns,
            self.planned_end_ns,
            self.cleanup_command_start_ns,
            self.cleanup_confirmed_ns,
        )
        if any(not isinstance(value, int) or value < 0 for value in values):
            raise ValueError("fault lifecycle timestamps must be non-negative integers")
        if not (
            self.apply_command_start_ns <= self.effect_confirmed_ns
            <= self.cleanup_command_start_ns <= self.cleanup_confirmed_ns
        ):
            raise ValueError("fault lifecycle timestamps are not ordered")
        if self.planned_start_ns > self.planned_end_ns:
            raise ValueError("planned fault interval is not ordered")

    def as_dict(self) -> dict[str, int]:
        return {
            "planned_start_ns": self.planned_start_ns,
            "apply_command_start_ns": self.apply_command_start_ns,
            "effect_confirmed_ns": self.effect_confirmed_ns,
            "planned_end_ns": self.planned_end_ns,
            "cleanup_command_start_ns": self.cleanup_command_start_ns,
            "cleanup_confirmed_ns": self.cleanup_confirmed_ns,
        }


def classify_window_phase(
    window_start_ns: int,
    window_end_ns: int,
    lifecycle: FaultLifecycle,
) -> str:
    """Classify only fully enclosed windows as pre, active, or recovery."""
    if window_start_ns < 0 or window_end_ns <= window_start_ns:
        raise ValueError("window boundary is invalid")
    if window_end_ns <= lifecycle.apply_command_start_ns:
        return "HEALTHY_PRE"
    if (
        window_start_ns >= lifecycle.effect_confirmed_ns
        and window_end_ns <= lifecycle.cleanup_command_start_ns
    ):
        return "FAULT_ACTIVE"
    if window_start_ns >= lifecycle.cleanup_confirmed_ns:
        return "RECOVERY"
    if window_start_ns < lifecycle.effect_confirmed_ns:
        return "TRANSITION_APPLY"
    return "TRANSITION_CLEANUP"
