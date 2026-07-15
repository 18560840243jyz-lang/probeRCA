"""UTC-epoch event-time scheduler with monotonic wait semantics."""
from __future__ import annotations

from dataclasses import asdict, dataclass


class MissedWindowError(RuntimeError):
    """Wall-clock advancement exceeded the configured catch-up policy."""


@dataclass(frozen=True)
class LiveWindow:
    start_ns: int
    end_ns: int
    sequence: int


class LiveWindowScheduler:
    def __init__(self, config, *, next_start_ns=None, next_sequence=1):
        config.validate()
        self.config = config
        self.window_ns = config.window_sec * 1_000_000_000
        self.delay_ns = int(config.collection_delay_sec * 1_000_000_000)
        self.next_start_ns = next_start_ns
        self.next_sequence = next_sequence


    @classmethod
    def from_run_state(cls, config, record):
        committed_sequence = int(record.committed_sequence)
        if committed_sequence < 0:
            raise ValueError("committed sequence must be non-negative")
        last_window_end_ns = record.last_window_end_ns
        if committed_sequence == 0:
            if last_window_end_ns not in {None, 0}:
                raise ValueError("uncommitted RunState has a window cursor")
            next_start_ns = None
        else:
            if not isinstance(last_window_end_ns, int) or last_window_end_ns <= 0:
                raise ValueError("committed RunState lacks a valid window cursor")
            next_start_ns = last_window_end_ns
        return cls(
            config, next_start_ns=next_start_ns,
            next_sequence=committed_sequence + 1,
        )

    def eligible_windows(self, now_ns: int) -> tuple[LiveWindow, ...]:
        ready_end = ((now_ns - self.delay_ns) // self.window_ns) * self.window_ns
        if ready_end <= 0:
            return ()
        start = self.next_start_ns
        if start is None:
            start = ready_end - self.window_ns
        if start + self.window_ns > ready_end:
            return ()
        count = (ready_end - start) // self.window_ns
        if count > self.config.maximum_catchup_windows:
            if self.config.fail_on_missed_window:
                raise MissedWindowError(
                    f"missed {count} windows; limit={self.config.maximum_catchup_windows}")
            count = self.config.maximum_catchup_windows
        return tuple(LiveWindow(
            start + index * self.window_ns, start + (index + 1) * self.window_ns,
            self.next_sequence + index) for index in range(count))

    def advance(self, window: LiveWindow) -> None:
        if window.sequence != self.next_sequence:
            raise ValueError("window sequence is not next")
        if self.next_start_ns is not None and window.start_ns != self.next_start_ns:
            raise ValueError("window start is not next")
        self.next_start_ns = window.end_ns
        self.next_sequence += 1

    def commit(self, window: LiveWindow) -> None:
        """Deprecated temporal cursor alias; never a live commit authority."""
        self.advance(window)

    def to_dict(self) -> dict:
        return {"version": "1", "next_start_ns": self.next_start_ns,
                "next_sequence": self.next_sequence}

    @classmethod
    def restore(cls, config, payload):
        if set(payload) != {"version", "next_start_ns", "next_sequence"} or payload["version"] != "1":
            raise ValueError("scheduler checkpoint version mismatch")
        return cls(config, next_start_ns=payload["next_start_ns"],
                   next_sequence=payload["next_sequence"])
