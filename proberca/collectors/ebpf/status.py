"""Credential-free status and metric projection for the P12 controller."""
from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class ProbeStatusSnapshot:
    probe_state: str
    probe_types: tuple[str, ...]
    attach_epoch: int
    active_candidate_count: int
    ttl_remaining_sec: float
    events_received: int
    events_emitted: int
    events_filtered: int
    ring_buffer_drops: int
    mapping_failures: int
    last_error: str | None
    capability_status: str

    def to_dict(self) -> dict:
        return asdict(self)


__all__ = ["ProbeStatusSnapshot"]
