"""Strict binary decoder for libbpf ring-buffer records."""
from __future__ import annotations

from .contracts import EVENT_ABI_SIZE, KernelEvent


def decode_event(payload: bytes) -> KernelEvent:
    if not isinstance(payload, bytes):
        raise TypeError("ring buffer payload must be bytes")
    if len(payload) != EVENT_ABI_SIZE:
        raise ValueError("ring buffer event size mismatch")
    return KernelEvent.unpack(payload)


__all__ = ["decode_event"]
