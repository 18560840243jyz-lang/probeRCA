"""Fenced live-window transaction state and sanitized diagnostics."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import Enum

from .leader import LeaseFenceToken


def _sha(value):
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode()).hexdigest()


class LiveTransactionError(RuntimeError):
    """A live transaction attempted an invalid transition."""


class DiagnosticRecorder:
    ALLOWED_FIELDS = frozenset({
        "leader_state", "lease_epoch_fingerprint", "sequence", "generation_id",
        "temporary_generation_id", "current_generation_id",
        "checkpoint_fingerprint", "output_ledger_fingerprint", "reason_code",
        "transaction_id", "window_start_ns", "window_end_ns",
    })

    def __init__(self, instance_fingerprint):
        if not instance_fingerprint:
            raise ValueError("instance fingerprint is required")
        self.instance_fingerprint = str(instance_fingerprint)
        self._events = []

    def record(self, component, operation, **fields):
        unknown = set(fields) - self.ALLOWED_FIELDS
        if unknown:
            raise ValueError(f"unsafe diagnostic fields: {sorted(unknown)}")
        if operation not in {"read", "prepare", "publish", "commit", "abort"}:
            raise ValueError("invalid diagnostic operation")
        event = {
            "event_index": len(self._events) + 1,
            "instance_fingerprint": self.instance_fingerprint,
            "component": str(component), "operation": operation,
            **fields,
        }
        self._events.append(event)
        return event

    def to_list(self):
        return [dict(item) for item in self._events]


@dataclass
class LiveWindowTransaction:
    transaction_id: str
    lease_fence_token: LeaseFenceToken
    expected_sequence: int
    window_start_ns: int
    window_end_ns: int
    starting_current_generation: str | None
    starting_ledger_fingerprint: str
    state: str = "prepared"
    engine_result_fingerprint: str | None = None
    planned_output_ledger_fingerprint: str | None = None
    planned_checkpoint_fingerprint: str | None = None
    abort_reason: str | None = None

    @classmethod
    def create(cls, *, fence_token, expected_sequence, window_start_ns,
               window_end_ns, starting_current_generation,
               starting_ledger_fingerprint):
        if not isinstance(fence_token, LeaseFenceToken):
            raise TypeError("fence_token must be LeaseFenceToken")
        if expected_sequence <= 0 or window_start_ns >= window_end_ns:
            raise ValueError("invalid live window transaction bounds")
        seed = {
            "fence": fence_token.token_fingerprint,
            "sequence": expected_sequence,
            "window_start_ns": window_start_ns,
            "window_end_ns": window_end_ns,
            "current": starting_current_generation,
            "ledger": starting_ledger_fingerprint,
        }
        return cls(
            _sha(seed), fence_token, expected_sequence,
            window_start_ns, window_end_ns, starting_current_generation,
            starting_ledger_fingerprint)

    def _require(self, *states):
        if self.state not in states:
            raise LiveTransactionError(
                f"invalid transaction transition from state={self.state}")

    def engine_completed(self, result_fingerprint):
        self._require("prepared")
        if not result_fingerprint:
            raise ValueError("engine result fingerprint is required")
        self.engine_result_fingerprint = str(result_fingerprint)
        self.state = "engine_completed"

    def output_prepared(self, ledger_fingerprint):
        self._require("engine_completed")
        if not ledger_fingerprint:
            raise ValueError("output ledger fingerprint is required")
        self.planned_output_ledger_fingerprint = str(ledger_fingerprint)
        self.state = "output_prepared"

    def checkpoint_prepared(self, checkpoint_fingerprint):
        self._require("output_prepared")
        if not checkpoint_fingerprint:
            raise ValueError("checkpoint fingerprint is required")
        self.planned_checkpoint_fingerprint = str(checkpoint_fingerprint)
        self.state = "checkpoint_prepared"

    def commit(self):
        self._require("checkpoint_prepared")
        self.state = "committed"

    def abort(self, reason):
        self._require("prepared", "engine_completed", "output_prepared",
                      "checkpoint_prepared")
        self.abort_reason = str(reason)
        self.state = "aborted"


class CommitPhase(str, Enum):
    PRE_COMMIT = "pre_commit"
    COMMITTED = "committed"
    OUTPUT_DEGRADED = "output_degraded"


@dataclass
class LiveWindowTransactionState:
    """Small phase model documenting RunState CAS as the only commit point."""

    expected_sequence: int
    committed_sequence: int
    phase: CommitPhase = CommitPhase.PRE_COMMIT
    aborted: bool = False
    reason_code: str | None = None

    def __post_init__(self):
        if self.expected_sequence != self.committed_sequence + 1:
            raise ValueError("expected sequence must follow committed sequence")

    @property
    def next_sequence(self):
        return self.committed_sequence + 1

    def enter_phase(self, phase):
        if phase is not CommitPhase.PRE_COMMIT or self.phase is not CommitPhase.PRE_COMMIT:
            raise RuntimeError("invalid pre-commit phase transition")

    def abort(self, reason_code):
        if self.phase is not CommitPhase.PRE_COMMIT:
            raise RuntimeError("committed transaction cannot be aborted")
        self.reason_code = str(reason_code)
        self.aborted = True

    def mark_run_state_committed(self):
        if self.phase is not CommitPhase.PRE_COMMIT:
            raise RuntimeError("transaction already committed")
        if self.aborted:
            raise RuntimeError("aborted transaction cannot commit")
        self.committed_sequence = self.expected_sequence
        self.phase = CommitPhase.COMMITTED

    def mark_output_degraded(self, reason_code):
        if self.phase is not CommitPhase.COMMITTED:
            raise RuntimeError("output can degrade only after RunState commit")
        self.reason_code = str(reason_code)
        self.phase = CommitPhase.OUTPUT_DEGRADED

    def should_replay_sequence(self, sequence):
        return int(sequence) > self.committed_sequence
