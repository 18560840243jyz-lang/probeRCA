"""Durable fenced live sequence journal and continuity validation."""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path


class SequenceContinuityError(RuntimeError):
    """A live sequence is duplicated, missing, or attributed twice."""


@dataclass(frozen=True)
class SequenceContinuity:
    sequence_range: tuple[int, int] | None
    sequence_count: int
    gap_count: int
    duplicate_count: int
    max_holders_per_sequence: int


def validate_sequence_continuity(entries) -> SequenceContinuity:
    values = [int(item["sequence"]) for item in entries]
    if not values:
        return SequenceContinuity(None, 0, 0, 0, 0)
    unique = sorted(set(values))
    gaps = sum(max(0, right - left - 1) for left, right in zip(unique, unique[1:]))
    holders = {}
    for item in entries:
        holder = item.get("holder_fingerprint") or \
            item.get("lease_fence_fingerprint")
        holders.setdefault(int(item["sequence"]), set()).add(holder)
    return SequenceContinuity(
        (unique[0], unique[-1]), len(values), gaps, len(values) - len(unique),
        max((len(value) for value in holders.values()), default=0))


def _canonical(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _commit_fingerprint(payload):
    stable = dict(payload)
    stable.pop("committed_timestamp_ns", None)
    stable.pop("commit_fingerprint", None)
    return hashlib.sha256(_canonical(stable).encode()).hexdigest()


FULL_FIELDS = {
    "sequence", "window_start_ns", "window_end_ns", "holder_fingerprint",
    "lease_fence_fingerprint", "transaction_id", "engine_result_fingerprint",
    "output_ledger_fingerprint", "checkpoint_generation_id",
    "committed_timestamp_ns", "commit_fingerprint",
}
LEGACY_FIELDS = {"sequence", "holder_fingerprint", "checkpoint_fingerprint"}


class LiveSequenceJournal:
    def __init__(self, path):
        self.path = Path(path)
        self.lock_path = self.path.with_name("." + self.path.name + ".lock")

    def read(self):
        if not self.path.exists():
            return []
        output = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            value = json.loads(line)
            fields = set(value)
            if fields == LEGACY_FIELDS:
                output.append(value)
                continue
            if fields != FULL_FIELDS or \
                    _commit_fingerprint(value) != value["commit_fingerprint"]:
                raise SequenceContinuityError("sequence journal fields mismatch")
            output.append(value)
        continuity = validate_sequence_continuity(output)
        if continuity.gap_count or continuity.duplicate_count or \
                continuity.max_holders_per_sequence > 1:
            raise SequenceContinuityError("sequence journal continuity mismatch")
        return output

    def next_sequence(self):
        entries = self.read()
        return int(entries[-1]["sequence"]) + 1 if entries else 1

    @staticmethod
    def make_entry(*, sequence, window_start_ns, window_end_ns,
                   lease_fence_fingerprint, transaction_id,
                   engine_result_fingerprint, output_ledger_fingerprint,
                   checkpoint_generation_id, committed_timestamp_ns,
                   holder_fingerprint=None):
        if int(sequence) <= 0 or int(window_start_ns) >= int(window_end_ns):
            raise SequenceContinuityError("invalid sequence journal window")
        required = {
            "lease_fence_fingerprint": lease_fence_fingerprint,
            "transaction_id": transaction_id,
            "engine_result_fingerprint": engine_result_fingerprint,
            "output_ledger_fingerprint": output_ledger_fingerprint,
            "checkpoint_generation_id": checkpoint_generation_id,
        }
        if any(not str(value) for value in required.values()):
            raise SequenceContinuityError("sequence journal identity is incomplete")
        payload = {
            "sequence": int(sequence),
            "window_start_ns": int(window_start_ns),
            "window_end_ns": int(window_end_ns),
            "holder_fingerprint": str(
                holder_fingerprint or str(lease_fence_fingerprint)[:24]),
            "lease_fence_fingerprint": str(lease_fence_fingerprint),
            "transaction_id": str(transaction_id),
            "engine_result_fingerprint": str(engine_result_fingerprint),
            "output_ledger_fingerprint": str(output_ledger_fingerprint),
            "checkpoint_generation_id": str(checkpoint_generation_id),
            "committed_timestamp_ns": int(committed_timestamp_ns),
        }
        payload["commit_fingerprint"] = _commit_fingerprint(payload)
        return payload

    def _write(self, entries, *, fence_token=None, fence_validator=None):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_name(
            f".{self.path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
        try:
            with temp.open("x", encoding="utf-8") as stream:
                for item in entries:
                    stream.write(_canonical(item) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            if fence_validator is not None:
                fence_validator("sequence_commit", fence_token)
            os.replace(temp, self.path)
            descriptor = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        finally:
            if temp.exists():
                temp.unlink()

    def commit_entry(self, entry):
        if set(entry) != FULL_FIELDS:
            raise SequenceContinuityError("invalid full sequence commit")
        existing = self.read()
        same = [item for item in existing
                if int(item["sequence"]) == int(entry["sequence"])]
        if same and same[0] != entry:
            raise SequenceContinuityError("conflicting duplicate sequence commit")
        expected = int(existing[-1]["sequence"]) + 1 if existing else 1
        if not same and int(entry["sequence"]) != expected:
            raise SequenceContinuityError(
                f"sequence gap: expected={expected} actual={entry['sequence']}")
        if _commit_fingerprint(entry) != entry["commit_fingerprint"]:
            raise SequenceContinuityError("invalid full sequence commit")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+b") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            entries = self.read()
            same = [item for item in entries
                    if int(item["sequence"]) == int(entry["sequence"])]
            if same:
                if same[0].get("commit_fingerprint") == entry["commit_fingerprint"]:
                    return False
                raise SequenceContinuityError("conflicting duplicate sequence commit")
            expected = int(entries[-1]["sequence"]) + 1 if entries else 1
            if int(entry["sequence"]) != expected:
                raise SequenceContinuityError(
                    f"sequence gap: expected={expected} actual={entry['sequence']}")
            self._write([*entries, dict(entry)])
            return True

    def materialize(self, entries, *, fence_token=None, fence_validator=None):
        values = [dict(item) for item in entries]
        continuity = validate_sequence_continuity(values)
        if continuity.gap_count or continuity.duplicate_count or \
                continuity.max_holders_per_sequence > 1:
            raise SequenceContinuityError("cannot materialize invalid sequence journal")
        self._write(
            values, fence_token=fence_token, fence_validator=fence_validator)

    def commit(self, sequence: int, holder_fingerprint: str,
               checkpoint_fingerprint: str) -> None:
        if not holder_fingerprint or not checkpoint_fingerprint or sequence <= 0:
            raise SequenceContinuityError("invalid sequence journal entry")
        entries = self.read()
        same = [item for item in entries if int(item["sequence"]) == int(sequence)]
        legacy = {
            "sequence": int(sequence), "holder_fingerprint": holder_fingerprint,
            "checkpoint_fingerprint": checkpoint_fingerprint,
        }
        if same:
            if same[0] == legacy:
                return
            raise SequenceContinuityError("conflicting duplicate sequence commit")
        expected = int(entries[-1]["sequence"]) + 1 if entries else int(sequence)
        if int(sequence) != expected:
            raise SequenceContinuityError(
                f"sequence gap: expected={expected} actual={sequence}")
        self._write([*entries, legacy])
