"""Normalized, target-explicit evidence validation for P7."""

from __future__ import annotations

from dataclasses import asdict

from proberca.data.schema import EvidenceObservationRecord


class EvidenceAlignmentError(ValueError):
    """Evidence target or source metadata does not align with P6."""


class EvidenceConflictError(ValueError):
    """The same evidence ID carries conflicting content."""


class CircularEvidenceError(ValueError):
    """Evidence independence requirements are structurally invalid."""


class EvidenceTimeWindowError(ValueError):
    """Evidence violates the declared analysis cutoff or window."""


class EvidenceQualityError(ValueError):
    """Evidence quality metadata is invalid."""


def canonicalize_evidence(records) -> list[EvidenceObservationRecord]:
    output: dict[str, EvidenceObservationRecord] = {}
    for item in records:
        if not isinstance(item, EvidenceObservationRecord):
            raise TypeError("P7 accepts only normalized EvidenceObservationRecord values")
        existing = output.get(item.evidence_id)
        if existing is not None and asdict(existing) != asdict(item):
            raise EvidenceConflictError(f"conflicting evidence_id={item.evidence_id}")
        output[item.evidence_id] = item
    return [output[key] for key in sorted(output)]
