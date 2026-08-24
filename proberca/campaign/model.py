"""Immutable campaign records that never enter the RCA feature path."""

from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass, field
from typing import Any


def canonical_json(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        allow_nan=False,
    )


def fingerprint(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def private_commitment(value: Any, secret: bytes) -> str:
    if not isinstance(secret, bytes) or len(secret) < 32:
        raise ValueError("campaign commitment secret must contain at least 32 bytes")
    return hmac.new(
        secret, canonical_json(value).encode("utf-8"), hashlib.sha256,
    ).hexdigest()


@dataclass(frozen=True)
class RootCoordinate:
    coordinate_id: str
    entity_kind: str
    entity_id: str
    metric: str
    mechanism: str
    injector_profile_id: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "coordinate_id": self.coordinate_id,
            "entity_kind": self.entity_kind,
            "entity_id": self.entity_id,
            "metric": self.metric,
            "mechanism": self.mechanism,
            "injector_profile_id": self.injector_profile_id,
        }


@dataclass(frozen=True)
class CampaignCase:
    case_id: str
    split: str
    case_kind: str
    duration_seconds: int
    repeat: int | None = None
    coordinate: RootCoordinate | None = None
    seed: int | None = None
    storage_class: str = "ordinary"
    metadata: dict[str, Any] = field(default_factory=dict)

    def private_dict(self) -> dict[str, Any]:
        payload = {
            "case_id": self.case_id,
            "split": self.split,
            "case_kind": self.case_kind,
            "duration_seconds": self.duration_seconds,
            "repeat": self.repeat,
            "coordinate": self.coordinate.as_dict() if self.coordinate else None,
            "seed": self.seed,
            "storage_class": self.storage_class,
            "metadata": self.metadata,
        }
        return payload

    def public_dict(self, commitment_secret: bytes) -> dict[str, Any]:
        if self.split == "test" and self.case_kind != "healthy":
            private = self.private_dict()
            return {
                "case_id": self.case_id,
                "split": "test",
                "duration_seconds": self.duration_seconds,
                "storage_class": "sealed-test",
                "private_metadata_hmac_sha256": private_commitment(
                    private, commitment_secret,
                ),
            }
        return self.private_dict()
