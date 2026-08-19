"""Read-only normalization and alignment of independent Burst archives."""

from __future__ import annotations

import json
import math
import os
import tempfile
from dataclasses import dataclass, fields
from itertools import zip_longest
from pathlib import Path
from typing import Any, Iterable, Iterator

from .archive import CollectionArchive, _validate_window_contract
from .burst_archive import BurstArchive
from .burst_collection import (
    BURST_CHANNEL_MODES,
    BurstChannelCalibration,
    BurstEvidenceCollector,
)
from .contracts import CollectedWindow, assert_label_safe, fingerprint
from .raw import RawCollectionError


BURST_CALIBRATION_ARTIFACT_SCHEMA_VERSION = (
    "probeRCA-final-burst-calibration-v1"
)
BURST_CALIBRATION_POLICY_SCHEMA_VERSION = (
    "probeRCA-final-burst-calibration-policy-v1"
)


def _sha256(name: str, value: Any) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise RawCollectionError(f"{name} must be lowercase SHA-256")


def _positive(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RawCollectionError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise RawCollectionError(f"{name} must be finite and positive")
    return result


@dataclass(frozen=True)
class BurstCalibrationPolicy:
    """Frozen label-free parameters used to calibrate raw Burst channels."""

    schema_version: str
    rare_event_thresholds: dict[str, float]
    continuous_transform: str
    continuous_polarity: int
    continuous_z_cap: float
    continuous_minimum_healthy_samples: int
    continuous_minimum_scale: float
    policy_fingerprint: str

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "BurstCalibrationPolicy":
        if not isinstance(payload, dict) or set(payload) != set(
            cls.__dataclass_fields__
        ):
            raise RawCollectionError("Burst calibration policy fields mismatch")
        result = cls(**payload)
        result.validate()
        return result

    @classmethod
    def create(cls, **values: Any) -> "BurstCalibrationPolicy":
        payload = dict(values)
        payload["schema_version"] = BURST_CALIBRATION_POLICY_SCHEMA_VERSION
        payload.pop("policy_fingerprint", None)
        payload["rare_event_thresholds"] = dict(
            payload["rare_event_thresholds"]
        )
        result = cls(
            **payload,
            policy_fingerprint=fingerprint(payload),
        )
        result.validate()
        return result

    def validate(self) -> None:
        if self.schema_version != BURST_CALIBRATION_POLICY_SCHEMA_VERSION:
            raise RawCollectionError("unsupported Burst calibration policy")
        if self.continuous_transform not in {"identity", "log1p"}:
            raise RawCollectionError("Burst calibration transform is invalid")
        if self.continuous_polarity not in {-1, 1}:
            raise RawCollectionError("Burst calibration polarity is invalid")
        _positive("continuous_z_cap", self.continuous_z_cap)
        _positive("continuous_minimum_scale", self.continuous_minimum_scale)
        if (
            isinstance(self.continuous_minimum_healthy_samples, bool)
            or not isinstance(self.continuous_minimum_healthy_samples, int)
            or self.continuous_minimum_healthy_samples <= 0
        ):
            raise RawCollectionError(
                "continuous_minimum_healthy_samples must be positive"
            )
        if not isinstance(self.rare_event_thresholds, dict):
            raise RawCollectionError("rare_event_thresholds must be a mapping")
        for channel_id, threshold in self.rare_event_thresholds.items():
            if BURST_CHANNEL_MODES.get(channel_id) != "rare":
                raise RawCollectionError(
                    f"rare threshold targets non-rare channel {channel_id}"
                )
            _positive(f"rare threshold {channel_id}", threshold)
        payload = self.to_dict()
        supplied = payload.pop("policy_fingerprint")
        if supplied != fingerprint(payload):
            raise RawCollectionError("Burst calibration policy fingerprint mismatch")
        assert_label_safe(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "rare_event_thresholds": dict(sorted(
                self.rare_event_thresholds.items()
            )),
            "continuous_transform": self.continuous_transform,
            "continuous_polarity": self.continuous_polarity,
            "continuous_z_cap": self.continuous_z_cap,
            "continuous_minimum_healthy_samples": (
                self.continuous_minimum_healthy_samples
            ),
            "continuous_minimum_scale": self.continuous_minimum_scale,
            "policy_fingerprint": self.policy_fingerprint,
        }


@dataclass(frozen=True)
class BurstCalibrationArtifact:
    """Immutable Healthy-only calibration used for later Burst normalization."""

    schema_version: str
    source_dataset_id: str
    source_burst_manifest_fingerprint: str
    burst_config_fingerprint: str
    policy_fingerprint: str
    calibrations: tuple[BurstChannelCalibration, ...]
    artifact_fingerprint: str

    @classmethod
    def create(
        cls,
        *,
        source_archive: BurstArchive,
        policy_fingerprint: str,
        calibrations: Iterable[BurstChannelCalibration],
    ) -> "BurstCalibrationArtifact":
        values = tuple(sorted(
            calibrations, key=lambda item: item.channel_id
        ))
        payload = {
            "schema_version": BURST_CALIBRATION_ARTIFACT_SCHEMA_VERSION,
            "source_dataset_id": source_archive.dataset_id,
            "source_burst_manifest_fingerprint": (
                source_archive.manifest_fingerprint
            ),
            "burst_config_fingerprint": (
                source_archive.burst_config_fingerprint
            ),
            "policy_fingerprint": policy_fingerprint,
            "calibrations": [item.to_dict() for item in values],
        }
        result = cls(
            schema_version=payload["schema_version"],
            source_dataset_id=payload["source_dataset_id"],
            source_burst_manifest_fingerprint=payload[
                "source_burst_manifest_fingerprint"
            ],
            burst_config_fingerprint=payload["burst_config_fingerprint"],
            policy_fingerprint=policy_fingerprint,
            calibrations=values,
            artifact_fingerprint=fingerprint(payload),
        )
        result.validate()
        return result

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "BurstCalibrationArtifact":
        if not isinstance(payload, dict) or set(payload) != set(
            cls.__dataclass_fields__
        ):
            raise RawCollectionError("Burst calibration artifact fields mismatch")
        if not isinstance(payload["calibrations"], list):
            raise RawCollectionError("Burst calibrations must be a list")
        result = cls(
            schema_version=payload["schema_version"],
            source_dataset_id=payload["source_dataset_id"],
            source_burst_manifest_fingerprint=payload[
                "source_burst_manifest_fingerprint"
            ],
            burst_config_fingerprint=payload["burst_config_fingerprint"],
            policy_fingerprint=payload["policy_fingerprint"],
            calibrations=tuple(
                BurstChannelCalibration.from_dict(item)
                for item in payload["calibrations"]
            ),
            artifact_fingerprint=payload["artifact_fingerprint"],
        )
        result.validate()
        return result

    @classmethod
    def load(cls, path: str | Path) -> "BurstCalibrationArtifact":
        try:
            payload = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise RawCollectionError(
                "cannot read Burst calibration artifact"
            ) from error
        return cls.from_dict(payload)

    def save(self, path: str | Path) -> None:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        content = (
            json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))
            + "\n"
        )
        if target.exists():
            if target.read_text(encoding="utf-8") != content:
                raise RawCollectionError(
                    "Burst calibration artifact already exists with other content"
                )
            return
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.replace(temporary, target)
        finally:
            if temporary.exists():
                temporary.unlink()

    def validate(self) -> None:
        if self.schema_version != BURST_CALIBRATION_ARTIFACT_SCHEMA_VERSION:
            raise RawCollectionError("unsupported Burst calibration artifact")
        for name in (
            "source_dataset_id",
            "source_burst_manifest_fingerprint",
            "burst_config_fingerprint",
            "policy_fingerprint",
            "artifact_fingerprint",
        ):
            _sha256(name, getattr(self, name))
        channels = [item.channel_id for item in self.calibrations]
        if not channels or channels != sorted(set(channels)):
            raise RawCollectionError(
                "Burst calibrations must be non-empty, sorted, and unique"
            )
        for item in self.calibrations:
            item.validate()
        payload = self.to_dict()
        supplied = payload.pop("artifact_fingerprint")
        if supplied != fingerprint(payload):
            raise RawCollectionError("Burst calibration artifact fingerprint mismatch")
        assert_label_safe(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "source_dataset_id": self.source_dataset_id,
            "source_burst_manifest_fingerprint": (
                self.source_burst_manifest_fingerprint
            ),
            "burst_config_fingerprint": self.burst_config_fingerprint,
            "policy_fingerprint": self.policy_fingerprint,
            "calibrations": [item.to_dict() for item in self.calibrations],
            "artifact_fingerprint": self.artifact_fingerprint,
        }


def calibrate_healthy_burst(
    *,
    archive: BurstArchive,
    collection_contract: dict[str, Any],
    policy: BurstCalibrationPolicy,
) -> BurstCalibrationArtifact:
    """Build a label-free calibration artifact from a sealed Healthy archive."""

    archive.validate()
    policy.validate()
    if archive.burst_config_fingerprint != collection_contract.get(
        "burst_config_fingerprint"
    ):
        raise RawCollectionError(
            "Healthy Burst archive and collection contract do not match"
        )
    roles = collection_contract.get("burst_channel_roles")
    if not isinstance(roles, list) or not roles:
        raise RawCollectionError("collection contract has no Burst roles")
    expected = {item["channel_id"] for item in roles}
    rare = {
        channel_id for channel_id in expected
        if BURST_CHANNEL_MODES.get(channel_id) == "rare"
    }
    continuous = {
        channel_id for channel_id in expected
        if BURST_CHANNEL_MODES.get(channel_id) == "continuous"
    }
    if rare | continuous != expected:
        raise RawCollectionError("collection contract has unknown Burst channels")
    if set(policy.rare_event_thresholds) != rare:
        raise RawCollectionError(
            "rare thresholds must cover every formal rare channel exactly"
        )
    healthy_values = {channel_id: [] for channel_id in continuous}
    observed = set()
    for window in archive.iter_windows():
        for sample in window.samples:
            if sample.channel_id not in expected:
                raise RawCollectionError(
                    "Healthy Burst archive contains a non-formal channel"
                )
            observed.add(sample.channel_id)
            if sample.channel_id in continuous:
                healthy_values[sample.channel_id].append(sample.value)
    if observed != expected:
        missing = sorted(expected - observed)
        raise RawCollectionError(
            "Healthy Burst archive lacks formal channels: " + ",".join(missing)
        )
    calibrations = []
    for channel_id in sorted(expected):
        mode = BURST_CHANNEL_MODES[channel_id]
        calibrations.append(BurstChannelCalibration.create(
            channel_id=channel_id,
            mode=mode,
            rare_event_threshold=(
                policy.rare_event_thresholds[channel_id]
                if mode == "rare" else None
            ),
            healthy_values=(
                () if mode == "rare" else healthy_values[channel_id]
            ),
            transform=policy.continuous_transform,
            polarity=policy.continuous_polarity,
            z_cap=policy.continuous_z_cap,
            minimum_healthy_samples=(
                policy.continuous_minimum_healthy_samples
            ),
            minimum_scale=policy.continuous_minimum_scale,
        ))
    return BurstCalibrationArtifact.create(
        source_archive=archive,
        policy_fingerprint=policy.policy_fingerprint,
        calibrations=calibrations,
    )


@dataclass(frozen=True)
class BurstJoinedCollectionArchive(CollectionArchive):
    """A read-only view that aligns Normal windows with normalized Burst."""

    normal_archive: CollectionArchive
    burst_archive: BurstArchive
    calibration_artifact: BurstCalibrationArtifact

    @classmethod
    def create(
        cls,
        *,
        normal_archive: CollectionArchive,
        burst_archive: BurstArchive,
        calibration_artifact: BurstCalibrationArtifact,
    ) -> "BurstJoinedCollectionArchive":
        values = {
            item.name: getattr(normal_archive, item.name)
            for item in fields(CollectionArchive)
        }
        values["manifest_fingerprint"] = fingerprint({
            "normal_manifest_fingerprint": (
                normal_archive.manifest_fingerprint
            ),
            "burst_manifest_fingerprint": burst_archive.manifest_fingerprint,
            "burst_calibration_fingerprint": (
                calibration_artifact.artifact_fingerprint
            ),
            "join_semantics": "aligned_read_only_burst_v1",
        })
        result = cls(
            **values,
            normal_archive=normal_archive,
            burst_archive=burst_archive,
            calibration_artifact=calibration_artifact,
        )
        result.validate()
        return result

    def validate(self) -> None:
        self.normal_archive.validate()
        self.burst_archive.validate()
        self.calibration_artifact.validate()
        if (
            self.dataset_id != self.burst_archive.dataset_id
            or self.cluster_id != self.burst_archive.cluster_id
            or self.start_ns != self.burst_archive.start_ns
            or self.end_ns != self.burst_archive.end_ns
            or self.window_count != self.burst_archive.window_count
        ):
            raise RawCollectionError("Normal/Burst archive identities do not align")
        expected_burst = self.collection_contract["burst_config_fingerprint"]
        if (
            self.burst_archive.burst_config_fingerprint != expected_burst
            or self.calibration_artifact.burst_config_fingerprint
            != expected_burst
        ):
            raise RawCollectionError("Burst configuration fingerprint mismatch")

    def iter_windows(self) -> Iterator[CollectedWindow]:
        collector = BurstEvidenceCollector(
            collection_contract=self.collection_contract,
            collector_build_id=self.collection_metadata[
                "collector_build_fingerprint"
            ],
            calibrations=self.calibration_artifact.calibrations,
        )
        normal_sources: set[str] = set()
        burst_sources: set[str] = set()
        count = 0
        for normal, burst in zip_longest(
            self.normal_archive.iter_windows(),
            self.burst_archive.iter_windows(),
        ):
            if normal is None or burst is None:
                raise RawCollectionError("Normal/Burst window counts differ")
            if (
                normal.sequence != burst.sequence
                or normal.window_start_ns != burst.window_start_ns
                or normal.window_end_ns != burst.window_end_ns
                or normal.cluster_id != burst.cluster_id
            ):
                raise RawCollectionError("Normal/Burst window boundaries differ")
            evidence = collector.collect(
                samples=burst.samples,
                window_start_ns=normal.window_start_ns,
                window_end_ns=normal.window_end_ns,
                residual_source_record_ids=(
                    normal.residual_source_record_ids
                ),
            )
            current_normal = set(normal.residual_source_record_ids)
            current_burst = {
                source_id
                for item in evidence
                for source_id in item.source_record_ids
            }
            if current_normal & burst_sources or current_burst & normal_sources:
                raise RawCollectionError(
                    "Burst and residual sources overlap across windows"
                )
            normal_sources.update(current_normal)
            burst_sources.update(current_burst)
            count += 1
            joined = CollectedWindow.create(
                sequence=normal.sequence,
                window_start_ns=normal.window_start_ns,
                window_end_ns=normal.window_end_ns,
                node_metrics=normal.node_metrics,
                edge_metrics=normal.edge_metrics,
                topology_events=normal.topology_events,
                burst_evidence=evidence,
                residual_source_record_ids=(
                    normal.residual_source_record_ids
                ),
                collection_metadata=normal.collection_metadata,
            )
            _validate_window_contract(joined, self.collection_contract)
            yield joined
        if count != self.window_count:
            raise RawCollectionError("joined Burst window count mismatch")
