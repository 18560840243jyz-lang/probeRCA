"""Write-once archive for raw, algorithm-free Burst probe windows."""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator

from .burst_collection import RawBurstSample
from .contracts import assert_label_safe, canonical_json, fingerprint
from .raw import RawCollectionError


BURST_ARCHIVE_SCHEMA_VERSION = "probeRCA-final-burst-archive-v1"
BURST_WINDOW_SCHEMA_VERSION = "probeRCA-final-burst-window-v1"
BURST_MANIFEST_NAME = "burst-manifest.json"
BURST_WINDOWS_NAME = "burst-windows.jsonl"
_HEX = frozenset("0123456789abcdef")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256(name: str, value: Any) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in _HEX for character in value)
    ):
        raise RawCollectionError(f"{name} must be a lowercase SHA-256")
    return value


@dataclass(frozen=True)
class RawBurstWindow:
    schema_version: str
    sequence: int
    window_start_ns: int
    window_end_ns: int
    cluster_id: str
    samples: tuple[RawBurstSample, ...]
    event_source_fingerprint: str
    burst_config_fingerprint: str
    event_loss_rate: float
    window_fingerprint: str

    @classmethod
    def create(
        cls,
        *,
        sequence: int,
        window_start_ns: int,
        window_end_ns: int,
        cluster_id: str,
        samples: Iterable[RawBurstSample],
        event_source_fingerprint: str,
        burst_config_fingerprint: str,
        event_loss_rate: float,
    ) -> "RawBurstWindow":
        values = tuple(sorted(
            samples,
            key=lambda item: (
                item.entity_type,
                item.entity_id,
                item.channel_id,
                item.source_record_id,
            ),
        ))
        if any(not isinstance(item, RawBurstSample) for item in values):
            raise RawCollectionError(
                "raw Burst window samples must be RawBurstSample records"
            )
        for item in values:
            item.validate()
        return cls._create_from_validated_samples(
            sequence=sequence,
            window_start_ns=window_start_ns,
            window_end_ns=window_end_ns,
            cluster_id=cluster_id,
            samples=values,
            event_source_fingerprint=event_source_fingerprint,
            burst_config_fingerprint=burst_config_fingerprint,
            event_loss_rate=event_loss_rate,
        )

    @classmethod
    def _create_from_validated_samples(
        cls,
        *,
        sequence: int,
        window_start_ns: int,
        window_end_ns: int,
        cluster_id: str,
        samples: Iterable[RawBurstSample],
        event_source_fingerprint: str,
        burst_config_fingerprint: str,
        event_loss_rate: float,
    ) -> "RawBurstWindow":
        values = tuple(sorted(
            samples,
            key=lambda item: (
                item.entity_type,
                item.entity_id,
                item.channel_id,
                item.source_record_id,
            ),
        ))
        if any(not isinstance(item, RawBurstSample) for item in values):
            raise RawCollectionError(
                "raw Burst window samples must be RawBurstSample records"
            )
        payload = {
            "schema_version": BURST_WINDOW_SCHEMA_VERSION,
            "sequence": sequence,
            "window_start_ns": window_start_ns,
            "window_end_ns": window_end_ns,
            "cluster_id": cluster_id,
            "samples": [item.to_dict() for item in values],
            "event_source_fingerprint": event_source_fingerprint,
            "burst_config_fingerprint": burst_config_fingerprint,
            "event_loss_rate": float(event_loss_rate),
        }
        result = cls(
            **{**payload, "samples": values},
            window_fingerprint=fingerprint(payload),
        )
        result._validate_structure(validate_samples=False)
        assert_label_safe({
            "schema_version": result.schema_version,
            "sequence": result.sequence,
            "window_start_ns": result.window_start_ns,
            "window_end_ns": result.window_end_ns,
            "cluster_id": result.cluster_id,
            "event_source_fingerprint": result.event_source_fingerprint,
            "burst_config_fingerprint": result.burst_config_fingerprint,
            "event_loss_rate": result.event_loss_rate,
        })
        return result

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "RawBurstWindow":
        expected = set(cls.__dataclass_fields__)
        if not isinstance(payload, dict) or set(payload) != expected:
            raise RawCollectionError("raw Burst window fields mismatch")
        values = dict(payload)
        if not isinstance(values["samples"], list):
            raise RawCollectionError("raw Burst window samples must be a list")
        values["samples"] = tuple(
            RawBurstSample.from_dict(item) for item in values["samples"]
        )
        result = cls(**values)
        result._validate_structure(validate_samples=False)
        result._validate_fingerprint()
        return result

    def _validate_structure(self, *, validate_samples: bool) -> None:
        if self.schema_version != BURST_WINDOW_SCHEMA_VERSION:
            raise RawCollectionError("unsupported raw Burst window schema")
        if (
            isinstance(self.sequence, bool)
            or not isinstance(self.sequence, int)
            or self.sequence <= 0
            or self.window_start_ns < 0
            or self.window_end_ns - self.window_start_ns != 1_000_000_000
        ):
            raise RawCollectionError("raw Burst window time/sequence is invalid")
        if not isinstance(self.cluster_id, str) or not self.cluster_id:
            raise RawCollectionError("raw Burst window cluster_id is required")
        _sha256("event_source_fingerprint", self.event_source_fingerprint)
        _sha256("burst_config_fingerprint", self.burst_config_fingerprint)
        if not 0.0 <= float(self.event_loss_rate) <= 1.0:
            raise RawCollectionError("raw Burst event_loss_rate is invalid")
        source_ids = []
        for sample in self.samples:
            if not isinstance(sample, RawBurstSample):
                raise RawCollectionError(
                    "raw Burst window contains a non-sample record"
                )
            if validate_samples:
                sample.validate()
            if sample.cluster_id != self.cluster_id:
                raise RawCollectionError("raw Burst sample cluster mismatch")
            if not (
                self.window_start_ns
                <= sample.timestamp_ns
                < self.window_end_ns
            ):
                raise RawCollectionError("raw Burst sample is outside its window")
            source_ids.append(sample.source_record_id)
        if len(source_ids) != len(set(source_ids)):
            raise RawCollectionError("raw Burst window has duplicate sources")

    def _validate_fingerprint(self) -> None:
        payload = self.to_dict()
        supplied = payload.pop("window_fingerprint")
        if supplied != fingerprint(payload):
            raise RawCollectionError("raw Burst window fingerprint mismatch")
        assert_label_safe({
            key: payload[key]
            for key in (
                "schema_version", "sequence", "window_start_ns",
                "window_end_ns", "cluster_id", "event_source_fingerprint",
                "burst_config_fingerprint", "event_loss_rate",
            )
        })

    def validate(self) -> None:
        self._validate_structure(validate_samples=True)
        self._validate_fingerprint()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "sequence": self.sequence,
            "window_start_ns": self.window_start_ns,
            "window_end_ns": self.window_end_ns,
            "cluster_id": self.cluster_id,
            "samples": [item.to_dict() for item in self.samples],
            "event_source_fingerprint": self.event_source_fingerprint,
            "burst_config_fingerprint": self.burst_config_fingerprint,
            "event_loss_rate": self.event_loss_rate,
            "window_fingerprint": self.window_fingerprint,
        }


@dataclass(frozen=True)
class BurstArchive:
    schema_version: str
    dataset_id: str
    cluster_id: str
    start_ns: int
    end_ns: int
    window_count: int
    windows_file: str
    windows_sha256: str
    event_source_fingerprint: str
    burst_config_fingerprint: str
    created_at_ns: int
    sealed: bool
    manifest_fingerprint: str
    root: Path

    @classmethod
    def load(cls, root: str | Path) -> "BurstArchive":
        directory = Path(root).resolve()
        manifest = directory / BURST_MANIFEST_NAME
        if not manifest.is_file():
            raise RawCollectionError("raw Burst archive is not sealed")
        try:
            payload = json.loads(manifest.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise RawCollectionError("cannot read raw Burst manifest") from error
        expected = set(cls.__dataclass_fields__) - {"root"}
        if not isinstance(payload, dict) or set(payload) != expected:
            raise RawCollectionError("raw Burst manifest fields mismatch")
        supplied = payload.pop("manifest_fingerprint")
        if supplied != fingerprint(payload):
            raise RawCollectionError("raw Burst manifest fingerprint mismatch")
        result = cls(
            **payload,
            manifest_fingerprint=supplied,
            root=directory,
        )
        result.validate()
        tuple(result.iter_windows())
        return result

    def validate(self) -> None:
        if self.schema_version != BURST_ARCHIVE_SCHEMA_VERSION:
            raise RawCollectionError("unsupported raw Burst archive")
        _sha256("dataset_id", self.dataset_id)
        _sha256("windows_sha256", self.windows_sha256)
        _sha256("event_source_fingerprint", self.event_source_fingerprint)
        _sha256("burst_config_fingerprint", self.burst_config_fingerprint)
        if (
            self.sealed is not True
            or self.window_count <= 0
            or self.start_ns < 0
            or self.start_ns >= self.end_ns
            or self.windows_file != BURST_WINDOWS_NAME
        ):
            raise RawCollectionError("raw Burst archive range/state is invalid")
        windows = self.root / self.windows_file
        if not windows.is_file() or _sha256_file(windows) != self.windows_sha256:
            raise RawCollectionError("raw Burst archive content hash mismatch")

    def iter_windows(self) -> Iterator[RawBurstWindow]:
        previous_sequence = 0
        previous_end = None
        count = 0
        with (self.root / self.windows_file).open(
            "r", encoding="utf-8"
        ) as handle:
            for line_number, line in enumerate(handle, 1):
                if not line.strip():
                    raise RawCollectionError(
                        f"blank raw Burst line {line_number}"
                    )
                try:
                    window = RawBurstWindow.from_dict(json.loads(line))
                except Exception as error:
                    raise RawCollectionError(
                        f"invalid raw Burst line {line_number}: {error}"
                    ) from error
                if window.sequence != previous_sequence + 1:
                    raise RawCollectionError(
                        "raw Burst window sequence is not contiguous"
                    )
                if previous_end is not None \
                        and window.window_start_ns < previous_end:
                    raise RawCollectionError("raw Burst windows overlap")
                if (
                    window.cluster_id != self.cluster_id
                    or window.event_source_fingerprint
                    != self.event_source_fingerprint
                    or window.burst_config_fingerprint
                    != self.burst_config_fingerprint
                ):
                    raise RawCollectionError(
                        "raw Burst window conflicts with manifest"
                    )
                previous_sequence = window.sequence
                previous_end = window.window_end_ns
                count += 1
                yield window
        if (
            count != self.window_count
            or previous_end != self.end_ns
        ):
            raise RawCollectionError("raw Burst manifest/window count mismatch")


class BurstArchiveWriter:
    def __init__(
        self,
        root: str | Path,
        *,
        dataset_id: str,
        cluster_id: str,
        event_source_fingerprint: str,
        burst_config_fingerprint: str,
    ):
        self.root = Path(root).resolve()
        self.dataset_id = _sha256("dataset_id", dataset_id)
        self.cluster_id = cluster_id
        self.event_source_fingerprint = _sha256(
            "event_source_fingerprint", event_source_fingerprint
        )
        self.burst_config_fingerprint = _sha256(
            "burst_config_fingerprint", burst_config_fingerprint
        )
        self.root.mkdir(parents=True, exist_ok=False)
        self.windows_path = self.root / BURST_WINDOWS_NAME
        self._handle = self.windows_path.open("x", encoding="utf-8")
        self._count = 0
        self._start_ns = None
        self._end_ns = None
        self._sealed = False

    def append(self, window: RawBurstWindow) -> None:
        if self._sealed:
            raise RawCollectionError("cannot append to sealed Burst archive")
        window.validate()
        if (
            window.sequence != self._count + 1
            or window.cluster_id != self.cluster_id
            or window.event_source_fingerprint
            != self.event_source_fingerprint
            or window.burst_config_fingerprint
            != self.burst_config_fingerprint
        ):
            raise RawCollectionError("raw Burst append contract mismatch")
        if self._end_ns is not None \
                and window.window_start_ns < self._end_ns:
            raise RawCollectionError("raw Burst append overlaps")
        self._handle.write(canonical_json(window.to_dict()) + "\n")
        self._handle.flush()
        os.fsync(self._handle.fileno())
        self._count += 1
        self._start_ns = (
            window.window_start_ns
            if self._start_ns is None else self._start_ns
        )
        self._end_ns = window.window_end_ns

    def seal(self) -> BurstArchive:
        if self._sealed or self._count <= 0:
            raise RawCollectionError("raw Burst archive cannot be sealed")
        self._handle.close()
        payload = {
            "schema_version": BURST_ARCHIVE_SCHEMA_VERSION,
            "dataset_id": self.dataset_id,
            "cluster_id": self.cluster_id,
            "start_ns": self._start_ns,
            "end_ns": self._end_ns,
            "window_count": self._count,
            "windows_file": BURST_WINDOWS_NAME,
            "windows_sha256": _sha256_file(self.windows_path),
            "event_source_fingerprint": self.event_source_fingerprint,
            "burst_config_fingerprint": self.burst_config_fingerprint,
            "created_at_ns": time.time_ns(),
            "sealed": True,
        }
        manifest = {
            **payload,
            "manifest_fingerprint": fingerprint(payload),
        }
        path = self.root / BURST_MANIFEST_NAME
        with path.open("x", encoding="utf-8") as handle:
            handle.write(canonical_json(manifest) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        self._sealed = True
        return BurstArchive.load(self.root)
