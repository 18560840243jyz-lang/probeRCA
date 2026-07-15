"""Derived output view projection from immutable committed generations."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path

from .generation import ImmutableGenerationStore


class OutputProjectionError(ValueError):
    """The materialized output view does not match a committed generation."""


def _canonical(value) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    )


def _sha(value) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _sha_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _atomic_write(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with temporary.open("x", encoding="utf-8") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    descriptor = os.open(
        path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _bundle_hashes(generation) -> dict[str, str]:
    bundle = generation.path / "output_bundle"
    return {
        path.relative_to(bundle).as_posix(): _sha_file(path)
        for path in sorted(item for item in bundle.rglob("*") if item.is_file())
    }


@dataclass(frozen=True)
class OutputViewMarker:
    schema_version: str
    materialized_generation_id: str
    materialized_sequence: int
    output_ledger_fingerprint: str
    output_bundle_fingerprint: str
    managed_file_hashes: dict[str, str]
    marker_fingerprint: str

    @classmethod
    def create(cls, generation) -> "OutputViewMarker":
        payload = {
            "schema_version": "1.0",
            "materialized_generation_id": generation.generation_id,
            "materialized_sequence": generation.manifest["proposed_sequence"],
            "output_ledger_fingerprint": generation.manifest[
                "output_ledger_fingerprint"
            ],
            "output_bundle_fingerprint": generation.manifest[
                "output_bundle_fingerprint"
            ],
            "managed_file_hashes": _bundle_hashes(generation),
        }
        return cls(**payload, marker_fingerprint=_sha(payload))

    def to_dict(self) -> dict:
        return asdict(self)


class OutputProjector:
    marker_name = ".proberca-output-view.json"

    def __init__(self, directory, generation_store: ImmutableGenerationStore):
        self.directory = Path(directory)
        self.store = generation_store

    def _marker(self):
        path = self.directory / self.marker_name
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            fingerprint = payload.pop("marker_fingerprint")
            if _sha(payload) != fingerprint:
                raise OutputProjectionError(
                    "output marker fingerprint mismatch",
                )
            return OutputViewMarker(
                **payload, marker_fingerprint=fingerprint,
            )
        except OutputProjectionError:
            raise
        except Exception as error:
            raise OutputProjectionError(
                f"output marker is unreadable: {error}",
            ) from error

    def _known_chain(self, target):
        chain = {}
        current = target
        while current is not None:
            chain[current.generation_id] = current
            previous = current.manifest["previous_generation_id"]
            if not previous:
                current = None
            elif (self.store.root / previous).is_dir():
                current = self.store.load(previous)
            else:
                current = None
        return chain

    def _disk_files(self):
        if not self.directory.exists():
            return {}
        return {
            path.relative_to(self.directory).as_posix(): path
            for path in sorted(
                item for item in self.directory.rglob("*") if item.is_file()
            )
            if path.name != self.marker_name
            and not path.name.endswith(".tmp")
        }

    def _validate_existing_view(self, marker, marker_generation) -> bool:
        expected = marker.managed_file_hashes
        if (
            marker.output_ledger_fingerprint
            != marker_generation.manifest["output_ledger_fingerprint"]
            or marker.output_bundle_fingerprint
            != marker_generation.manifest["output_bundle_fingerprint"]
            or expected != _bundle_hashes(marker_generation)
        ):
            raise OutputProjectionError(
                "output marker does not match its generation",
            )
        disk = self._disk_files()
        unknown = sorted(set(disk) - set(expected))
        if unknown:
            raise OutputProjectionError(
                f"unknown output content: {unknown[0]}",
            )
        for name, path in disk.items():
            if _sha_file(path) != expected[name]:
                raise OutputProjectionError(
                    f"unknown output payload: {name}",
                )
        return set(disk) == set(expected)

    def _materialize(self, generation, marker) -> OutputViewMarker:
        bundle = generation.path / "output_bundle"
        expected = _bundle_hashes(generation)
        disk = self._disk_files()
        unknown = sorted(set(disk) - set(marker.managed_file_hashes))
        if unknown:
            raise OutputProjectionError(
                f"unknown output content: {unknown[0]}",
            )
        for name in sorted(set(disk) - set(expected)):
            disk[name].unlink()
        for name in sorted(expected):
            source = bundle / name
            _atomic_write(
                self.directory / name,
                source.read_text(encoding="utf-8"),
            )
        result = OutputViewMarker.create(generation)
        _atomic_write(
            self.directory / self.marker_name,
            _canonical(result.to_dict()),
        )
        return result

    def validate_initial_empty(self) -> None:
        if self._marker() is not None or self._disk_files():
            raise OutputProjectionError(
                "uncommitted RunState requires an empty output view",
            )

    def initialize_empty_view(self) -> None:
        self.validate_initial_empty()
        self.directory.mkdir(parents=True, exist_ok=True)
        self.validate_initial_empty()
        descriptor = os.open(
            self.directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def project(self, generation_id: str) -> OutputViewMarker:
        generation = self.store.load(generation_id)
        marker = self._marker()
        if marker is None:
            if self._disk_files():
                raise OutputProjectionError(
                    "output marker is missing for non-empty output",
                )
            self.directory.mkdir(parents=True, exist_ok=True)
            marker = OutputViewMarker(
                schema_version="1.0",
                materialized_generation_id="",
                materialized_sequence=0,
                output_ledger_fingerprint="",
                output_bundle_fingerprint="",
                managed_file_hashes={},
                marker_fingerprint="",
            )
            return self._materialize(generation, marker)
        try:
            marker_generation = self.store.load(
                marker.materialized_generation_id,
            )
        except Exception as error:
            raise OutputProjectionError(
                "output marker references unknown generation",
            ) from error
        if marker.materialized_sequence > generation.manifest["proposed_sequence"]:
            raise OutputProjectionError("output marker is ahead of RunState")
        chain = self._known_chain(generation)
        if marker.materialized_generation_id not in chain:
            raise OutputProjectionError(
                "output marker is not an ancestor of RunState",
            )
        complete = self._validate_existing_view(
            marker, marker_generation,
        )
        if (
            marker.materialized_generation_id == generation.generation_id
            and complete
        ):
            return marker
        return self._materialize(generation, marker)
