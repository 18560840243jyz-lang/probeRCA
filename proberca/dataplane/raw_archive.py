"""Write-once archive of raw counter boundaries and histogram buckets."""

from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Iterator

from .contracts import canonical_json, fingerprint
from .raw import RawCollectionError, RawCollectionWindow


RAW_ARCHIVE_SCHEMA_VERSION = "probeRCA-raw-primitive-archive-v1"
RAW_WINDOWS_NAME = "raw-primitive-windows.jsonl"
RAW_MANIFEST_NAME = "raw-primitive-manifest.json"
_HEX = frozenset("0123456789abcdef")
_MANIFEST_FIELDS = frozenset({
    "schema_version", "dataset_id", "cluster_id", "start_ns", "end_ns",
    "window_count", "windows_file", "windows_sha256", "source_fingerprint",
    "created_at_ns", "sealed", "manifest_fingerprint",
})


def _sha256_identity(name: str, value: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in _HEX for character in value)
    ):
        raise RawCollectionError(f"{name} must be a lowercase SHA-256")
    return value


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


class RawPrimitiveArchiveWriter:
    def __init__(self, root: Path, *, dataset_id: str, source_fingerprint: str) -> None:
        _sha256_identity("raw primitive dataset_id", dataset_id)
        _sha256_identity("raw primitive source_fingerprint", source_fingerprint)
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=False)
        self.dataset_id = dataset_id
        self.source_fingerprint = source_fingerprint
        self.path = self.root / RAW_WINDOWS_NAME
        self.handle = self.path.open("x", encoding="utf-8", newline="\n")
        self.count = 0
        self.first: RawCollectionWindow | None = None
        self.last: RawCollectionWindow | None = None
        self.sealed = False

    def append(self, window: RawCollectionWindow) -> None:
        if self.sealed:
            raise RawCollectionError("raw primitive archive is already sealed")
        window.validate()
        if self.last is None and window.sequence != 1:
            raise RawCollectionError("raw primitive archive must start at sequence 1")
        if self.last is not None and (
            window.sequence != self.last.sequence + 1
            or window.window_start_ns != self.last.window_end_ns
        ):
            raise RawCollectionError("raw primitive windows are not contiguous")
        self.handle.write(canonical_json(window.to_dict()) + "\n")
        self.handle.flush()
        os.fsync(self.handle.fileno())
        if self.first is None:
            self.first = window
        self.last = window
        self.count += 1

    def close_partial(self) -> None:
        if not self.handle.closed:
            self.handle.flush()
            os.fsync(self.handle.fileno())
            self.handle.close()

    def seal(self) -> dict:
        if self.count <= 0 or self.first is None or self.last is None:
            raise RawCollectionError("cannot seal an empty raw primitive archive")
        self.close_partial()
        core = {
            "schema_version": RAW_ARCHIVE_SCHEMA_VERSION,
            "dataset_id": self.dataset_id,
            "cluster_id": self.first.cluster_id,
            "start_ns": self.first.window_start_ns,
            "end_ns": self.last.window_end_ns,
            "window_count": self.count,
            "windows_file": RAW_WINDOWS_NAME,
            "windows_sha256": _sha(self.path),
            "source_fingerprint": self.source_fingerprint,
            "created_at_ns": time.time_ns(),
            "sealed": True,
        }
        manifest = {**core, "manifest_fingerprint": fingerprint(core)}
        (self.root / RAW_MANIFEST_NAME).write_text(
            canonical_json(manifest) + "\n", encoding="utf-8",
        )
        self.sealed = True
        return manifest


class RawPrimitiveArchive:
    def __init__(self, root: Path) -> None:
        self.root = Path(root).resolve()
        path = self.root / RAW_MANIFEST_NAME
        if not path.is_file():
            raise RawCollectionError("raw primitive archive is not sealed")
        self.manifest = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(self.manifest, dict) or set(self.manifest) != _MANIFEST_FIELDS:
            raise RawCollectionError("raw primitive archive manifest fields mismatch")
        core = dict(self.manifest)
        supplied = core.pop("manifest_fingerprint", None)
        if (
            core.get("schema_version") != RAW_ARCHIVE_SCHEMA_VERSION
            or supplied != fingerprint(core)
            or core.get("sealed") is not True
            or _sha(self.root / core["windows_file"]) != core["windows_sha256"]
        ):
            raise RawCollectionError("raw primitive archive integrity failed")
        _sha256_identity("raw primitive dataset_id", core.get("dataset_id"))
        _sha256_identity(
            "raw primitive source_fingerprint", core.get("source_fingerprint")
        )
        if (
            isinstance(core.get("window_count"), bool)
            or not isinstance(core.get("window_count"), int)
            or core["window_count"] <= 0
            or core.get("start_ns", -1) < 0
            or core.get("end_ns", 0) <= core.get("start_ns", -1)
        ):
            raise RawCollectionError("raw primitive archive range is invalid")

    def iter_windows(self) -> Iterator[RawCollectionWindow]:
        previous = None
        count = 0
        with (self.root / self.manifest["windows_file"]).open(
            "r", encoding="utf-8"
        ) as stream:
            for line in stream:
                window = RawCollectionWindow.from_dict(json.loads(line))
                if previous is None and window.sequence != 1:
                    raise RawCollectionError(
                        "raw primitive archive must start at sequence 1"
                    )
                if previous is not None and (
                    window.sequence != previous.sequence + 1
                    or window.window_start_ns != previous.window_end_ns
                ):
                    raise RawCollectionError("raw primitive archive windows are not contiguous")
                previous = window
                count += 1
                yield window
        if count != self.manifest["window_count"]:
            raise RawCollectionError("raw primitive archive window count mismatch")
