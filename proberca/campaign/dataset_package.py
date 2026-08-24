"""Seal a complete campaign dataset directory for immutable upload."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

from proberca.dataplane.archive import CollectionArchive
from proberca.dataplane.burst_archive import BurstArchive
from proberca.dataplane.contracts import canonical_json, fingerprint
from proberca.dataplane.raw_archive import RawPrimitiveArchive


class DatasetPackageError(RuntimeError):
    pass


_REQUIRED_METADATA = frozenset({
    "dataset_id", "case_id", "git_sha", "load_profile_id",
    "load_profile_fingerprint", "public_manifest_fingerprint",
    "campaign_config_fingerprint",
})
_PRIVATE_TEST_KEYS = frozenset({
    "coordinate", "coordinate_id", "entity_id", "entity_kind", "metric",
    "mechanism", "target", "fault", "injector_profile_id", "ground_truth",
})


def _contains_private_test_semantics(value: Any) -> bool:
    if isinstance(value, dict):
        return bool(set(value) & _PRIVATE_TEST_KEYS) or any(
            _contains_private_test_semantics(item) for item in value.values()
        )
    if isinstance(value, (list, tuple)):
        return any(_contains_private_test_semantics(item) for item in value)
    return False


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def seal_dataset_directory(root: Path, metadata: dict[str, Any]) -> dict[str, Any]:
    root = Path(root).resolve()
    if (root / "SHA256SUMS").exists() or (root / "dataset-manifest.json").exists():
        raise DatasetPackageError("dataset directory is already sealed")
    normal = CollectionArchive.load(root / "normal")
    burst = BurstArchive.load(root / "burst")
    if normal.dataset_id != burst.dataset_id:
        raise DatasetPackageError("formal Normal/Burst Dataset IDs differ")
    worker_primitives = sorted(root.glob("primitives/*"))
    expected_workers = {"worker-1", "worker-2", "worker-3"}
    if (
        len(worker_primitives) != 3
        or {item.name for item in worker_primitives} != expected_workers
    ):
        raise DatasetPackageError("dataset must contain three worker raw primitive archives")
    primitives = [RawPrimitiveArchive(path) for path in worker_primitives]
    if any(item.manifest["dataset_id"] != normal.dataset_id for item in primitives):
        raise DatasetPackageError("worker raw primitives use another Dataset ID")
    if any(item.manifest["window_count"] != normal.window_count for item in primitives):
        raise DatasetPackageError("worker raw primitive count differs from formal archive")
    for item in primitives:
        if (
            item.manifest["cluster_id"] != normal.cluster_id
            or item.manifest["start_ns"] != normal.start_ns
            or item.manifest["end_ns"] != normal.end_ns
        ):
            raise DatasetPackageError(
                "worker raw primitive identity/range differs from formal archive"
            )
        windows = tuple(item.iter_windows())
        if (
            len(windows) != normal.window_count
            or windows[0].window_start_ns != normal.start_ns
            or windows[-1].window_end_ns != normal.end_ns
        ):
            raise DatasetPackageError(
                "worker raw primitive boundaries differ from formal archive"
            )
    raw_event_roots = sorted(root.glob("raw-events/*"))
    if (
        len(raw_event_roots) != 3
        or {item.name for item in raw_event_roots} != expected_workers
    ):
        raise DatasetPackageError(
            "dataset must contain three worker filtered raw-event archives"
        )
    for raw_event_root in raw_event_roots:
        manifest_path = raw_event_root / "raw-event-manifest.json"
        if not manifest_path.is_file():
            raise DatasetPackageError("worker raw-event manifest is missing")
        event_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        core = {
            key: value for key, value in event_manifest.items()
            if key != "manifest_fingerprint"
        }
        if (
            event_manifest.get("schema_version")
            != "probeRCA-filtered-burst-raw-events-v1"
            or event_manifest.get("manifest_fingerprint") != fingerprint(core)
            or event_manifest.get("start_ns") != normal.start_ns
            or event_manifest.get("end_ns") != normal.end_ns
            or event_manifest.get("boundary_count") != normal.window_count + 1
        ):
            raise DatasetPackageError(
                "worker raw-event range or fingerprint differs from formal archive"
            )
        for name, sha_field in (
            ("filtered-ebpf-events.jsonl", "events_sha256"),
            ("checkpoints.jsonl", "checkpoints_sha256"),
        ):
            path = raw_event_root / name
            if not path.is_file() or _sha(path) != event_manifest.get(sha_field):
                raise DatasetPackageError("worker raw-event content integrity failed")
    if metadata.get("dataset_id") != normal.dataset_id:
        raise DatasetPackageError("package metadata Dataset ID mismatch")
    missing_metadata = sorted(_REQUIRED_METADATA - set(metadata))
    if missing_metadata:
        raise DatasetPackageError(
            f"package metadata is incomplete: {missing_metadata}"
        )
    git_sha = str(metadata.get("git_sha", ""))
    if len(git_sha) != 40 or any(item not in "0123456789abcdef" for item in git_sha):
        raise DatasetPackageError("package Git SHA is invalid")
    for name in (
        "load_profile_fingerprint", "public_manifest_fingerprint",
        "campaign_config_fingerprint",
    ):
        value = str(metadata.get(name, ""))
        if len(value) != 64 or any(item not in "0123456789abcdef" for item in value):
            raise DatasetPackageError(f"package {name} is invalid")
    case_id = str(metadata.get("case_id", ""))
    if case_id.startswith("T"):
        if _contains_private_test_semantics(metadata):
            raise DatasetPackageError("Test metadata contains private label semantics")
        plaintext = []
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            relative = path.relative_to(root)
            encrypted = path.name.lower().endswith((".cms", ".enc"))
            if (
                ("labels" in relative.parts and not encrypted)
                or ("label" in path.name.lower() and not encrypted)
            ):
                plaintext.append(path)
        if plaintext:
            raise DatasetPackageError("Test dataset contains plaintext labels")
    manifest_core = {
        "schema_version": "probeRCA-campaign-dataset-manifest-v1",
        "dataset_id": normal.dataset_id,
        "case_id": case_id,
        "window_count": normal.window_count,
        "start_ns": normal.start_ns,
        "end_ns": normal.end_ns,
        "normal_manifest_fingerprint": normal.manifest_fingerprint,
        "burst_manifest_fingerprint": burst.manifest_fingerprint,
        "worker_raw_primitive_manifest_fingerprints": sorted(
            item.manifest["manifest_fingerprint"] for item in primitives
        ),
        "worker_raw_event_manifest_fingerprints": sorted(
            json.loads((path / "raw-event-manifest.json").read_text(
                encoding="utf-8"
            ))["manifest_fingerprint"] for path in raw_event_roots
        ),
        "metadata": metadata,
    }
    manifest = {**manifest_core, "manifest_fingerprint": fingerprint(manifest_core)}
    manifest_path = root / "dataset-manifest.json"
    manifest_path.write_text(canonical_json(manifest) + "\n", encoding="utf-8")
    files = sorted(
        path for path in root.rglob("*") if path.is_file()
        and path.name != "SHA256SUMS"
    )
    if any(path.is_symlink() for path in files):
        raise DatasetPackageError("dataset package cannot contain symbolic links")
    lines = []
    for path in files:
        relative = path.relative_to(root).as_posix()
        lines.append(f"{_sha(path)}  {relative}")
    sums = root / "SHA256SUMS"
    temporary = root / ".SHA256SUMS.tmp"
    temporary.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.replace(temporary, sums)
    return {
        "dataset_id": normal.dataset_id,
        "file_count": len(files),
        "sha256sums_sha256": _sha(sums),
        "dataset_manifest_fingerprint": manifest["manifest_fingerprint"],
        "sealed": True,
    }
