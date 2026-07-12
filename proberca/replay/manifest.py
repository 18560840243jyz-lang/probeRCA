"""Strict, label-isolated Replay dataset manifest."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

import yaml


class ReplayManifestError(ValueError):
    """Replay manifest fields, paths, or declared files are invalid."""


class ReplayIntegrityError(ValueError):
    """A dataset file does not match its declared SHA-256 digest."""


_PATH_FIELDS = (
    "node_metrics_file", "edge_metrics_file", "topology_file", "evidence_file",
    "labels_file", "config_file",
)
_TRUTH_KEYS = {"root_service", "root_metric", "root_edge", "fault_mode", "edge_subtype"}


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _contains_truth(value) -> bool:
    if isinstance(value, dict):
        return bool(_TRUTH_KEYS & set(value)) or any(_contains_truth(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_truth(item) for item in value)
    return False


@dataclass(frozen=True)
class ReplayDatasetManifest:
    schema_version: str
    dataset_id: str
    dataset_version: str
    cluster_id: str
    namespaces: list[str]
    start_ns: int
    end_ns: int
    window_sec: int
    node_metrics_file: str
    edge_metrics_file: str
    topology_file: str
    evidence_file: str | None
    labels_file: str | None
    config_file: str
    file_sha256: dict[str, str]
    expected_schema_versions: list[str]
    allowed_record_types: list[str]
    evidence_semantics: str
    source_description: str
    created_at_ns: int
    metadata: dict
    dataset_root: Path = field(repr=False, compare=False)
    manifest_sha256: str

    @classmethod
    def load(cls, dataset_root: str | Path) -> "ReplayDatasetManifest":
        root = Path(dataset_root).resolve()
        manifest_path = root / "manifest.yaml"
        if not manifest_path.is_file():
            raise ReplayManifestError(f"missing Replay manifest: {manifest_path}")
        payload = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
        expected = {item.name for item in cls.__dataclass_fields__.values()} - {
            "dataset_root", "manifest_sha256"
        }
        if not isinstance(payload, dict) or set(payload) != expected:
            unknown = sorted(set(payload or {}) - expected)
            missing = sorted(expected - set(payload or {}))
            raise ReplayManifestError(f"manifest fields mismatch; unknown={unknown}, missing={missing}")
        if _contains_truth(payload):
            raise ReplayManifestError("manifest must not contain ground truth fields")
        for name in ("schema_version", "dataset_id", "dataset_version", "cluster_id",
                     "source_description"):
            if not isinstance(payload[name], str) or not payload[name].strip():
                raise ReplayManifestError(f"{name} must be a non-empty string")
        if payload["schema_version"] != "1.0":
            raise ReplayManifestError("unsupported manifest schema_version")
        if not isinstance(payload["start_ns"], int) or not isinstance(payload["end_ns"], int) \
                or payload["start_ns"] < 0 or payload["start_ns"] >= payload["end_ns"]:
            raise ReplayManifestError("manifest time range is invalid")
        if isinstance(payload["window_sec"], bool) or not isinstance(payload["window_sec"], int) \
                or payload["window_sec"] <= 0:
            raise ReplayManifestError("window_sec must be positive")
        if not isinstance(payload["namespaces"], list) or not payload["namespaces"] \
                or payload["namespaces"] != sorted(set(payload["namespaces"])):
            raise ReplayManifestError("namespaces must be sorted, unique, and non-empty")
        if payload["evidence_semantics"] != "normalized_only":
            raise ReplayManifestError("evidence_semantics must be normalized_only")
        resolved: dict[str, Path] = {}
        for name in _PATH_FIELDS:
            value = payload[name]
            if value is None and name in {"evidence_file", "labels_file"}:
                continue
            if not isinstance(value, str) or not value:
                raise ReplayManifestError(f"{name} must be a relative path")
            pure = PurePosixPath(value.replace("\\", "/"))
            if pure.is_absolute() or ".." in pure.parts:
                raise ReplayManifestError(f"{name} contains path traversal")
            target = (root / Path(*pure.parts)).resolve()
            try:
                target.relative_to(root)
            except ValueError as error:
                raise ReplayManifestError(f"{name} escapes dataset root") from error
            if not target.is_file():
                raise ReplayManifestError(f"missing dataset file: {value}")
            resolved[value] = target
        if not isinstance(payload["file_sha256"], dict) or set(payload["file_sha256"]) != set(resolved):
            raise ReplayManifestError("file_sha256 must cover every declared file exactly")
        labels_relative = payload["labels_file"]
        for relative, target in resolved.items():
            expected_hash = payload["file_sha256"][relative]
            if not isinstance(expected_hash, str) or len(expected_hash) != 64:
                raise ReplayIntegrityError(f"invalid SHA-256 declaration for {relative}")
            if relative != labels_relative and _sha(target) != expected_hash:
                raise ReplayIntegrityError(f"SHA-256 mismatch for {relative}")
        return cls(**payload, dataset_root=root, manifest_sha256=_sha(manifest_path))

    def resolve_data_path(self, relative: str) -> Path:
        if relative not in self.file_sha256:
            raise ReplayManifestError(f"undeclared dataset path: {relative}")
        return (self.dataset_root / relative).resolve()
