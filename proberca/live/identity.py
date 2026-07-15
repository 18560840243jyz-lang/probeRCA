"""Stable source and runtime image identity for uncommitted live builds."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path


class ImageIdentityMismatchError(RuntimeError):
    """The running image does not match the requested source identity."""


INCLUDED_ROOTS = (
    "proberca", "pyproject.toml", "Dockerfile", ".dockerignore",
    "deploy/kubernetes/base",
    "deploy/kubernetes/test/p11-smoke", "scripts",
)


def _included(path: Path, root: Path) -> bool:
    relative = path.relative_to(root).as_posix()
    if relative.startswith(("tests/", ".git/", "data/", "artifacts/", "external/")):
        return False
    if "__pycache__" in path.parts or path.suffix in {".pyc", ".pyo"}:
        return False
    if relative.startswith("scripts/") and not path.name.startswith("p11_smoke_"):
        return False
    return any(relative == item or relative.startswith(item + "/")
               for item in INCLUDED_ROOTS)


def compute_source_fingerprint(root, *, head_revision: str) -> str:
    root = Path(root).resolve()
    entries = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        if not _included(path, root):
            continue
        relative = path.relative_to(root).as_posix()
        entries.append({
            "path": relative,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        })
    payload = {"head_revision": str(head_revision), "files": entries}
    return hashlib.sha256(json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode()).hexdigest()


def verify_runtime_identity(expected: str, actual: str) -> None:
    if not expected or expected != actual:
        raise ImageIdentityMismatchError("runtime source fingerprint mismatch")
