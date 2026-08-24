"""Offline archive copy and SHA verification without Kubernetes access."""

from __future__ import annotations

import hashlib
from pathlib import Path


class RestoreVerificationError(RuntimeError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            block = stream.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def verify_sha256s(dataset_root: Path) -> dict[str, str]:
    root = Path(dataset_root).resolve()
    sums = root / "SHA256SUMS"
    if not sums.is_file():
        raise RestoreVerificationError("dataset has no SHA256SUMS")
    verified = {}
    for number, line in enumerate(sums.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            expected, relative = line.split("  ", 1)
        except ValueError as error:
            raise RestoreVerificationError(
                f"invalid SHA256SUMS line {number}"
            ) from error
        if len(expected) != 64 or any(character not in "0123456789abcdef" for character in expected):
            raise RestoreVerificationError(f"invalid SHA-256 on line {number}")
        candidate = (root / relative).resolve()
        if candidate == root or root not in candidate.parents:
            raise RestoreVerificationError("SHA256SUMS path escapes dataset root")
        if not candidate.is_file():
            raise RestoreVerificationError(f"dataset file is missing: {relative}")
        actual = _sha256(candidate)
        if actual != expected:
            raise RestoreVerificationError(f"SHA-256 mismatch: {relative}")
        verified[relative] = actual
    if not verified:
        raise RestoreVerificationError("SHA256SUMS contains no files")
    return verified


def compare_restored_copy(source_root: Path, restored_root: Path) -> dict[str, int]:
    source = verify_sha256s(source_root)
    restored = verify_sha256s(restored_root)
    if source != restored:
        missing = sorted(set(source) - set(restored))
        extra = sorted(set(restored) - set(source))
        changed = sorted(
            key for key in set(source) & set(restored)
            if source[key] != restored[key]
        )
        raise RestoreVerificationError(
            f"restored copy differs: missing={missing}, extra={extra}, changed={changed}"
        )
    return {"verified_file_count": len(source), "byte_identical": True}
