"""Manifest-driven archive upload, independent readback, and restoration."""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Protocol

from .restore import RestoreVerificationError, verify_sha256s


class ArchiveStoreError(RuntimeError):
    pass


class ArchiveStore(Protocol):
    def put_file(self, source: Path, object_name: str) -> None: ...
    def open_object(self, object_name: str) -> BinaryIO: ...


def _safe_object_name(value: str) -> str:
    path = PurePosixPath(value)
    if not value or path.is_absolute() or ".." in path.parts:
        raise ArchiveStoreError("object name escapes the campaign prefix")
    return str(path)


class FilesystemArchiveStore:
    """A second-filesystem implementation used for offline restore rehearsals."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, object_name: str) -> Path:
        candidate = (self.root / _safe_object_name(object_name)).resolve()
        if self.root not in candidate.parents:
            raise ArchiveStoreError("object path escapes store root")
        return candidate

    def put_file(self, source: Path, object_name: str) -> None:
        target = self._path(object_name)
        if target.exists():
            raise ArchiveStoreError("immutable object already exists")
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(target.suffix + ".tmp")
        shutil.copyfile(source, temporary)
        os.replace(temporary, target)

    def open_object(self, object_name: str) -> BinaryIO:
        return self._path(object_name).open("rb")


class RcloneArchiveStore:
    """S3-compatible adapter using a preconfigured rclone remote.

    ``rclone`` is invoked directly (never through a shell), and uploads use
    ``--immutable`` so a rerun cannot overwrite already sealed evidence.
    """

    def __init__(
        self, remote_prefix: str, *, executable: str = "rclone",
        timeout_seconds: int = 600,
    ) -> None:
        if not remote_prefix or remote_prefix.endswith("/"):
            raise ValueError("rclone remote prefix must be non-empty without trailing slash")
        self.remote_prefix = remote_prefix
        self.executable = executable
        self.timeout_seconds = timeout_seconds

    def _remote(self, object_name: str) -> str:
        return f"{self.remote_prefix}/{_safe_object_name(object_name)}"

    def put_file(self, source: Path, object_name: str) -> None:
        completed = subprocess.run(
            [self.executable, "copyto", "--immutable", str(source),
             self._remote(object_name)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=self.timeout_seconds, check=False,
        )
        if completed.returncode != 0:
            raise ArchiveStoreError(
                "rclone immutable upload failed: "
                + completed.stderr.decode("utf-8", errors="replace").strip()
            )

    def open_object(self, object_name: str) -> BinaryIO:
        process = subprocess.Popen(
            [self.executable, "cat", self._remote(object_name)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        if process.stdout is None:
            process.kill()
            raise ArchiveStoreError("rclone produced no readback stream")
        return _CheckedProcessStream(process)


class _CheckedProcessStream:
    def __init__(self, process: subprocess.Popen) -> None:
        self.process = process
        self.stream = process.stdout

    def read(self, size: int = -1) -> bytes:
        return self.stream.read(size)

    def __enter__(self) -> "_CheckedProcessStream":
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        self.stream.close()
        stderr = self.process.stderr.read() if self.process.stderr else b""
        returncode = self.process.wait()
        if exc_type is None and returncode != 0:
            raise ArchiveStoreError(
                "rclone readback failed: "
                + stderr.decode("utf-8", errors="replace").strip()
            )
        return False


def _stream_sha256(stream: BinaryIO) -> str:
    digest = hashlib.sha256()
    while True:
        block = stream.read(1024 * 1024)
        if not block:
            break
        digest.update(block)
    return digest.hexdigest()


def upload_and_verify_dataset(
    dataset_root: Path, store: ArchiveStore, *, object_prefix: str,
) -> dict[str, object]:
    """Upload only sealed files and verify each object through a fresh read."""

    root = Path(dataset_root).resolve()
    verified = verify_sha256s(root)
    prefix = _safe_object_name(object_prefix)
    for relative in sorted(verified):
        store.put_file(root / relative, f"{prefix}/{relative}")
    sums = root / "SHA256SUMS"
    store.put_file(sums, f"{prefix}/SHA256SUMS")
    readback: dict[str, str] = {}
    for relative, expected in sorted(verified.items()):
        with store.open_object(f"{prefix}/{relative}") as stream:
            actual = _stream_sha256(stream)
        if actual != expected:
            raise ArchiveStoreError(f"independent object SHA mismatch: {relative}")
        readback[relative] = actual
    with store.open_object(f"{prefix}/SHA256SUMS") as stream:
        sums_sha256 = _stream_sha256(stream)
    if sums_sha256 != hashlib.sha256(sums.read_bytes()).hexdigest():
        raise ArchiveStoreError("object-store SHA256SUMS readback mismatch")
    return {
        "object_prefix": prefix,
        "verified_file_count": len(readback),
        "sha256sums_sha256": sums_sha256,
        "independent_readback_passed": True,
    }


def restore_dataset(
    store: ArchiveStore, *, object_prefix: str, output_root: Path,
) -> dict[str, object]:
    prefix = _safe_object_name(object_prefix)
    output = Path(output_root).resolve()
    if output.exists() and any(output.iterdir()):
        raise ArchiveStoreError("restore output directory is not empty")
    output.mkdir(parents=True, exist_ok=True)
    sums_path = output / "SHA256SUMS"
    with store.open_object(f"{prefix}/SHA256SUMS") as stream, \
            sums_path.open("xb") as target:
        shutil.copyfileobj(stream, target)
    entries: list[str] = []
    for line in sums_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            _digest, relative = line.split("  ", 1)
        except ValueError as error:
            raise RestoreVerificationError("invalid restored SHA256SUMS") from error
        safe = _safe_object_name(relative)
        target = (output / safe).resolve()
        if output not in target.parents:
            raise ArchiveStoreError("restored path escapes output directory")
        target.parent.mkdir(parents=True, exist_ok=True)
        with store.open_object(f"{prefix}/{safe}") as stream, target.open("xb") as sink:
            shutil.copyfileobj(stream, sink)
        entries.append(safe)
    verified = verify_sha256s(output)
    if set(verified) != set(entries):
        raise ArchiveStoreError("restored file set differs from SHA256SUMS")
    return {
        "output_root": str(output),
        "verified_file_count": len(verified),
        "offline_restore_passed": True,
    }
