"""Immutable, content-addressed generation v5 storage for live transactions."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path


GENERATION_SCHEMA_VERSION = "generation_v5"


class GenerationIntegrityError(ValueError):
    """A generation is incomplete, corrupt, or conflicts with its content address."""


def _canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha_json(value) -> str:
    return _sha_bytes(_canonical(value).encode("utf-8"))


def _fsync_file(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with temporary.open("x", encoding="utf-8") as handle:
        handle.write(_canonical(value))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    _fsync_directory(path.parent)


def _write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())


def _file_hashes(root: Path) -> dict[str, str]:
    values = {}
    for path in sorted(item for item in root.rglob("*") if item.is_file() and item.name != "manifest.json"):
        values[path.relative_to(root).as_posix()] = _sha_bytes(path.read_bytes())
    return values


@dataclass(frozen=True)
class ImmutableGeneration:
    generation_id: str
    generation_fingerprint: str
    path: Path
    manifest: dict


class ImmutableGenerationStore:
    def __init__(self, root):
        self.root = Path(root)

    def initialize_root(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        _fsync_directory(self.root)
        _fsync_directory(self.root.parent)

    def _validate(self, path: Path) -> ImmutableGeneration:
        manifest_path = path / "manifest.json"
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception as error:
            raise GenerationIntegrityError(f"generation manifest is unreadable: {error}") from error
        required = {
            "schema_version", "generation_id", "previous_generation_id", "proposed_sequence",
            "window_start_ns", "window_end_ns", "leadership_epoch", "holder_fingerprint",
            "engine_state_fingerprint", "output_ledger_fingerprint",
            "output_bundle_fingerprint", "commit_entry_fingerprint",
            "config_fingerprint", "code_schema_version", "file_hashes",
            "generation_fingerprint",
        }
        if set(manifest) != required or manifest["schema_version"] != GENERATION_SCHEMA_VERSION:
            raise GenerationIntegrityError("generation manifest fields are invalid")
        if manifest["window_start_ns"] >= manifest["window_end_ns"] or                 int(manifest["proposed_sequence"]) <= 0:
            raise GenerationIntegrityError("generation window or sequence is invalid")
        file_hashes = _file_hashes(path)
        if file_hashes != manifest["file_hashes"]:
            raise GenerationIntegrityError("generation file hash mismatch")
        identity = dict(manifest)
        fingerprint = identity.pop("generation_fingerprint")
        identity["generation_id"] = None
        identity["generation_fingerprint"] = None
        if _sha_json(identity) != fingerprint or manifest["generation_id"] != fingerprint:
            raise GenerationIntegrityError("generation fingerprint mismatch")
        return ImmutableGeneration(manifest["generation_id"], fingerprint, path, manifest)

    def prepare(self, *, previous_generation_id, proposed_sequence, window_start_ns,
                window_end_ns, leadership_epoch, holder_fingerprint, engine_state,
                output_ledger, output_bundle, config_fingerprint, code_schema_version,
                transaction_id=None, instance_fingerprint=None) -> ImmutableGeneration:
        if proposed_sequence <= 0 or window_start_ns >= window_end_ns:
            raise GenerationIntegrityError("generation window or sequence is invalid")
        self.root.mkdir(parents=True, exist_ok=True)
        nonce = uuid.uuid4().hex
        temp = self.root / f".pending.{instance_fingerprint or 'local'}.{transaction_id or nonce}.tmp"
        if temp.exists():
            raise FileExistsError("generation temporary path already exists")
        temp.mkdir()
        try:
            engine_directory = temp / "engine_state"
            engine_directory.mkdir()
            if callable(engine_state):
                engine_state(engine_directory)
            else:
                _atomic_json(engine_directory / "state.json", engine_state)
            engine_hashes = {
                path.relative_to(engine_directory).as_posix(): _sha_bytes(
                    path.read_bytes(),
                )
                for path in sorted(
                    item for item in engine_directory.rglob("*") if item.is_file()
                )
            }
            if not engine_hashes:
                raise GenerationIntegrityError("engine state bundle is empty")
            _atomic_json(temp / "output_ledger.json", output_ledger)
            bundle = temp / "output_bundle"
            bundle.mkdir()
            _write_text(bundle / "alerts.jsonl", str(output_bundle.get("alerts.jsonl", "")))
            _write_text(bundle / "failures.jsonl", str(output_bundle.get("failures.jsonl", "")))
            reports = output_bundle.get("reports", {})
            if not isinstance(reports, dict):
                raise GenerationIntegrityError("output reports must be a dictionary")
            for report_id, payload in sorted(reports.items()):
                _write_text(bundle / "reports" / f"{report_id}.json",
                            payload if isinstance(payload, str) else _canonical(payload))
            commit_entry = {
                "proposed_sequence": proposed_sequence, "previous_generation_id": previous_generation_id,
                "window_start_ns": window_start_ns, "window_end_ns": window_end_ns,
                "leadership_epoch": leadership_epoch, "holder_fingerprint": holder_fingerprint,
            }
            _atomic_json(temp / "commit_entry.json", commit_entry)
            hashes = _file_hashes(temp)
            manifest = {
                "schema_version": GENERATION_SCHEMA_VERSION,
                "generation_id": None, "previous_generation_id": previous_generation_id,
                "proposed_sequence": int(proposed_sequence), "window_start_ns": int(window_start_ns),
                "window_end_ns": int(window_end_ns), "leadership_epoch": int(leadership_epoch),
                "holder_fingerprint": str(holder_fingerprint),
                "engine_state_fingerprint": _sha_json(engine_hashes),
                "output_ledger_fingerprint": (
                    output_ledger.get("ledger_fingerprint")
                    if isinstance(output_ledger, dict)
                    and output_ledger.get("ledger_fingerprint")
                    else _sha_json(output_ledger)
                ),
                "output_bundle_fingerprint": _sha_json(output_bundle),
                "commit_entry_fingerprint": _sha_json(commit_entry),
                "config_fingerprint": str(config_fingerprint),
                "code_schema_version": str(code_schema_version), "file_hashes": hashes,
                "generation_fingerprint": None,
            }
            identity = dict(manifest)
            identity["generation_id"] = None
            identity["generation_fingerprint"] = None
            fingerprint = _sha_json(identity)
            manifest["generation_id"] = fingerprint
            manifest["generation_fingerprint"] = fingerprint
            _atomic_json(temp / "manifest.json", manifest)
            for item in sorted(temp.rglob("*"), key=lambda value: len(value.parts), reverse=True):
                if item.is_file():
                    _fsync_file(item)
                elif item.is_dir():
                    _fsync_directory(item)
            final = self.root / fingerprint
            if final.exists():
                existing = self._validate(final)
                if existing.generation_fingerprint != fingerprint:
                    raise GenerationIntegrityError("generation ID content conflict")
                shutil.rmtree(temp)
                return existing
            try:
                os.replace(temp, final)
            except OSError:
                if not final.is_dir():
                    raise
                existing = self._validate(final)
                if existing.generation_fingerprint != fingerprint:
                    raise GenerationIntegrityError(
                        "generation ID content conflict",
                    )
                shutil.rmtree(temp)
                return existing
            _fsync_directory(self.root)
            return self._validate(final)
        except Exception:
            if temp.exists():
                shutil.rmtree(temp)
            raise

    def load(self, generation_id: str) -> ImmutableGeneration:
        if not generation_id or Path(generation_id).name != generation_id:
            raise GenerationIntegrityError("generation ID is invalid")
        path = self.root / generation_id
        if not path.is_dir():
            raise GenerationIntegrityError("generation does not exist")
        return self._validate(path)

    def apply_retention(
        self,
        current_generation_id: str,
        *,
        keep_generations: int,
        minimum_age_sec: float,
        now: float,
    ) -> list[dict]:
        if keep_generations < 2 or minimum_age_sec < 0:
            raise ValueError("generation retention configuration is invalid")
        current = self.load(current_generation_id)
        protected = set()
        cursor = current
        while cursor is not None and len(protected) < keep_generations:
            protected.add(cursor.generation_id)
            previous = cursor.manifest["previous_generation_id"]
            cursor = self.load(previous) if previous else None
        issues = []
        for path in sorted(self.root.iterdir()):
            if not path.is_dir() or path.name.startswith("."):
                continue
            if path.name in protected:
                continue
            age = float(now) - path.stat().st_mtime
            if age < minimum_age_sec:
                continue
            try:
                self._validate(path)
                shutil.rmtree(path)
            except Exception as error:
                issues.append({
                    "reason_code": "generation_retention_failed",
                    "generation_id": path.name,
                    "detail": str(error),
                })
        if any(not path.name.startswith(".") for path in self.root.iterdir()):
            _fsync_directory(self.root)
        return issues
