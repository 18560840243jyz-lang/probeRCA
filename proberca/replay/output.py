"""Deterministic Replay output materialized atomically from an OutputLedger."""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path

from proberca.orchestration.state import OutputLedger


class ReplayOutputError(ValueError):
    """Replay output is partial, conflicting, or unsafe to overwrite."""


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha(value):
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    _fsync_directory(path.parent)


def _entries_text(entries: list[dict]) -> str:
    return "".join(_canonical(item["payload"]) + "\n" for item in entries)


@dataclass(frozen=True)
class ReplayRunManifest:
    schema_version: str
    run_id: str
    dataset_id: str
    dataset_version: str
    dataset_manifest_hash: str
    config_hash: str
    code_version: str
    start_ns: int
    end_ns: int
    processed_windows: int
    alert_count: int
    hard_incident_count: int
    report_count: int
    failure_count: int
    report_ids: list[str]
    failure_ids: list[str]
    run_fingerprint: str
    runtime_summary: dict


class ReplayOutputWriter:
    def __init__(self, directory, *, overwrite=False,
                 resume_ledger: OutputLedger | None = None):
        self.directory = Path(directory)
        if resume_ledger is None:
            if self.directory.exists() and any(self.directory.iterdir()) and not overwrite:
                raise ReplayOutputError("Replay output directory is not empty")
            self.directory.mkdir(parents=True, exist_ok=True)
            (self.directory / "reports").mkdir(exist_ok=True)
            return
        if not isinstance(resume_ledger, OutputLedger):
            raise TypeError("resume_ledger must be OutputLedger")
        self.directory.mkdir(parents=True, exist_ok=True)
        self._materialize_resume(resume_ledger)

    def _validate_or_write(self, path: Path, expected: str) -> None:
        if path.exists():
            try:
                actual = path.read_text(encoding="utf-8")
            except Exception as error:
                raise ReplayOutputError(f"cannot read existing output {path.name}: {error}") from error
            if actual != expected:
                raise ReplayOutputError(f"existing output conflicts with checkpoint ledger: {path.name}")
            return
        _atomic_write(path, expected)

    def _materialize_resume(self, ledger: OutputLedger) -> None:
        allowed = {
            "alerts.jsonl", "failures.jsonl", "run_manifest.json", "reports",
            "checkpoint", "evaluation.json",
        }
        extras = sorted(item.name for item in self.directory.iterdir() if item.name not in allowed)
        if extras:
            raise ReplayOutputError(f"resume output contains unknown files: {extras}")
        self._validate_or_write(
            self.directory / "alerts.jsonl", _entries_text(ledger.alert_entries))
        self._validate_or_write(
            self.directory / "failures.jsonl", _entries_text(ledger.failure_entries))
        reports = self.directory / "reports"
        reports.mkdir(exist_ok=True)
        expected_reports = {
            f"{item['object_id']}.json": _canonical(item["payload"])
            for item in ledger.report_entries
        }
        existing = {item.name for item in reports.glob("*.json")}
        unexpected = sorted(existing - set(expected_reports))
        if unexpected:
            raise ReplayOutputError(f"resume output contains conflicting reports: {unexpected}")
        for name, payload in expected_reports.items():
            self._validate_or_write(reports / name, payload)
        if ledger.run_manifest_payload is not None:
            self._validate_or_write(
                self.directory / "run_manifest.json",
                _canonical(ledger.run_manifest_payload))
        _fsync_directory(reports)
        _fsync_directory(self.directory)

    @staticmethod
    def _unique(values, identity, label):
        ids = [identity(item) for item in values]
        if len(ids) != len(set(ids)):
            raise ReplayOutputError(f"duplicate {label} ID in output ledger")

    def write_alerts(self, alerts):
        self._unique(alerts, lambda item: item.alert_id, "alert")
        _atomic_write(
            self.directory / "alerts.jsonl",
            "".join(_canonical(item.to_dict()) + "\n" for item in alerts))

    def write_failures(self, failures):
        self._unique(failures, lambda item: item.failure_id, "failure")
        _atomic_write(
            self.directory / "failures.jsonl",
            "".join(_canonical(asdict(item)) + "\n" for item in failures))

    def write_reports(self, reports):
        self._unique(
            reports, lambda item: item.report_fingerprint or item.incident_id, "report")
        output = self.directory / "reports"
        output.mkdir(exist_ok=True)
        expected = {
            f"{item.report_fingerprint or item.incident_id}.json": _canonical(item.to_dict())
            for item in reports
        }
        unexpected = sorted({item.name for item in output.glob("*.json")} - set(expected))
        if unexpected:
            raise ReplayOutputError(f"output contains reports outside ledger: {unexpected}")
        for name, payload in expected.items():
            path = output / name
            if path.exists() and path.read_text(encoding="utf-8") != payload:
                raise ReplayOutputError(f"report content conflict: {name}")
            _atomic_write(path, payload)
        _fsync_directory(output)

    def write_manifest(self, *, dataset, config_hash, code_version, windows,
                       alerts, reports, failures, runtime_summary):
        stable = {
            "dataset_manifest_hash": dataset.manifest_sha256,
            "config_hash": config_hash, "code_version": code_version,
            "alerts": [item.alert_id for item in alerts],
            "reports": [item.report_fingerprint for item in reports],
            "failures": [item.failure_fingerprint for item in failures],
        }
        fingerprint = _sha(stable)
        result = ReplayRunManifest(
            "1.0", _sha([dataset.dataset_id, fingerprint]), dataset.dataset_id,
            dataset.dataset_version, dataset.manifest_sha256, config_hash, code_version,
            dataset.start_ns, dataset.end_ns, windows, len(alerts),
            sum(item.state == "hard" for item in alerts), len(reports), len(failures),
            [item.report_fingerprint or item.incident_id for item in reports],
            [item.failure_id for item in failures], fingerprint, runtime_summary)
        _atomic_write(
            self.directory / "run_manifest.json", _canonical(asdict(result)))
        return result
