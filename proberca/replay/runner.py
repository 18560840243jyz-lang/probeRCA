"""Replay runner that delegates all algorithmic work to ProbeRCAEngine."""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict

from proberca.config import load_config_yaml
from proberca.orchestration import (
    ProbeRCAEngine, restore_engine_checkpoint, save_engine_checkpoint,
)
from proberca.orchestration.state import OutputLedger

from .manifest import ReplayDatasetManifest
from .output import ReplayOutputWriter
from .reader import ReplayRecordReader


class ReplayRunner:
    def __init__(self, dataset_root, output, *, config_path=None, strict_order=True,
                 allow_explicit_reorder=False, overwrite=False, engine=None,
                 resume_from=None, checkpoint_every_windows=0):
        self.manifest = ReplayDatasetManifest.load(dataset_root)
        path = config_path or self.manifest.resolve_data_path(self.manifest.config_file)
        self.config = load_config_yaml(path)
        if self.config.window_sec != self.manifest.window_sec:
            raise ValueError("Replay manifest window_sec conflicts with config")
        self.engine = engine or ProbeRCAEngine.from_config(self.config)
        self.resume_sequence = 0
        resume_ledger = None
        if resume_from:
            self.resume_sequence = restore_engine_checkpoint(
                self.engine, resume_from, manifest_hash=self.manifest.manifest_sha256)
            resume_ledger = getattr(self.engine, "_output_ledger", None)
            if not isinstance(resume_ledger, OutputLedger):
                raise ValueError("checkpoint does not contain an OutputLedger")
        self.writer = ReplayOutputWriter(
            output, overwrite=overwrite, resume_ledger=resume_ledger)
        self.reader = ReplayRecordReader(
            dataset_root, self.manifest, strict_order=strict_order,
            allow_explicit_reorder=allow_explicit_reorder,
            parquet_batch_size=self.config.replay.parquet_batch_size)
        if isinstance(checkpoint_every_windows, bool) or \
                not isinstance(checkpoint_every_windows, int) or checkpoint_every_windows < 0:
            raise ValueError("checkpoint_every_windows must be a non-negative integer")
        self.checkpoint_every_windows = checkpoint_every_windows

    def _refresh_ledger(self, sequence, run_manifest_payload=None):
        previous = getattr(self.engine, "_output_ledger", None)
        if run_manifest_payload is None and isinstance(previous, OutputLedger):
            run_manifest_payload = previous.run_manifest_payload
        self.engine._output_ledger = OutputLedger.create(
            alerts=self.engine._alerts, reports=self.engine._reports,
            failures=self.engine._failures, processed_window_count=sequence,
            last_processed_timestamp=self.engine._last_timestamp,
            pending_incident=self.engine.pending_incident,
            dataset_fingerprint=self.manifest.manifest_sha256,
            config_fingerprint=self.engine.config_fingerprint,
            run_manifest_payload=run_manifest_payload,
        )

    def run(self, *, stop_after_window=None):
        started = time.perf_counter()
        results = []
        last_sequence = self.resume_sequence
        for window in self.reader.iter_windows():
            if window.replay_sequence_number <= self.resume_sequence:
                continue
            result = self.engine.process_window(window)
            results.append(result)
            last_sequence = window.replay_sequence_number
            self._refresh_ledger(last_sequence)
            if self.checkpoint_every_windows and \
                    last_sequence % self.checkpoint_every_windows == 0:
                save_engine_checkpoint(
                    self.engine, self.writer.directory / "checkpoint",
                    manifest_hash=self.manifest.manifest_sha256,
                    replay_sequence=last_sequence)
            if stop_after_window is not None and len(results) >= stop_after_window:
                break

        alerts = list(self.engine._alerts)
        reports = list(self.engine._reports)
        failures = list(self.engine._failures)
        self.writer.write_alerts(alerts)
        self.writer.write_reports(reports)
        self.writer.write_failures(failures)
        config_hash = hashlib.sha256(json.dumps(
            self.config.to_dict(), sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        manifest = self.writer.write_manifest(
            dataset=self.manifest, config_hash=config_hash, code_version="p10.1-schema-2",
            windows=last_sequence, alerts=alerts, reports=reports, failures=failures,
            runtime_summary={"runtime_ms": (time.perf_counter() - started) * 1000.0})
        self._refresh_ledger(last_sequence, asdict(manifest))
        if self.checkpoint_every_windows and last_sequence and \
                last_sequence % self.checkpoint_every_windows == 0:
            save_engine_checkpoint(
                self.engine, self.writer.directory / "checkpoint",
                manifest_hash=self.manifest.manifest_sha256,
                replay_sequence=last_sequence)
        return manifest, results
