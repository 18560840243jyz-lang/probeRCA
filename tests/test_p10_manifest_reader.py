from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

import test_p1_data_contracts as p1
from proberca.data.io import write_records_jsonl, write_records_parquet
from proberca.replay.manifest import (
    ReplayDatasetManifest, ReplayIntegrityError, ReplayManifestError,
)
from proberca.replay.reader import (
    ReplayOrderingError, ReplayRecordConflictError, ReplayRecordReader,
)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_dataset(root: Path, *, node_records=None, edge_records=None,
                  topology_records=None, labels=True) -> Path:
    root.mkdir()
    node_records = node_records or [p1.make_node(timestamp_ns=999_999_999, window_sec=1)]
    edge_records = edge_records or [p1.make_edge(timestamp_ns=999_999_999, window_sec=1)]
    topology_records = topology_records or [replace(
        p1.make_topology(), valid_from_ns=0, valid_to_ns=3_000_000_000)]
    write_records_parquet(root / "node.parquet", node_records)
    write_records_parquet(root / "edge.parquet", edge_records)
    write_records_jsonl(root / "topology.jsonl", topology_records)
    (root / "config.yaml").write_text("config: fixture\n", encoding="utf-8")
    if labels:
        write_records_jsonl(root / "labels.jsonl", [p1.make_label()])
    files = ["node.parquet", "edge.parquet", "topology.jsonl", "config.yaml"]
    if labels:
        files.append("labels.jsonl")
    payload = {
        "schema_version": "1.0", "dataset_id": "dataset-a", "dataset_version": "1",
        "cluster_id": "cluster-a", "namespaces": ["observability"],
        "start_ns": 0, "end_ns": 3_000_000_000, "window_sec": 1,
        "node_metrics_file": "node.parquet", "edge_metrics_file": "edge.parquet",
        "topology_file": "topology.jsonl", "evidence_file": None,
        "labels_file": "labels.jsonl" if labels else None, "config_file": "config.yaml",
        "file_sha256": {name: sha(root / name) for name in files},
        "expected_schema_versions": ["1.0"],
        "allowed_record_types": ["node_metric", "edge_metric", "topology_snapshot"],
        "evidence_semantics": "normalized_only", "source_description": "deterministic fixture",
        "created_at_ns": 1, "metadata": {"purpose": "reader-test"},
    }
    (root / "manifest.yaml").write_text(yaml.safe_dump(payload, sort_keys=True), encoding="utf-8")
    return root


def load(root: Path) -> ReplayDatasetManifest:
    return ReplayDatasetManifest.load(root)


def test_manifest_loads_and_validates_all_declared_hashes(tmp_path):
    root = build_dataset(tmp_path / "dataset")
    manifest = load(root)
    assert manifest.dataset_id == "dataset-a"
    assert manifest.dataset_root == root.resolve()
    assert manifest.manifest_sha256 == sha(root / "manifest.yaml")


@pytest.mark.parametrize("bad_path", ["../escape", "/tmp/escape", "sub/../../escape"])
def test_manifest_rejects_path_traversal_and_absolute_paths(tmp_path, bad_path):
    root = build_dataset(tmp_path / "dataset")
    payload = yaml.safe_load((root / "manifest.yaml").read_text())
    payload["node_metrics_file"] = bad_path
    (root / "manifest.yaml").write_text(yaml.safe_dump(payload))
    with pytest.raises(ReplayManifestError):
        load(root)


def test_manifest_rejects_missing_file(tmp_path):
    root = build_dataset(tmp_path / "dataset")
    (root / "edge.parquet").unlink()
    with pytest.raises(ReplayManifestError, match="missing"):
        load(root)


def test_manifest_rejects_hash_mismatch(tmp_path):
    root = build_dataset(tmp_path / "dataset")
    (root / "node.parquet").write_bytes(b"corrupt")
    with pytest.raises(ReplayIntegrityError, match="SHA-256"):
        load(root)


@pytest.mark.parametrize("truth_key", ["root_service", "root_metric", "fault_mode", "root_edge"])
def test_manifest_rejects_embedded_ground_truth(tmp_path, truth_key):
    root = build_dataset(tmp_path / "dataset")
    payload = yaml.safe_load((root / "manifest.yaml").read_text())
    payload["metadata"][truth_key] = "forbidden"
    (root / "manifest.yaml").write_text(yaml.safe_dump(payload))
    with pytest.raises(ReplayManifestError, match="ground truth"):
        load(root)


def test_reader_streams_parquet_and_jsonl_into_window_input(tmp_path):
    root = build_dataset(tmp_path / "dataset")
    windows = list(ReplayRecordReader(root, load(root), parquet_batch_size=1).iter_windows())
    assert len(windows) == 1
    window = windows[0]
    assert len(window.node_metric_records) == len(window.edge_metric_records) == 1
    assert window.topology_snapshot_events[0].record_type == "topology_snapshot"
    assert window.timestamp_ns == 1_000_000_000
    assert "incident_label" not in window.source_record_ids


def test_reader_coalesces_raw_event_timestamps_in_same_half_open_window(tmp_path):
    records = [p1.make_node(timestamp_ns=value, window_sec=1)
               for value in (10, 999_999_999)]
    root = build_dataset(tmp_path / "dataset", node_records=records)
    windows = list(ReplayRecordReader(root, load(root)).iter_windows())
    assert len(windows) == 1
    assert windows[0].window_start_ns == 0 and windows[0].window_end_ns == 1_000_000_000
    assert [item.timestamp_ns for item in windows[0].node_metric_records] == [10, 999_999_999]


def test_reader_strict_mode_rejects_file_internal_disorder(tmp_path):
    ordered = [p1.make_node(timestamp_ns=value, window_sec=1)
               for value in (1_500_000_000, 500_000_000)]
    root = build_dataset(tmp_path / "dataset", node_records=ordered)
    with pytest.raises(ReplayOrderingError):
        list(ReplayRecordReader(root, load(root), strict_order=True).iter_windows())


def test_reader_explicit_reorder_is_deterministic_and_records_issue(tmp_path):
    records = [p1.make_node(timestamp_ns=value, window_sec=1)
               for value in (1_500_000_000, 500_000_000)]
    root = build_dataset(tmp_path / "dataset", node_records=records)
    windows = list(ReplayRecordReader(
        root, load(root), strict_order=False, allow_explicit_reorder=True,
        parquet_batch_size=1,
    ).iter_windows())
    assert [item.timestamp_ns for item in windows] == [1_000_000_000, 2_000_000_000]
    assert all(item.reorder_issues for item in windows)


def test_duplicate_identical_record_is_idempotent(tmp_path):
    record = p1.make_node(timestamp_ns=999_999_999, window_sec=1)
    root = build_dataset(tmp_path / "dataset", node_records=[record, record])
    windows = list(ReplayRecordReader(root, load(root)).iter_windows())
    assert windows[0].node_metric_records == [record]


def test_duplicate_same_identity_different_value_fails(tmp_path):
    record = p1.make_node(timestamp_ns=999_999_999, window_sec=1)
    root = build_dataset(tmp_path / "dataset", node_records=[record, replace(record, value=99.0)])
    with pytest.raises(ReplayRecordConflictError):
        list(ReplayRecordReader(root, load(root)).iter_windows())


def test_reader_never_opens_labels_file(tmp_path, monkeypatch):
    root = build_dataset(tmp_path / "dataset")
    labels = (root / "labels.jsonl").resolve()
    original = Path.open

    def guarded(path, *args, **kwargs):
        if path.resolve() == labels:
            raise AssertionError("inference reader opened labels")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded)
    assert list(ReplayRecordReader(root, load(root)).iter_windows())


def test_reader_does_not_emit_empty_windows_or_fill_missing_values(tmp_path):
    records = [p1.make_node(timestamp_ns=value, window_sec=1)
               for value in (999_999_999, 2_999_999_999)]
    root = build_dataset(tmp_path / "dataset", node_records=records)
    windows = list(ReplayRecordReader(root, load(root)).iter_windows())
    assert [item.timestamp_ns for item in windows] == [1_000_000_000, 3_000_000_000]
    assert all(item.node_metric_records for item in windows)


def test_reader_rejects_record_schema_not_declared_by_manifest(tmp_path):
    root = build_dataset(tmp_path / "dataset")
    payload = yaml.safe_load((root / "manifest.yaml").read_text())
    payload["expected_schema_versions"] = ["9.9"]
    (root / "manifest.yaml").write_text(yaml.safe_dump(payload))
    with pytest.raises(ReplayIntegrityError, match="schema"):
        list(ReplayRecordReader(root, load(root)).iter_windows())


def test_reader_rejects_record_cluster_mismatch(tmp_path):
    root = build_dataset(tmp_path / "dataset")
    payload = yaml.safe_load((root / "manifest.yaml").read_text())
    payload["cluster_id"] = "cluster-b"
    (root / "manifest.yaml").write_text(yaml.safe_dump(payload))
    with pytest.raises(ReplayIntegrityError, match="cluster"):
        list(ReplayRecordReader(root, load(root)).iter_windows())


def test_reader_rejects_non_evidence_record_in_evidence_file(tmp_path):
    root = build_dataset(tmp_path / "dataset")
    write_records_parquet(root / "evidence.parquet", [
        p1.make_node(timestamp_ns=999_999_999, window_sec=1)])
    payload = yaml.safe_load((root / "manifest.yaml").read_text())
    payload["evidence_file"] = "evidence.parquet"
    payload["file_sha256"]["evidence.parquet"] = sha(root / "evidence.parquet")
    (root / "manifest.yaml").write_text(yaml.safe_dump(payload))
    with pytest.raises(ReplayIntegrityError, match="evidence"):
        list(ReplayRecordReader(root, load(root)).iter_windows())
