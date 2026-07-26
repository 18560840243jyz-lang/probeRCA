"""Adapters that end at the data-plane contract and never execute RCA logic."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from .archive import CollectionArchive, CollectionArchiveWriter
from .contracts import CollectedWindow, assert_label_safe


def from_engine_window(
    window: Any, *, collection_metadata: dict[str, Any] | None = None,
) -> CollectedWindow:
    """Convert an orchestration window without importing an algorithmic type."""
    required = (
        "window_start_ns", "window_end_ns", "node_metric_records",
        "edge_metric_records", "topology_snapshot_events",
        "evidence_observations_available_by_cutoff", "source_record_ids",
        "replay_sequence_number",
    )
    missing = [name for name in required if not hasattr(window, name)]
    if missing:
        raise TypeError("engine window fields missing: " + ",".join(missing))
    metadata = dict(collection_metadata or {})
    reorder_issues = getattr(window, "reorder_issues", ())
    if reorder_issues:
        metadata["reorder_issues"] = list(reorder_issues)
    assert_label_safe(metadata)
    return CollectedWindow.create(
        sequence=window.replay_sequence_number,
        window_start_ns=window.window_start_ns,
        window_end_ns=window.window_end_ns,
        node_metrics=window.node_metric_records,
        edge_metrics=window.edge_metric_records,
        topology_events=window.topology_snapshot_events,
        burst_evidence=window.evidence_observations_available_by_cutoff,
        source_record_ids=window.source_record_ids,
        collection_metadata=metadata,
    )


def seal_engine_windows(
    windows: Iterable[Any],
    output: str | Path,
    *,
    dataset_id: str,
    collection_contract: dict[str, Any],
    source_description: str,
    collection_metadata: dict[str, Any] | None = None,
) -> CollectionArchive:
    """Finish collection and publish a sealed archive; no control code is called."""
    writer = CollectionArchiveWriter(
        output,
        dataset_id=dataset_id,
        collection_contract=collection_contract,
        source_description=source_description,
        collection_metadata=collection_metadata,
    )
    for window in windows:
        writer.append(from_engine_window(window))
    return writer.seal()
