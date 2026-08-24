"""Merge three disjoint worker archives into one formal 156-record dataset."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any, Iterable

from proberca.dataplane.archive import CollectionArchive, CollectionArchiveWriter
from proberca.dataplane.burst_archive import (
    BurstArchive,
    BurstArchiveWriter,
    RawBurstWindow,
)
from proberca.dataplane.contracts import CollectedWindow, fingerprint


class MultiNodeMergeError(RuntimeError):
    pass


def _aligned(values: Iterable[Any], attributes: tuple[str, ...]) -> None:
    records = tuple(values)
    for name in attributes:
        if len({getattr(item, name) for item in records}) != 1:
            raise MultiNodeMergeError(f"worker archives disagree on {name}")


def _unique_metrics(windows: tuple[CollectedWindow, ...], field: str) -> tuple:
    result = {}
    for window in windows:
        for record in getattr(window, field):
            key = (record.record_type, record.stable_id)
            if key in result:
                raise MultiNodeMergeError(
                    f"metric has more than one worker owner: {record.stable_id}"
                )
            result[key] = record
    return tuple(result[key] for key in sorted(result))


def _topology(
    windows: tuple[CollectedWindow, ...],
    residual_source_record_ids: tuple[str, ...],
) -> tuple:
    nonempty = [window.topology_events for window in windows if window.topology_events]
    if not nonempty:
        return ()
    if len(nonempty) != len(windows) or any(len(values) != 1 for values in nonempty):
        raise MultiNodeMergeError(
            "each worker window must contain one topology snapshot"
        )
    snapshots = tuple(values[0] for values in nonempty)
    semantic = []
    for item in snapshots:
        payload = item.to_dict()
        for field_name in (
            "snapshot_id", "inventory_revision_id",
            "call_edge_provider_fingerprint", "resource_version_vector",
        ):
            payload.pop(field_name)
        semantic.append(payload)
    if len({repr(item) for item in semantic}) != 1:
        raise MultiNodeMergeError(
            "worker global topology/runtime snapshots disagree"
        )
    first = snapshots[0]
    resource_keys = set(first.resource_version_vector)
    if any(set(item.resource_version_vector) != resource_keys for item in snapshots):
        raise MultiNodeMergeError(
            "worker topology snapshots have different resource kinds"
        )
    merged = replace(
        first,
        inventory_revision_id=fingerprint(sorted(
            item.inventory_revision_id for item in snapshots
        )),
        call_edge_provider_fingerprint=fingerprint({
            "edges": [item.to_dict() for item in first.call_edges],
            "raw_source_ids": list(residual_source_record_ids),
        }),
        resource_version_vector={
            key: fingerprint(sorted(
                item.resource_version_vector[key] for item in snapshots
            ))
            for key in sorted(resource_keys)
        },
    )
    return (merged,)


def merge_worker_archives(
    *,
    normal_roots: list[Path],
    burst_roots: list[Path],
    normal_output: Path,
    burst_output: Path,
    expected_service_count: int = 11,
    expected_host_count: int = 3,
    expected_tcp_edge_count: int = 15,
) -> dict[str, Any]:
    if len(normal_roots) != 3 or len(burst_roots) != 3:
        raise MultiNodeMergeError("exactly three worker archive pairs are required")
    normal = tuple(CollectionArchive.load(path) for path in normal_roots)
    burst = tuple(BurstArchive.load(path) for path in burst_roots)
    projections = tuple(item.projection for item in normal)
    if any(item is None for item in projections):
        raise MultiNodeMergeError(
            "worker Normal archives must declare their local projections"
        )
    if len({item["owner"] for item in projections}) != 3 \
            or len({item["node_name"] for item in projections}) != 3:
        raise MultiNodeMergeError(
            "worker projection owners and nodes must be unique"
        )
    formal_services = {
        tuple(item["formal_service_entity_ids"]) for item in projections
    }
    topology_edges = {
        tuple(item["topology_tcp_edge_entity_ids"]) for item in projections
    }
    if len(formal_services) != 1 or len(topology_edges) != 1:
        raise MultiNodeMergeError("worker formal projection scopes disagree")
    service_owners = [
        service for item in projections for service in item["service_entity_ids"]
    ]
    edge_owners = [
        edge for item in projections for edge in item["tcp_edge_entity_ids"]
    ]
    if (
        len(service_owners) != len(set(service_owners))
        or set(service_owners) != set(next(iter(formal_services)))
        or len(edge_owners) != len(set(edge_owners))
        or set(edge_owners) != set(next(iter(topology_edges)))
    ):
        raise MultiNodeMergeError(
            "formal service/TCP coordinates do not have exactly one worker owner"
        )
    _aligned(normal, (
        "dataset_id", "cluster_id", "window_count", "start_ns", "end_ns",
        "collection_contract_fingerprint",
    ))
    _aligned(burst, (
        "dataset_id", "cluster_id", "window_count", "start_ns", "end_ns",
        "burst_config_fingerprint",
    ))
    if normal[0].dataset_id != burst[0].dataset_id:
        raise MultiNodeMergeError("Normal/Burst Dataset IDs differ")
    if normal[0].window_count != burst[0].window_count:
        raise MultiNodeMergeError("Normal/Burst worker counts differ")
    contract = normal[0].collection_contract
    metadata_values = tuple(item.collection_metadata for item in normal)
    if any(item.collection_contract != contract for item in normal[1:]):
        raise MultiNodeMergeError("worker collection contracts differ")
    for field_name in (
        "aggregation_config_fingerprint", "burst_config_fingerprint",
    ):
        if len({item[field_name] for item in metadata_values}) != 1:
            raise MultiNodeMergeError(
                f"worker collector metadata disagree on {field_name}"
            )
    metadata = {
        "collector_build_fingerprint": fingerprint(sorted(
            item["collector_build_fingerprint"] for item in metadata_values
        )),
        "aggregation_config_fingerprint": metadata_values[0][
            "aggregation_config_fingerprint"
        ],
        "burst_config_fingerprint": metadata_values[0][
            "burst_config_fingerprint"
        ],
    }
    combined_burst_source = fingerprint(sorted(
        item.event_source_fingerprint for item in burst
    ))
    normal_writer = CollectionArchiveWriter(
        normal_output, dataset_id=normal[0].dataset_id,
        collection_contract=contract,
        source_description=normal[0].source_description,
        collection_metadata=metadata,
    )
    burst_writer = BurstArchiveWriter(
        burst_output, dataset_id=normal[0].dataset_id,
        cluster_id=normal[0].cluster_id,
        event_source_fingerprint=combined_burst_source,
        burst_config_fingerprint=burst[0].burst_config_fingerprint,
    )
    normal_iterators = [iter(item.iter_windows()) for item in normal]
    burst_iterators = [iter(item.iter_windows()) for item in burst]
    try:
        for _index in range(normal[0].window_count):
            normal_windows = tuple(next(iterator) for iterator in normal_iterators)
            burst_windows = tuple(next(iterator) for iterator in burst_iterators)
            _aligned(normal_windows, (
                "sequence", "window_start_ns", "window_end_ns", "cluster_id",
            ))
            _aligned(burst_windows, (
                "sequence", "window_start_ns", "window_end_ns", "cluster_id",
            ))
            if (
                normal_windows[0].sequence != burst_windows[0].sequence
                or normal_windows[0].window_start_ns != burst_windows[0].window_start_ns
                or normal_windows[0].window_end_ns != burst_windows[0].window_end_ns
            ):
                raise MultiNodeMergeError("Normal/Burst worker boundaries differ")
            node_metrics = _unique_metrics(normal_windows, "node_metrics")
            edge_metrics = _unique_metrics(normal_windows, "edge_metrics")
            service_entities = {
                (item.namespace, item.service_name)
                for item in node_metrics if item.scope != "node"
            }
            host_entities = {
                item.node_name for item in node_metrics if item.scope == "node"
            }
            edge_entities = {
                (item.namespace, item.src_service, item.dst_service, item.protocol)
                for item in edge_metrics
            }
            if (
                len(service_entities) != expected_service_count
                or len(host_entities) != expected_host_count
                or len(edge_entities) != expected_tcp_edge_count
                or len(node_metrics) != expected_service_count * 9 + expected_host_count * 4
                or len(edge_metrics) != expected_tcp_edge_count * 3
            ):
                raise MultiNodeMergeError("merged window violates formal 11/3/15 9/4/3 scope")
            if any(item.protocol != "tcp" for item in edge_metrics):
                raise MultiNodeMergeError("merged formal archive contains non-TCP edge metrics")
            residual = tuple(sorted({
                source for window in normal_windows
                for source in window.residual_source_record_ids
            }))
            merged_normal = CollectedWindow.create(
                sequence=normal_windows[0].sequence,
                window_start_ns=normal_windows[0].window_start_ns,
                window_end_ns=normal_windows[0].window_end_ns,
                node_metrics=node_metrics, edge_metrics=edge_metrics,
                topology_events=_topology(normal_windows, residual),
                burst_evidence=(),
                residual_source_record_ids=residual,
                collection_metadata=metadata,
            )
            samples = tuple(
                sample for window in burst_windows for sample in window.samples
            )
            merged_burst = RawBurstWindow.create(
                sequence=burst_windows[0].sequence,
                window_start_ns=burst_windows[0].window_start_ns,
                window_end_ns=burst_windows[0].window_end_ns,
                cluster_id=burst_windows[0].cluster_id,
                samples=samples,
                event_source_fingerprint=combined_burst_source,
                burst_config_fingerprint=burst[0].burst_config_fingerprint,
                event_loss_rate=max(item.event_loss_rate for item in burst_windows),
            )
            normal_writer.append(merged_normal)
            burst_writer.append(merged_burst)
    except Exception:
        normal_writer.close_partial()
        burst_writer.close_partial()
        raise
    merged_normal_archive = normal_writer.seal()
    merged_burst_archive = burst_writer.seal()
    return {
        "dataset_id": merged_normal_archive.dataset_id,
        "window_count": merged_normal_archive.window_count,
        "normal_manifest_fingerprint": merged_normal_archive.manifest_fingerprint,
        "burst_manifest_fingerprint": merged_burst_archive.manifest_fingerprint,
        "formal_records_per_window": (
            expected_service_count * 9 + expected_host_count * 4
            + expected_tcp_edge_count * 3
        ),
        "dns_formal_records": 0,
    }
