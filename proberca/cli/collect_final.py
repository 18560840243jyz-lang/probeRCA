"""Collect and seal final-scheme windows without running RCA."""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import yaml

from proberca.dataplane.archive import CollectionArchiveWriter
from proberca.dataplane.burst_archive import BurstArchiveWriter
from proberca.dataplane.burst_live import (
    FinalLiveBurstSource,
    load_final_live_burst_config,
)
from proberca.dataplane.collector import (
    FinalLiveCollectionRunner,
    FinalLiveCollectorConfig,
)
from proberca.dataplane.contracts import canonical_json, fingerprint
from proberca.dataplane.sources import PrometheusPrimitiveSource
from proberca.dataplane.raw_archive import RawPrimitiveArchiveWriter


def _mapping(path: Path) -> dict:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a mapping")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Collect Kubernetes/Prometheus raw primitives, perform only the "
            "frozen 9/4/3 aggregation, and seal aligned archives. "
            "No alerting or RCA algorithm is imported or executed."
        ),
    )
    parser.add_argument("--source-config", type=Path, required=True)
    parser.add_argument("--collection-contract", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--burst-config", type=Path, required=True)
    parser.add_argument("--burst-output", type=Path, required=True)
    parser.add_argument("--windows", type=int, required=True)
    parser.add_argument(
        "--raw-primitives-output", type=Path,
        help="write raw cumulative counter boundaries for offline reaggregation",
    )
    parser.add_argument(
        "--raw-events-output", type=Path,
        help="preserve filtered formal eBPF events and loss checkpoints",
    )
    parser.add_argument(
        "--dataset-id",
        help="precommitted SHA-256 shared by all distributed worker collectors",
    )
    parser.add_argument(
        "--first-window-start-ns", type=int,
        help="shared, future, epoch-aligned first boundary for distributed capture",
    )
    parser.add_argument(
        "--capture-complete-marker", type=Path,
        help="atomically record completion of exact boundary capture",
    )
    parser.add_argument(
        "--phase-boundary-window", type=int,
        help="emit one exact boundary marker after this completed window",
    )
    parser.add_argument(
        "--phase-boundary-marker", type=Path,
        help="atomic marker written at --phase-boundary-window",
    )

    return parser


def _write_capture_complete_marker(
    marker: Path, final_target_ns: int,
) -> None:
    marker.parent.mkdir(parents=True, exist_ok=True)
    temporary = marker.with_name(
        f".{marker.name}.{os.getpid()}.tmp"
    )
    temporary.write_text(canonical_json({
        "final_target_ns": final_target_ns,
        "phase": "capture_complete",
        "timestamp_ns": time.time_ns(),
    }) + "\n", encoding="utf-8")
    os.replace(temporary, marker)


def _write_phase_boundary_marker(
    marker: Path, sequence: int, target_ns: int,
) -> None:
    marker.parent.mkdir(parents=True, exist_ok=True)
    temporary = marker.with_name(
        f".{marker.name}.{os.getpid()}.tmp"
    )
    temporary.write_text(canonical_json({
        "boundary_sequence": sequence,
        "boundary_target_ns": target_ns,
        "phase": "phase_boundary",
        "timestamp_ns": time.time_ns(),
    }) + "\n", encoding="utf-8")
    os.replace(temporary, marker)


def _write_aligned_windows(
    *,
    runner,
    normal_writer: CollectionArchiveWriter,
    burst_writer: BurstArchiveWriter,
    window_count: int,
    capture_complete_marker: Path | None = None,
    phase_boundary_window: int | None = None,
    phase_boundary_marker: Path | None = None,
    first_window_start_ns: int | None = None,
    raw_writer: RawPrimitiveArchiveWriter | None = None,
) -> None:
    try:
        if normal_writer.dataset_id != burst_writer.dataset_id:
            raise ValueError("Normal and Burst Dataset IDs differ")
        iterator_kwargs = {}
        if capture_complete_marker is not None:
            iterator_kwargs["capture_complete_callback"] = (
                lambda final_target_ns: _write_capture_complete_marker(
                    capture_complete_marker, final_target_ns,
                )
            )
        if phase_boundary_window is not None:
            if phase_boundary_marker is None:
                raise ValueError("phase boundary marker path is required")
            iterator_kwargs["boundary_callback"] = (
                lambda sequence, target_ns: (
                    _write_phase_boundary_marker(
                        phase_boundary_marker, sequence, target_ns,
                    )
                    if sequence == phase_boundary_window else None
                )
                )
        if first_window_start_ns is not None:
            iterator_kwargs["first_window_start_ns"] = first_window_start_ns
        pending_raw = []
        if raw_writer is not None:
            iterator_kwargs["raw_window_callback"] = pending_raw.append
        for normal_window, burst_window in runner.iter_collect_aligned(
            window_count, **iterator_kwargs,
        ):
            if raw_writer is not None:
                if len(pending_raw) != 1:
                    raise ValueError("runner did not provide exactly one aligned raw window")
                raw_writer.append(pending_raw.pop())
            normal_writer.append(normal_window)
            burst_writer.append(burst_window)
    except Exception:
        normal_writer.close_partial()
        burst_writer.close_partial()
        if raw_writer is not None:
            raw_writer.close_partial()
        aligned = (
            normal_writer.window_count == burst_writer.window_count
        )
        print(canonical_json({
            "burst_partial_window_count": burst_writer.window_count,
            "last_committed_sequence": (
                normal_writer.last_committed_sequence
                if aligned else None
            ),
            "normal_partial_window_count": normal_writer.window_count,
            "partial_archives_aligned": aligned,
            "phase": "collection_failed",
        }), file=sys.stderr)
        raise
    if (
        normal_writer.window_count != window_count
        or burst_writer.window_count != window_count
        or normal_writer.last_committed_sequence
        != burst_writer.last_committed_sequence
    ):
        normal_writer.close_partial()
        burst_writer.close_partial()
        raise ValueError("Normal/Burst completed window counts differ")


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    if (args.phase_boundary_window is None) \
            != (args.phase_boundary_marker is None):
        raise ValueError(
            "phase boundary window and marker must be configured together"
        )
    if args.phase_boundary_window is not None and not (
        0 < args.phase_boundary_window < args.windows
    ):
        raise ValueError(
            "phase boundary window must be inside the collection interval"
        )
    source_payload = _mapping(args.source_config)
    contract = _mapping(args.collection_contract)
    burst_payload = _mapping(args.burst_config)
    source_config = FinalLiveCollectorConfig.from_dict(source_payload)
    burst_config = load_final_live_burst_config(burst_payload)
    if burst_config.cluster_id != source_config.cluster_id:
        raise ValueError("normal and Burst cluster identities differ")
    primitive_source = PrometheusPrimitiveSource(source_config.prometheus)
    started_at_ns = time.time_ns()
    generated_dataset_id = fingerprint({
        "cluster_id": source_config.cluster_id,
        "source_config_fingerprint": source_config.public_fingerprint,
        "burst_source_config_fingerprint": burst_config.public_fingerprint,
        "collection_contract_fingerprint": fingerprint(contract),
        "requested_window_count": args.windows,
        "started_at_ns": started_at_ns,
    })
    dataset_id = args.dataset_id or generated_dataset_id
    if (
        not isinstance(dataset_id, str) or len(dataset_id) != 64
        or any(character not in "0123456789abcdef" for character in dataset_id)
    ):
        raise ValueError("dataset ID must be a lowercase SHA-256")
    if (args.dataset_id is None) != (args.first_window_start_ns is None):
        raise ValueError(
            "distributed collection requires both dataset ID and first boundary"
        )
    burst_source = FinalLiveBurstSource(
        burst_config,
        burst_config_fingerprint=contract["burst_config_fingerprint"],
        formal_tcp_edge_entity_ids=(
            source_config.formal_tcp_edge_entity_ids
        ),
        strict_formal_tcp_edge_scope=source_config.is_worker_projection,
    )
    burst_writer = BurstArchiveWriter(
        args.burst_output,
        dataset_id=dataset_id,
        cluster_id=source_config.cluster_id,
        event_source_fingerprint=burst_source.event_source_fingerprint,
        burst_config_fingerprint=contract["burst_config_fingerprint"],
    )
    runner = FinalLiveCollectionRunner(
        config=source_config,
        collection_contract=contract,
        primitive_source=primitive_source,
        raw_burst_source=burst_source,
    )
    metadata = {
        "collector_build_fingerprint": (
            runner.assembler.collector_build_id
        ),
        "aggregation_config_fingerprint": contract[
            "aggregation_config_fingerprint"
        ],
        "burst_config_fingerprint": contract[
            "burst_config_fingerprint"
        ],
    }
    projection = None
    if source_config.is_worker_projection:
        projection = {
            "schema_version": "probeRCA-worker-projection-v1",
            "owner": source_config.projection_owner,
            "node_name": source_config.projection_node_name,
            "service_entity_ids": sorted(
                source_config.projection_service_entity_ids
            ),
            "tcp_edge_entity_ids": sorted(
                source_config.formal_tcp_edge_entity_ids
            ),
            "formal_service_entity_ids": sorted(
                source_config.formal_service_entity_ids
            ),
            "topology_tcp_edge_entity_ids": sorted(
                source_config.topology_tcp_edge_entity_ids
            ),
        }
        projection["projection_fingerprint"] = fingerprint(projection)
    writer = CollectionArchiveWriter(
        args.output,
        dataset_id=dataset_id,
        collection_contract=contract,
        source_description=contract["source_description"],
        collection_metadata=metadata,
        projection=projection,
    )
    if args.raw_events_output is not None:
        burst_source.configure_capture_audit(args.raw_events_output)
    raw_writer = (
        RawPrimitiveArchiveWriter(
            args.raw_primitives_output,
            dataset_id=dataset_id,
            source_fingerprint=source_config.public_fingerprint,
        )
        if args.raw_primitives_output is not None else None
    )
    try:
        _write_aligned_windows(
            runner=runner,
            normal_writer=writer,
            burst_writer=burst_writer,
            window_count=args.windows,
            capture_complete_marker=args.capture_complete_marker,
            phase_boundary_window=args.phase_boundary_window,
            phase_boundary_marker=args.phase_boundary_marker,
            first_window_start_ns=args.first_window_start_ns,
            raw_writer=raw_writer,
        )
    finally:
        burst_source.finalize_capture_audit()
    archive = writer.seal()
    burst_archive = burst_writer.seal()
    raw_archive = raw_writer.seal() if raw_writer is not None else None
    print(canonical_json({
        "burst_manifest_fingerprint": (
            burst_archive.manifest_fingerprint
        ),
        "burst_output": str(burst_archive.root),
        "dataset_id": archive.dataset_id,
        "manifest_fingerprint": archive.manifest_fingerprint,
        "output": str(archive.root),
        "phase": "collection_sealed",
        "prometheus_range_query_stats": (
            primitive_source.last_range_query_stats
        ),
        "raw_primitives_manifest_fingerprint": (
            raw_archive["manifest_fingerprint"] if raw_archive else None
        ),
        "window_count": archive.window_count,
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
