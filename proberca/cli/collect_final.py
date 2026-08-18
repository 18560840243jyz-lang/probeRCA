"""Collect and seal final-scheme windows without running RCA."""

from __future__ import annotations

import argparse
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
    return parser


def _write_aligned_windows(
    *,
    runner,
    normal_writer: CollectionArchiveWriter,
    burst_writer: BurstArchiveWriter,
    window_count: int,
) -> None:
    try:
        if normal_writer.dataset_id != burst_writer.dataset_id:
            raise ValueError("Normal and Burst Dataset IDs differ")
        for normal_window, burst_window in runner.iter_collect_aligned(
            window_count
        ):
            normal_writer.append(normal_window)
            burst_writer.append(burst_window)
    except Exception:
        normal_writer.close_partial()
        burst_writer.close_partial()
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
    source_payload = _mapping(args.source_config)
    contract = _mapping(args.collection_contract)
    burst_payload = _mapping(args.burst_config)
    source_config = FinalLiveCollectorConfig.from_dict(source_payload)
    burst_config = load_final_live_burst_config(burst_payload)
    if burst_config.cluster_id != source_config.cluster_id:
        raise ValueError("normal and Burst cluster identities differ")
    primitive_source = PrometheusPrimitiveSource(source_config.prometheus)
    started_at_ns = time.time_ns()
    dataset_id = fingerprint({
        "cluster_id": source_config.cluster_id,
        "source_config_fingerprint": source_config.public_fingerprint,
        "burst_source_config_fingerprint": burst_config.public_fingerprint,
        "collection_contract_fingerprint": fingerprint(contract),
        "requested_window_count": args.windows,
        "started_at_ns": started_at_ns,
    })
    burst_source = FinalLiveBurstSource(
        burst_config,
        burst_config_fingerprint=contract["burst_config_fingerprint"],
        formal_tcp_edge_entity_ids=(
            source_config.formal_tcp_edge_entity_ids
        ),
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
    writer = CollectionArchiveWriter(
        args.output,
        dataset_id=dataset_id,
        collection_contract=contract,
        source_description=contract["source_description"],
        collection_metadata=metadata,
    )
    _write_aligned_windows(
        runner=runner,
        normal_writer=writer,
        burst_writer=burst_writer,
        window_count=args.windows,
    )
    archive = writer.seal()
    burst_archive = burst_writer.seal()
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
        "window_count": archive.window_count,
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
