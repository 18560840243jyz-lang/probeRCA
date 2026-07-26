"""Collect and seal final-scheme windows without running RCA."""

from __future__ import annotations

import argparse
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
            "frozen 9/4/3/3 aggregation, and seal a collection archive. "
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
        raw_burst_sink=burst_writer,
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
    for window in runner.collect(args.windows):
        writer.append(window)
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
        "window_count": archive.window_count,
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
