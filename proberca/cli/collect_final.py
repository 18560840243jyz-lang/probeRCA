"""Collect and seal final-scheme windows without running RCA."""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import yaml

from proberca.dataplane.archive import CollectionArchiveWriter
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
    parser.add_argument("--windows", type=int, required=True)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    source_payload = _mapping(args.source_config)
    contract = _mapping(args.collection_contract)
    source_config = FinalLiveCollectorConfig.from_dict(source_payload)
    primitive_source = PrometheusPrimitiveSource(source_config.prometheus)
    runner = FinalLiveCollectionRunner(
        config=source_config,
        collection_contract=contract,
        primitive_source=primitive_source,
    )
    started_at_ns = time.time_ns()
    dataset_id = fingerprint({
        "cluster_id": source_config.cluster_id,
        "source_config_fingerprint": source_config.public_fingerprint,
        "collection_contract_fingerprint": fingerprint(contract),
        "requested_window_count": args.windows,
        "started_at_ns": started_at_ns,
    })
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
    print(canonical_json({
        "dataset_id": archive.dataset_id,
        "manifest_fingerprint": archive.manifest_fingerprint,
        "output": str(archive.root),
        "phase": "collection_sealed",
        "window_count": archive.window_count,
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
