"""Seal already collected windows without importing or running RCA algorithms."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import yaml

from proberca.dataplane import CollectionArchiveWriter, CollectedWindow
from proberca.dataplane.contracts import canonical_json


def _mapping(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    value = yaml.safe_load(text)
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a mapping")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Seal ProbeRCA data-plane windows; no RCA algorithm is executed.",
    )
    parser.add_argument("--windows-jsonl", type=Path, required=True)
    parser.add_argument("--collection-contract", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--source-description", required=True)
    parser.add_argument("--collection-metadata", type=Path)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    contract = _mapping(args.collection_contract)
    metadata = (
        _mapping(args.collection_metadata) if args.collection_metadata else {}
    )
    writer = CollectionArchiveWriter(
        args.output,
        dataset_id=args.dataset_id,
        collection_contract=contract,
        source_description=args.source_description,
        collection_metadata=metadata,
    )
    with args.windows_jsonl.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                raise ValueError(f"blank collected window line {line_number}")
            try:
                writer.append(CollectedWindow.from_dict(json.loads(line)))
            except Exception as error:
                raise ValueError(
                    f"invalid collected window line {line_number}: {error}"
                ) from error
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
