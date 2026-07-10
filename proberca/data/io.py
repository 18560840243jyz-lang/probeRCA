"""JSONL file IO helpers for probeRCA P0 records."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Any

from proberca.data.schema import to_dict


def write_jsonl(path: str | Path, records: Iterable[Any]) -> None:
    """Write dataclass or dictionary records to a UTF-8 JSONL file."""

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(to_dict(record), ensure_ascii=False) + "\n")


def read_jsonl(path: str | Path) -> list[dict]:
    """Read a UTF-8 JSONL file into a list of dictionaries."""

    input_path = Path(path)
    if not input_path.exists():
        raise FileNotFoundError(f"JSONL file not found: {input_path}")

    rows: list[dict] = []
    with input_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows
