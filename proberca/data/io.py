"""JSONL file IO helpers for probeRCA P0 records."""

from __future__ import annotations

import json
from dataclasses import fields
from pathlib import Path
from typing import Iterable, Any, Iterator

import pyarrow as pa
import pyarrow.parquet as pq

from proberca.data.schema import STRICT_RECORD_TYPES, StrictRecord, to_dict

PARQUET_FORMAT_VERSION = "2"


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


def _record_envelope(record: StrictRecord) -> dict[str, Any]:
    if not isinstance(record, StrictRecord):
        raise TypeError("typed serialization requires a StrictRecord")
    record_type = getattr(record, "record_type", None)
    if record_type not in STRICT_RECORD_TYPES or STRICT_RECORD_TYPES[record_type] is not type(record):
        raise TypeError("typed serialization requires a registered top-level record")
    record_payload = record.to_dict()
    if record_payload["record_type"] != record_type:
        raise ValueError("record_type conflicts with concrete record class")
    return {"record_type": record_type, "record": record_payload}


def _record_from_envelope(payload: dict[str, Any]) -> StrictRecord:
    if not isinstance(payload, dict) or set(payload) != {"record_type", "record"}:
        raise ValueError("record envelope must contain exactly record_type and record")
    record_type = payload["record_type"]
    if record_type not in STRICT_RECORD_TYPES:
        raise ValueError(f"unknown record_type {record_type!r}")
    record_payload = payload["record"]
    if not isinstance(record_payload, dict) or record_payload.get("record_type") != record_type:
        raise ValueError("envelope record_type conflicts with record payload")
    return STRICT_RECORD_TYPES[record_type].from_dict(record_payload)


def write_record_json(path: str | Path, record: StrictRecord) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(_record_envelope(record), ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )


def read_record_json(path: str | Path) -> StrictRecord:
    return _record_from_envelope(json.loads(Path(path).read_text(encoding="utf-8")))


def write_records_jsonl(path: str | Path, records: Iterable[StrictRecord]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(
                json.dumps(_record_envelope(record), ensure_ascii=False, sort_keys=True) + "\n"
            )


def read_records_jsonl(path: str | Path) -> list[StrictRecord]:
    input_path = Path(path)
    if not input_path.exists():
        raise FileNotFoundError(f"JSONL file not found: {input_path}")
    records: list[StrictRecord] = []
    with input_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                records.append(_record_from_envelope(json.loads(line)))
            except (json.JSONDecodeError, TypeError, ValueError) as exc:
                raise ValueError(f"invalid JSONL record at line {line_number}: {exc}") from exc
    return records


def iter_records_jsonl(path: str | Path) -> Iterator[StrictRecord]:
    """Stream strict record envelopes from JSONL without materializing the file."""
    input_path = Path(path)
    if not input_path.exists():
        raise FileNotFoundError(f"JSONL file not found: {input_path}")
    with input_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                yield _record_from_envelope(json.loads(line))
            except (json.JSONDecodeError, TypeError, ValueError) as exc:
                raise ValueError(f"invalid JSONL record at line {line_number}: {exc}") from exc


def write_records_parquet(path: str | Path, records: Iterable[StrictRecord]) -> None:
    values = list(records)
    if not values:
        raise ValueError("Parquet record batch must not be empty")
    serialized: list[tuple[str, dict[str, Any]]] = []
    field_names: set[str] = set()
    composite_fields: set[str] = set()
    for record in values:
        envelope = _record_envelope(record)
        payload = envelope["record"]
        field_names.update(set(payload) - {"record_type"})
        composite_fields.update(
            name for name, value in payload.items()
            if name != "record_type" and isinstance(value, (list, dict))
        )
        serialized.append((envelope["record_type"], payload))

    ordered_fields = sorted(field_names)
    rows = []
    for record_type, payload in serialized:
        row = {"record_type": record_type}
        for name in ordered_fields:
            value = payload.get(name)
            if name in composite_fields and value is not None:
                value = json.dumps(value, ensure_ascii=False, sort_keys=True)
            row[name] = value
        rows.append(row)
    table = pa.Table.from_pylist(rows)
    metadata = dict(table.schema.metadata or {})
    metadata.update(
        {
            b"proberca_parquet_format": PARQUET_FORMAT_VERSION.encode("ascii"),
            b"proberca_fields": json.dumps(ordered_fields).encode("utf-8"),
            b"proberca_composite_fields": json.dumps(sorted(composite_fields)).encode("utf-8"),
        }
    )
    table = table.replace_schema_metadata(metadata)
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, output)


def read_records_parquet(path: str | Path) -> list[StrictRecord]:
    table = pq.read_table(Path(path))
    metadata = table.schema.metadata or {}
    if metadata.get(b"proberca_parquet_format") != PARQUET_FORMAT_VERSION.encode("ascii"):
        raise ValueError("missing or incompatible ProbeRCA Parquet format metadata")
    expected_fields = json.loads(metadata[b"proberca_fields"].decode("utf-8"))
    composite_fields = set(
        json.loads(metadata[b"proberca_composite_fields"].decode("utf-8"))
    )
    if set(table.column_names) != {"record_type", *expected_fields}:
        raise ValueError("Parquet columns do not match declared ProbeRCA fields")
    records: list[StrictRecord] = []
    for row in table.to_pylist():
        record_type = row.pop("record_type")
        if record_type not in STRICT_RECORD_TYPES:
            raise ValueError(f"unknown record_type {record_type!r}")
        record_fields = {field.name for field in fields(STRICT_RECORD_TYPES[record_type])}
        payload = {"record_type": record_type}
        payload.update({name: row[name] for name in record_fields - {"record_type"}})
        for name in (record_fields - {"record_type"}) & composite_fields:
            if payload[name] is not None:
                payload[name] = json.loads(payload[name])
        records.append(STRICT_RECORD_TYPES[record_type].from_dict(payload))
    return records


def iter_records_parquet(path: str | Path, *, batch_size: int = 1024) -> Iterator[StrictRecord]:
    """Stream strict records from ProbeRCA Parquet record batches."""
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("Parquet batch_size must be a positive integer")
    parquet = pq.ParquetFile(Path(path))
    metadata = parquet.schema_arrow.metadata or {}
    if metadata.get(b"proberca_parquet_format") != PARQUET_FORMAT_VERSION.encode("ascii"):
        raise ValueError("missing or incompatible ProbeRCA Parquet format metadata")
    expected_fields = json.loads(metadata[b"proberca_fields"].decode("utf-8"))
    composite_fields = set(json.loads(metadata[b"proberca_composite_fields"].decode("utf-8")))
    if set(parquet.schema_arrow.names) != {"record_type", *expected_fields}:
        raise ValueError("Parquet columns do not match declared ProbeRCA fields")
    for batch in parquet.iter_batches(batch_size=batch_size):
        for row in batch.to_pylist():
            record_type = row.pop("record_type")
            if record_type not in STRICT_RECORD_TYPES:
                raise ValueError(f"unknown record_type {record_type!r}")
            record_fields = {item.name for item in fields(STRICT_RECORD_TYPES[record_type])}
            payload = {"record_type": record_type}
            payload.update({name: row[name] for name in record_fields - {"record_type"}})
            for name in (record_fields - {"record_type"}) & composite_fields:
                if payload[name] is not None:
                    payload[name] = json.loads(payload[name])
            yield STRICT_RECORD_TYPES[record_type].from_dict(payload)
