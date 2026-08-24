"""Crash-safe campaign progress without schedule mutation."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .model import fingerprint


class CampaignStateError(RuntimeError):
    pass


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


class CampaignState:
    schema_version = "probeRCA-multinode-campaign-state-v1"

    def __init__(self, path: Path, manifest_fingerprint: str, case_ids: list[str]):
        self.path = Path(path)
        self.manifest_fingerprint = manifest_fingerprint
        if len(case_ids) != len(set(case_ids)):
            raise CampaignStateError("campaign case IDs must be unique")
        self.case_ids = tuple(case_ids)
        if self.path.exists():
            self._value = json.loads(self.path.read_text(encoding="utf-8"))
            self._validate()
        else:
            self._value = {
                "schema_version": self.schema_version,
                "manifest_fingerprint": manifest_fingerprint,
                "ordered_case_ids": list(case_ids),
                "cases": {
                    case_id: {"status": "pending", "attempts": 0}
                    for case_id in case_ids
                },
            }
            self._write()

    def _validate(self) -> None:
        if self._value.get("schema_version") != self.schema_version:
            raise CampaignStateError("campaign state schema mismatch")
        if self._value.get("manifest_fingerprint") != self.manifest_fingerprint:
            raise CampaignStateError("campaign manifest changed; resume is forbidden")
        if tuple(self._value.get("ordered_case_ids", ())) != self.case_ids:
            raise CampaignStateError("campaign order changed; resume is forbidden")
        if set(self._value.get("cases", {})) != set(self.case_ids):
            raise CampaignStateError("campaign state case set mismatch")
        expected_fingerprint = fingerprint({
            key: item for key, item in self._value.items()
            if key != "state_fingerprint"
        })
        if self._value.get("state_fingerprint") != expected_fingerprint:
            raise CampaignStateError("campaign state fingerprint mismatch")
        valid_statuses = {"pending", "running", "failed", "complete"}
        if any(
            not isinstance(record, dict)
            or record.get("status") not in valid_statuses
            or not isinstance(record.get("attempts"), int)
            or record["attempts"] < 0
            for record in self._value["cases"].values()
        ):
            raise CampaignStateError("campaign state case record is invalid")

    def _write(self) -> None:
        value = dict(self._value)
        value["state_fingerprint"] = fingerprint({
            key: item for key, item in value.items() if key != "state_fingerprint"
        })
        self._value = value
        _atomic_json(self.path, value)

    def next_case_id(self) -> str | None:
        for case_id in self.case_ids:
            if self._value["cases"][case_id]["status"] != "complete":
                return case_id
        return None

    def start(self, case_id: str) -> int:
        if case_id != self.next_case_id():
            raise CampaignStateError("campaign cases must execute in frozen order")
        record = self._value["cases"][case_id]
        if record["status"] == "running":
            raise CampaignStateError("case is already running")
        record["status"] = "running"
        record["attempts"] += 1
        self._write()
        return int(record["attempts"])

    def finish(self, case_id: str, *, dataset_id: str, sha256: str) -> None:
        record = self._value["cases"].get(case_id)
        if not record or record["status"] != "running":
            raise CampaignStateError("only the running case can be completed")
        if not dataset_id or len(sha256) != 64:
            raise CampaignStateError("sealed dataset identity and SHA-256 are required")
        record.update({
            "status": "complete", "dataset_id": dataset_id, "sha256": sha256,
        })
        self._write()

    def fail(self, case_id: str, reason: str) -> None:
        record = self._value["cases"].get(case_id)
        if not record or record["status"] != "running":
            raise CampaignStateError("only the running case can fail")
        record.update({"status": "failed", "reason": str(reason)})
        self._write()
