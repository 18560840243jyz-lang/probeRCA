"""One-way Test protocol: freeze, predict, seal, then reveal and score."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .model import fingerprint


class EvaluationProtocolError(RuntimeError):
    pass


def _write_new(path: Path, value: Any) -> None:
    if path.exists():
        raise EvaluationProtocolError(f"refusing to overwrite {path.name}")
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )
    os.replace(temporary, path)


class BlindEvaluationProtocol:
    schema_version = "probeRCA-test-evaluation-protocol-v1"

    def __init__(self, root: Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.state_path = self.root / "test-protocol-state.json"
        if self.state_path.exists():
            self.state = json.loads(self.state_path.read_text(encoding="utf-8"))
        else:
            self.state = {
                "schema_version": self.schema_version,
                "stage": "CREATED",
            }
            self._save()

    def _save(self) -> None:
        temporary = self.state_path.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps(self.state, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, self.state_path)

    def freeze(self, *, git_sha: str, config_fingerprint: str) -> None:
        if self.state["stage"] != "CREATED":
            raise EvaluationProtocolError("Test freeze can occur only once")
        if len(git_sha) != 40 or len(config_fingerprint) != 64:
            raise EvaluationProtocolError("Git SHA and config fingerprint are required")
        self.state.update({
            "stage": "FROZEN", "git_sha": git_sha,
            "config_fingerprint": config_fingerprint,
        })
        self._save()

    def seal_predictions(self, predictions: list[dict[str, Any]]) -> str:
        if self.state["stage"] != "FROZEN":
            raise EvaluationProtocolError("freeze code/config before Test inference")
        if not predictions:
            raise EvaluationProtocolError("Test predictions cannot be empty")
        ids = [item.get("case_id") for item in predictions]
        if any(not value for value in ids) or len(ids) != len(set(ids)):
            raise EvaluationProtocolError("Test predictions require unique opaque case IDs")
        payload = {
            "schema_version": "probeRCA-sealed-test-predictions-v1",
            "git_sha": self.state["git_sha"],
            "config_fingerprint": self.state["config_fingerprint"],
            "predictions": predictions,
        }
        prediction_fingerprint = fingerprint(payload)
        payload["prediction_fingerprint"] = prediction_fingerprint
        _write_new(self.root / "sealed-test-predictions.json", payload)
        self.state.update({
            "stage": "PREDICTIONS_SEALED",
            "prediction_fingerprint": prediction_fingerprint,
        })
        self._save()
        return prediction_fingerprint

    def attest_labels_revealed(self, *, label_manifest_sha256: str) -> None:
        if self.state["stage"] != "PREDICTIONS_SEALED":
            raise EvaluationProtocolError("labels cannot be revealed before predictions are sealed")
        if len(label_manifest_sha256) != 64:
            raise EvaluationProtocolError("revealed label manifest SHA-256 is required")
        self.state.update({
            "stage": "LABELS_REVEALED",
            "label_manifest_sha256": label_manifest_sha256,
        })
        self._save()

    def record_score(self, score_payload: dict[str, Any]) -> None:
        if self.state["stage"] != "LABELS_REVEALED":
            raise EvaluationProtocolError("Test scoring requires sealed predictions and revealed labels")
        _write_new(self.root / "test-score.json", score_payload)
        self.state["stage"] = "SCORED"
        self._save()
