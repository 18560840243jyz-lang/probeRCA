"""Deterministic worker-agent substitute for pre-rental protocol rehearsal.

This backend is deliberately marked simulated.  Its reports exercise the same
orchestrator contract but are rejected by ``freeze_injector_registry``.
"""

from __future__ import annotations

import copy
from typing import Any

from .model import fingerprint


class FakeWorkerAgent:
    def __init__(self) -> None:
        self._targets: dict[tuple[str, str], dict[str, Any]] = {}

    def invoke(
        self, node_id: str, action: str, payload: dict[str, Any],
    ) -> dict[str, Any]:
        target = payload["target"]
        key = (node_id, target["entity_id"])
        record = self._targets.setdefault(key, {
            "runtime_identity_fingerprint": target["runtime_identity_fingerprint"],
            "state": {"fault": None, "generation": 1},
            "profile": None,
        })
        if record["runtime_identity_fingerprint"] != \
                target["runtime_identity_fingerprint"]:
            raise RuntimeError("fake target runtime identity changed")
        if action == "snapshot":
            return {
                "runtime_identity_fingerprint": record["runtime_identity_fingerprint"],
                "state_fingerprint": fingerprint(record["state"]),
            }
        if action == "apply":
            if record["state"]["fault"] is not None:
                raise RuntimeError("fake target already has an active mutation")
            record["state"]["fault"] = payload["profile_id"]
            record["profile"] = copy.deepcopy(payload)
            return {"applied": True}
        if action == "cleanup":
            record["state"]["fault"] = None
            record["profile"] = None
            return {"cleaned": True}
        if action == "measure":
            profile = record["profile"]
            active = profile is not None
            criterion = profile.get("effectiveness_criterion", {}) if profile else {}
            if not criterion:
                criterion = payload.get("effectiveness_criterion", {})
            metric = criterion.get("metric") or payload.get("metric")
            conditions = {
                key: True for key, value in criterion.items()
                if key.endswith("_required") and bool(value)
            }
            counters = {
                key[:-10]: (10 if active else 0)
                for key in criterion if key.endswith("_delta_min")
            }
            contamination_names = payload.get("contamination_checks", ())
            return {
                "metric": metric,
                "primary_value": 10.0 if active else 1.0,
                "valid_window_count": (
                    int(criterion.get("active_valid_windows_min", 0))
                    if active else 0
                ),
                "positive_window_count": (
                    int(criterion.get("active_positive_windows_min", 0))
                    if active else 0
                ),
                "conditions": conditions,
                "counters": counters,
                "contamination": {name: False for name in contamination_names},
            }
        if action == "preflight":
            return {
                "node_id": node_id,
                "cgroup_v2": True,
                "btf": True,
                "clock_synchronized": True,
                "simulated": True,
            }
        raise RuntimeError("fake worker received unsupported action")
