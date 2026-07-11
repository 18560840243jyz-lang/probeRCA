"""Deterministic Healthy/Soft/Hard/Recovery state machine for P2 scores."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path

from proberca.baseline import AnomalyScore, StateScores
from proberca.config import AlertStateConfig, CompositeAlertRule
from proberca.data.schema import AlertEvent, PROBERCA_SCHEMA_VERSION

ALERT_STATE_VERSION = "1"


@dataclass(frozen=True)
class UpdateGate:
    update_node_baselines: bool
    update_edge_baselines: bool
    frozen_node_ids: list[str]
    frozen_edge_ids: list[str]
    update_service_model: bool
    prepare_metric_model: bool
    freeze_metric_model: bool
    request_burst: bool
    request_rca: bool
    baseline_ready: bool


@dataclass(frozen=True)
class StateMachineResult:
    state: str
    events: list[AlertEvent]
    gate: UpdateGate
    scores: StateScores


class AlertStateMachine:
    """Stateful alert transitions with independent isolated edge anomalies."""

    def __init__(self, config: AlertStateConfig, window_sec: int,
                 composite_rules: list[CompositeAlertRule] | None = None):
        if not isinstance(config, AlertStateConfig):
            raise TypeError("config must be AlertStateConfig")
        if isinstance(window_sec, bool) or not isinstance(window_sec, int) or window_sec <= 0:
            raise ValueError("window_sec must be a positive integer")
        rules = list(composite_rules or [])
        if any(not isinstance(rule, CompositeAlertRule) for rule in rules):
            raise TypeError("composite_rules must contain CompositeAlertRule")
        if len({rule.rule_id for rule in rules}) != len(rules):
            raise ValueError("duplicate composite rule_id")
        self.config = config
        self.window_sec = window_sec
        self.rules = rules
        self.state = "healthy"
        self._service_soft: dict[str, int] = {}
        self._service_hard: dict[str, int] = {}
        self._edge_soft: dict[str, int] = {}
        self._edge_hard: dict[str, int] = {}
        self._rule_counts: dict[str, int] = {}
        self._recovery_count = 0
        self._cooldown_remaining = 0
        self._trigger_services: list[str] = []
        self._trigger_edges: list[str] = []
        self._isolated_edges: list[str] = []
        self._last_event_signature: tuple | None = None

    @staticmethod
    def _advance(counters: dict[str, int], values: dict[str, float], threshold: float) -> None:
        for key in set(counters) | set(values):
            counters[key] = counters.get(key, 0) + 1 if values.get(key, 0.0) > threshold else 0

    def _edge_has_business_impact(self, edge_id: str, scores: StateScores) -> bool:
        parts = edge_id.split("::")
        if len(parts) != 4 or "->" not in parts[2]:
            raise ValueError(f"invalid edge state ID {edge_id!r}")
        src, dst = parts[2].split("->", 1)
        for service in (src, dst):
            service_id = f"{parts[0]}::{parts[1]}::{service}"
            state = scores.services.get(service_id)
            if state and state.family_scores.get("request", 0.0) > self.config.edge_business_impact_threshold:
                return True
        return False

    def _evaluate_composites(self, metrics: list[AnomalyScore]):
        soft_services: set[str] = set()
        soft_edges: set[str] = set()
        hard_services: set[str] = set()
        hard_edges: set[str] = set()
        reason_code = None
        for rule in self.rules:
            grouped: dict[str, dict[str, float]] = {}
            for score in metrics:
                target_id = score.service_id if rule.target == "same_service" else score.edge_id
                if target_id is not None:
                    grouped.setdefault(target_id, {})[score.stable_id] = score.anomaly
            matched_targets = []
            selectors = rule.all_of or rule.any_of
            for target_id, observed in grouped.items():
                checks = [observed.get(selector, float("-inf")) > rule.threshold for selector in selectors]
                if (all(checks) if rule.all_of else any(checks)):
                    matched_targets.append(target_id)
            active_keys = {f"{rule.rule_id}::{target_id}" for target_id in matched_targets}
            known_keys = {key for key in self._rule_counts if key.partition("::")[0] == rule.rule_id}
            for key in known_keys | active_keys:
                self._rule_counts[key] = self._rule_counts.get(key, 0) + 1 if key in active_keys else 0
            triggered = [target_id for target_id in matched_targets
                         if self._rule_counts[f"{rule.rule_id}::{target_id}"] >= rule.consecutive_windows]
            if triggered:
                if rule.resulting_level == "hard":
                    (hard_services if rule.target == "same_service" else hard_edges).update(triggered)
                    reason_code = "composite_hard"
                else:
                    (soft_services if rule.target == "same_service" else soft_edges).update(triggered)
                    reason_code = "composite_soft"
        return soft_services, soft_edges, hard_services, hard_edges, reason_code

    def step(self, timestamp_ns: int, scores: StateScores, metric_scores: list[AnomalyScore],
             baseline_ready: bool) -> StateMachineResult:
        if isinstance(timestamp_ns, bool) or not isinstance(timestamp_ns, int) or timestamp_ns < 0:
            raise ValueError("timestamp_ns must be a non-negative integer")
        if not isinstance(scores, StateScores):
            raise TypeError("scores must be StateScores")
        if any(not isinstance(score, AnomalyScore) for score in metric_scores):
            raise TypeError("metric_scores must contain AnomalyScore")

        if not baseline_ready:
            self._service_soft.clear()
            self._service_hard.clear()
            self._edge_soft.clear()
            self._edge_hard.clear()
            self._rule_counts.clear()
            self._isolated_edges = []
            return StateMachineResult(self.state, [], self._gate(False, scores), scores)

        service_values = {key: state.score for key, state in scores.services.items() if state.score is not None}
        edge_values = {key: state.score for key, state in scores.edges.items()}
        self._advance(self._service_soft, service_values, self.config.soft_threshold)
        self._advance(self._service_hard, service_values, self.config.hard_threshold)
        self._advance(self._edge_soft, edge_values, self.config.soft_threshold)
        self._advance(self._edge_hard, edge_values, self.config.hard_threshold)

        soft_services = {key for key, count in self._service_soft.items() if count >= self.config.soft_consecutive_windows}
        hard_services = {key for key, count in self._service_hard.items() if count >= self.config.hard_consecutive_windows}
        edge_hard_candidates = {key for key, count in self._edge_hard.items() if count >= self.config.hard_consecutive_windows}
        impact_edges = {key for key in edge_hard_candidates if self._edge_has_business_impact(key, scores)}
        isolated_edges = edge_hard_candidates - impact_edges
        soft_edges = {key for key, count in self._edge_soft.items() if count >= self.config.soft_consecutive_windows} - isolated_edges

        direct_services = {score.service_id for score in metric_scores if score.direct_hard and score.service_id}
        direct_edges = {score.edge_id for score in metric_scores if score.direct_hard and score.edge_id}
        composite_soft_services, composite_soft_edges, composite_hard_services, composite_hard_edges, composite_code = self._evaluate_composites(metric_scores)
        soft_services |= composite_soft_services
        soft_edges |= composite_soft_edges
        hard_services |= direct_services | composite_hard_services
        hard_edges = impact_edges | direct_edges | composite_hard_edges
        direct_code = "direct_hard" if direct_services or direct_edges else (
            "composite_hard" if composite_hard_services or composite_hard_edges else None
        )

        previous_state = self.state
        previous_trigger_services = list(self._trigger_services)
        previous_trigger_edges = list(self._trigger_edges)
        reason_code: str | None = None
        trigger_services = sorted(hard_services or soft_services)
        trigger_edges = sorted(hard_edges or soft_edges)

        if self.state == "recovery":
            hard_services |= {key for key, value in service_values.items() if value > self.config.hard_threshold}
            hard_edges |= {
                key for key, value in edge_values.items()
                if value > self.config.hard_threshold and self._edge_has_business_impact(key, scores)
            }
            if not hard_services and not hard_edges:
                soft_services |= {key for key, value in service_values.items() if value > self.config.soft_threshold}
                soft_edges |= {key for key, value in edge_values.items() if value > self.config.soft_threshold}

        if hard_services or hard_edges:
            self.state = "hard"
            self._recovery_count = 0
            self._cooldown_remaining = 0
            reason_code = direct_code or "hard_threshold"
            self._trigger_services = sorted(hard_services)
            self._trigger_edges = sorted(hard_edges)
        elif self.state == "hard":
            if scores.global_anomaly < self.config.recovery_threshold:
                self._recovery_count += 1
            else:
                self._recovery_count = 0
            if self._recovery_count >= self.config.recovery_windows:
                self.state = "recovery"
                self._cooldown_remaining = math.ceil(self.config.recovery_cooldown_sec / self.window_sec)
                reason_code = "recovery_threshold"
        elif self.state == "recovery":
            if soft_services or soft_edges:
                self.state = "soft"
                reason_code = "recovery_realert_soft"
                self._trigger_services = sorted(soft_services)
                self._trigger_edges = sorted(soft_edges)
            elif self._cooldown_remaining > 0:
                self._cooldown_remaining -= 1
            if self._cooldown_remaining == 0 and scores.global_anomaly < self.config.healthy_threshold:
                self.state = "healthy"
                reason_code = "cooldown_complete"
                self._trigger_services = []
                self._trigger_edges = []
        elif soft_services or soft_edges:
            self.state = "soft"
            reason_code = "composite_soft" if composite_code == "composite_soft" else "soft_threshold"
            self._trigger_services = sorted(soft_services)
            self._trigger_edges = sorted(soft_edges)
            self._recovery_count = 0
        elif self.state == "soft":
            if scores.global_anomaly < self.config.healthy_threshold:
                self._recovery_count += 1
            else:
                self._recovery_count = 0
            if self._recovery_count >= self.config.recovery_windows:
                self.state = "healthy"
                reason_code = "soft_cleared"
                self._trigger_services = []
                self._trigger_edges = []

        events: list[AlertEvent] = []
        if self.state != previous_state or (
            self.state in {"soft", "hard"}
            and (self._trigger_services != previous_trigger_services or self._trigger_edges != previous_trigger_edges)
        ):
            if reason_code is None:
                reason_code = f"{self.state}_trigger_changed"
            events.append(self._event(timestamp_ns, self.state, reason_code, scores,
                                      self._trigger_services, self._trigger_edges))

        isolated_sorted = sorted(isolated_edges)
        if isolated_sorted != self._isolated_edges and isolated_sorted:
            events.append(self._event(timestamp_ns, "edge_anomaly", "isolated_edge_anomaly", scores, [], isolated_sorted))
        self._isolated_edges = isolated_sorted
        return StateMachineResult(self.state, events, self._gate(baseline_ready, scores), scores)

    def _event(self, timestamp_ns, state, code, scores, services, edges) -> AlertEvent:
        reason = json.dumps({"code": code, "detail": {"state": state}}, sort_keys=True, separators=(",", ":"))
        material = f"{timestamp_ns}|{state}|{','.join(services)}|{','.join(edges)}|{code}"
        alert_id = hashlib.sha256(material.encode("utf-8")).hexdigest()[:20]
        frozen_all = state in {"hard", "recovery"}
        return AlertEvent(
            schema_version=PROBERCA_SCHEMA_VERSION,
            alert_id=alert_id,
            timestamp_ns=timestamp_ns,
            state=state,
            trigger_services=list(services),
            trigger_edges=list(edges),
            service_scores={key: float(value.score) for key, value in scores.services.items() if value.score is not None},
            edge_scores={key: float(value.score) for key, value in scores.edges.items()},
            reason=reason,
            frozen_baseline=frozen_all,
            frozen_service_model=state in {"soft", "hard", "recovery"},
            frozen_metric_model=frozen_all,
        )

    def _gate(self, baseline_ready: bool, scores: StateScores) -> UpdateGate:
        if self.state in {"hard", "recovery"}:
            return UpdateGate(False, False, [], [], False, False, True,
                              self.state == "hard", self.state == "hard", baseline_ready)
        if self.state == "soft":
            return UpdateGate(True, True, list(self._trigger_services),
                              sorted(set(self._trigger_edges) | set(self._isolated_edges)),
                              False, True, False, False, False, baseline_ready)
        if self._isolated_edges:
            return UpdateGate(True, True, [], list(self._isolated_edges), baseline_ready,
                              False, False, False, False, baseline_ready)
        healthy_window = scores.global_anomaly < self.config.healthy_threshold
        return UpdateGate(healthy_window, healthy_window, [], [], baseline_ready and healthy_window,
                          False, False, False, False, baseline_ready)

    def to_dict(self) -> dict:
        return {
            "format_version": ALERT_STATE_VERSION,
            "schema_version": PROBERCA_SCHEMA_VERSION,
            "window_sec": self.window_sec,
            "config": asdict(self.config),
            "rules": [asdict(rule) for rule in self.rules],
            "state": self.state,
            "service_soft": self._service_soft,
            "service_hard": self._service_hard,
            "edge_soft": self._edge_soft,
            "edge_hard": self._edge_hard,
            "rule_counts": self._rule_counts,
            "recovery_count": self._recovery_count,
            "cooldown_remaining": self._cooldown_remaining,
            "trigger_services": self._trigger_services,
            "trigger_edges": self._trigger_edges,
            "isolated_edges": self._isolated_edges,
        }

    def save_json(self, path: str | Path) -> None:
        Path(path).write_text(json.dumps(self.to_dict(), sort_keys=True), encoding="utf-8")

    @classmethod
    def load_json(cls, path: str | Path, config: AlertStateConfig, window_sec: int,
                  composite_rules: list[CompositeAlertRule] | None = None) -> "AlertStateMachine":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        required = {"format_version", "schema_version", "window_sec", "config", "rules", "state", "service_soft",
                    "service_hard", "edge_soft", "edge_hard", "rule_counts", "recovery_count",
                    "cooldown_remaining", "trigger_services", "trigger_edges", "isolated_edges"}
        if set(payload) != required:
            raise ValueError("invalid alert snapshot fields")
        if payload["format_version"] != ALERT_STATE_VERSION or payload["schema_version"] != PROBERCA_SCHEMA_VERSION:
            raise ValueError("incompatible alert snapshot version")
        if payload["window_sec"] != window_sec or payload["config"] != asdict(config):
            raise ValueError("alert snapshot configuration mismatch")
        if payload["rules"] != [asdict(rule) for rule in (composite_rules or [])]:
            raise ValueError("alert snapshot composite rule mismatch")
        result = cls(config, window_sec, composite_rules)
        result.state = payload["state"]
        result._service_soft = {key: int(value) for key, value in payload["service_soft"].items()}
        result._service_hard = {key: int(value) for key, value in payload["service_hard"].items()}
        result._edge_soft = {key: int(value) for key, value in payload["edge_soft"].items()}
        result._edge_hard = {key: int(value) for key, value in payload["edge_hard"].items()}
        result._rule_counts = {key: int(value) for key, value in payload["rule_counts"].items()}
        result._recovery_count = int(payload["recovery_count"])
        result._cooldown_remaining = int(payload["cooldown_remaining"])
        result._trigger_services = list(payload["trigger_services"])
        result._trigger_edges = list(payload["trigger_edges"])
        result._isolated_edges = list(payload["isolated_edges"])
        return result
