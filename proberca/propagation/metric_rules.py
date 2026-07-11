"""Configuration-driven structural mask for metric propagation."""

from __future__ import annotations

from dataclasses import dataclass

from proberca.config import MetricParentRule, PropagationConfig
from proberca.data.schema import CandidateSubgraph, NodeAnomalyRecord


@dataclass(frozen=True, order=True)
class MetricFeatureKey:
    parent_node_id: str
    lag: int
    relation_types: list[str]
    relation_ids: list[str]
    rule_ids: list[str]


def _service_id(node_id: str) -> str:
    parts = node_id.split("::")
    if len(parts) != 4:
        raise ValueError(f"invalid cluster-aware node_id {node_id!r}")
    return "::".join(parts[:3])


class MetricParentRuleRegistry:
    """Build masked parent features only from exact rules and candidate relations."""

    def __init__(self, config: PropagationConfig):
        if not isinstance(config, PropagationConfig):
            raise TypeError("config must be PropagationConfig")
        if not config.metric_parent_rules:
            raise ValueError("production metric_parent_rules must not be empty")
        self.config = config

    @staticmethod
    def _relation_ids(candidate: CandidateSubgraph, relation_type: str,
                      parent_service: str, target_service: str) -> list[str]:
        if relation_type == "self_history":
            return [f"{target_service}::self_history"] if parent_service == target_service else []
        if relation_type == "same_service":
            return [f"{target_service}::same_service"] if parent_service == target_service else []
        field = {"impact": "impact_edges", "host": "host_relations",
                 "resource": "resource_relations"}[relation_type]
        output = []
        for relation in getattr(candidate, field):
            src, dst = relation["src_service_id"], relation["dst_service_id"]
            matched = src == parent_service and dst == target_service
            if relation_type in {"host", "resource"}:
                matched = matched or (dst == parent_service and src == target_service)
            if matched:
                output.append(relation["relation_id"])
        return sorted(output)

    def build(self, candidate: CandidateSubgraph,
              anomaly_index: dict[str, NodeAnomalyRecord]) -> dict[str, list[MetricFeatureKey]]:
        if not isinstance(candidate, CandidateSubgraph):
            raise TypeError("candidate must be CandidateSubgraph")
        candidate_nodes = sorted(node_id for node_id in candidate.candidate_node_ids
                                 if node_id in anomaly_index)
        if any(not isinstance(anomaly_index[node_id], NodeAnomalyRecord) for node_id in candidate_nodes):
            raise TypeError("candidate metric index must contain NodeAnomalyRecord")
        merged: dict[tuple[str, str, int], dict[str, set[str]]] = {}
        for target_id in candidate_nodes:
            target = anomaly_index[target_id]
            target_service = _service_id(target_id)
            for parent_id in candidate_nodes:
                parent = anomaly_index[parent_id]
                parent_service = _service_id(parent_id)
                for rule in self.config.metric_parent_rules:
                    if not rule.enabled or target.metric_family != rule.target_family \
                            or parent.metric_family != rule.parent_family:
                        continue
                    if rule.target_metric_names is not None and target.metric_name not in rule.target_metric_names:
                        continue
                    if rule.parent_metric_names is not None and parent.metric_name not in rule.parent_metric_names:
                        continue
                    if rule.require_signal_spec and (not target.signal_spec_id or not parent.signal_spec_id):
                        continue
                    if rule.relation_type == "self_history" and parent_id != target_id:
                        continue
                    relation_ids = self._relation_ids(candidate, rule.relation_type,
                                                      parent_service, target_service)
                    if not relation_ids:
                        continue
                    for lag in rule.lags:
                        key = (target_id, parent_id, lag)
                        entry = merged.setdefault(key, {"relations": set(), "ids": set(), "rules": set()})
                        entry["relations"].add(rule.relation_type)
                        entry["ids"].update(relation_ids)
                        entry["rules"].add(rule.rule_id)
        output: dict[str, list[MetricFeatureKey]] = {}
        for (target, parent, lag), entry in sorted(merged.items()):
            output.setdefault(target, []).append(MetricFeatureKey(
                parent, lag, sorted(entry["relations"]), sorted(entry["ids"]),
                sorted(entry["rules"]),
            ))
        return output
