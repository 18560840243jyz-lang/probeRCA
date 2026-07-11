"""Sparse propagation dictionary from P5 structural lag contributions."""

from __future__ import annotations

import hashlib
import json

from scipy import sparse

from .contracts import PropagationDictionaryError, PropagationVariableRef


def _fingerprint(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def build_propagation_dictionary(node_ids, edge_count, prediction_index, model_snapshot_id, config):
    allowed = set(config.allowed_relation_types)
    node_row = {node_id: index for index, node_id in enumerate(node_ids)}
    merged = {}
    for target_id in node_ids:
        prediction = prediction_index[target_id]
        for item in prediction.contributions:
            relation_types = sorted(set(item.relation_types) & allowed)
            if not relation_types:
                continue
            if item.parent_node_id not in node_row or item.target_node_id != target_id:
                raise PropagationDictionaryError(
                    f"model={model_snapshot_id} target={target_id} has an out-of-candidate feature"
                )
            key = (item.parent_node_id, item.target_node_id, item.lag)
            entry = merged.setdefault(key, {
                "coefficient": item.coefficient,
                "positive_support": item.positive_support,
                "parent_value": item.parent_value,
                "relation_types": set(), "relation_ids": set(), "rule_ids": set(),
            })
            if entry["parent_value"] != item.parent_value or entry["coefficient"] != item.coefficient:
                raise PropagationDictionaryError(f"duplicate structural feature conflicts: {key}")
            entry["relation_types"].update(relation_types)
            entry["relation_ids"].update(item.relation_ids)
            entry["rule_ids"].update(item.rule_ids)
    rows, columns, data, refs = [], [], [], []
    for column, (key, entry) in enumerate(sorted(merged.items())):
        parent_id, target_id, lag = key
        payload = {"parent_node_id": parent_id, "target_node_id": target_id, "lag": lag}
        value = float(entry["parent_value"])
        if value != 0.0:
            rows.append(node_row[target_id]); columns.append(column); data.append(value)
        refs.append(PropagationVariableRef(
            column, _fingerprint(payload), parent_id, target_id, lag,
            float(entry["coefficient"]), float(entry["positive_support"]),
            sorted(entry["relation_types"]), sorted(entry["relation_ids"]),
            sorted(entry["rule_ids"]), value, node_row[target_id], model_snapshot_id,
        ))
    matrix = sparse.csc_matrix(
        (data, (rows, columns)), shape=(len(node_ids) + edge_count, len(refs)), dtype=float,
    )
    return matrix, refs
