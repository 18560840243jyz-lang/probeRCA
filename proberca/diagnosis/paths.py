"""Read-only positive-support propagation path construction for P9 explanations."""

from __future__ import annotations

import hashlib
import json
import math

from .contracts import PropagationPath, PropagationPathError


def _id(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _path(root, target, nodes, steps, config, level="metric"):
    root_support = min(max(root.relative_delta_loss or 0.0, 0.0), 1.0)
    support = math.prod(step["support"] for step in steps)
    score = root_support * support * math.exp(-config.path_length_penalty * len(steps))
    payload = {"root": root.candidate_id, "target": target, "nodes": nodes, "steps": steps,
               "level": level}
    return PropagationPath(_id(payload), root.candidate_id, target, level, nodes, steps, score)


def build_propagation_paths(root, target_node_ids, metric_coefficients, service_coefficients,
                            joint_system, config):
    positive = [item for item in metric_coefficients if item.ready and item.positive_support > 0]
    maximum = max((item.positive_support for item in positive), default=0.0)
    adjacency = {}
    for item in positive:
        support = item.positive_support / maximum
        if support < config.minimum_path_edge_support:
            continue
        adjacency.setdefault(item.parent_node_id, []).append((item.target_node_id, {
            "step_type": "metric_propagation", "parent_node_id": item.parent_node_id,
            "target_node_id": item.target_node_id, "lag": item.lag,
            "coefficient": item.coefficient, "support": support,
            "relation_types": item.relation_types, "relation_ids": item.relation_ids,
            "rule_ids": item.rule_ids,
        }))
    for edges in adjacency.values():
        edges.sort(key=lambda value: (value[0], value[1]["lag"], value[1]["relation_ids"]))
    starts = []
    if root.variable_block == "node":
        starts.append((root.metadata["node_id"], [root.metadata["node_id"]], []))
    elif root.variable_block == "propagation":
        index = root.variable_ids.index(root.dominant_member_id)
        parent = root.metadata["member_parent_node_ids"][index]
        target = root.metadata["member_target_node_ids"][index]
        coefficient = root.metadata["member_coefficients"][index]
        support = max(coefficient, 0.0) / maximum if maximum > 0 else 1.0
        starts.append((target, [parent, target], [{
            "step_type": "propagation_root", "parent_node_id": parent,
            "target_node_id": target, "coefficient": coefficient,
            "support": min(max(support, 0.0), 1.0),
        }]))
    else:
        ref = next(item for item in joint_system.shock_variable_refs
                   if item.shock_id == root.dominant_member_id)
        for row, weight in sorted(zip(ref.projected_node_rows, ref.projection_weights)):
            node = joint_system.node_row_refs[row].object_id
            starts.append((node, [root.candidate_id, node], [{
                "step_type": "shock_projection", "shock_id": ref.shock_id,
                "target_node_id": node, "support": abs(weight),
                "projection_weight": weight,
            }]))
    targets = set(target_node_ids)
    paths = []

    def visit(current, nodes, steps):
        metric_nodes = [item for item in nodes if "::" in item]
        if current in targets:
            paths.append(_path(root, current, nodes, steps, config))
        if len(steps) >= config.max_path_length:
            return
        for following, step in adjacency.get(current, []):
            if following in metric_nodes:
                continue
            visit(following, [*nodes, following], [*steps, step])

    for start, nodes, steps in starts:
        if len(steps) <= config.max_path_length:
            visit(start, nodes, steps)
    unique = {item.path_id: item for item in paths}
    return sorted(unique.values(), key=lambda item: (-item.path_score, item.path_id))[:config.max_paths_per_root]
