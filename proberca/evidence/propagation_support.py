"""Healthy-topology stability and signed learned support for propagation variables."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from proberca.config import ProbeRCAConfig
from proberca.inversion.contracts import JointInversionSystem
from proberca.topology import TopologyStore, build_topology_graph


class PropagationSupportError(ValueError):
    """Propagation coefficients or training timestamps are unusable."""


class TopologySupportError(ValueError):
    """A required healthy-time topology snapshot cannot be validated."""


@dataclass(frozen=True)
class PropagationSupportResult:
    learned_support: np.ndarray
    topology_support: np.ndarray
    propagation_h: np.ndarray


def compute_propagation_support(joint_system, topology_store, training_timestamps, config):
    if not isinstance(joint_system, JointInversionSystem):
        raise TypeError("joint_system must be JointInversionSystem")
    if not isinstance(topology_store, TopologyStore) or not isinstance(config, ProbeRCAConfig):
        raise TypeError("propagation support requires TopologyStore and ProbeRCAConfig")
    if not isinstance(training_timestamps, dict):
        raise TypeError("training_timestamps must map target node IDs to real Ridge rows")
    positive = np.asarray([
        max(float(item.learned_coefficient), 0.0)
        for item in joint_system.propagation_variable_refs
    ], dtype=float)
    if not np.isfinite(positive).all():
        raise PropagationSupportError("learned propagation support is non-finite")
    maximum = float(positive.max()) if positive.size else 0.0
    learned = positive / maximum if maximum > 0 else np.zeros_like(positive)
    topology = np.zeros_like(learned)
    namespaces = sorted({item.node_id.split("::")[1] for item in joint_system.node_variable_refs})
    for item in joint_system.propagation_variable_refs:
        timestamps = training_timestamps.get(item.target_node_id)
        if not isinstance(timestamps, list) or not timestamps:
            raise PropagationSupportError(
                f"target={item.target_node_id} lacks actual healthy Ridge row timestamps"
            )
        if timestamps != sorted(set(timestamps)) or any(
            isinstance(value, bool) or not isinstance(value, int) or value >= joint_system.timestamp_ns
            for value in timestamps
        ):
            raise PropagationSupportError(
                f"target={item.target_node_id} training timestamps are invalid or include incident data"
            )
        present = 0
        for timestamp in timestamps:
            try:
                snapshot = topology_store.query(
                    joint_system.node_row_refs[0].object_id.split("::", 1)[0],
                    timestamp, namespaces,
                )
                graph = build_topology_graph(
                    snapshot, config.impact_derivation_rules,
                    config.candidate_graph.allow_cross_namespace,
                )
            except Exception as error:
                raise TopologySupportError(
                    f"target={item.target_node_id} timestamp={timestamp} topology unavailable: {error}"
                ) from error
            relation_ids = {
                relation.relation_id for relation in [
                    *graph.impact_edges, *graph.host_relations, *graph.resource_relations,
                ]
            }
            same_service = "same_service" in item.relation_types and item.parent_node_id.rsplit("::", 1)[0] \
                == item.target_node_id.rsplit("::", 1)[0] \
                and item.target_node_id.rsplit("::", 1)[0] in graph.service_ids
            if same_service or set(item.relation_ids) & relation_ids:
                present += 1
        topology[item.column_index] = present / len(timestamps)
    result = learned * topology
    if np.any(result < 0) or np.any(result > 1) or not np.isfinite(result).all():
        raise PropagationSupportError("propagation support must be finite in [0, 1]")
    return PropagationSupportResult(learned, topology, result)
