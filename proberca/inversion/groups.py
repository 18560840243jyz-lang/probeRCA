"""Deterministic non-overlapping sparse-group partitions for P8."""

from __future__ import annotations

import json
from dataclasses import dataclass

from proberca.inversion.contracts import JointInversionSystem


class GroupPartitionError(ValueError):
    """A variable group partition overlaps, omits, or mixes variable types."""


@dataclass(frozen=True)
class VariableGroup:
    variable_type: str
    group_key: str
    indices: list[int]

    def __post_init__(self) -> None:
        if self.variable_type not in {"node", "propagation", "shock"}:
            raise GroupPartitionError("invalid group variable_type")
        if not self.group_key or not self.indices or self.indices != sorted(set(self.indices)):
            raise GroupPartitionError("group key and unique sorted indices are required")


@dataclass(frozen=True)
class GroupPartitionResult:
    node_groups: list[VariableGroup]
    propagation_groups: list[VariableGroup]
    shock_groups: list[VariableGroup]


def _service(node_id: str) -> str:
    parts = node_id.split("::")
    if len(parts) != 4:
        raise GroupPartitionError(f"invalid node_id={node_id}")
    return "::".join(parts[:3])


def _groups(variable_type, mapping):
    return [
        VariableGroup(variable_type, key, sorted(indices))
        for key, indices in sorted(mapping.items())
    ]


def _validate_partition(groups, size, variable_type):
    indices = [index for group in groups for index in group.indices]
    if any(group.variable_type != variable_type for group in groups) \
            or sorted(indices) != list(range(size)) or len(indices) != len(set(indices)):
        raise GroupPartitionError(f"{variable_type} groups are not a complete non-overlapping partition")


def build_group_partitions(joint_system):
    if not isinstance(joint_system, JointInversionSystem):
        raise TypeError("joint_system must be JointInversionSystem")
    nodes = {}
    for item in joint_system.node_variable_refs:
        nodes.setdefault(_service(item.node_id), []).append(item.column_index)
    propagation = {}
    for item in joint_system.propagation_variable_refs:
        key = json.dumps({
            "parent_service_id": _service(item.parent_node_id),
            "target_service_id": _service(item.target_node_id),
            "relation_types": sorted(item.relation_types),
        }, sort_keys=True, separators=(",", ":"))
        propagation.setdefault(key, []).append(item.column_index)
    shocks = {}
    for item in joint_system.shock_variable_refs:
        shocks.setdefault(item.physical_edge_id, []).append(item.column_index)
    result = GroupPartitionResult(
        _groups("node", nodes), _groups("propagation", propagation), _groups("shock", shocks)
    )
    _validate_partition(result.node_groups, len(joint_system.node_variable_refs), "node")
    _validate_partition(result.propagation_groups, len(joint_system.propagation_variable_refs), "propagation")
    _validate_partition(result.shock_groups, len(joint_system.shock_variable_refs), "shock")
    return result
