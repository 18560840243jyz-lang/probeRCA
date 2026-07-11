"""Configuration-driven exogenous edge-shock dictionary."""

from __future__ import annotations

import math

from scipy import sparse

from .contracts import (
    ShockProjectionError,
    ShockTemplateConflictError,
    ShockVariableRef,
)


def _parse_shock_id(shock_id: str):
    if "::shock::" not in shock_id:
        raise ShockProjectionError(f"invalid shock_id={shock_id}")
    physical_id, metric_name = shock_id.split("::shock::", 1)
    edge_metric_id = f"{physical_id}::{metric_name}"
    prefix, protocol = physical_id.rsplit("::", 1)
    cluster_namespace, pair = prefix.rsplit("::", 1)
    src, dst = pair.split("->", 1)
    cluster, namespace = cluster_namespace.split("::", 1)
    return physical_id, edge_metric_id, cluster, namespace, src, dst, protocol, metric_name


def _match_template(templates, metric_name, protocol):
    matches = [item for item in templates if item.enabled and item.edge_metric_name == metric_name
               and item.protocol in {None, protocol}]
    if len(matches) > 1:
        raise ShockTemplateConflictError(
            f"multiple shock templates match metric={metric_name} protocol={protocol}"
        )
    return matches[0] if matches else None


def build_shock_dictionary(candidate, node_ids, edge_ids, node_index, edge_index, templates):
    physical = {item["physical_edge_id"]: item for item in candidate.physical_edges}
    node_row = {node_id: index for index, node_id in enumerate(node_ids)}
    edge_row = {edge_id: len(node_ids) + index for index, edge_id in enumerate(edge_ids)}
    rows, columns, data, refs = [], [], [], []
    for shock_id in sorted(candidate.candidate_shock_ids):
        physical_id, edge_metric_id, cluster, namespace, src, dst, protocol, metric_name = _parse_shock_id(shock_id)
        if physical_id not in physical or edge_metric_id not in edge_row or edge_metric_id not in edge_index:
            raise ShockProjectionError(f"shock={shock_id} lacks a physical edge or current edge row")
        edge = physical[physical_id]
        if (
            edge.get("src_service_id") != f"{cluster}::{namespace}::{src}"
            or edge.get("dst_service_id") != f"{cluster}::{namespace}::{dst}"
            or edge.get("protocol") != protocol
        ):
            raise ShockProjectionError(f"shock={shock_id} conflicts with physical edge")
        template = _match_template(templates, metric_name, protocol)
        if template is None:
            raise ShockProjectionError(
                f"candidate shock={shock_id} has no exact shock projection template"
            )
        raw = {}
        for projection in template.projections:
            expected_service = src if projection.endpoint_role == "source" else dst
            for node_id in node_ids:
                record = node_index[node_id]
                if (
                    record.cluster_id == cluster and record.namespace == namespace
                    and record.service_name == expected_service
                    and record.metric_family == projection.metric_family
                    and (projection.metric_names is None or record.metric_name in projection.metric_names)
                ):
                    raw[node_id] = raw.get(node_id, 0.0) + projection.raw_weight
        if not raw:
            raise ShockProjectionError(f"shock={shock_id} has no candidate node projection")
        norm = math.sqrt(sum(value * value for value in raw.values()))
        projected = sorted(raw)
        weights = [raw[node_id] / norm for node_id in projected]
        column = len(refs)
        for node_id, weight in zip(projected, weights):
            rows.append(node_row[node_id]); columns.append(column); data.append(weight)
        rows.append(edge_row[edge_metric_id]); columns.append(column); data.append(1.0)
        refs.append(ShockVariableRef(
            column, shock_id, edge_metric_id, physical_id,
            edge["src_service_id"], edge["dst_service_id"], protocol, metric_name,
            template.template_id, edge_row[edge_metric_id],
            [node_row[node_id] for node_id in projected], weights,
            edge_index[edge_metric_id].source_metric_record_id,
        ))
    matrix = sparse.csc_matrix(
        (data, (rows, columns)), shape=(len(node_ids) + len(edge_ids), len(refs)), dtype=float,
    )
    return matrix, refs
