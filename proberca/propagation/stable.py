"""Stable propagation learner for probeRCA P0 Step 4."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np

from proberca.data.io import read_jsonl, write_jsonl

REQUEST_METRICS = {
    "request.rps",
    "request.error_rate",
    "request.p50_latency_ms",
    "request.p95_latency_ms",
    "request.p99_latency_ms",
    "request.in_flight",
}

GRAPH_EDGE_FLAGS = {
    "call": "include_call_edges",
    "trace": "include_call_edges",
    "cohost": "include_cohost_edges",
    "resource": "include_resource_edges",
    "synthetic": "include_synthetic_edges",
}


@dataclass
class PropagationConfig:
    """Configuration for stable one-step propagation learning."""

    ridge_lambda: float = 1.0
    coefficient_threshold: float = 1e-10
    include_self_edges: bool = True
    include_same_service_edges: bool = True
    include_call_edges: bool = True
    include_cohost_edges: bool = True
    include_resource_edges: bool = True
    include_synthetic_edges: bool = True
    target_request_metrics_only_for_cross_service: bool = True


@dataclass
class PropagationCoefficient:
    """Learned parent-to-target stable propagation coefficient."""

    incident_id: str
    target: str
    parent: str
    coefficient: float
    edge_type: str


@dataclass
class PropagationResidualRecord:
    """Residual from stable propagation prediction for one service-metric node."""

    incident_id: str
    timestamp: float
    service: str
    metric: str
    z_value: float
    predicted_z: float
    residual: float
    source: str = "stable_propagation"


def load_required_dataset(input_dir: str | Path) -> tuple[list[dict], list[dict], list[dict]]:
    """Load normalized metrics, incident labels, and service graph edges."""

    input_path = Path(input_dir)
    required = {
        "normalized_metrics": input_path / "normalized_metrics.jsonl",
        "incidents": input_path / "incidents.jsonl",
        "service_graph": input_path / "service_graph.jsonl",
    }
    for name, path in required.items():
        if not path.exists():
            raise FileNotFoundError(f"missing required {name} file: {path}")
    return read_jsonl(required["normalized_metrics"]), read_jsonl(required["incidents"]), read_jsonl(required["service_graph"])


def _known_services_from_nodes(nodes: list[str]) -> list[str]:
    services = {node.split(".", 1)[0] for node in nodes}
    return sorted(services, key=len, reverse=True)


def extract_service(node_key: str, known_services: list[str]) -> str | None:
    """Extract a service name from a service or service.metric key using longest prefix match."""

    for service in sorted(known_services, key=len, reverse=True):
        if node_key == service:
            return service
        if node_key.startswith(service + "."):
            return service
        if node_key.startswith(service + "-"):
            return service
    return None


def _metric_from_node(node_key: str, known_services: list[str]) -> str | None:
    service = extract_service(node_key, known_services)
    if service is None:
        return None
    prefix = service + "."
    if node_key.startswith(prefix):
        return node_key[len(prefix):]
    return None


def _infer_window_step(timestamps: list[float]) -> float:
    ordered = sorted(set(float(timestamp) for timestamp in timestamps))
    diffs = [right - left for left, right in zip(ordered, ordered[1:]) if right > left]
    if not diffs:
        raise ValueError("cannot infer window step from fewer than two timestamps")
    return float(min(diffs))


def _contiguous_baseline_timestamps(normalized_records: list[dict], start_ts: float, window_step: float) -> set[float]:
    candidates = sorted(
        {
            float(record["timestamp"])
            for record in normalized_records
            if float(record["timestamp"]) < start_ts and record.get("incident_id") is None
        }
    )
    if not candidates:
        return set()

    selected = [candidates[-1]]
    previous = candidates[-1]
    tolerance = max(1e-9, abs(window_step) * 1e-6)
    for timestamp in reversed(candidates[:-1]):
        if abs((previous - timestamp) - window_step) <= tolerance:
            selected.append(timestamp)
            previous = timestamp
        else:
            break
    return set(selected)


def _records_for_incident(normalized_records: list[dict], incident: dict) -> list[dict]:
    incident_id = str(incident["incident_id"])
    start_ts = float(incident["start_ts"])
    end_ts = float(incident["end_ts"])
    window_step = _infer_window_step([float(record["timestamp"]) for record in normalized_records])
    baseline_timestamps = _contiguous_baseline_timestamps(normalized_records, start_ts, window_step)
    faulty_timestamps = {
        float(record["timestamp"])
        for record in normalized_records
        if start_ts <= float(record["timestamp"]) < end_ts and record.get("incident_id") == incident_id
    }
    selected_timestamps = baseline_timestamps | faulty_timestamps

    result: list[dict] = []
    for record in normalized_records:
        timestamp = float(record["timestamp"])
        if timestamp not in selected_timestamps:
            continue
        if timestamp < start_ts and record.get("incident_id") is None:
            result.append(record)
        elif start_ts <= timestamp < end_ts and record.get("incident_id") == incident_id:
            result.append(record)
    return result


def build_service_metric_matrix(normalized_records: list[dict], incident: dict) -> tuple[list[float], list[str], np.ndarray]:
    """Build a service-metric z-value matrix for one incident, aggregating instances by mean."""

    records = _records_for_incident(normalized_records, incident)
    if not records:
        raise ValueError(f"no normalized records found for incident_id={incident.get('incident_id')}")

    timestamps = sorted({float(record["timestamp"]) for record in records})
    nodes = sorted({f"{record['service']}.{record['metric']}" for record in records})
    timestamp_index = {timestamp: index for index, timestamp in enumerate(timestamps)}
    node_index = {node: index for index, node in enumerate(nodes)}

    buckets: dict[tuple[float, str], list[float]] = {}
    for record in records:
        node = f"{record['service']}.{record['metric']}"
        buckets.setdefault((float(record["timestamp"]), node), []).append(float(record["z_value"]))

    z_matrix = np.zeros((len(timestamps), len(nodes)), dtype=float)
    for (timestamp, node), values in buckets.items():
        z_matrix[timestamp_index[timestamp], node_index[node]] = float(np.mean(values))

    # P0 synthetic data uses conservative 0.0 filling for missing timestamp-node pairs;
    # real systems will add missing mask handling in P1.
    return timestamps, nodes, z_matrix


def _edge_enabled(edge_type: str, config: PropagationConfig) -> bool:
    flag = GRAPH_EDGE_FLAGS.get(edge_type)
    if flag is None:
        return False
    return bool(getattr(config, flag))


def _nodes_for_endpoint(endpoint: str, nodes: list[str], known_services: list[str]) -> list[str]:
    if endpoint in nodes:
        return [endpoint]
    service = extract_service(endpoint, known_services)
    if service is None:
        return []
    metric = _metric_from_node(endpoint, known_services)
    if metric is not None:
        node = f"{service}.{metric}"
        return [node] if node in nodes else []
    return [node for node in nodes if extract_service(node, known_services) == service]


def _allow_cross_service_target(parent: str, target: str, known_services: list[str], config: PropagationConfig) -> bool:
    parent_service = extract_service(parent, known_services)
    target_service = extract_service(target, known_services)
    if parent_service is None or target_service is None:
        return False
    if parent_service == target_service:
        return True
    if not config.target_request_metrics_only_for_cross_service:
        return True
    target_metric = _metric_from_node(target, known_services)
    return target_metric in REQUEST_METRICS


def build_parent_mask(nodes: list[str], graph_edges: list[dict], config: PropagationConfig) -> dict[str, list[tuple[str, str]]]:
    """Build allowed parent nodes for each target node from graph structure and config."""

    known_services = _known_services_from_nodes(nodes)
    allowed: dict[str, dict[str, str]] = {node: {} for node in nodes}

    if config.include_self_edges:
        for node in nodes:
            allowed[node][node] = "self"

    if config.include_same_service_edges:
        by_service: dict[str, list[str]] = {}
        for node in nodes:
            service = extract_service(node, known_services)
            if service is not None:
                by_service.setdefault(service, []).append(node)
        for service_nodes in by_service.values():
            for target in service_nodes:
                for parent in service_nodes:
                    allowed[target].setdefault(parent, "same_service")

    for edge in graph_edges:
        edge_type = str(edge.get("edge_type", ""))
        if not _edge_enabled(edge_type, config):
            continue
        src_nodes = _nodes_for_endpoint(str(edge.get("src")), nodes, known_services)
        dst_nodes = _nodes_for_endpoint(str(edge.get("dst")), nodes, known_services)
        for parent in src_nodes:
            for target in dst_nodes:
                if _allow_cross_service_target(parent, target, known_services, config):
                    allowed[target].setdefault(parent, edge_type)

    return {target: sorted(parents.items()) for target, parents in allowed.items()}


def _fit_ridge(x: np.ndarray, y: np.ndarray, ridge_lambda: float) -> np.ndarray:
    xtx = x.T @ x
    rhs = x.T @ y
    regularizer = ridge_lambda * np.eye(xtx.shape[0], dtype=float)
    return np.linalg.solve(xtx + regularizer, rhs)


def _rmse(values: list[float]) -> float:
    if not values:
        return 0.0
    array = np.asarray(values, dtype=float)
    return float(np.sqrt(np.mean(array * array)))


def fit_stable_propagation_for_incident(
    timestamps: list[float],
    nodes: list[str],
    z_matrix: np.ndarray,
    incident: dict,
    graph_edges: list[dict],
    config: PropagationConfig,
) -> tuple[list[dict], list[dict], dict]:
    """Fit one-step stable propagation for one incident and compute residuals."""

    if len(timestamps) < 3:
        raise ValueError("at least 3 timestamps are required for stable propagation")

    incident_id = str(incident["incident_id"])
    start_ts = float(incident["start_ts"])
    known_services = _known_services_from_nodes(nodes)
    baseline_pair_indices = [index for index in range(1, len(timestamps)) if timestamps[index] < start_ts]
    if len(baseline_pair_indices) < 2:
        raise ValueError(f"baseline adjacent window pairs fewer than 2 for incident_id={incident_id}")

    allowed_parents = build_parent_mask(nodes, graph_edges, config)
    coefficients: list[PropagationCoefficient] = []
    beta_by_target: dict[str, tuple[list[str], np.ndarray]] = {}

    for target_index, target in enumerate(nodes):
        parent_pairs = allowed_parents.get(target, [])
        parents = [parent for parent, _edge_type in parent_pairs]
        parent_indices = [nodes.index(parent) for parent in parents]
        if not parent_indices:
            beta_by_target[target] = ([], np.asarray([], dtype=float))
            continue

        x = np.asarray([z_matrix[index - 1, parent_indices] for index in baseline_pair_indices], dtype=float)
        y = np.asarray([z_matrix[index, target_index] for index in baseline_pair_indices], dtype=float)
        beta = _fit_ridge(x, y, config.ridge_lambda)
        beta_by_target[target] = (parents, beta)

        for (parent, edge_type), coefficient in zip(parent_pairs, beta):
            coefficient = float(coefficient)
            if abs(coefficient) >= config.coefficient_threshold:
                coefficients.append(
                    PropagationCoefficient(
                        incident_id=incident_id,
                        target=target,
                        parent=parent,
                        coefficient=coefficient,
                        edge_type=edge_type,
                    )
                )

    residual_records: list[PropagationResidualRecord] = []
    baseline_residuals: list[float] = []
    faulty_residuals: list[float] = []

    for index in range(1, len(timestamps)):
        timestamp = timestamps[index]
        for target_index, target in enumerate(nodes):
            parents, beta = beta_by_target[target]
            if parents:
                parent_indices = [nodes.index(parent) for parent in parents]
                predicted = float(z_matrix[index - 1, parent_indices] @ beta)
            else:
                predicted = 0.0
            z_value = float(z_matrix[index, target_index])
            residual = z_value - predicted
            service = extract_service(target, known_services)
            metric = _metric_from_node(target, known_services)
            if service is None or metric is None:
                raise ValueError(f"cannot parse service/metric from node={target}")
            residual_records.append(
                PropagationResidualRecord(
                    incident_id=incident_id,
                    timestamp=float(timestamp),
                    service=service,
                    metric=metric,
                    z_value=z_value,
                    predicted_z=predicted,
                    residual=float(residual),
                )
            )
            if timestamp < start_ts:
                baseline_residuals.append(float(residual))
            else:
                faulty_residuals.append(float(residual))

    model_summary = {
        "incident_id": incident_id,
        "timestamp_count": len(timestamps),
        "node_count": len(nodes),
        "train_pairs": len(baseline_pair_indices),
        "residual_count": len(residual_records),
        "coefficient_count": len(coefficients),
        "baseline_rmse": _rmse(baseline_residuals),
        "faulty_rmse": _rmse(faulty_residuals),
    }

    return [asdict(item) for item in coefficients], [asdict(item) for item in residual_records], model_summary


def train_stable_propagation(
    input_dir: str | Path,
    output_dir: str | Path | None = None,
    config: PropagationConfig | None = None,
) -> dict:
    """Train stable propagation models for all incidents and write residual outputs."""

    input_path = Path(input_dir)
    output_path = Path(output_dir) if output_dir is not None else input_path
    config = config or PropagationConfig()
    normalized_metrics, incidents, graph_edges = load_required_dataset(input_path)

    all_coefficients: list[dict] = []
    all_residuals: list[dict] = []
    summaries: list[dict] = []
    incident_models: list[dict] = []

    for incident in incidents:
        timestamps, nodes, z_matrix = build_service_metric_matrix(normalized_metrics, incident)
        coefficients, residuals, summary = fit_stable_propagation_for_incident(
            timestamps=timestamps,
            nodes=nodes,
            z_matrix=z_matrix,
            incident=incident,
            graph_edges=graph_edges,
            config=config,
        )
        all_coefficients.extend(coefficients)
        all_residuals.extend(residuals)
        summaries.append(summary)
        incident_models.append(
            {
                "incident_id": incident["incident_id"],
                "nodes": nodes,
                "coefficients": coefficients,
                "summary": summary,
            }
        )

    output_path.mkdir(parents=True, exist_ok=True)
    model_path = output_path / "stable_propagation_model.json"
    residuals_path = output_path / "stable_residuals.jsonl"
    metadata_path = output_path / "propagation_metadata.json"

    model = {
        "config": asdict(config),
        "incidents": incident_models,
        "summaries": summaries,
    }
    expected_residuals_count = sum((summary["timestamp_count"] - 1) * summary["node_count"] for summary in summaries)
    metadata = {
        "input_dir": str(input_path),
        "output_dir": str(output_path),
        "incidents_count": len(incidents),
        "coefficients_count": len(all_coefficients),
        "expected_residuals_count": int(expected_residuals_count),
        "residuals_count": len(all_residuals),
        "residuals_count_matches_expected": len(all_residuals) == expected_residuals_count,
        "ridge_lambda": float(config.ridge_lambda),
        "coefficient_threshold": float(config.coefficient_threshold),
    }

    model_path.write_text(json.dumps(model, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    write_jsonl(residuals_path, all_residuals)
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    return {
        "stable_propagation_model_path": str(model_path),
        "stable_residuals_path": str(residuals_path),
        "propagation_metadata_path": str(metadata_path),
        "metadata": metadata,
        "summaries": summaries,
    }
