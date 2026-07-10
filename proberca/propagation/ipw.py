"""IPW-masked stable propagation learner for probeRCA P1B."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np

from proberca.data.io import read_jsonl, write_jsonl
from proberca.data.synthetic import SyntheticConfig, generate_dataset
from proberca.features.robust import normalize_dataset
from proberca.observation.adaptive import ObservationPolicyConfig, simulate_adaptive_observation
from proberca.propagation.stable import (
    build_parent_mask as build_stable_parent_mask,
    extract_service,
    _metric_from_node,
    _known_services_from_nodes,
)


@dataclass
class IPWPropagationConfig:
    """Configuration for IPW-masked stable propagation learning."""

    ridge_lambda: float = 1.0
    coefficient_threshold: float = 1e-10
    min_sampling_probability: float = 0.05
    max_ipw_weight: float = 20.0
    use_ipw: bool = True
    use_parent_ipw: bool = True
    use_target_ipw: bool = True
    include_self_edges: bool = True
    include_same_service_edges: bool = True
    include_call_edges: bool = True
    include_cohost_edges: bool = True
    include_resource_edges: bool = True
    include_synthetic_edges: bool = True
    target_request_metrics_only_for_cross_service: bool = True


@dataclass
class IPWPropagationCoefficient:
    """One parent-to-target coefficient learned under partial observation."""

    incident_id: str
    target: str
    parent: str
    coefficient: float
    edge_type: str
    source: str = "ipw_masked_stable_propagation"


@dataclass
class IPWResidualRecord:
    """Observed target residual from the IPW-masked stable propagation model."""

    incident_id: str
    timestamp: float
    service: str
    metric: str
    z_value: float
    predicted_z: float
    residual: float
    observed: bool
    sampling_probability: float
    ipw_weight: float
    observation_mode: str
    source: str = "ipw_masked_stable_propagation"


def safe_ipw_weight(probability: float, min_probability: float, max_weight: float) -> float:
    """Return clipped inverse-propensity weight."""

    denominator = max(float(probability), float(min_probability))
    weight = 1.0 / denominator
    return float(min(float(max_weight), weight))


def load_required_dataset(input_dir: str | Path) -> tuple[list[dict], list[dict], list[dict], list[dict], list[dict]]:
    """Load P1B observed metrics, sampling logs, masks, incidents, and graph edges."""

    input_path = Path(input_dir)
    required = {
        "observed_metrics": input_path / "observed_metrics.jsonl",
        "sampling_log": input_path / "sampling_log.jsonl",
        "observation_mask": input_path / "observation_mask.jsonl",
        "incidents": input_path / "incidents.jsonl",
        "service_graph": input_path / "service_graph.jsonl",
    }
    for name, path in required.items():
        if not path.exists():
            raise FileNotFoundError(f"missing required input file for {name}: {path}")
    return (
        read_jsonl(required["observed_metrics"]),
        read_jsonl(required["sampling_log"]),
        read_jsonl(required["observation_mask"]),
        read_jsonl(required["incidents"]),
        read_jsonl(required["service_graph"]),
    )


def _record_incident_id(record: dict) -> str:
    value = record.get("incident_id")
    return "" if value is None else str(value)


def _infer_window_step(records: list[dict]) -> float:
    timestamps = sorted({float(record["timestamp"]) for record in records})
    diffs = [right - left for left, right in zip(timestamps, timestamps[1:]) if right > left]
    if not diffs:
        raise ValueError("cannot infer window step from fewer than two timestamps")
    return float(np.median(np.asarray(diffs, dtype=float)))


def _contiguous_baseline_timestamps(records: list[dict], start_ts: float, window_step: float) -> set[float]:
    candidates = sorted(
        {
            float(record["timestamp"])
            for record in records
            if float(record["timestamp"]) < start_ts and _record_incident_id(record) == ""
        }
    )
    if not candidates:
        return set()
    selected = [candidates[-1]]
    previous = candidates[-1]
    tolerance = max(1e-9, abs(window_step) * 0.5)
    for timestamp in reversed(candidates[:-1]):
        if 0 < (previous - timestamp) <= window_step + tolerance:
            selected.append(timestamp)
            previous = timestamp
        else:
            break
    return set(selected)


def _selected_timestamps(reference_records: list[dict], incident: dict) -> set[float]:
    incident_id = str(incident["incident_id"])
    start_ts = float(incident["start_ts"])
    end_ts = float(incident["end_ts"])
    window_step = _infer_window_step(reference_records)
    baseline = _contiguous_baseline_timestamps(reference_records, start_ts, window_step)
    faulty = {
        float(record["timestamp"])
        for record in reference_records
        if start_ts <= float(record["timestamp"]) < end_ts and _record_incident_id(record) == incident_id
    }
    return baseline | faulty


def _belongs_to_incident_window(record: dict, incident: dict, selected_timestamps: set[float]) -> bool:
    timestamp = float(record["timestamp"])
    if timestamp not in selected_timestamps:
        return False
    incident_id = str(incident["incident_id"])
    start_ts = float(incident["start_ts"])
    if timestamp < start_ts:
        return _record_incident_id(record) == ""
    return _record_incident_id(record) == incident_id


def _mode_priority(mode: str) -> int:
    return {
        "hard_alert_burst": 5,
        "soft_alert_burst": 4,
        "always_on": 3,
        "normal_sampled": 2,
        "not_observed": 1,
    }.get(mode, 0)


def _aggregate_mode(modes: list[str]) -> str:
    if not modes:
        return "not_observed"
    return sorted(modes, key=lambda mode: (-_mode_priority(mode), mode))[0]


def build_observed_service_metric_panel(
    observed_metrics: list[dict],
    sampling_log: list[dict],
    incident: dict,
) -> tuple[list[float], list[str], np.ndarray, np.ndarray, np.ndarray, list[list[str]]]:
    """Build service-metric matrices under partial observation, aggregating instances."""

    selected_timestamps = _selected_timestamps(sampling_log, incident)
    if not selected_timestamps:
        raise ValueError(f"no sampling log records found for incident_id={incident.get('incident_id')}")

    log_records = [record for record in sampling_log if _belongs_to_incident_window(record, incident, selected_timestamps)]
    metric_records = [record for record in observed_metrics if _belongs_to_incident_window(record, incident, selected_timestamps)]
    timestamps = sorted({float(record["timestamp"]) for record in log_records})
    nodes = sorted({f"{record['service']}.{record['metric']}" for record in log_records})
    timestamp_index = {timestamp: index for index, timestamp in enumerate(timestamps)}
    node_index = {node: index for index, node in enumerate(nodes)}

    z_buckets: dict[tuple[float, str], list[float]] = {}
    probability_buckets: dict[tuple[float, str], list[float]] = {}
    observed_buckets: dict[tuple[float, str], list[bool]] = {}
    mode_buckets: dict[tuple[float, str], list[str]] = {}

    for record in metric_records:
        node = f"{record['service']}.{record['metric']}"
        z_buckets.setdefault((float(record["timestamp"]), node), []).append(float(record["z_value"]))

    for record in log_records:
        node = f"{record['service']}.{record['metric']}"
        key = (float(record["timestamp"]), node)
        probability_buckets.setdefault(key, []).append(float(record["sampling_probability"]))
        observed_buckets.setdefault(key, []).append(bool(record["observed"]))
        mode_buckets.setdefault(key, []).append(str(record.get("observation_mode", "not_observed")))

    z_matrix = np.zeros((len(timestamps), len(nodes)), dtype=float)
    mask_matrix = np.zeros((len(timestamps), len(nodes)), dtype=float)
    prob_matrix = np.ones((len(timestamps), len(nodes)), dtype=float)
    mode_matrix: list[list[str]] = [["not_observed" for _ in nodes] for _ in timestamps]

    for (timestamp, node), values in z_buckets.items():
        if timestamp in timestamp_index and node in node_index:
            z_matrix[timestamp_index[timestamp], node_index[node]] = float(np.mean(values))

    for (timestamp, node), probabilities in probability_buckets.items():
        row = timestamp_index[timestamp]
        col = node_index[node]
        prob_matrix[row, col] = float(np.mean(probabilities))
        mask_matrix[row, col] = 1.0 if any(observed_buckets.get((timestamp, node), [])) else 0.0
        mode_matrix[row][col] = _aggregate_mode(mode_buckets.get((timestamp, node), []))

    return timestamps, nodes, z_matrix, mask_matrix, prob_matrix, mode_matrix


def build_parent_mask(nodes: list[str], graph_edges: list[dict], config: IPWPropagationConfig) -> dict[str, list[tuple[str, str]]]:
    """Build allowed parent nodes for IPW propagation."""

    return build_stable_parent_mask(nodes, graph_edges, config)


def _fit_weighted_ridge(x: np.ndarray, y: np.ndarray, weights: np.ndarray, ridge_lambda: float) -> np.ndarray:
    sqrt_weights = np.sqrt(weights).reshape(-1, 1)
    xw = x * sqrt_weights
    yw = y * sqrt_weights.ravel()
    xtx = xw.T @ xw
    rhs = xw.T @ yw
    return np.linalg.solve(xtx + ridge_lambda * np.eye(xtx.shape[0], dtype=float), rhs)


def _rmse(values: list[float]) -> float:
    if not values:
        return 0.0
    arr = np.asarray(values, dtype=float)
    return float(np.sqrt(np.mean(arr * arr)))


def _prediction_features(
    row_index: int,
    parent_indices: list[int],
    z_matrix: np.ndarray,
    mask_matrix: np.ndarray,
    prob_matrix: np.ndarray,
    config: IPWPropagationConfig,
) -> np.ndarray:
    if not parent_indices:
        return np.asarray([], dtype=float)
    z_parent = z_matrix[row_index, parent_indices]
    mask_parent = mask_matrix[row_index, parent_indices]
    if not config.use_ipw or not config.use_parent_ipw:
        return z_parent * mask_parent
    weights = np.asarray(
        [safe_ipw_weight(prob_matrix[row_index, parent_index], config.min_sampling_probability, config.max_ipw_weight) for parent_index in parent_indices],
        dtype=float,
    )
    return z_parent * mask_parent * weights


def fit_ipw_masked_propagation_for_incident(
    timestamps: list[float],
    nodes: list[str],
    z_matrix: np.ndarray,
    mask_matrix: np.ndarray,
    prob_matrix: np.ndarray,
    mode_matrix: list[list[str]],
    incident: dict,
    graph_edges: list[dict],
    config: IPWPropagationConfig,
) -> tuple[list[dict], list[dict], dict]:
    """Fit one incident's IPW-masked stable propagation model and residuals."""

    if len(timestamps) < 3:
        raise ValueError("at least 3 timestamps are required for IPW propagation")

    incident_id = str(incident["incident_id"])
    start_ts = float(incident["start_ts"])
    baseline_pair_indices = [index for index in range(1, len(timestamps)) if timestamps[index] < start_ts]
    if len(baseline_pair_indices) < 2:
        raise ValueError(f"baseline adjacent window pairs fewer than 2 for incident_id={incident_id}")

    known_services = _known_services_from_nodes(nodes)
    node_to_index = {node: index for index, node in enumerate(nodes)}
    allowed_parents = build_parent_mask(nodes, graph_edges, config)
    beta_by_target: dict[str, tuple[list[str], np.ndarray]] = {}
    coefficients: list[IPWPropagationCoefficient] = []
    usable_train_rows_total = 0
    train_pairs_total = len(baseline_pair_indices) * len(nodes)
    target_train_weight_values: list[float] = []

    for target_index, target in enumerate(nodes):
        parent_pairs = allowed_parents.get(target, [])
        parents = [parent for parent, _edge_type in parent_pairs]
        parent_indices = [node_to_index[parent] for parent in parents]
        if not parent_indices:
            beta_by_target[target] = ([], np.asarray([], dtype=float))
            continue

        x_rows: list[np.ndarray] = []
        y_rows: list[float] = []
        weight_rows: list[float] = []
        for index in baseline_pair_indices:
            if mask_matrix[index, target_index] != 1.0:
                continue
            x_rows.append(_prediction_features(index - 1, parent_indices, z_matrix, mask_matrix, prob_matrix, config))
            y_rows.append(float(z_matrix[index, target_index]))
            if config.use_ipw and config.use_target_ipw:
                weight = safe_ipw_weight(prob_matrix[index, target_index], config.min_sampling_probability, config.max_ipw_weight)
            else:
                weight = 1.0
            weight_rows.append(weight)
            target_train_weight_values.append(weight)

        if len(x_rows) < 2:
            beta_by_target[target] = (parents, np.zeros(len(parents), dtype=float))
            continue

        x = np.asarray(x_rows, dtype=float)
        y = np.asarray(y_rows, dtype=float)
        weights = np.asarray(weight_rows, dtype=float)
        beta = _fit_weighted_ridge(x, y, weights, config.ridge_lambda)
        beta_by_target[target] = (parents, beta)
        usable_train_rows_total += len(x_rows)

        for (parent, edge_type), coefficient in zip(parent_pairs, beta):
            coefficient = float(coefficient)
            if abs(coefficient) >= config.coefficient_threshold:
                coefficients.append(
                    IPWPropagationCoefficient(
                        incident_id=incident_id,
                        target=target,
                        parent=parent,
                        coefficient=coefficient,
                        edge_type=edge_type,
                    )
                )

    residual_records: list[IPWResidualRecord] = []
    baseline_residuals: list[float] = []
    faulty_residuals: list[float] = []
    residual_ipw_weights: list[float] = []
    residual_probabilities: list[float] = []

    for index in range(1, len(timestamps)):
        timestamp = timestamps[index]
        for target_index, target in enumerate(nodes):
            if mask_matrix[index, target_index] != 1.0:
                continue
            parents, beta = beta_by_target[target]
            if parents:
                parent_indices = [node_to_index[parent] for parent in parents]
                features = _prediction_features(index - 1, parent_indices, z_matrix, mask_matrix, prob_matrix, config)
                predicted = float(features @ beta)
            else:
                predicted = 0.0
            z_value = float(z_matrix[index, target_index])
            residual = z_value - predicted
            target_probability = float(prob_matrix[index, target_index])
            ipw_weight = safe_ipw_weight(target_probability, config.min_sampling_probability, config.max_ipw_weight) if config.use_ipw else 1.0
            service = extract_service(target, known_services)
            metric = _metric_from_node(target, known_services)
            if service is None or metric is None:
                continue
            residual_records.append(
                IPWResidualRecord(
                    incident_id=incident_id,
                    timestamp=float(timestamp),
                    service=service,
                    metric=metric,
                    z_value=z_value,
                    predicted_z=predicted,
                    residual=residual,
                    observed=True,
                    sampling_probability=target_probability,
                    ipw_weight=ipw_weight,
                    observation_mode=mode_matrix[index][target_index],
                )
            )
            residual_ipw_weights.append(ipw_weight)
            residual_probabilities.append(target_probability)
            if timestamp < start_ts:
                baseline_residuals.append(residual)
            else:
                faulty_residuals.append(residual)

    summary = {
        "incident_id": incident_id,
        "timestamp_count": len(timestamps),
        "node_count": len(nodes),
        "observed_cells": int(np.sum(mask_matrix)),
        "train_pairs": train_pairs_total,
        "usable_train_rows": int(usable_train_rows_total),
        "residual_count": len(residual_records),
        "coefficient_count": len(coefficients),
        "baseline_rmse": _rmse(baseline_residuals),
        "faulty_rmse": _rmse(faulty_residuals),
        "mean_sampling_probability": float(np.mean(residual_probabilities)) if residual_probabilities else 0.0,
        "mean_ipw_weight": float(np.mean(residual_ipw_weights)) if residual_ipw_weights else (1.0 if not config.use_ipw else 0.0),
        "use_ipw": bool(config.use_ipw),
    }

    return [asdict(item) for item in coefficients], [asdict(item) for item in residual_records], summary


def train_ipw_masked_propagation(
    input_dir: str | Path,
    output_dir: str | Path | None = None,
    config: IPWPropagationConfig | None = None,
) -> dict:
    """Train IPW-masked stable propagation for all incidents in a P1A directory."""

    cfg = config or IPWPropagationConfig()
    input_path = Path(input_dir)
    output_path = Path(output_dir) if output_dir is not None else input_path
    observed_metrics, sampling_log, observation_mask, incidents, graph_edges = load_required_dataset(input_path)
    _ = observation_mask  # Required for contract; sampling_log carries the aggregated probabilities used here.

    all_coefficients: list[dict] = []
    all_residuals: list[dict] = []
    summaries: list[dict] = []

    for incident in incidents:
        timestamps, nodes, z_matrix, mask_matrix, prob_matrix, mode_matrix = build_observed_service_metric_panel(
            observed_metrics,
            sampling_log,
            incident,
        )
        coefficients, residuals, summary = fit_ipw_masked_propagation_for_incident(
            timestamps,
            nodes,
            z_matrix,
            mask_matrix,
            prob_matrix,
            mode_matrix,
            incident,
            graph_edges,
            cfg,
        )
        all_coefficients.extend(coefficients)
        all_residuals.extend(residuals)
        summaries.append(summary)

    output_path.mkdir(parents=True, exist_ok=True)
    model_path = output_path / "ipw_stable_propagation_model.json"
    residuals_path = output_path / "ipw_stable_residuals.jsonl"
    metadata_path = output_path / "ipw_propagation_metadata.json"

    model = {
        "config": asdict(cfg),
        "coefficients": all_coefficients,
        "summaries": summaries,
    }
    model_path.write_text(json.dumps(model, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    write_jsonl(residuals_path, all_residuals)

    mean_sampling_probability = float(np.mean([summary["mean_sampling_probability"] for summary in summaries])) if summaries else 0.0
    mean_ipw_weight = float(np.mean([summary["mean_ipw_weight"] for summary in summaries])) if summaries else (1.0 if not cfg.use_ipw else 0.0)
    metadata = {
        "input_dir": str(input_path),
        "output_dir": str(output_path),
        "incidents_count": len(incidents),
        "coefficients_count": len(all_coefficients),
        "residuals_count": len(all_residuals),
        "ridge_lambda": float(cfg.ridge_lambda),
        "min_sampling_probability": float(cfg.min_sampling_probability),
        "max_ipw_weight": float(cfg.max_ipw_weight),
        "use_ipw": bool(cfg.use_ipw),
        "mean_sampling_probability": mean_sampling_probability,
        "mean_ipw_weight": mean_ipw_weight,
    }
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    return {
        "ipw_stable_propagation_model_path": str(model_path),
        "ipw_stable_residuals_path": str(residuals_path),
        "ipw_propagation_metadata_path": str(metadata_path),
        "metadata": metadata,
        "summaries": summaries,
    }


def run_p1b_pipeline(
    output_dir: str | Path,
    seed: int = 7,
    baseline_windows: int = 30,
    faulty_windows: int = 30,
    instances_per_service: int = 2,
    config: IPWPropagationConfig | None = None,
) -> dict:
    """Run generate, normalize, adaptive observation, then IPW propagation."""

    output_path = Path(output_dir)
    generate_dataset(
        SyntheticConfig(
            seed=seed,
            output_dir=str(output_path),
            baseline_windows=baseline_windows,
            faulty_windows=faulty_windows,
            instances_per_service=instances_per_service,
        )
    )
    normalize_dataset(output_path, output_path)
    simulate_adaptive_observation(output_path, output_path, ObservationPolicyConfig(seed=seed))
    return train_ipw_masked_propagation(output_path, output_path, config or IPWPropagationConfig())
