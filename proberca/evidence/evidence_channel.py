"""A7 C h_t evidence channel and residual calibration preview.

This module maps blind evidence and adaptive probe-policy outputs into a
fine-grained evidence term C h_t. It does not run RCA and does not use root or
experiment target labels for channel construction.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import math
from pathlib import Path
from typing import Any

import numpy as np

from proberca.adapters.online_boutique.service_metric_identity import (
    assert_or_repair_node_ownership,
    validate_node_ownership,
)


@dataclass
class EvidenceChannelConfig:
    residual_calibration_method: str = "family_robust_clip"
    residual_clip_value: float = 10.0
    residual_eps: float = 1e-6
    min_evidence_score: float = 0.05
    max_evidence_effect: float = 5.0
    evidence_effect_scale: float = 1.0
    family_prior_weight: float = 0.5
    service_prior_weight: float = 0.3
    metric_prior_weight: float = 0.2
    use_probe_sampling_weight: bool = True
    min_sampling_probability: float = 0.05
    max_ipw_weight: float = 20.0


METRIC_PREFIXES = ("cpu", "net", "io", "lock", "memory", "request")


def load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    p = Path(path)
    if not p.exists():
        return rows
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def write_jsonl(path: str | Path, rows: list[dict[str, Any]]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _write_json(path: str | Path, payload: dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_json_optional(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _mean(values: list[float]) -> float:
    return float(np.mean(np.asarray(values, dtype=float))) if values else 0.0


def _max(values: list[float]) -> float:
    return float(np.max(np.asarray(values, dtype=float))) if values else 0.0


def metric_family(metric: str) -> str:
    if metric.startswith("cpu."):
        return "CPU"
    if metric.startswith("net."):
        return "network"
    if metric.startswith("io."):
        return "storage I/O"
    if metric.startswith("lock."):
        return "lock contention"
    if metric.startswith("memory."):
        return "memory"
    if metric.startswith("request."):
        return "load"
    return "unknown"


def split_node_id(node_id: str, service: str | None = None, metric: str | None = None) -> tuple[str, str]:
    if service and metric:
        return service, metric
    for prefix in METRIC_PREFIXES:
        marker = f".{prefix}."
        if marker in node_id:
            before, after = node_id.split(marker, 1)
            return before, f"{prefix}.{after}"
    parts = node_id.split(".", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return service or "unknown", metric or node_id


def _first_existing(paths: list[Path]) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def _score(row: dict[str, Any]) -> float:
    value = row.get("evidence_score", row.get("value", 0.0))
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def load_blind_evidence(path_or_dir: str) -> dict[str, Any]:
    base = Path(path_or_dir)
    if base.is_dir():
        evidence_path = _first_existing([
            base / "blind_evidence.jsonl",
            base / "input" / "blind_evidence.jsonl",
            base / "evidence.jsonl",
            base / "input" / "evidence.jsonl",
        ])
    else:
        evidence_path = base
        base = base.parent
    if evidence_path is None or not evidence_path.exists():
        raise FileNotFoundError(f"blind evidence file not found: {path_or_dir}")

    metadata_paths = [
        evidence_path.parent / "blind_evidence_metadata.json",
        evidence_path.parent / "blind_input_metadata.json",
        base / "blind_evidence_metadata.json",
        base / "input" / "blind_evidence_metadata.json",
        base / "blind_input_metadata.json",
        base / "input" / "blind_input_metadata.json",
    ]
    metadata = {}
    for path in metadata_paths:
        metadata.update(_read_json_optional(path))

    rows = load_jsonl(evidence_path)
    unsafe_reasons: list[str] = []
    if evidence_path.name == "evidence.jsonl" and metadata.get("uses_blind_evidence") is not True and metadata.get("blind_evidence") is not True:
        unsafe_reasons.append("evidence.jsonl is not proven to be blind evidence")
    if metadata.get("uses_blind_evidence") is False or metadata.get("blind_evidence") is False:
        unsafe_reasons.append("metadata marks evidence as non-blind")
    if metadata.get("uses_root_labels") is True or metadata.get("uses_target_config") is True or metadata.get("uses_injected_path") is True:
        unsafe_reasons.append("metadata marks root/target/injected-path use")

    evidence_by_node: dict[str, dict[str, Any]] = {}
    evidence_by_service_family: dict[tuple[str, str], float] = {}
    evidence_by_family: dict[str, float] = {}
    for row in rows:
        source = str(row.get("source", ""))
        if "legacy" in source.lower() or row.get("target_aware") is True:
            unsafe_reasons.append("legacy or target-aware evidence row detected")
            continue
        fixed = assert_or_repair_node_ownership(row)
        service = str(fixed.get("service") or "")
        metric = str(fixed.get("metric") or "")
        node = str(fixed.get("node_id") or fixed.get("node") or (f"{service}.{metric}" if service and metric else ""))
        family = str(fixed.get("metric_family") or fixed.get("evidence_type") or metric_family(metric))
        score = max(0.0, min(1.0, _score(fixed)))
        if score <= 0:
            continue
        if node not in evidence_by_node or score > float(evidence_by_node[node].get("evidence_score", 0.0)):
            evidence_by_node[node] = {**fixed, "node_id": node, "service": service, "metric": metric, "metric_family": family, "evidence_score": score}
        key = (service, family)
        evidence_by_service_family[key] = max(score, evidence_by_service_family.get(key, 0.0))
        evidence_by_family[family] = max(score, evidence_by_family.get(family, 0.0))

    return {
        "evidence_path": str(evidence_path),
        "metadata": metadata,
        "rows": rows,
        "evidence_count": len(rows),
        "evidence_by_node": evidence_by_node,
        "evidence_by_service_family": evidence_by_service_family,
        "evidence_by_family": evidence_by_family,
        "unsafe_input": bool(unsafe_reasons),
        "unsafe_reasons": sorted(set(unsafe_reasons)),
    }


def load_probe_policy(path_or_dir: str) -> dict[str, Any]:
    base = Path(path_or_dir)
    sampling_path = base / "sampling_log.jsonl"
    mask_path = base / "observation_mask.jsonl"
    metadata_path = base / "adaptive_probe_metadata.json"
    sampling_rows = load_jsonl(sampling_path)
    mask_rows = load_jsonl(mask_path)
    metadata = _read_json_optional(metadata_path)
    sampling_probability_by_node: dict[str, float] = {}
    selected_probe_by_service_family: dict[tuple[str, str], str] = {}
    observed_probability_by_node: dict[str, float] = {}
    for row in sampling_rows:
        service = str(row.get("service") or "")
        metric = str(row.get("metric") or "")
        node = str(row.get("node_id") or (f"{service}.{metric}" if service and metric else ""))
        if not service or not metric:
            service, metric = split_node_id(node, service or None, metric or None)
        family = str(row.get("evidence_type") or metric_family(metric))
        p = float(row.get("sampling_probability", row.get("observed_probability", 1.0)) or 0.0)
        if node:
            sampling_probability_by_node[node] = max(p, sampling_probability_by_node.get(node, 0.0))
        if row.get("selected") is True:
            selected_probe_by_service_family[(service, family)] = str(row.get("probe_name") or row.get("observed_by_probe") or "selected_probe")
    for row in mask_rows:
        service = str(row.get("service") or "")
        metric = str(row.get("metric") or "")
        node = str(row.get("node_id") or (f"{service}.{metric}" if service and metric else ""))
        if not service or not metric:
            service, metric = split_node_id(node, service or None, metric or None)
        p = float(row.get("observed_probability", row.get("sampling_probability", 1.0)) or 0.0)
        if node:
            observed_probability_by_node[node] = max(p, observed_probability_by_node.get(node, 0.0))
    return {
        "sampling_rows": sampling_rows,
        "mask_rows": mask_rows,
        "metadata": metadata,
        "sampling_probability_by_node": sampling_probability_by_node,
        "selected_probe_by_service_family": selected_probe_by_service_family,
        "observed_probability_by_node": observed_probability_by_node,
    }


def load_ipw_rls_outputs(path_or_dir: str) -> dict[str, Any]:
    base = Path(path_or_dir)
    residual_rows = load_jsonl(base / "ipw_rls_residuals.jsonl")
    prediction_rows = load_jsonl(base / "ipw_rls_predictions.jsonl")
    metadata = _read_json_optional(base / "ipw_rls_metadata.json")
    unsafe_reasons: list[str] = []
    if metadata.get("update_mode") != "online_rls":
        unsafe_reasons.append("update_mode is not online_rls")
    if metadata.get("batch_ridge_used") is not False:
        unsafe_reasons.append("batch_ridge_used is not false")
    if metadata.get("consumes_sampling_probability") is not True:
        unsafe_reasons.append("consumes_sampling_probability is not true")
    if metadata.get("consumes_observation_mask") is not True:
        unsafe_reasons.append("consumes_observation_mask is not true")
    return {
        "residual_rows": residual_rows,
        "prediction_rows": prediction_rows,
        "metadata": metadata,
        "unsafe_input": bool(unsafe_reasons),
        "unsafe_reasons": unsafe_reasons,
    }


def compute_evidence_vector_for_node(
    node_id: str,
    service: str,
    metric: str,
    evidence_maps: dict[str, Any],
    probe_maps: dict[str, Any],
    config: EvidenceChannelConfig,
) -> dict[str, Any]:
    family = metric_family(metric)
    node_info = evidence_maps.get("evidence_by_node", {}).get(node_id, {})
    node_score = float(node_info.get("evidence_score", 0.0) or 0.0)
    service_family_score = float(evidence_maps.get("evidence_by_service_family", {}).get((service, family), 0.0) or 0.0)
    family_score = float(evidence_maps.get("evidence_by_family", {}).get(family, 0.0) or 0.0)
    if node_score < config.min_evidence_score:
        node_score = 0.0
    if service_family_score < config.min_evidence_score:
        service_family_score = 0.0
    if family_score < config.min_evidence_score:
        family_score = 0.0
    base_h = (
        config.metric_prior_weight * node_score
        + config.service_prior_weight * service_family_score
        + config.family_prior_weight * family_score
    )
    p = float(probe_maps.get("sampling_probability_by_node", {}).get(node_id, 0.0) or 0.0)
    if p <= 0:
        p = float(probe_maps.get("observed_probability_by_node", {}).get(node_id, 0.0) or 0.0)
    if p <= 0:
        p = 1.0 if family == "load" else config.min_sampling_probability
    probe_weight = 1.0
    if config.use_probe_sampling_weight:
        probe_weight = min(1.0 / max(p, config.min_sampling_probability), config.max_ipw_weight)
    h_value = base_h * math.sqrt(probe_weight)
    h_value = max(0.0, min(float(config.max_evidence_effect), float(h_value)))
    probe_name = probe_maps.get("selected_probe_by_service_family", {}).get((service, family))
    ownership = validate_node_ownership({"node_id": node_id, "service": service, "metric": metric})
    return {
        "node_id": ownership["node_id"],
        "service": ownership["service"],
        "metric": ownership["metric"],
        "metric_family": family,
        "ownership_valid": ownership["ownership_valid"],
        "ownership_issue": ownership["ownership_issue"],
        "service_matches_node_id": ownership["service_matches_node_id"],
        "node_evidence_score": node_score,
        "service_family_evidence_score": service_family_score,
        "family_evidence_score": family_score,
        "probe_sampling_probability": p,
        "probe_selected": probe_name is not None,
        "selected_probe": probe_name,
        "probe_weight": probe_weight,
        "h_value": h_value,
        "h_components": {
            "metric_prior_component": config.metric_prior_weight * node_score,
            "service_prior_component": config.service_prior_weight * service_family_score,
            "family_prior_component": config.family_prior_weight * family_score,
            "probe_weight_sqrt": math.sqrt(probe_weight),
        },
        "evidence_source": "blind_evidence_and_probe_policy",
        "uses_root_labels": False,
        "uses_target_config": False,
        "uses_injected_path": False,
    }


def estimate_evidence_effect(residual_row: dict[str, Any], evidence_vector: dict[str, Any], config: EvidenceChannelConfig) -> dict[str, Any]:
    raw_residual = float(residual_row.get("raw_residual", residual_row.get("residual", 0.0)) or 0.0)
    if raw_residual > 0:
        sign = 1.0
    elif raw_residual < 0:
        sign = -1.0
    else:
        sign = 0.0
    h_value = float(evidence_vector.get("h_value", 0.0) or 0.0)
    family = str(evidence_vector.get("metric_family") or "unknown")
    scale = 0.5 if family == "load" else 1.0
    effect = config.evidence_effect_scale * h_value * scale * sign
    cap = min(abs(raw_residual), float(config.max_evidence_effect))
    effect = max(-cap, min(cap, effect))
    reason = "evidence aligned with residual sign"
    if family == "load":
        reason = "load symptom evidence effect damped"
    if sign == 0.0 or h_value <= 0.0:
        reason = "no signed residual or evidence strength"
    return {
        "evidence_effect": float(effect),
        "h_value": h_value,
        "effect_reason": reason,
    }


def _calibration_stats(values: list[float], eps: float) -> tuple[float, float, float]:
    arr = np.asarray(values, dtype=float)
    median = float(np.median(arr)) if arr.size else 0.0
    mad = float(np.median(np.abs(arr - median))) if arr.size else 0.0
    scale = 1.4826 * mad + eps
    return median, mad, scale


def calibrate_residuals(residual_rows: list[dict[str, Any]], config: EvidenceChannelConfig) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    global_values: list[float] = []
    family_values: dict[str, list[float]] = {}
    for row in residual_rows:
        node_id = str(row.get("node_id") or row.get("target_node") or "")
        service = str(row.get("service") or "")
        metric = str(row.get("metric") or "")
        if not service or not metric:
            service, metric = split_node_id(node_id, service or None, metric or None)
        family = str(row.get("metric_family") or metric_family(metric))
        raw = float(row.get("raw_residual", row.get("residual", 0.0)) or 0.0)
        effect = float(row.get("evidence_effect", 0.0) or 0.0)
        adjusted = raw - effect
        prepared_row = {**row, "node_id": node_id, "service": service, "metric": metric, "metric_family": family, "raw_residual": raw, "evidence_effect": effect, "raw_adjusted_residual": adjusted}
        prepared.append(prepared_row)
        global_values.append(adjusted)
        family_values.setdefault(family, []).append(adjusted)
    global_stats = _calibration_stats(global_values, config.residual_eps)
    family_stats = {
        family: _calibration_stats(values, config.residual_eps) if len(values) >= 3 else global_stats
        for family, values in family_values.items()
    }
    out: list[dict[str, Any]] = []
    for row in prepared:
        family = row["metric_family"]
        median, mad, scale = family_stats.get(family, global_stats)
        calibrated = (float(row["raw_adjusted_residual"]) - median) / scale
        calibrated = max(-float(config.residual_clip_value), min(float(config.residual_clip_value), float(calibrated)))
        out.append({
            "timestamp": row.get("timestamp"),
            "node_id": row["node_id"],
            "service": row["service"],
            "metric": row["metric"],
            "metric_family": family,
            "predicted_z": row.get("predicted_z"),
            "actual_z": row.get("actual_z"),
            "raw_residual": row["raw_residual"],
            "evidence_effect": row["evidence_effect"],
            "raw_adjusted_residual": row["raw_adjusted_residual"],
            "calibrated_residual": calibrated,
            "abs_calibrated_residual": abs(calibrated),
            "h_value": float(row.get("h_value", 0.0) or 0.0),
            "evidence_source": row.get("evidence_source", "none"),
            "calibration_family": family if len(family_values.get(family, [])) >= 3 else "global_fallback",
            "calibration_median": median,
            "calibration_mad": mad,
            "calibration_method": config.residual_calibration_method,
            "uses_root_labels": False,
            "uses_target_config": False,
        })
    return out


def build_evidence_channel(
    blind_evidence_dir: str,
    probe_policy_dir: str,
    ipw_rls_dir: str,
    output_dir: str,
    config: EvidenceChannelConfig | None = None,
) -> dict[str, Any]:
    cfg = config or EvidenceChannelConfig()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    evidence_maps = load_blind_evidence(blind_evidence_dir)
    if evidence_maps.get("unsafe_input"):
        raise ValueError(f"unsafe blind evidence input: {evidence_maps.get('unsafe_reasons')}")
    probe_maps = load_probe_policy(probe_policy_dir)
    rls = load_ipw_rls_outputs(ipw_rls_dir)
    if rls.get("unsafe_input"):
        raise ValueError(f"unsafe IPW RLS input: {rls.get('unsafe_reasons')}")

    vector_by_node: dict[str, dict[str, Any]] = {}
    effects: list[dict[str, Any]] = []
    enriched_residuals: list[dict[str, Any]] = []
    for row in rls["residual_rows"]:
        node_id = str(row.get("target_node") or row.get("node_id") or "")
        fixed = assert_or_repair_node_ownership({"node_id": node_id, "service": row.get("service"), "metric": row.get("metric")})
        node_id = str(fixed.get("node_id") or node_id)
        service = str(fixed.get("service") or "unknown")
        metric = str(fixed.get("metric") or "unknown")
        vector = compute_evidence_vector_for_node(node_id, service, metric, evidence_maps, probe_maps, cfg)
        vector_by_node[node_id] = vector
        effect = estimate_evidence_effect(row, vector, cfg)
        enriched = {
            **row,
            "node_id": node_id,
            "service": service,
            "metric": metric,
            "metric_family": vector["metric_family"],
            "raw_residual": float(row.get("raw_residual", row.get("residual", 0.0)) or 0.0),
            "evidence_effect": effect["evidence_effect"],
            "h_value": effect["h_value"],
            "evidence_source": vector["evidence_source"],
        }
        effects.append({
            "timestamp": row.get("timestamp"),
            "node_id": node_id,
            "service": service,
            "metric": metric,
            "metric_family": vector["metric_family"],
            "raw_residual": enriched["raw_residual"],
            "evidence_effect": effect["evidence_effect"],
            "h_value": effect["h_value"],
            "effect_reason": effect["effect_reason"],
            "uses_root_labels": False,
            "uses_target_config": False,
            "uses_injected_path": False,
        })
        enriched_residuals.append(enriched)

    calibrated = calibrate_residuals(enriched_residuals, cfg)
    vectors = sorted(vector_by_node.values(), key=lambda row: row["node_id"])
    write_jsonl(output / "evidence_vectors.jsonl", vectors)
    write_jsonl(output / "evidence_effects.jsonl", effects)
    write_jsonl(output / "calibrated_residuals.jsonl", calibrated)

    raw_abs = [abs(float(row.get("raw_residual", 0.0) or 0.0)) for row in enriched_residuals]
    cal_abs = [abs(float(row.get("calibrated_residual", 0.0) or 0.0)) for row in calibrated]
    metadata = {
        "blind_evidence_dir": blind_evidence_dir,
        "probe_policy_dir": probe_policy_dir,
        "ipw_rls_dir": ipw_rls_dir,
        "output_dir": output_dir,
        "residual_count": len(enriched_residuals),
        "calibrated_residual_count": len(calibrated),
        "average_abs_raw_residual": _mean(raw_abs),
        "average_abs_calibrated_residual": _mean(cal_abs),
        "max_abs_raw_residual": _max(raw_abs),
        "max_abs_calibrated_residual": _max(cal_abs),
        "evidence_vectors_count": len(vectors),
        "uses_root_labels": False,
        "uses_target_config": False,
        "uses_injected_path": False,
        "uses_incident_start_end": False,
        "consumes_blind_evidence": True,
        "consumes_probe_policy": True,
        "consumes_ipw_rls_residuals": True,
        "produces_calibrated_residuals": True,
        "raw_residual_directly_used_for_sparse_inversion": False,
        "residual_clip_value": cfg.residual_clip_value,
        "source": "a7_c_h_t_evidence_channel",
    }
    _write_json(output / "evidence_channel_metadata.json", metadata)
    return {"metadata": metadata, "evidence_vectors": vectors, "evidence_effects": effects, "calibrated_residuals": calibrated}


def evaluate_evidence_channel_debug(output_dir: str, incidents_path: str) -> dict[str, Any]:
    residuals = load_jsonl(Path(output_dir) / "calibrated_residuals.jsonl")
    incidents = load_jsonl(incidents_path)
    node_scores: dict[str, list[float]] = {}
    service_scores: dict[str, list[float]] = {}
    effect_scores: dict[str, list[float]] = {}
    for row in residuals:
        node = str(row.get("node_id") or "")
        service = str(row.get("service") or "")
        node_scores.setdefault(node, []).append(abs(float(row.get("calibrated_residual", 0.0) or 0.0)))
        service_scores.setdefault(service, []).append(abs(float(row.get("calibrated_residual", 0.0) or 0.0)))
        effect_scores.setdefault(node, []).append(abs(float(row.get("evidence_effect", 0.0) or 0.0)))
    node_ranked = [node for node, _ in sorted(((node, _mean(vals)) for node, vals in node_scores.items()), key=lambda item: item[1], reverse=True)]
    service_ranked = [svc for svc, _ in sorted(((svc, _mean(vals)) for svc, vals in service_scores.items()), key=lambda item: item[1], reverse=True)]
    metric_ranks: list[int] = []
    service_ranks: list[int] = []
    root_effects: list[float] = []
    for incident in incidents:
        root_service = incident.get("root_service")
        root_metric = incident.get("root_metric")
        root_node = f"{root_service}.{root_metric}" if root_service and root_metric else None
        if root_node and root_node in node_ranked:
            metric_ranks.append(node_ranked.index(root_node) + 1)
            root_effects.append(_mean(effect_scores.get(root_node, [])))
        if root_service and root_service in service_ranked:
            service_ranks.append(service_ranked.index(root_service) + 1)
    return {
        "debug_only": True,
        "root_metric_calibrated_residual_rank_debug": metric_ranks,
        "root_service_calibrated_residual_rank_debug": service_ranks,
        "root_metric_calibrated_residual_rank_mean": _mean([float(v) for v in metric_ranks]),
        "root_service_calibrated_residual_rank_mean": _mean([float(v) for v in service_ranks]),
        "root_metric_evidence_effect_debug": root_effects,
        "uses_root_labels_for_channel": False,
        "notes": "Root labels are read only after evidence channel outputs are written, for debug ranking.",
    }
