"""Canonical P9 orchestration, unified RCAReport construction, and persistence."""

from __future__ import annotations

import hashlib
import json
import math
import time
from dataclasses import asdict, replace
from pathlib import Path

from proberca.config import ProbeRCAConfig
from proberca.data.schema import AlertEvent, RCAReport, RootCause

from .candidates import build_root_candidates, validate_diagnosis_inputs
from .contracts import (
    DiagnosisCandidate, DiagnosisFingerprintError, DiagnosisInputMismatchError,
    DiagnosisResult, EvidenceChainItem, NodeRootCandidate, PropagatedSymptom,
    PropagationEdgeCandidate, PropagationPath, RCAReportValidationError,
    ShockEdgeCandidate,
)
from .counterfactual import evaluate_counterfactuals
from .paths import build_propagation_paths
from .ranking import rank_candidates
from .symptoms import identify_propagated_symptoms


def _canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _sha(value) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _config_fingerprint(config):
    return _sha({"diagnosis": config.diagnosis.to_dict(), "confidence": asdict(config.confidence)})


def _metric_view(metric_model):
    if isinstance(metric_model, dict) and set(("info", "coefficients", "predictions")) <= set(metric_model):
        return metric_model["info"], list(metric_model["coefficients"]), list(metric_model["predictions"])
    if hasattr(metric_model, "current_bundle") and metric_model.current_bundle is not None:
        return (metric_model.current_bundle.info, metric_model.export_sparse_coefficients(),
                metric_model.predict_window(metric_model.current_bundle.info.healthy_history_cutoff_ns + 1, None))
    raise DiagnosisInputMismatchError("metric_model must expose frozen info, coefficients, and predictions")


def _validate_stage_inputs(problem, result, joint, metric_model, service_model,
                           anomalies, alert, candidate):
    validate_diagnosis_inputs(problem, result, joint)
    if not isinstance(alert, AlertEvent) or alert.state != "hard":
        raise DiagnosisInputMismatchError("P9 requires the formal Hard alert")
    info, coefficients, predictions = _metric_view(metric_model)
    identities = (
        (alert.alert_id, problem.alert_id), (alert.timestamp_ns, problem.timestamp_ns),
        (candidate.alert_id, problem.alert_id), (candidate.candidate_id, problem.candidate_id),
        (candidate.topology_snapshot_id, problem.topology_snapshot_id),
        (candidate.alert_timestamp_ns, problem.timestamp_ns),
        (info.alert_id, problem.alert_id), (info.candidate_id, problem.candidate_id),
        (info.model_snapshot_id, problem.metric_model_snapshot_id),
    )
    if any(left != right for left, right in identities):
        raise DiagnosisInputMismatchError("P9 stage identities do not align")
    if not candidate.rca_eligible or not info.global_ready or not info.frozen \
            or info.lifecycle_state != "FROZEN":
        raise DiagnosisInputMismatchError("P9 requires an eligible candidate and frozen ready P5 model")
    if any(not coefficient.ready for coefficient in coefficients):
        raise DiagnosisInputMismatchError("P9 paths require ready metric coefficients")
    if any(item.timestamp_ns != problem.timestamp_ns for item in anomalies):
        raise DiagnosisInputMismatchError("current node anomalies do not match diagnosis timestamp")
    return info, coefficients, predictions


def _evidence_chain(problem, joint, candidates, paths):
    output = []
    first_candidate = candidates[0].candidate_id if candidates else "audit"
    for candidate in candidates:
        sources = []
        if candidate.variable_block == "node":
            sources = [joint.source_anomaly_record_ids[candidate.variable_indices[0]]]
        elif candidate.variable_block == "shock":
            sources = [joint.shock_variable_refs[index].source_edge_anomaly_record_id
                       for index in candidate.variable_indices]
        else:
            sources = candidate.variable_ids
        output.append(EvidenceChainItem(
            _sha([candidate.candidate_id, "source"]), candidate.candidate_id,
            "source_anomaly", sources, problem.timestamp_ns,
            candidate.weighted_contribution_energy, None, "source_records", {},
        ))
        output.append(EvidenceChainItem(
            _sha([candidate.candidate_id, "component"]), candidate.candidate_id,
            "solver_contribution", candidate.variable_ids, problem.timestamp_ns,
            candidate.weighted_contribution_energy, None, "P8_fitted_component", {},
        ))
        if candidate.counterfactual_status == "evaluated":
            output.append(EvidenceChainItem(
                _sha([candidate.candidate_id, "counterfactual"]), candidate.candidate_id,
                "counterfactual", [candidate.counterfactual_solver_result_id], problem.timestamp_ns,
                candidate.delta_loss, candidate.relative_delta_loss,
                "delete_and_reoptimize", {"solver_iterations": candidate.counterfactual_iterations},
            ))
    for index, item in enumerate(problem.evidence_provenance):
        source = str(item.get("evidence_id") or item.get("source_id") or f"included-{index}")
        output.append(EvidenceChainItem(
            _sha([source, "normalized"]), first_candidate, "normalized_evidence", [source],
            problem.timestamp_ns, float(item.get("normalized_strength", item.get("strength", 0.0))),
            float(item.get("normalized_strength", item.get("strength", 0.0))),
            "P7_normalized_evidence", item,
        ))
    for index, item in enumerate(problem.excluded_evidence):
        source = str(item.get("evidence_id") or item.get("source_id") or f"excluded-{index}")
        output.append(EvidenceChainItem(
            _sha([source, "excluded"]), first_candidate, "excluded_evidence", [source],
            problem.timestamp_ns, 0.0, None, str(item.get("reason_code", "excluded")),
            {**item, "excluded": True},
        ))
    for path in paths:
        output.append(EvidenceChainItem(
            _sha([path.path_id, "path"]), path.root_candidate_id, "propagation_path",
            [path.path_id], problem.timestamp_ns, path.path_score, path.path_score,
            "positive_propagation_support", {"steps": path.steps},
        ))
    return sorted(output, key=lambda item: item.evidence_item_id)


def _diagnosis_payload(diagnosis, problem_fingerprint, result_fingerprint):
    return {
        "problem_fingerprint": problem_fingerprint, "result_fingerprint": result_fingerprint,
        "status": diagnosis.status, "primary_candidate_id": diagnosis.primary_candidate_id,
        "ranked_candidates": [asdict(item) for item in diagnosis.ranked_candidates],
        "symptoms": [asdict(item) for item in diagnosis.symptoms],
        "paths": [asdict(item) for item in diagnosis.paths],
        "evidence_chain": [asdict(item) for item in diagnosis.evidence_chain],
        "ambiguity_reasons": diagnosis.ambiguity_reasons,
        "config_fingerprint": diagnosis.config_fingerprint,
        "quality_issues": diagnosis.quality_issues,
    }


def select_primary_candidate(ranked, config):
    """Apply the configured root gate without forcing an ineligible Top-1."""
    eligible = [item for item in ranked if item.counterfactual_status == "evaluated"
                and item.relative_delta_loss >= config.diagnosis.minimum_relative_counterfactual_delta
                and item.confidence >= config.confidence.weak
                and item.identifiability >= config.diagnosis.minimum_identifiability_threshold
                and item.margin >= config.diagnosis.minimum_margin_for_root
                and item.weighted_contribution_energy > 0]
    primary = eligible[0] if eligible else None
    if primary is not None:
        status = "strong" if (
            primary.confidence >= config.confidence.strong
            and primary.identifiability >= config.diagnosis.strong_identifiability_threshold
        ) else "weak"
        return primary, status, []
    reasons = []
    if not ranked:
        reasons.append("no_active_candidate")
    else:
        top = ranked[0]
        if top.counterfactual_status != "evaluated":
            reasons.append("counterfactual_unavailable")
        if (top.relative_delta_loss is None
                or top.relative_delta_loss < config.diagnosis.minimum_relative_counterfactual_delta):
            reasons.append("low_counterfactual")
        if top.margin is None or top.margin < config.diagnosis.minimum_margin_for_root:
            reasons.append("low_margin")
        if (top.identifiability is None
                or top.identifiability < config.diagnosis.minimum_identifiability_threshold):
            reasons.append("low_identifiability")
        if top.confidence is None or top.confidence < config.confidence.weak:
            reasons.append("low_confidence")
        if top.weighted_contribution_energy == 0:
            reasons.append("zero_candidate_signature")
    return None, "ambiguous", reasons


def diagnose_weighted_solution(weighted_problem, solver_result, joint_system, metric_model,
                               service_model, current_node_anomalies, alert_event,
                               candidate_subgraph, config) -> DiagnosisResult:
    started = time.perf_counter()
    if not isinstance(config, ProbeRCAConfig):
        raise TypeError("config must be ProbeRCAConfig")
    info, coefficients, predictions = _validate_stage_inputs(
        weighted_problem, solver_result, joint_system, metric_model, service_model,
        current_node_anomalies, alert_event, candidate_subgraph)
    candidate_started = time.perf_counter()
    candidates = build_root_candidates(weighted_problem, solver_result, joint_system, config.diagnosis)
    candidate_ms = (time.perf_counter() - candidate_started) * 1000
    cf_started = time.perf_counter()
    candidates = evaluate_counterfactuals(weighted_problem, solver_result, candidates, config)
    counterfactual_ms = (time.perf_counter() - cf_started) * 1000
    symptoms = identify_propagated_symptoms(current_node_anomalies, predictions, None, config.diagnosis)
    targets = {item.node_id for item in symptoms}
    targets.update(item.node_id for item in current_node_anomalies
                   if item.metric_family == "request" and item.signed_z >= config.diagnosis.symptom_anomaly_threshold)
    path_started = time.perf_counter(); paths = []
    with_paths = []
    for candidate in candidates:
        candidate_paths = build_propagation_paths(
            candidate, sorted(targets), coefficients, service_model or [], joint_system, config.diagnosis)
        paths.extend(candidate_paths)
        with_paths.append(replace(candidate, best_path_score=max(
            (path.path_score for path in candidate_paths), default=0.0)))
    path_ms = (time.perf_counter() - path_started) * 1000
    ranked = rank_candidates(with_paths, weighted_problem, joint_system, config)
    primary, status, reasons = select_primary_candidate(ranked, config)
    if primary and primary.variable_block == "node" and not config.diagnosis.include_root_node_as_symptom:
        symptoms = [item for item in symptoms if item.node_id != primary.metadata["node_id"]]
    paths = sorted({item.path_id: item for item in paths}.values(), key=lambda item: item.path_id)
    evidence = _evidence_chain(weighted_problem, joint_system, ranked, paths)
    config_fingerprint = _config_fingerprint(config)
    runtime = {
        "candidate_build_ms": candidate_ms, "counterfactual_total_ms": counterfactual_ms,
        "path_build_ms": path_ms, "report_build_ms": 0.0,
        "total_diagnosis_ms": (time.perf_counter() - started) * 1000,
        "counterfactual_solver_iterations": sum(item.counterfactual_iterations for item in ranked),
    }
    provisional = DiagnosisResult(
        "pending", weighted_problem.problem_id, solver_result.result_id, status,
        primary.candidate_id if primary else None, ranked, symptoms, paths, evidence,
        sorted(set(reasons)), config_fingerprint, "pending", runtime, [],
    )
    fingerprint = _sha(_diagnosis_payload(
        provisional, weighted_problem.problem_fingerprint, solver_result.result_fingerprint))
    result_id = _sha([weighted_problem.problem_id, solver_result.result_id, fingerprint])
    return replace(provisional, diagnosis_result_id=result_id, diagnosis_fingerprint=fingerprint)


def _ranked_dict(candidate):
    return {
        "rank": candidate.rank, "candidate_id": candidate.candidate_id,
        "object_type": candidate.candidate_type, "fault_mode": candidate.fault_mode,
        "edge_subtype": candidate.edge_subtype, "raw_solver_values": candidate.raw_values,
        "contribution_energy": candidate.weighted_contribution_energy,
        "counterfactual_status": candidate.counterfactual_status,
        "delta_loss": candidate.delta_loss, "relative_delta_loss": candidate.relative_delta_loss,
        "counterfactual_support": candidate.counterfactual_support, "margin": candidate.margin,
        "candidate_quality": candidate.candidate_quality, "coherence": candidate.coherence,
        "lag_entropy": candidate.lag_entropy, "best_path_score": candidate.best_path_score,
        "identifiability": candidate.identifiability, "confidence": candidate.confidence,
        "status": candidate.status, "member_variables": candidate.variable_ids,
        "dominant_member": candidate.dominant_member_id, "provenance": candidate.metadata,
    }


def _root(diagnosis):
    if diagnosis.status == "ambiguous" or diagnosis.primary_candidate_id is None:
        return RootCause("ambiguous", None, None, None, "ambiguous", None,
                         ambiguity_reasons=diagnosis.ambiguity_reasons)
    candidate = next(item for item in diagnosis.ranked_candidates
                     if item.candidate_id == diagnosis.primary_candidate_id)
    if candidate.variable_block == "node":
        return RootCause("node", candidate.metadata["service_id"].split("::")[-1],
                         candidate.metadata["metric_name"], None, "self", None,
                         node_id=candidate.metadata["node_id"], service_id=candidate.metadata["service_id"])
    edge_id = candidate.metadata.get("physical_edge_id", candidate.candidate_id)
    return RootCause(
        "edge", None, None, edge_id, "edge", candidate.edge_subtype,
        edge_kind=candidate.edge_kind,
        parent_service_id=candidate.metadata.get("parent_service_id"),
        target_service_id=candidate.metadata.get("target_service_id"),
        relation_types=candidate.metadata.get("relation_types", []),
        physical_edge_id=candidate.metadata.get("physical_edge_id"),
        src_service_id=candidate.metadata.get("src_service_id"),
        dst_service_id=candidate.metadata.get("dst_service_id"),
        protocol=candidate.metadata.get("protocol"),
        dominant_member=candidate.dominant_member_id,
        dominant_metric_name=(candidate.metadata.get("member_metric_names") or [None])[
            candidate.variable_ids.index(candidate.dominant_member_id)] if candidate.variable_block == "shock" else None,
    )


def _report_fingerprint(report):
    payload = report.to_dict(); payload.pop("runtime", None); payload["report_fingerprint"] = None
    return _sha(payload)


def build_rca_report(diagnosis, weighted_problem, solver_result, alert_event, config):
    started = time.perf_counter()
    if diagnosis.problem_id != weighted_problem.problem_id or diagnosis.solver_result_id != solver_result.result_id:
        raise RCAReportValidationError("diagnosis does not match problem/result")
    namespace = weighted_problem.node_variable_ids[0].split("::")[1] if weighted_problem.node_variable_ids else None
    cluster = weighted_problem.node_variable_ids[0].split("::")[0] if weighted_problem.node_variable_ids else None
    counterfactual_runs = sorted(item.counterfactual_solver_result_id for item in diagnosis.ranked_candidates
                                 if item.counterfactual_solver_result_id)
    runtime = dict(diagnosis.runtime); runtime["report_build_ms"] = (time.perf_counter() - started) * 1000
    report = RCAReport(
        "1.0", alert_event.alert_id, weighted_problem.timestamp_ns, alert_event,
        _root(diagnosis), [_ranked_dict(item) for item in diagnosis.ranked_candidates],
        [asdict(item) for item in diagnosis.symptoms], [asdict(item) for item in diagnosis.paths],
        [asdict(item) for item in diagnosis.evidence_chain],
        {
            "diagnosis_status": diagnosis.status, "solver_converged": solver_result.converged,
            "solver_gradient_mapping_norm": solver_result.gradient_mapping_norm,
            "node_coverage": float(min(weighted_problem.node_quality_weights, default=0.0)),
            "edge_coverage": float(min(weighted_problem.edge_quality_weights, default=0.0)),
            "excluded_evidence_count": len(weighted_problem.excluded_evidence),
            "active_candidate_count": len(diagnosis.ranked_candidates),
            "counterfactual_evaluated_count": sum(item.counterfactual_status == "evaluated" for item in diagnosis.ranked_candidates),
            "counterfactual_failed_count": sum(item.counterfactual_status == "counterfactual_unavailable" for item in diagnosis.ranked_candidates),
            "candidate_coherence_summary": max((item.coherence for item in diagnosis.ranked_candidates), default=0.0),
            "missing_paths": [item.candidate_id for item in diagnosis.ranked_candidates if item.best_path_score == 0],
            "ambiguity_reasons": diagnosis.ambiguity_reasons,
        }, runtime, cluster, namespace, weighted_problem.problem_id, solver_result.result_id,
        diagnosis.diagnosis_result_id, counterfactual_runs, None, diagnosis.config_fingerprint,
    )
    return replace(report, report_fingerprint=_report_fingerprint(report))


def _candidate_from_dict(payload):
    cls = {"node": NodeRootCandidate, "propagation": PropagationEdgeCandidate,
           "shock": ShockEdgeCandidate}[payload["variable_block"]]
    return cls(**payload)


def save_diagnosis_result(path, diagnosis):
    output = Path(path); output.mkdir(parents=True, exist_ok=False)
    (output / "metadata.json").write_bytes(_canonical(asdict(diagnosis)))


def load_diagnosis_result(path, problem, result, config):
    try:
        payload = json.loads((Path(path) / "metadata.json").read_text())
        if payload["problem_id"] != problem.problem_id or payload["solver_result_id"] != result.result_id \
                or payload["config_fingerprint"] != _config_fingerprint(config):
            raise DiagnosisFingerprintError("diagnosis context mismatch")
        diagnosis = DiagnosisResult(
            payload["diagnosis_result_id"], payload["problem_id"], payload["solver_result_id"],
            payload["status"], payload["primary_candidate_id"],
            [_candidate_from_dict(item) for item in payload["ranked_candidates"]],
            [PropagatedSymptom(**item) for item in payload["symptoms"]],
            [PropagationPath(**item) for item in payload["paths"]],
            [EvidenceChainItem(**item) for item in payload["evidence_chain"]],
            payload["ambiguity_reasons"], payload["config_fingerprint"],
            payload["diagnosis_fingerprint"], payload["runtime"], payload["quality_issues"],
        )
        computed = _sha(_diagnosis_payload(diagnosis, problem.problem_fingerprint, result.result_fingerprint))
        if computed != diagnosis.diagnosis_fingerprint:
            raise DiagnosisFingerprintError("diagnosis fingerprint mismatch")
        return diagnosis
    except DiagnosisFingerprintError:
        raise
    except Exception as error:
        raise DiagnosisFingerprintError(f"invalid diagnosis serialization: {error}") from error


def save_rca_report(path, report):
    Path(path).write_bytes(_canonical(report.to_dict()))


def load_rca_report(path, problem, result, diagnosis, config):
    try:
        report = RCAReport.from_dict(json.loads(Path(path).read_text()))
        if report.weighted_problem_id != problem.problem_id or report.solver_result_id != result.result_id \
                or report.diagnosis_result_id != diagnosis.diagnosis_result_id \
                or report.config_fingerprint != _config_fingerprint(config):
            raise DiagnosisFingerprintError("report context mismatch")
        if _report_fingerprint(report) != report.report_fingerprint:
            raise DiagnosisFingerprintError("report fingerprint mismatch")
        return report
    except DiagnosisFingerprintError:
        raise
    except Exception as error:
        raise DiagnosisFingerprintError(f"invalid report serialization: {error}") from error
