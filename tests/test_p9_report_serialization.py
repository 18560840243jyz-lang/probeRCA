from __future__ import annotations

import json
from dataclasses import replace

import pytest

import test_p6_joint_system as p6
from proberca.data.schema import RCAReport
from proberca.diagnosis.contracts import (
    DiagnosisFingerprintError, DiagnosisInputMismatchError, RCAReportValidationError,
)
from proberca.diagnosis.report import (
    build_rca_report, diagnose_weighted_solution, load_diagnosis_result,
    load_rca_report, save_diagnosis_result, save_rca_report, select_primary_candidate,
)
from proberca.propagation.metric_model import MetricPropagationCoefficient

from test_p9_candidates_counterfactual import context
from test_p9_ranking_paths import coefficients


def metric_view():
    return {"info": p6.model_info(), "coefficients": coefficients(),
            "predictions": p6.predictions()}


def permissive_config(config):
    diagnosis = replace(
        config.diagnosis, minimum_relative_counterfactual_delta=0.0,
        minimum_margin_for_root=0.0, minimum_identifiability_threshold=0.0,
        strong_identifiability_threshold=0.0,
    )
    confidence = replace(config.confidence, weak=0.0, strong=0.01)
    return replace(config, diagnosis=diagnosis, confidence=confidence)


def diagnose(config_transform=permissive_config):
    weighted, result, joint, config = context(); config = config_transform(config)
    return diagnose_weighted_solution(
        weighted, result, joint, metric_view(), None, p6.node_records(),
        p6.hard_alert(), p6.hard_candidate(), config,
    ), weighted, result, joint, config


def test_canonical_diagnosis_returns_strict_result_and_keeps_all_ranked_candidates():
    diagnosis, _, _, _, _ = diagnose()
    assert diagnosis.status in {"strong", "weak", "ambiguous"}
    assert diagnosis.ranked_candidates
    assert [item.rank for item in diagnosis.ranked_candidates] == list(
        range(1, len(diagnosis.ranked_candidates) + 1))


@pytest.mark.parametrize("mismatch", ["candidate", "alert", "topology", "model", "timestamp"])
def test_canonical_input_alignment_fails_fast(mismatch):
    weighted, result, joint, config = context()
    candidate = p6.hard_candidate(); alert = p6.hard_alert(); view = metric_view()
    if mismatch == "candidate": candidate = replace(candidate, candidate_id="other")
    elif mismatch == "alert": alert = replace(alert, alert_id="other")
    elif mismatch == "topology": candidate = replace(candidate, topology_snapshot_id="other")
    elif mismatch == "model": view = {**view, "info": replace(view["info"], model_snapshot_id="other")}
    else: alert = replace(alert, timestamp_ns=alert.timestamp_ns + 1)
    with pytest.raises(DiagnosisInputMismatchError):
        diagnose_weighted_solution(weighted, result, joint, view, None, p6.node_records(),
                                   alert, candidate, config)


def test_low_counterfactual_produces_ambiguous_without_forced_top1():
    diagnosis, _, _, _, config = diagnose()
    candidate = replace(diagnosis.ranked_candidates[0], relative_delta_loss=0.0)
    strict = replace(config, diagnosis=replace(
        config.diagnosis, minimum_relative_counterfactual_delta=0.1))
    primary, status, reasons = select_primary_candidate([candidate], strict)
    assert primary is None and status == "ambiguous"
    assert reasons == ["low_counterfactual"]


@pytest.mark.parametrize("reason,changes", [
    ("low_margin", {"minimum_margin_for_root": 1.0}),
    ("low_identifiability", {
        "minimum_identifiability_threshold": 1.0,
        "strong_identifiability_threshold": 1.0,
    }),
])
def test_ambiguity_reasons_are_structured(reason, changes):
    diagnosis, _, _, _, _ = diagnose(lambda config: replace(
        config, diagnosis=replace(config.diagnosis, **changes)))
    assert diagnosis.status == "ambiguous" and reason in diagnosis.ambiguity_reasons


def test_weak_status_is_distinct_from_strong():
    diagnosis, _, _, _, config = diagnose(lambda value: replace(
        permissive_config(value), confidence=replace(value.confidence, weak=0.0, strong=1.0)))
    assert diagnosis.status == "weak"


def test_primary_candidate_is_counterfactually_evaluated_and_never_propagated_symptom():
    diagnosis, _, _, _, _ = diagnose()
    if diagnosis.primary_candidate_id:
        primary = next(item for item in diagnosis.ranked_candidates
                       if item.candidate_id == diagnosis.primary_candidate_id)
        assert primary.counterfactual_status == "evaluated"
        assert primary.candidate_type in {"node", "edge"}
    assert all(item.mode == "propagated" for item in diagnosis.symptoms)


@pytest.mark.parametrize("item_type", [
    "source_anomaly", "normalized_evidence",
    "solver_contribution", "counterfactual",
])
def test_evidence_chain_contains_traceable_existing_sources(item_type):
    diagnosis, _, _, _, _ = diagnose()
    matching = [item for item in diagnosis.evidence_chain if item.item_type == item_type]
    assert matching
    assert all(item.source_ids and item.reason_code for item in matching)


def test_evidence_chain_preserves_real_excluded_evidence_without_fabrication():
    weighted, result, joint, config = context()
    weighted = replace(weighted, excluded_evidence=[{
        "evidence_id": "excluded-probe", "reason_code": "low_quality",
    }])
    diagnosis = diagnose_weighted_solution(
        weighted, result, joint, metric_view(), None, p6.node_records(),
        p6.hard_alert(), p6.hard_candidate(), permissive_config(config),
    )
    matching = [item for item in diagnosis.evidence_chain
                if item.item_type == "excluded_evidence"]
    assert len(matching) == 1
    assert matching[0].source_ids == ["excluded-probe"]
    assert matching[0].provenance["excluded"] is True


def test_builds_existing_unified_rca_report_not_parallel_top_level():
    diagnosis, weighted, result, _, config = diagnose()
    report = build_rca_report(diagnosis, weighted, result, p6.hard_alert(), config)
    assert isinstance(report, RCAReport)
    assert report.record_type == "rca_report"
    assert report.weighted_problem_id == weighted.problem_id
    assert report.solver_result_id == result.result_id
    assert report.diagnosis_result_id == diagnosis.diagnosis_result_id


@pytest.mark.parametrize("status", ["strong", "weak", "ambiguous"])
def test_report_uses_one_top_level_schema_for_every_status(status):
    diagnosis, weighted, result, _, config = diagnose()
    if status == "ambiguous":
        diagnosis = replace(diagnosis, status="ambiguous", primary_candidate_id=None,
                            ambiguity_reasons=["low_confidence"])
    elif status == "weak":
        diagnosis = replace(diagnosis, status="weak")
    report = build_rca_report(diagnosis, weighted, result, p6.hard_alert(), config)
    assert set(report.to_dict()) == set(build_rca_report(
        diagnosis, weighted, result, p6.hard_alert(), config).to_dict())


def test_ambiguous_report_root_has_no_node_or_edge_and_has_reasons():
    diagnosis, weighted, result, _, config = diagnose()
    diagnosis = replace(diagnosis, status="ambiguous", primary_candidate_id=None,
                        ambiguity_reasons=["low_confidence"])
    report = build_rca_report(diagnosis, weighted, result, p6.hard_alert(), config)
    assert report.primary_root.kind == "ambiguous"
    assert report.primary_root.node_id is None and report.primary_root.edge_id is None
    assert report.primary_root.ambiguity_reasons == ["low_confidence"]


def test_propagated_only_appears_in_symptoms_not_ranked_root_role():
    diagnosis, weighted, result, _, config = diagnose()
    report = build_rca_report(diagnosis, weighted, result, p6.hard_alert(), config)
    assert all(item["mode"] == "propagated" for item in report.symptoms)
    assert all(item.get("fault_mode") != "propagated" for item in report.ranked_candidates)


def test_report_quality_and_runtime_fields_are_complete():
    diagnosis, weighted, result, _, config = diagnose()
    report = build_rca_report(diagnosis, weighted, result, p6.hard_alert(), config)
    for key in ("diagnosis_status", "solver_converged", "solver_gradient_mapping_norm",
                "active_candidate_count", "counterfactual_evaluated_count",
                "counterfactual_failed_count", "ambiguity_reasons"):
        assert key in report.quality
    for key in ("candidate_build_ms", "counterfactual_total_ms", "path_build_ms",
                "report_build_ms", "total_diagnosis_ms", "counterfactual_solver_iterations"):
        assert key in report.runtime


def test_diagnosis_json_round_trip_and_fingerprint(tmp_path):
    diagnosis, weighted, result, _, config = diagnose()
    path = tmp_path / "diagnosis"
    save_diagnosis_result(path, diagnosis)
    restored = load_diagnosis_result(path, weighted, result, config)
    assert restored.diagnosis_fingerprint == diagnosis.diagnosis_fingerprint
    assert [item.candidate_id for item in restored.ranked_candidates] == [
        item.candidate_id for item in diagnosis.ranked_candidates]


def test_report_json_round_trip_and_fingerprint(tmp_path):
    diagnosis, weighted, result, _, config = diagnose()
    report = build_rca_report(diagnosis, weighted, result, p6.hard_alert(), config)
    path = tmp_path / "report.json"
    save_rca_report(path, report)
    restored = load_rca_report(path, weighted, result, diagnosis, config)
    assert restored.report_fingerprint == report.report_fingerprint
    assert restored.to_dict() == report.to_dict()


@pytest.mark.parametrize("corruption", ["problem", "result", "config", "fingerprint", "candidate"])
def test_diagnosis_corruption_and_context_mismatch_fail_fast(tmp_path, corruption):
    diagnosis, weighted, result, _, config = diagnose()
    path = tmp_path / corruption
    save_diagnosis_result(path, diagnosis)
    metadata = path / "metadata.json"; payload = json.loads(metadata.read_text())
    if corruption == "problem": payload["problem_id"] = "other"
    elif corruption == "result": payload["solver_result_id"] = "other"
    elif corruption == "config": payload["config_fingerprint"] = "f" * 64
    elif corruption == "fingerprint": payload["diagnosis_fingerprint"] = "e" * 64
    else: payload["ranked_candidates"][0]["candidate_id"] = "other"
    metadata.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    with pytest.raises(DiagnosisFingerprintError):
        load_diagnosis_result(path, weighted, result, config)


def test_runtime_does_not_affect_diagnosis_or_report_fingerprint():
    diagnosis, weighted, result, _, config = diagnose()
    changed = replace(diagnosis, runtime={**diagnosis.runtime, "total_diagnosis_ms": 9999})
    assert changed.diagnosis_fingerprint == diagnosis.diagnosis_fingerprint
    report = build_rca_report(diagnosis, weighted, result, p6.hard_alert(), config)
    object.__setattr__(report, "runtime", {**report.runtime, "total_diagnosis_ms": 9999})
    assert report.report_fingerprint == build_rca_report(
        diagnosis, weighted, result, p6.hard_alert(), config).report_fingerprint


@pytest.mark.parametrize("forbidden", [
    "graph_sparse_admm", "IncidentLabel", "paymentservice", "checkoutservice",
    "Online Boutique", "pytest.skip", "pytest.xfail", "TODO",
])
def test_p9_canonical_modules_have_no_forbidden_fallback_or_hardcoding(forbidden):
    from pathlib import Path
    root = Path(__file__).parents[1] / "proberca" / "diagnosis"
    source = "\n".join(path.read_text() for path in root.glob("*.py"))
    assert forbidden.lower() not in source.lower()
