from __future__ import annotations

from dataclasses import asdict, replace
from pathlib import Path

import numpy as np
import pytest

import test_p6_joint_system as p6
from proberca.diagnosis.contracts import (
    CandidateConstructionError, CandidateContributionError, CandidateOverflowError,
    ConfidenceComputationError, CounterfactualNegativeDeltaError,
    CounterfactualProblemError, CounterfactualSolverError, DiagnosisFingerprintError,
    DiagnosisInputMismatchError, DiagnosisSerializationError, IdentifiabilityError,
    PropagationPathError, RCAReportValidationError, SolverResultNotUsableError,
    SymptomAlignmentError,
)
from proberca.diagnosis.report import (
    build_rca_report, diagnose_weighted_solution, select_primary_candidate,
)

from test_p9_candidates_counterfactual import context
from test_p9_ranking_paths import coefficients
from test_p9_report_serialization import metric_view, permissive_config


def _diagnose():
    weighted, result, joint, config = context()
    config = permissive_config(config)
    diagnosis = diagnose_weighted_solution(
        weighted, result, joint,
        {"info": p6.model_info(), "coefficients": coefficients(),
         "predictions": p6.predictions()},
        None, p6.node_records(), p6.hard_alert(), p6.hard_candidate(), config,
    )
    return diagnosis, weighted, result, joint, config


@pytest.mark.parametrize("error_type", [
    DiagnosisInputMismatchError, SolverResultNotUsableError,
    CandidateConstructionError, CandidateOverflowError, CounterfactualProblemError,
    CounterfactualSolverError, CounterfactualNegativeDeltaError,
    CandidateContributionError, SymptomAlignmentError, PropagationPathError,
    IdentifiabilityError, ConfidenceComputationError, RCAReportValidationError,
    DiagnosisSerializationError, DiagnosisFingerprintError,
])
def test_p9_errors_are_explicit_value_errors(error_type):
    error = error_type("context")
    assert isinstance(error, ValueError)
    assert str(error) == "context"


@pytest.mark.parametrize("field,value", [
    ("confidence_cf_weight", -0.1),
    ("confidence_margin_weight", -0.1),
    ("confidence_quality_weight", -0.1),
    ("ident_cf_weight", -0.1),
    ("ident_path_weight", -0.1),
    ("ident_margin_weight", -0.1),
    ("ident_coherence_weight", -0.1),
    ("ident_lag_entropy_weight", -0.1),
    ("minimum_path_edge_support", 1.1),
    ("minimum_margin_for_root", -0.1),
])
def test_diagnosis_rejects_invalid_weight_or_probability(field, value):
    _, _, _, _, config = _diagnose()
    invalid = replace(config.diagnosis, **{field: value})
    with pytest.raises((TypeError, ValueError)):
        invalid.validate()


@pytest.mark.parametrize("block,kind,subtype", [
    ("node", "node", None),
    ("propagation", "edge", "propagated-edge"),
    ("shock", "edge", "exogenous-edge-shock"),
])
def test_unified_report_root_schema_is_driven_by_candidate_block(block, kind, subtype):
    diagnosis, weighted, result, _, config = _diagnose()
    candidate = next(item for item in diagnosis.ranked_candidates
                     if item.variable_block == block)
    diagnosis = replace(diagnosis, status="weak", primary_candidate_id=candidate.candidate_id)
    report = build_rca_report(diagnosis, weighted, result, p6.hard_alert(), config)
    assert report.primary_root.kind == kind
    assert report.primary_root.edge_subtype == subtype
    if kind == "node":
        assert report.primary_root.node_id and report.primary_root.edge_id is None
    else:
        assert report.primary_root.edge_id and report.primary_root.node_id is None


@pytest.mark.parametrize("field", [
    "joint_residual", "node_evidence_h", "propagation_evidence_h",
    "shock_evidence_h", "lambda_u_effective", "lambda_delta_effective",
    "lambda_xi_effective", "u_values", "delta_values", "xi_values",
])
def test_canonical_diagnosis_does_not_mutate_p6_p7_or_p8_numeric_inputs(field):
    weighted, result, joint, config = context()
    owner = result if field in {"u_values", "delta_values", "xi_values"} else weighted
    before = np.asarray(getattr(owner, field), dtype=float).copy()
    diagnose_weighted_solution(
        weighted, result, joint, metric_view(), None, p6.node_records(),
        p6.hard_alert(), p6.hard_candidate(), permissive_config(config),
    )
    assert np.array_equal(np.asarray(getattr(owner, field), dtype=float), before)


def test_candidate_provenance_is_strict_json_data_not_runtime_tuples():
    diagnosis, _, _, _, _ = _diagnose()

    def check(value):
        if isinstance(value, dict):
            return all(isinstance(key, str) and check(item) for key, item in value.items())
        if isinstance(value, list):
            return all(check(item) for item in value)
        return value is None or type(value) in {str, bool, int, float}

    assert all(check(asdict(candidate)) for candidate in diagnosis.ranked_candidates)


@pytest.mark.parametrize("change,reason", [
    ({"relative_delta_loss": 0.0}, "low_counterfactual"),
    ({"margin": 0.0}, "low_margin"),
    ({"identifiability": 0.0}, "low_identifiability"),
    ({"confidence": 0.0}, "low_confidence"),
])
def test_root_gate_reports_each_ineligibility_reason_without_forced_top1(change, reason):
    diagnosis, _, _, _, config = _diagnose()
    candidate = replace(diagnosis.ranked_candidates[0], **change)
    diagnosis_config = replace(
        config.diagnosis,
        minimum_relative_counterfactual_delta=0.1,
        minimum_margin_for_root=0.1,
        minimum_identifiability_threshold=0.1,
        strong_identifiability_threshold=max(
            config.diagnosis.strong_identifiability_threshold, 0.1),
    )
    confidence = replace(config.confidence, weak=0.1)
    primary, status, reasons = select_primary_candidate(
        [candidate], replace(config, diagnosis=diagnosis_config, confidence=confidence))
    assert primary is None and status == "ambiguous"
    assert reason in reasons


@pytest.mark.parametrize("forbidden", [
    "graph_sparse_admm", "evidence_channel", "sklearn", "lasso",
    "incidentlabel", "paymentservice", "checkoutservice", "online boutique",
    "numpy.linalg.lstsq", "numpy.linalg.pinv", ".toarray(", ".todense(",
    "pytest.skip", "pytest.xfail", "todo", "\n    pass\n",
])
def test_p9_production_has_no_forbidden_solver_fallback_hardcoding_or_stub(forbidden):
    root = Path(__file__).parents[1] / "proberca" / "diagnosis"
    source = "\n".join(path.read_text() for path in sorted(root.glob("*.py"))).lower()
    assert forbidden not in source
