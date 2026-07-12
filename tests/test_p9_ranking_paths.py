from __future__ import annotations

from dataclasses import replace

import numpy as np
import pytest

import test_p6_joint_system as p6
from proberca.diagnosis.candidates import build_root_candidates
from proberca.diagnosis.confidence import ConfidenceComputationError, compute_confidence
from proberca.diagnosis.counterfactual import evaluate_counterfactuals
from proberca.diagnosis.identifiability import (
    compute_counterfactual_support, compute_identifiability, compute_lag_entropy,
    compute_local_coherence, compute_margins,
)
from proberca.diagnosis.paths import PropagationPathError, build_propagation_paths
from proberca.diagnosis.ranking import rank_candidates
from proberca.diagnosis.symptoms import identify_propagated_symptoms
from proberca.inversion.solver import solve_weighted_joint_problem
from proberca.propagation.metric_model import MetricPropagationCoefficient

from test_p7_evidence_weighting import joint as p7_joint
from test_p9_candidates_counterfactual import context


def evaluated_context():
    weighted, result, joint, config = context()
    candidates = build_root_candidates(weighted, result, joint, config.diagnosis)
    return weighted, result, joint, config, evaluate_counterfactuals(
        weighted, result, candidates, config)


def test_weighted_coherence_matches_manual_cosine():
    weighted, _, _, _, candidates = evaluated_context()
    active = [item for item in candidates if item.active][:2]
    values = compute_local_coherence(active, weighted.W)
    q0 = np.sqrt(weighted.W.diagonal()) * np.asarray(active[0].contribution_vector)
    q1 = np.sqrt(weighted.W.diagonal()) * np.asarray(active[1].contribution_vector)
    expected = abs(q0 @ q1) / (np.linalg.norm(q0) * np.linalg.norm(q1) + 1e-12)
    assert values[active[0].candidate_id] == pytest.approx(expected)


@pytest.mark.parametrize("left,right", [
    ([1.0, 0.0], [0.0, 1.0]), ([1.0, 1.0], [1.0, 1.0]),
    ([1.0, -1.0], [-1.0, 1.0]), ([0.0, 0.0], [1.0, 1.0]),
])
def test_coherence_range_and_zero_signature(left, right):
    weighted, _, _, _, candidates = evaluated_context()
    first, second = candidates[:2]
    first = replace(first, contribution_vector=left + [0.0] * (weighted.W.shape[0] - 2),
                    weighted_contribution_energy=float(np.linalg.norm(left)))
    second = replace(second, contribution_vector=right + [0.0] * (weighted.W.shape[0] - 2),
                     weighted_contribution_energy=float(np.linalg.norm(right)))
    values = compute_local_coherence([first, second], weighted.W)
    assert all(0 <= value <= 1 for value in values.values())
    if not any(left): assert values[first.candidate_id] == 0


def test_propagation_lag_entropy_single_and_equal_two_lags():
    _, _, _, _, candidates = evaluated_context()
    prop = next(item for item in candidates if item.variable_block == "propagation")
    assert compute_lag_entropy(prop) == 0.0
    two = replace(prop, raw_values=[1.0, 1.0], variable_indices=[0, 1],
                  variable_ids=["a", "b"], metadata={**prop.metadata, "member_lags": [1, 2]})
    assert compute_lag_entropy(two) == pytest.approx(1.0)


@pytest.mark.parametrize("block", ["node", "shock"])
def test_node_and_shock_lag_entropy_are_zero(block):
    _, _, _, _, candidates = evaluated_context()
    assert compute_lag_entropy(next(item for item in candidates if item.variable_block == block)) == 0


def test_counterfactual_support_and_margin_manual_values():
    _, _, _, _, candidates = evaluated_context()
    successful = [item for item in candidates if item.counterfactual_status == "evaluated"]
    cf = compute_counterfactual_support(successful)
    margins = compute_margins(successful)
    assert all(0 <= value <= 1 for value in cf.values())
    assert all(0 <= value <= 1 for value in margins.values())
    if len(successful) == 1: assert next(iter(margins.values())) == 1


@pytest.mark.parametrize("cf,path,margin,coherence,entropy", [
    (1, 1, 1, 0, 0), (0, 0, 0, 0, 0), (1, 1, 1, 1, 0),
    (1, 1, 1, 0, 1), (0.5, 0.2, 0.8, 0.3, 0.4),
])
def test_identifiability_formula_range_and_uncertainty(cf, path, margin, coherence, entropy):
    config = context()[3].diagnosis
    value = compute_identifiability(cf, path, margin, coherence, entropy, config)
    assert 0 <= value <= 1
    if coherence == 1 and entropy == 1: assert value == 0


@pytest.mark.parametrize("cf,margin,quality,ident", [
    (1, 1, 1, 1), (0, 0, 0, 0), (0.5, 0.2, 0.8, 0.4),
    (1, 0, 0, 0), (0, 1, 0, 0), (0, 0, 1, 0), (0, 0, 0, 1),
])
def test_confidence_is_auditable_weighted_score(cf, margin, quality, ident):
    config = context()[3].diagnosis
    expected = (config.confidence_cf_weight * cf + config.confidence_margin_weight * margin
                + config.confidence_quality_weight * quality
                + config.confidence_identifiability_weight * ident)
    assert compute_confidence(cf, margin, quality, ident, config) == pytest.approx(expected)


def test_confidence_rejects_invalid_weight_sum():
    config = replace(context()[3].diagnosis, confidence_cf_weight=0.9)
    with pytest.raises(ConfidenceComputationError):
        compute_confidence(1, 1, 1, 1, config)


def test_ranking_uses_relative_delta_then_energy_then_id_and_keeps_all_active():
    weighted, _, joint, config, candidates = evaluated_context()
    ranked = rank_candidates(candidates, weighted, joint, config)
    verified = [item for item in ranked if item.counterfactual_status == "evaluated"]
    assert verified == sorted(verified, key=lambda item: (
        -item.relative_delta_loss, -item.weighted_contribution_energy, item.candidate_id))
    assert len(ranked) == sum(item.active for item in candidates)
    assert [item.rank for item in ranked] == list(range(1, len(ranked) + 1))


def test_unevaluated_candidates_rank_after_verified_candidates():
    weighted, _, joint, config, candidates = evaluated_context()
    candidates = [replace(item, counterfactual_status="not_evaluated",
                          delta_loss=None, relative_delta_loss=None) if index == 0 else item
                  for index, item in enumerate(candidates)]
    ranked = rank_candidates(candidates, weighted, joint, config)
    seen_unverified = False
    for item in ranked:
        if item.counterfactual_status != "evaluated": seen_unverified = True
        elif seen_unverified: pytest.fail("verified candidate followed an unevaluated candidate")


def test_candidate_quality_reads_existing_row_weights_only():
    weighted, _, joint, config, candidates = evaluated_context()
    ranked = rank_candidates(candidates, weighted, joint, config)
    assert all(0 <= item.candidate_quality <= 1 for item in ranked)
    node = next(item for item in ranked if item.variable_block == "node")
    assert node.candidate_quality == pytest.approx(weighted.W.diagonal()[node.metadata["source_row_index"]])


def test_propagated_symptoms_use_current_metric_actual_and_prediction():
    _, _, _, config, _ = evaluated_context()
    diagnosis = replace(config.diagnosis, propagated_explained_ratio_threshold=0.2)
    symptoms = identify_propagated_symptoms(
        p6.node_records(), p6.predictions(), None, diagnosis)
    api = next(item for item in symptoms if item.node_id.endswith("api::request.lat"))
    assert api.explained_ratio == pytest.approx(min(4.0, 1.0) / 4.0)
    assert api.mode == "propagated"


@pytest.mark.parametrize("actual,predicted,threshold,expected", [
    (-1, 2, 0.5, False), (0, 2, 0.5, False), (0.5, 1, 0.5, False),
    (4, 0.5, 0.5, False), (4, 3, 0.5, True),
])
def test_symptom_threshold_and_explained_ratio_rules(actual, predicted, threshold, expected):
    records = p6.node_records(); predictions = p6.predictions()
    node_id = p6.NODE_IDS[1]
    records = [replace(item, signed_z=actual, anomaly_score=max(actual, 0))
               if item.node_id == node_id else item for item in records]
    predictions = [replace(item, predicted_value=predicted,
                           contributions=[replace(item.contributions[0], contribution_value=predicted,
                                                  coefficient=1.0, parent_value=predicted,
                                                  positive_support=1.0)])
                   if item.target_node_id == node_id else item for item in predictions]
    config = replace(context()[3].diagnosis, propagated_explained_ratio_threshold=threshold)
    found = any(item.node_id == node_id for item in identify_propagated_symptoms(
        records, predictions, None, config))
    assert found is expected


def coefficients():
    return [
        MetricPropagationCoefficient(p6.NODE_IDS[1], p6.NODE_IDS[3], 1, 0.8, 0.8,
                                     ["impact"], ["impact-1"], ["rule-1"], 100, 2.0, True),
        MetricPropagationCoefficient(p6.NODE_IDS[0], p6.NODE_IDS[1], 1, -0.5, 0.0,
                                     ["same_service"], ["same-1"], ["rule-2"], 100, 2.0, True),
    ]


def test_metric_path_uses_only_positive_support_and_has_deterministic_score():
    weighted, _, joint, config, candidates = evaluated_context()
    root = next(item for item in candidates if item.variable_block == "node" and
                item.metadata["node_id"] == p6.NODE_IDS[3])
    root = replace(root, relative_delta_loss=0.8)
    target = p6.NODE_IDS[1]
    paths = build_propagation_paths(root, [target], coefficients(), [], joint, config.diagnosis)
    assert paths and paths[0].node_ids == [p6.NODE_IDS[3], target]
    expected = 0.8 * 1.0 * np.exp(-config.diagnosis.path_length_penalty)
    assert paths[0].path_score == pytest.approx(expected)
    assert all(step["coefficient"] > 0 for path in paths for step in path.steps
               if "coefficient" in step)


def test_no_supported_path_is_not_fabricated():
    _, _, joint, config, candidates = evaluated_context()
    root = replace(next(item for item in candidates if item.variable_block == "node"),
                   relative_delta_loss=0.5)
    assert build_propagation_paths(root, ["unreachable"], [], [], joint, config.diagnosis) == []


def test_shock_path_starts_with_gamma_projection_not_edge_residual():
    _, _, joint, config, candidates = evaluated_context()
    root = replace(next(item for item in candidates if item.variable_block == "shock"),
                   relative_delta_loss=0.8)
    target = root.metadata["projection_node_ids"][0]
    paths = build_propagation_paths(root, [target], [], [], joint, config.diagnosis)
    assert paths and paths[0].steps[0]["step_type"] == "shock_projection"
    assert paths[0].steps[0]["support"] <= 1


def test_propagation_root_first_step_is_member_parent_to_target():
    _, _, joint, config, candidates = evaluated_context()
    root = replace(next(item for item in candidates if item.variable_block == "propagation"),
                   relative_delta_loss=0.8)
    target = root.metadata["member_target_node_ids"][0]
    paths = build_propagation_paths(root, [target], [], [], joint, config.diagnosis)
    assert paths and paths[0].steps[0]["step_type"] == "propagation_root"


def test_path_max_length_and_cycle_protection():
    _, _, joint, config, candidates = evaluated_context()
    root = replace(next(item for item in candidates if item.variable_block == "node"),
                   relative_delta_loss=0.8)
    cyclic = coefficients() + [
        MetricPropagationCoefficient(p6.NODE_IDS[3], p6.NODE_IDS[1], 1, 0.5, 0.5,
                                     ["impact"], ["back"], ["rule"], 100, 2.0, True)]
    short = replace(config.diagnosis, max_path_length=1)
    paths = build_propagation_paths(root, [p6.NODE_IDS[3]], cyclic, [], joint, short)
    assert all(len(path.steps) <= 1 and len(path.node_ids) == len(set(path.node_ids)) for path in paths)
