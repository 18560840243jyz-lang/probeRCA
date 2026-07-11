from __future__ import annotations

import ast
import json
from dataclasses import replace
from pathlib import Path

import numpy as np
import pytest
from scipy import sparse

import test_p1_data_contracts as p1
import test_p6_joint_system as p6
from proberca.config import ProbeRCAConfig
from proberca.data.schema import BurstEventRecord, EvidenceObservationRecord
from proberca.evidence.aggregation import EvidenceAlignmentError, aggregate_evidence
from proberca.evidence.observations import EvidenceTimeWindowError
from proberca.inversion.penalties import compute_penalties
from proberca.inversion.quality import ObservationWeightResult, build_observation_weights
from proberca.inversion.weighted_problem import (
    WeightedProblemDimensionError,
    WeightedProblemFingerprintError,
    WeightedProblemSerializationError,
    build_weighted_joint_problem,
    load_weighted_joint_problem,
    save_weighted_joint_problem,
)

from test_p7_evidence_weighting import NS, config, evidence, joint, topology_store


def training_rows():
    return {p6.NODE_IDS[1]: [2 * NS, 7 * NS]}


def evidence_set():
    return [
        evidence(evidence_id="node-a", target_type="node", target_id=p6.NODE_IDS[1],
                 channel="futex", strength=0.8, quality=0.5, reliability=0.5),
        evidence(evidence_id="shock-a", target_type="shock", target_id=p6.SHOCK_ID,
                 channel="tcp-rto", strength=0.9, quality=1.0, reliability=1.0),
    ]


def problem(records=None, cfg=None, cutoff=12 * NS):
    return build_weighted_joint_problem(
        joint(), evidence_set() if records is None else records, topology_store(),
        training_rows(), cfg or config(), cutoff,
    )


def test_weighted_problem_is_complete_and_preserves_p6_exactly():
    system = joint()
    before_r = system.joint_residual.copy()
    before_u = system.U.copy(); before_prop = system.X_prop.copy(); before_shock = system.X_shock.copy()
    result = build_weighted_joint_problem(
        system, evidence_set(), topology_store(), training_rows(), config(), 12 * NS
    )
    assert result.record_type == "weighted_joint_inversion_problem"
    assert result.solver_eligible and not hasattr(result, "solution")
    assert result.node_evidence_h[1] == pytest.approx(0.2)
    assert result.shock_evidence_h[0] == pytest.approx(0.9)
    assert result.propagation_evidence_h[0] == pytest.approx(0.5)
    assert sparse.isspmatrix_csr(result.W)
    assert np.array_equal(result.joint_residual, before_r)
    assert (result.U != before_u).nnz == (result.X_prop != before_prop).nnz == 0
    assert (result.X_shock != before_shock).nnz == 0
    assert result.p6_structure_fingerprint == system.structure_fingerprint


def test_weighted_problem_round_trip_reuses_verified_p6_matrices(tmp_path):
    system = joint(); original = build_weighted_joint_problem(
        system, evidence_set(), topology_store(), training_rows(), config(), 12 * NS)
    path = tmp_path / "weighted"
    save_weighted_joint_problem(path, original)
    assert sorted(item.name for item in path.iterdir()) == ["W.npz", "arrays.npz", "metadata.json"]
    restored = load_weighted_joint_problem(
        path, system, expected_config_fingerprint=original.config_fingerprint,
        expected_evidence_fingerprint=original.evidence_fingerprint,
        expected_analysis_cutoff_ns=12 * NS,
    )
    assert np.array_equal(restored.joint_residual, original.joint_residual)
    assert (restored.W != original.W).nnz == 0
    assert restored.node_groups == original.node_groups
    assert restored.problem_fingerprint == original.problem_fingerprint
    assert restored.U is system.U and restored.X_prop is system.X_prop and restored.X_shock is system.X_shock


def test_incomplete_group_partition_is_rejected():
    result = problem()
    with pytest.raises(WeightedProblemDimensionError):
        replace(result, node_groups=[])


def test_modified_p6_matrix_is_rejected_by_structure_fingerprint():
    system = joint()
    system.U.data[0] = 2.0
    with pytest.raises(WeightedProblemFingerprintError):
        build_weighted_joint_problem(
            system, evidence_set(), topology_store(), training_rows(), config(), 12 * NS
        )


@pytest.mark.parametrize("seed", list(range(24)))
def test_evidence_input_order_does_not_change_outputs_or_fingerprints(seed):
    records = evidence_set() + [
        evidence(evidence_id="node-b", target_type="node", target_id=p6.NODE_IDS[1],
                 channel="sched", strength=0.3, quality=1.0, reliability=1.0)
    ]
    shuffled = list(records)
    np.random.default_rng(seed).shuffle(shuffled)
    left, right = problem(records), problem(shuffled)
    assert np.array_equal(left.node_evidence_h, right.node_evidence_h)
    assert left.evidence_fingerprint == right.evidence_fingerprint
    assert left.problem_fingerprint == right.problem_fingerprint


@pytest.mark.parametrize("section,field,value", [
    ("evidence", "max_age_windows", -1),
    ("evidence", "min_observation_quality", -0.1),
    ("evidence", "min_observation_quality", 1.1),
    ("evidence", "require_independent_from_residual", False),
    ("evidence", "channel_aggregation", "sum"),
    ("quality", "quality_weight_floor", 0.0),
    ("quality", "quality_weight_floor", 1.1),
    ("penalties", "residual_scale_floor", 0.0),
    ("penalties", "c_u", 0.0), ("penalties", "c_delta", 0.0),
    ("penalties", "c_xi", 0.0), ("penalties", "eta_v", -1.0),
    ("penalties", "eta_p", -1.0), ("penalties", "eta_s", -1.0),
    ("penalties", "rho_v", -1.0), ("penalties", "rho_p", -1.0),
    ("penalties", "rho_s", -1.0), ("penalties", "rho_m", -1.0),
    ("penalties", "group_ratio_u", -1.0),
    ("penalties", "group_ratio_delta", -1.0),
    ("penalties", "group_ratio_xi", -1.0),
])
def test_p7_config_rejects_invalid_values(section, field, value):
    payload = config().to_dict(); payload[section][field] = value
    with pytest.raises((TypeError, ValueError)):
        ProbeRCAConfig.from_dict(payload)


@pytest.mark.parametrize("mutation", [
    "empty_id", "empty_channel", "empty_sources", "duplicate_sources",
    "bad_target_type", "bad_source_type", "timestamp_before_window",
    "timestamp_at_end", "timestamp_after_cutoff", "bad_cluster", "bad_namespace",
    "empty_calibration", "non_boolean_independence",
])
def test_evidence_contract_adversarial_mutations(mutation):
    item = evidence()
    changes = {
        "empty_id": {"evidence_id": ""}, "empty_channel": {"channel_id": ""},
        "empty_sources": {"source_record_ids": []},
        "duplicate_sources": {"source_record_ids": ["a", "a"]},
        "bad_target_type": {"target_type": "propagation"},
        "bad_source_type": {"source_type": "label"},
        "timestamp_before_window": {"timestamp_ns": 8 * NS},
        "timestamp_at_end": {"timestamp_ns": 12 * NS},
        "timestamp_after_cutoff": {"timestamp_ns": 13 * NS, "evidence_window_end_ns": 14 * NS},
        "bad_cluster": {"cluster_id": ""}, "bad_namespace": {"namespace": ""},
        "empty_calibration": {"provenance": {"calibration_id": ""}},
        "non_boolean_independence": {"independent_from_residual": 1},
    }[mutation]
    with pytest.raises((TypeError, ValueError)):
        replace(item, **changes)


def test_unknown_target_and_raw_burst_record_are_rejected():
    with pytest.raises(EvidenceAlignmentError):
        aggregate_evidence(joint(), [evidence(target_id="cluster-a::ns::api::missing")], config(), 12 * NS)
    with pytest.raises(TypeError):
        aggregate_evidence(joint(), [p1.make_burst()], config(), 12 * NS)


def test_cutoff_changes_evidence_and_problem_fingerprints():
    left = problem(cutoff=12 * NS)
    records = [replace(item, analysis_cutoff_ns=13 * NS,
                       evidence_window_end_ns=13 * NS) for item in evidence_set()]
    right = problem(records, cutoff=13 * NS)
    assert left.evidence_fingerprint != right.evidence_fingerprint
    assert left.problem_fingerprint != right.problem_fingerprint


def test_stale_evidence_is_explicitly_excluded():
    cfg = config(); cfg = replace(cfg, evidence=replace(cfg.evidence, max_age_windows=0))
    stale = replace(evidence(), timestamp_ns=9 * NS)
    result = aggregate_evidence(joint(), [stale], cfg, 12 * NS)
    assert result.node_h[1] == 0.0
    assert result.excluded_evidence[0]["reason_code"] == "stale_evidence"


def penalty_result(node_h, prop_h, shock_h, quality=None, cfg=None):
    system = joint(); quality = quality or build_observation_weights(system, config().quality)
    return compute_penalties(system, np.asarray(node_h), np.asarray(prop_h), np.asarray(shock_h),
                             quality, (cfg or config()).penalties)


def test_effective_penalty_monotonicity():
    zero = penalty_result([0, 0, 0, 0], [0], [0])
    node = penalty_result([0, 0.8, 0, 0], [0], [0])
    prop = penalty_result([0, 0, 0, 0], [0.8], [0])
    shock = penalty_result([0, 0, 0, 0], [0], [0.8])
    assert node.lambda_u_effective[1] < zero.lambda_u_effective[1]
    assert node.lambda_delta_effective[0] > zero.lambda_delta_effective[0]
    assert prop.lambda_u_effective[1] > zero.lambda_u_effective[1]
    assert prop.lambda_delta_effective[0] < zero.lambda_delta_effective[0]
    assert shock.lambda_u_effective[1] > zero.lambda_u_effective[1]
    assert shock.lambda_delta_effective[0] > zero.lambda_delta_effective[0]
    assert shock.lambda_xi_effective[0] < zero.lambda_xi_effective[0]


def test_lower_quality_never_lowers_any_effective_penalty():
    system = joint(); high = build_observation_weights(system, config().quality)
    low_node = np.full(4, 0.2); low_edge = np.asarray([0.2]); low_joint = np.concatenate((low_node, low_edge))
    low = ObservationWeightResult(low_node, low_edge, low_joint,
                                  sparse.diags(low_joint, format="csr"))
    left = compute_penalties(system, np.zeros(4), np.zeros(1), np.zeros(1), high, config().penalties)
    right = compute_penalties(system, np.zeros(4), np.zeros(1), np.zeros(1), low, config().penalties)
    assert np.all(right.lambda_u_effective >= left.lambda_u_effective)
    assert np.all(right.lambda_delta_effective >= left.lambda_delta_effective)
    assert np.all(right.lambda_xi_effective >= left.lambda_xi_effective)


@pytest.mark.parametrize("corruption", [
    "version", "joint_id", "p6_fingerprint", "problem_fingerprint",
    "config_fingerprint", "evidence_fingerprint", "cutoff", "missing_w",
])
def test_weighted_problem_corruption_and_mismatch_fail(tmp_path, corruption):
    system = joint(); original = build_weighted_joint_problem(
        system, evidence_set(), topology_store(), training_rows(), config(), 12 * NS)
    path = tmp_path / "weighted"; save_weighted_joint_problem(path, original)
    if corruption == "missing_w":
        (path / "W.npz").unlink()
    else:
        meta_path = path / "metadata.json"
        payload = json.loads(meta_path.read_text())
        key = {
            "version": "format_version", "joint_id": "joint_system_id",
            "p6_fingerprint": "p6_structure_fingerprint",
            "problem_fingerprint": "problem_fingerprint",
            "config_fingerprint": "config_fingerprint",
            "evidence_fingerprint": "evidence_fingerprint", "cutoff": "analysis_cutoff_ns",
        }[corruption]
        payload[key] = "broken" if key != "analysis_cutoff_ns" else 1
        meta_path.write_text(json.dumps(payload))
    with pytest.raises((WeightedProblemSerializationError, WeightedProblemFingerprintError)):
        load_weighted_joint_problem(
            path, system, expected_config_fingerprint=original.config_fingerprint,
            expected_evidence_fingerprint=original.evidence_fingerprint,
            expected_analysis_cutoff_ns=12 * NS,
        )


@pytest.mark.parametrize("forbidden", [
    "evidence_channel", "calibrate_residuals", "graph_sparse_admm", "fista",
    "lasso", "incidentlabel", "paymentservice", "checkoutservice",
    "online boutique", "fault_mode", "root_cause", "counterfactual",
    "ranking", "np.linalg.solve", "np.linalg.lstsq", "pinv(", "import pickle",
    "pytest.skip", "pytest.xfail", "TODO",
])
def test_p7_production_path_has_no_forbidden_solver_or_hardcoding(forbidden):
    files = [
        Path("proberca/evidence/observations.py"), Path("proberca/evidence/aggregation.py"),
        Path("proberca/evidence/propagation_support.py"), Path("proberca/inversion/quality.py"),
        Path("proberca/inversion/penalties.py"), Path("proberca/inversion/groups.py"),
        Path("proberca/inversion/weighted_problem.py"),
    ]
    source = "\n".join(path.read_text() for path in files)
    assert forbidden.lower() not in source.lower()


def test_p7_production_path_has_no_pass_or_python_hash():
    files = [*Path("proberca/evidence").glob("observations.py"),
             *Path("proberca/evidence").glob("aggregation.py"),
             *Path("proberca/evidence").glob("propagation_support.py"),
             *Path("proberca/inversion").glob("quality.py"),
             *Path("proberca/inversion").glob("penalties.py"),
             *Path("proberca/inversion").glob("groups.py"),
             *Path("proberca/inversion").glob("weighted_problem.py")]
    for path in files:
        tree = ast.parse(path.read_text())
        assert not any(isinstance(node, ast.Pass) for node in ast.walk(tree))
        assert not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                       and node.func.id == "hash" for node in ast.walk(tree))
