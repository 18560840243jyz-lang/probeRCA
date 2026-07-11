from __future__ import annotations

import ast
import json
from dataclasses import replace
from pathlib import Path

import numpy as np
import pytest
from scipy import sparse

from proberca.inversion import (
    DictionaryOverflowError,
    JointSystemSerializationError,
    ShockProjectionError,
    ShockTemplateConflictError,
    load_joint_inversion_system,
    save_joint_inversion_system,
)

from test_p6_joint_system import build, hard_candidate, p6_config, predictions


def test_sparse_json_npz_round_trip(tmp_path):
    original = build()
    path = tmp_path / "joint"
    save_joint_inversion_system(path, original)
    assert (path / "metadata.json").is_file()
    assert (path / "vectors.npz").is_file()
    assert (path / "U.npz").is_file()
    assert (path / "X_prop.npz").is_file()
    assert (path / "X_shock.npz").is_file()
    restored = load_joint_inversion_system(path)
    assert sparse.isspmatrix_csr(restored.U)
    assert (restored.U != original.U).nnz == 0
    assert (restored.X_prop != original.X_prop).nnz == 0
    assert (restored.X_shock != original.X_shock).nnz == 0
    assert np.array_equal(restored.joint_residual, original.joint_residual)
    assert restored.structure_fingerprint == original.structure_fingerprint


@pytest.mark.parametrize("expected_name,expected_value", [
    ("expected_alert_id", "other-alert"),
    ("expected_candidate_id", "other-candidate"),
    ("expected_model_snapshot_id", "other-model"),
    ("expected_topology_snapshot_id", "other-topology"),
    ("expected_config_fingerprint", "0" * 64),
])
def test_restore_identity_mismatch_fails(tmp_path, expected_name, expected_value):
    path = tmp_path / "joint"
    save_joint_inversion_system(path, build())
    with pytest.raises(JointSystemSerializationError):
        load_joint_inversion_system(path, **{expected_name: expected_value})


@pytest.mark.parametrize("file_name", ["metadata.json", "vectors.npz", "U.npz", "X_prop.npz", "X_shock.npz"])
def test_missing_serialized_component_fails(tmp_path, file_name):
    path = tmp_path / "joint"
    save_joint_inversion_system(path, build())
    (path / file_name).unlink()
    with pytest.raises(JointSystemSerializationError):
        load_joint_inversion_system(path)


def test_fingerprint_and_shape_corruption_fail(tmp_path):
    path = tmp_path / "joint"
    save_joint_inversion_system(path, build())
    metadata = json.loads((path / "metadata.json").read_text(encoding="utf-8"))
    metadata["U_nnz"] -= 1
    (path / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    with pytest.raises(JointSystemSerializationError):
        load_joint_inversion_system(path)


@pytest.mark.parametrize("limit,field", [
    (0, "max_joint_rows"),
    (0, "max_propagation_variables"),
    (0, "max_shock_variables"),
])
def test_config_rejects_nonpositive_limits(limit, field):
    with pytest.raises(ValueError):
        p6_config(**{field: limit})


def test_runtime_overflow_fails_without_truncation():
    with pytest.raises(DictionaryOverflowError):
        build(config=p6_config(max_joint_rows=1))


def test_conflicting_exact_shock_templates_fail():
    cfg = p6_config()
    duplicate = replace(cfg.shock_projection_templates[0], template_id="duplicate")
    cfg = replace(cfg, shock_projection_templates=[cfg.shock_projection_templates[0], duplicate])
    with pytest.raises(ShockTemplateConflictError):
        build(config=cfg)


@pytest.mark.parametrize("forbidden", [
    "IncidentLabel", "evidence_channel", "graph_sparse_admm", "lasso", "fista",
    "paymentservice", "checkoutservice", "online boutique", "np.linalg.inv",
    "np.linalg.lstsq", "pinv(", "TODO", "pytest.skip", "pytest.xfail",
])
def test_p6_production_path_has_no_forbidden_dependency_or_fallback(forbidden):
    root = Path("proberca/inversion")
    source = "\n".join(path.read_text(encoding="utf-8") for path in root.glob("*.py"))
    assert forbidden.lower() not in source.lower()


def test_p6_sources_do_not_use_python_hash_or_dense_primary_storage():
    root = Path("proberca/inversion")
    for path in root.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
        assert not any(isinstance(node.func, ast.Name) and node.func.id == "hash" for node in calls)
        assert not any(isinstance(node, ast.Pass) for node in ast.walk(tree))


@pytest.mark.parametrize("relation", ["self_history", "call"])
def test_excluded_relation_types_do_not_create_propagation_columns(relation):
    values = predictions()
    target = values[1]
    contributions = [replace(target.contributions[0],
        relation_type=relation, relation_types=[relation], relation_ids=[f"{relation}-1"],
        rule_ids=[f"rule-{relation}"])]
    changed = replace(target, contributions=contributions,
                      predicted_value=sum(item.contribution_value for item in contributions))
    system = build(metric_predictions=[values[0], changed, *values[2:]])
    assert system.X_prop.shape[1] == 0


def test_candidate_shock_without_exact_projection_template_fails():
    cfg = replace(p6_config(), shock_projection_templates=[])
    with pytest.raises(ShockProjectionError):
        build(config=cfg)


@pytest.mark.parametrize("seed", list(range(30)))
def test_determinism_under_repeated_input_permutations(seed):
    rng = np.random.default_rng(seed)
    candidate = hard_candidate()
    shuffled_nodes = list(candidate.candidate_node_ids)
    rng.shuffle(shuffled_nodes)
    candidate = replace(candidate, candidate_node_ids=shuffled_nodes)
    left = build()
    right = build(candidate_subgraph=candidate)
    assert left.structure_fingerprint == right.structure_fingerprint
    assert (left.U != right.U).nnz == 0
    assert (left.X_prop != right.X_prop).nnz == 0
    assert (left.X_shock != right.X_shock).nnz == 0
