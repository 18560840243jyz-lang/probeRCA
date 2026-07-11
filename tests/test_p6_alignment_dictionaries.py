from __future__ import annotations

import json
from dataclasses import replace

import numpy as np
import pytest

from proberca.config import ProbeRCAConfig
from proberca.inversion import (
    CandidateModelMismatchError,
    JointSystemSerializationError,
    MissingEdgeResidualError,
    MissingNodeResidualError,
    ResidualAlignmentError,
    ResidualNotReadyError,
    ShockProjectionError,
    load_joint_inversion_system,
    save_joint_inversion_system,
)

from test_p6_joint_system import (
    EDGE_ID,
    NODE_IDS,
    build,
    contribution,
    edge_record,
    hard_alert,
    hard_candidate,
    node_records,
    p6_config,
    predictions,
)


def test_joint_system_preserves_node_prediction_and_anomaly_source_ids():
    system = build()
    assert system.source_prediction_ids == NODE_IDS
    assert system.source_anomaly_record_ids == [
        f"source::{node_id.split('::')[2]}::{node_id.split('::')[3]}" for node_id in NODE_IDS
    ]


@pytest.mark.parametrize("mutation", [
    "candidate_alert", "candidate_timestamp", "candidate_not_eligible",
    "prediction_alert", "prediction_candidate", "prediction_topology",
    "prediction_model", "prediction_timestamp", "node_timestamp",
    "edge_timestamp", "node_cluster", "edge_namespace",
])
def test_formal_alignment_mutations_fail(mutation):
    values = {}
    if mutation == "candidate_alert":
        values["candidate_subgraph"] = replace(hard_candidate(), alert_id="other")
    elif mutation == "candidate_timestamp":
        values["candidate_subgraph"] = replace(hard_candidate(), alert_timestamp_ns=11_000_000_000)
    elif mutation == "candidate_not_eligible":
        candidate = hard_candidate()
        object.__setattr__(candidate, "rca_eligible", False)
        values["candidate_subgraph"] = candidate
    elif mutation.startswith("prediction_"):
        key = mutation.split("_", 1)[1]
        mapping = {
            "alert": {"alert_id": "other"}, "candidate": {"candidate_id": "other"},
            "topology": {"topology_snapshot_id": "other"},
            "model": {"model_snapshot_id": "other"}, "timestamp": {"timestamp_ns": 11_000_000_000},
        }
        changed = replace(predictions()[0], **mapping[key])
        values["metric_predictions"] = [changed, *predictions()[1:]]
    elif mutation == "node_timestamp":
        values["current_node_anomalies"] = [replace(node_records()[0], timestamp_ns=11_000_000_000,
                                                    window_end_ns=11_000_000_000), *node_records()[1:]]
    elif mutation == "edge_timestamp":
        values["current_edge_anomalies"] = [replace(edge_record(), timestamp_ns=11_000_000_000,
                                                    window_end_ns=11_000_000_000)]
    elif mutation == "node_cluster":
        item = node_records()[0]
        object.__setattr__(item, "cluster_id", "other")
        values["current_node_anomalies"] = [item, *node_records()[1:]]
    else:
        item = edge_record()
        object.__setattr__(item, "namespace", "other")
        values["current_edge_anomalies"] = [item]
    with pytest.raises((CandidateModelMismatchError, ResidualNotReadyError, ResidualAlignmentError)):
        build(**values)


@pytest.mark.parametrize("mutation", [
    "unavailable", "not_ready", "not_frozen", "provisional", "bad_sum",
])
def test_prediction_formality_mutations_fail(mutation):
    item = predictions()[0]
    if mutation == "unavailable":
        item = replace(item, available=False, predicted_value=None,
                       unavailable_reason="missing_prediction_feature", contributions=[])
    elif mutation == "not_ready":
        item = replace(item, ready=False)
    elif mutation == "not_frozen":
        item = replace(item, frozen=False)
    elif mutation == "provisional":
        item = replace(item, provisional=True)
    else:
        object.__setattr__(item, "predicted_value", item.predicted_value + 1.0)
    with pytest.raises((MissingNodeResidualError, ResidualAlignmentError)):
        build(metric_predictions=[item, *predictions()[1:]])


def test_unready_node_and_edge_baselines_fail_formal_system():
    node = node_records()[0]
    object.__setattr__(node, "baseline_ready", False)
    with pytest.raises(MissingNodeResidualError):
        build(current_node_anomalies=[node, *node_records()[1:]])
    with pytest.raises(MissingEdgeResidualError):
        build(current_edge_anomalies=[replace(edge_record(), baseline_ready=False)])


@pytest.mark.parametrize("relation", ["same_service", "impact", "host", "resource"])
def test_all_allowed_propagation_relations_create_one_sparse_column(relation):
    values = predictions()
    target = values[1]
    item = contribution(NODE_IDS[1], NODE_IDS[2], relation, -2.0, coefficient=-0.3)
    changed = replace(target, contributions=[item], predicted_value=item.contribution_value)
    system = build(metric_predictions=[values[0], changed, *values[2:]])
    assert system.X_prop.shape[1] == 1
    ref = system.propagation_variable_refs[0]
    assert ref.relation_types == [relation]
    assert ref.learned_coefficient == -0.3 and ref.positive_support == 0.0
    assert system.X_prop[ref.target_row_index, 0] == -2.0


@pytest.mark.parametrize("change", [
    {"edge_metric_name": "tcp.retrans"},
    {"edge_metric_name": "tcp.retrans_rate.extra"},
    {"protocol": "udp"},
])
def test_shock_template_matching_is_exact(change):
    cfg = p6_config()
    template = replace(cfg.shock_projection_templates[0], **change)
    with pytest.raises(ShockProjectionError):
        build(config=replace(cfg, shock_projection_templates=[template]))


@pytest.mark.parametrize("raw_weight", [0.0, -1.0, float("nan"), float("inf")])
def test_shock_projection_weight_must_be_positive_and_finite(raw_weight):
    payload = p6_config().to_dict()
    payload["shock_projection_templates"][0]["projections"][0]["raw_weight"] = raw_weight
    with pytest.raises(ValueError):
        ProbeRCAConfig.from_dict(payload)


def test_duplicate_projection_node_weights_are_merged_before_normalization():
    cfg = p6_config()
    first = cfg.shock_projection_templates[0].projections[0]
    duplicate = replace(first, raw_weight=3.0)
    template = replace(cfg.shock_projection_templates[0], projections=[first, duplicate])
    system = build(config=replace(cfg, shock_projection_templates=[template]))
    weights = system.shock_variable_refs[0].projection_weights
    assert weights == [1.0]
    assert system.X_shock[1, 0] == 1.0 and system.X_shock[4, 0] == 1.0


@pytest.mark.parametrize("corruption", ["version", "fingerprint", "shape", "nnz"])
def test_serialized_metadata_corruption_fails(tmp_path, corruption):
    path = tmp_path / "joint"
    save_joint_inversion_system(path, build())
    metadata_path = path / "metadata.json"
    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    if corruption == "version": payload["format_version"] = "999"
    elif corruption == "fingerprint": payload["structure_fingerprint"] = "0" * 64
    elif corruption == "shape": payload["X_shock_shape"] = [99, 1]
    else: payload["X_prop_nnz"] += 1
    metadata_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(JointSystemSerializationError):
        load_joint_inversion_system(path)


def test_edge_rows_are_after_nodes_and_protocol_identity_is_preserved():
    system = build()
    assert system.edge_row_refs[0].row_index == len(system.node_row_refs)
    assert system.edge_row_refs[0].object_id == EDGE_ID
    assert system.shock_variable_refs[0].protocol == "tcp"
    assert system.shock_variable_refs[0].edge_metric_id == EDGE_ID


def test_quality_is_preserved_without_evidence_adjustment():
    system = build()
    assert np.array_equal(system.node_observation_quality, np.ones(4))
    assert system.edge_observation_quality[0] == pytest.approx(0.72)
    assert system.node_residual[1] == 3.0
