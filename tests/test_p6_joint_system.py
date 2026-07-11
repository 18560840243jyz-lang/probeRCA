from __future__ import annotations

from dataclasses import asdict, replace

import numpy as np
import pytest
from scipy import sparse

import test_p1_data_contracts as p1
from proberca.alerting import UpdateGate
from proberca.baseline import RobustBaselineStore
from proberca.config import BaselineConfig, MetricSignalSpec, ProbeRCAConfig
from proberca.data.schema import CandidateProvenance, EdgeAnomalyRecord
from proberca.inversion import (
    CandidateModelMismatchError,
    MissingEdgeResidualError,
    MissingNodeResidualError,
    ResidualNotReadyError,
    ShockProjectionError,
    SignalKindMismatchError,
    build_joint_inversion_system,
    edge_anomaly_from_p2,
)
from proberca.propagation.metric_model import (
    MetricPropagationContribution,
    MetricPropagationModelInfo,
    MetricPropagationPrediction,
)

from test_p5_history_rules import NS, anomaly, candidate as p5_candidate
from test_p5_metric_ridge import alert as p5_alert


NODE_IDS = [
    "cluster-a::ns::api::cpu.use",
    "cluster-a::ns::api::request.lat",
    "cluster-a::ns::db::cpu.use",
    "cluster-a::ns::db::request.lat",
]
EDGE_ID = "cluster-a::ns::api->db::tcp::tcp.retrans_rate"
PHYSICAL_ID = "cluster-a::ns::api->db::tcp"
SHOCK_ID = "cluster-a::ns::api->db::tcp::shock::tcp.retrans_rate"


def p6_config(**residual_changes):
    payload = p1.valid_config_dict()
    payload["propagation"].update({
        "metric_history_sec": 20,
        "metric_min_training_rows": 3,
        "metric_max_gap_windows": 2,
        "metric_model_cache_size": 2,
        "metric_parent_rules": [{
            "rule_id": "self",
            "enabled": True,
            "target_family": "request",
            "target_metric_names": None,
            "relation_type": "self_history",
            "parent_family": "request",
            "parent_metric_names": None,
            "lags": [1],
            "require_signal_spec": True,
            "provenance_label": "self",
        }],
    })
    payload["residual"] = {
        "signal_kind": "signed_z",
        "require_hard_alert": True,
        "require_rca_eligible": True,
        "require_global_metric_model_ready": True,
        "require_complete_node_rows": True,
        "require_complete_edge_rows": True,
        "max_joint_rows": 100,
        "max_propagation_variables": 100,
        "max_shock_variables": 100,
        "fail_on_overflow": True,
    }
    payload["residual"].update(residual_changes)
    payload["propagation_dictionary"] = {
        "allowed_relation_types": ["same_service", "impact", "host", "resource"],
        "exclude_self_history": True,
        "exclude_call": True,
    }
    payload["shock_projection_templates"] = [{
        "template_id": "tcp-retrans-request",
        "enabled": True,
        "edge_metric_name": "tcp.retrans_rate",
        "protocol": "tcp",
        "projections": [
            {"endpoint_role": "source", "metric_family": "request",
             "metric_names": ["request.lat"], "raw_weight": 2.0},
            {"endpoint_role": "target", "metric_family": "request",
             "metric_names": ["request.lat"], "raw_weight": 1.0},
        ],
    }]
    return ProbeRCAConfig.from_dict(payload)


def hard_alert(timestamp=10 * NS):
    return replace(p5_alert("hard", timestamp, "alert-hard"),
                   trigger_edges=[PHYSICAL_ID],
                   edge_scores={PHYSICAL_ID: 5.0})


def hard_candidate():
    base = p5_candidate("hard", "alert-hard", "candidate-hard")
    provenance = list(base.provenance) + [
        CandidateProvenance(PHYSICAL_ID, "physical_edge", "call_descendant",
                            "cluster-a::ns::api", 1,
                            ["cluster-a::ns::api", "cluster-a::ns::db"], ["call-1"],
                            "top-1", "alert-hard", {"protocol": "tcp"}),
        CandidateProvenance(EDGE_ID, "edge_metric", "observed_edge_metric", PHYSICAL_ID,
                            0, [PHYSICAL_ID], [], "top-1", "alert-hard", {}),
        CandidateProvenance(SHOCK_ID, "shock", "observed_edge_metric", EDGE_ID,
                            0, [PHYSICAL_ID], [], "top-1", "alert-hard", {}),
    ]
    return replace(
        base,
        trigger_edges=[PHYSICAL_ID],
        candidate_edge_metric_ids=[EDGE_ID],
        candidate_shock_ids=[SHOCK_ID],
        physical_edges=[{
            "physical_edge_id": PHYSICAL_ID,
            "src_service_id": "cluster-a::ns::api",
            "dst_service_id": "cluster-a::ns::db",
            "protocol": "tcp",
            "source_relation_id": "call-1",
        }],
        provenance=provenance,
        physical_edge_count=1,
        shock_count=1,
    )


def node_records(timestamp=10 * NS):
    window = timestamp // NS - 1
    values = {
        "cluster-a::ns::api::cpu.use": -1.0,
        "cluster-a::ns::api::request.lat": 4.0,
        "cluster-a::ns::db::cpu.use": 0.0,
        "cluster-a::ns::db::request.lat": 2.5,
    }
    output = []
    for node_id in NODE_IDS:
        service = node_id.split("::")[2]
        metric = node_id.split("::")[3]
        family = "cpu" if metric == "cpu.use" else "request"
        output.append(anomaly(service, metric, family, values[node_id], window,
                              source_alert_state="hard"))
    return output


def source_edge(timestamp=10 * NS):
    return p1.make_edge(
        timestamp_ns=timestamp,
        window_sec=1,
        cluster_id="cluster-a",
        namespace="ns",
        src_service="api",
        dst_service="db",
        protocol="tcp",
        metric_name="tcp.retrans_rate",
        value=10.0,
        unit="ratio",
        sample_count=10,
        coverage=0.8,
        event_loss_rate=0.1,
        scope="service_pair",
        src_pod_uid=None,
        dst_pod_uid=None,
    )


def edge_spec(record):
    return MetricSignalSpec.from_dict({
        "record_type": "edge_metric",
        "metric_family": None,
        "metric_name": record.metric_name,
        "protocol": record.protocol,
        "transform": "identity",
        "polarity": "increase_bad",
        "rare_event_threshold": None,
        "direct_hard": False,
        "z_cap": 6.0,
        "aggregation_output_id": record.stable_id,
    })


def edge_record(timestamp=10 * NS, signed_z=6.0):
    source = source_edge(timestamp)
    cfg = BaselineConfig.from_dict({
        "healthy_history_sec": 10, "min_healthy_windows": 3,
        "min_scale": 0.1, "z_cap": 6.0,
    })
    store = RobustBaselineStore(cfg, 1)
    for index, value in enumerate((1.0, 1.1, 0.9)):
        healthy = replace(source, timestamp_ns=(index + 1) * NS, value=value)
        store.update(healthy, edge_spec(healthy), state="healthy")
    scored = store.score(source, edge_spec(source), timestamp - NS, timestamp).score
    assert scored is not None
    scored = replace(scored, signed_z=signed_z, anomaly=max(signed_z, 0.0))
    gate = UpdateGate(False, False, [], [], False, False, True, True, True, True)
    return edge_anomaly_from_p2(source, scored, gate, edge_spec(source), cfg, 1, "hard")


def model_info(**changes):
    values = dict(
        model_snapshot_id="model-1",
        candidate_id="candidate-hard",
        alert_id="alert-hard",
        lifecycle_state="FROZEN",
        global_ready=True,
        frozen=True,
        training_start_ns=NS,
        training_end_ns=8 * NS,
        healthy_history_cutoff_ns=8 * NS,
        target_count=4,
        ready_target_count=4,
        unready_targets=[],
        candidate_fingerprint="c" * 64,
        topology_fingerprint="t" * 64,
        rules_fingerprint="r" * 64,
        config_fingerprint="f" * 64,
        node_index_fingerprint="n" * 64,
        fit_duration_ms=1.0,
        quality_issues=[],
    )
    values.update(changes)
    return MetricPropagationModelInfo(**values)


def contribution(target, parent, relation, parent_value, coefficient=0.5, lag=1):
    relation_id = {
        "self_history": f"{target.rsplit('::', 1)[0]}::self_history",
        "same_service": f"{target.rsplit('::', 1)[0]}::same_service",
        "impact": "impact-1",
        "host": "host-1",
        "resource": "resource-1",
        "call": "call-1",
    }[relation]
    return MetricPropagationContribution(
        target, parent, lag, coefficient, parent_value, coefficient * parent_value,
        max(coefficient, 0.0), relation, [relation], [relation_id], [f"rule-{relation}"],
    )


def predictions(timestamp=10 * NS, parent_value=2.0):
    by_target = {
        NODE_IDS[0]: [contribution(NODE_IDS[0], NODE_IDS[0], "self_history", 1.0)],
        NODE_IDS[1]: [
            contribution(NODE_IDS[1], NODE_IDS[1], "self_history", 1.0),
            contribution(NODE_IDS[1], NODE_IDS[2], "impact", parent_value, coefficient=0.25),
        ],
        NODE_IDS[2]: [contribution(NODE_IDS[2], NODE_IDS[2], "self_history", 0.0)],
        NODE_IDS[3]: [contribution(NODE_IDS[3], NODE_IDS[3], "self_history", -1.0)],
    }
    output = []
    for target in NODE_IDS:
        items = by_target[target]
        output.append(MetricPropagationPrediction(
            "1.0", "metric_propagation_prediction", timestamp,
            "alert-hard", "candidate-hard", "top-1", "model-1", target,
            sum(item.contribution_value for item in items), None,
            True, True, False, True, None, 1.0, items, "f" * 64,
        ))
    return output


def build(**changes):
    values = dict(
        alert_event=hard_alert(),
        candidate_subgraph=hard_candidate(),
        metric_model_info=model_info(),
        metric_predictions=predictions(),
        current_node_anomalies=node_records(),
        current_edge_anomalies=[edge_record()],
        config=p6_config(),
    )
    values.update(changes)
    return build_joint_inversion_system(**values)


def test_p5_signal_kind_is_explicit_and_signed_z():
    from proberca.inversion.contracts import P5_METRIC_SIGNAL_KIND
    assert P5_METRIC_SIGNAL_KIND == "signed_z"
    assert all(item.signal_kind == "signed_z" for item in node_records())


def test_edge_anomaly_handoff_preserves_real_p2_scores_and_metadata():
    item = edge_record(signed_z=-2.0)
    assert item.record_type == "edge_anomaly"
    assert item.edge_metric_id == EDGE_ID and item.signal_kind == "signed_z"
    assert item.signed_z == -2.0 and item.anomaly_score == 0.0
    assert item.observation_quality == pytest.approx(0.72)
    assert EdgeAnomalyRecord.from_dict(item.to_dict()) == item


def test_node_edge_and_joint_residuals_are_exact_and_signed():
    system = build()
    expected_prediction = np.asarray([0.5, 1.0, 0.0, -0.5])
    expected_actual = np.asarray([-1.0, 4.0, 0.0, 2.5])
    assert np.array_equal(system.actual_node_values, expected_actual)
    assert np.array_equal(system.predicted_node_values, expected_prediction)
    assert np.array_equal(system.node_residual, expected_actual - expected_prediction)
    assert np.array_equal(system.edge_residual, np.asarray([6.0]))
    assert np.array_equal(system.joint_residual,
                          np.concatenate((expected_actual - expected_prediction, [6.0])))
    assert system.node_residual[0] < 0


def test_u_is_sparse_identity_over_nodes_and_zero_over_edges():
    system = build()
    assert sparse.isspmatrix_csr(system.U)
    assert system.U.shape == (5, 4)
    assert np.array_equal(system.U[:4].toarray(), np.eye(4))
    assert np.count_nonzero(system.U[4:].toarray()) == 0
    assert system.U_nnz == 4


def test_x_prop_uses_parent_lag_value_not_coefficient_and_excludes_self():
    system = build()
    assert sparse.issparse(system.X_prop)
    assert len(system.propagation_variable_refs) == 1
    ref = system.propagation_variable_refs[0]
    assert ref.parent_node_id == NODE_IDS[2]
    assert ref.target_node_id == NODE_IDS[1]
    assert ref.learned_coefficient == 0.25
    assert system.X_prop[ref.target_row_index, 0] == 2.0
    assert system.X_prop[:, 0].nnz == 1
    assert np.count_nonzero(system.X_prop[4:].toarray()) == 0


@pytest.mark.parametrize("parent_value", [0.0, -3.0, 2.5])
def test_x_prop_retains_zero_and_signed_parent_columns(parent_value):
    system = build(metric_predictions=predictions(parent_value=parent_value))
    assert system.X_prop.shape[1] == 1
    assert system.X_prop[1, 0] == parent_value
    assert system.propagation_variable_refs[0].parent_value == parent_value


def test_x_shock_has_normalized_node_projection_and_edge_one_hot():
    system = build()
    assert sparse.issparse(system.X_shock)
    assert system.X_shock.shape == (5, 1)
    node_part = system.X_shock[:4, 0].toarray().ravel()
    assert np.linalg.norm(node_part) == pytest.approx(1.0)
    assert node_part[1] == pytest.approx(2 / np.sqrt(5))
    assert node_part[3] == pytest.approx(1 / np.sqrt(5))
    assert system.X_shock[4, 0] == 1.0
    assert 6.0 not in system.X_shock.data


def test_edge_shock_zero_parent_case_has_required_joint_structure():
    system = build(metric_predictions=predictions(parent_value=0.0))
    assert system.X_prop[1, 0] == 0.0
    assert system.X_shock[1, 0] != 0.0
    assert system.X_shock[4, 0] == 1.0
    generated = system.X_shock @ np.asarray([3.0])
    assert generated[1] != 0.0 and generated[4] == 3.0
    assert np.all((system.X_prop @ np.asarray([3.0]))[4:] == 0.0)


@pytest.mark.parametrize("state", ["healthy", "soft", "recovery", "edge_anomaly"])
def test_non_hard_alert_is_rejected(state):
    with pytest.raises(ResidualNotReadyError):
        build(alert_event=replace(hard_alert(), state=state))


@pytest.mark.parametrize("change", [
    {"global_ready": False},
    {"frozen": False},
    {"lifecycle_state": "PREPARED"},
])
def test_unready_or_unfrozen_model_is_rejected(change):
    with pytest.raises(ResidualNotReadyError):
        build(metric_model_info=model_info(**change))


def test_candidate_and_signal_mismatches_fail_fast():
    with pytest.raises(CandidateModelMismatchError):
        build(metric_model_info=model_info(candidate_id="other"))
    changed = node_records()[0]
    object.__setattr__(changed, "signal_kind", "anomaly_score")
    with pytest.raises(SignalKindMismatchError):
        build(current_node_anomalies=[changed, *node_records()[1:]])


def test_missing_formal_rows_fail_instead_of_partial_success():
    with pytest.raises(MissingNodeResidualError):
        build(current_node_anomalies=node_records()[:-1])
    with pytest.raises(MissingEdgeResidualError):
        build(current_edge_anomalies=[])


def test_input_order_does_not_change_matrices_or_fingerprint():
    left = build()
    right = build(
        metric_predictions=list(reversed(predictions())),
        current_node_anomalies=list(reversed(node_records())),
        current_edge_anomalies=list(reversed([edge_record()])),
    )
    assert np.array_equal(left.joint_residual, right.joint_residual)
    assert (left.U != right.U).nnz == 0
    assert (left.X_prop != right.X_prop).nnz == 0
    assert (left.X_shock != right.X_shock).nnz == 0
    assert left.structure_fingerprint == right.structure_fingerprint


def test_joint_system_refs_dimensions_and_solver_eligibility_are_strict():
    system = build()
    assert [item.row_index for item in system.node_row_refs + system.edge_row_refs] == list(range(5))
    assert system.U_shape == [5, 4]
    assert system.X_prop_shape == [5, 1]
    assert system.X_shock_shape == [5, 1]
    assert system.solver_eligible is True
    assert len(system.structure_fingerprint) == 64
    assert len(system.system_id) == 64


def test_shock_projection_requires_matching_candidate_nodes():
    cfg = p6_config()
    projection = replace(cfg.shock_projection_templates[0].projections[0],
                         metric_names=["absent.metric"])
    template = replace(cfg.shock_projection_templates[0], projections=[projection])
    cfg = replace(cfg, shock_projection_templates=[template])
    with pytest.raises(ShockProjectionError):
        build(config=cfg)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_anomaly_fields_are_rejected(value):
    with pytest.raises(ValueError):
        replace(node_records()[0], signed_z=value)
    with pytest.raises(ValueError):
        replace(edge_record(), signed_z=value)
