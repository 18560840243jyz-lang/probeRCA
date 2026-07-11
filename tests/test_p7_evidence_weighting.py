from __future__ import annotations

from dataclasses import asdict, replace

import numpy as np
import pytest
from scipy import sparse

import test_p1_data_contracts as p1
import test_p6_joint_system as p6
from proberca.config import ProbeRCAConfig
from proberca.data.schema import EvidenceObservationRecord, TopologyEdge, TopologySnapshot
from proberca.evidence.aggregation import EvidenceConflictError, aggregate_evidence
from proberca.evidence.propagation_support import compute_propagation_support
from proberca.inversion.groups import build_group_partitions
from proberca.inversion.penalties import compute_penalties
from proberca.inversion.quality import build_observation_weights
from proberca.topology import TopologyStore


NS = 1_000_000_000
IMPACT_ID = "cluster-a::ns::db->api::impact"


def config(**penalty_changes):
    payload = p1.valid_config_dict()
    payload["evidence"] = {
        "max_age_windows": 20,
        "min_observation_quality": 0.0,
        "require_independent_from_residual": True,
        "channel_aggregation": "max_then_noisy_or",
    }
    payload["quality"] = {"quality_weight_floor": 0.2}
    payload["penalties"] = {
        "residual_scale_floor": 0.1,
        "c_u": 1.0, "c_delta": 1.5, "c_xi": 2.0,
        "eta_v": 1.0, "eta_p": 1.0, "eta_s": 1.0,
        "rho_v": 1.0, "rho_p": 1.0, "rho_s": 1.0, "rho_m": 1.0,
        "group_ratio_u": 0.2, "group_ratio_delta": 0.3, "group_ratio_xi": 0.4,
    }
    payload["penalties"].update(penalty_changes)
    return ProbeRCAConfig.from_dict(payload)


def evidence(target_type="node", target_id=p6.NODE_IDS[1], evidence_id="ev-1",
             channel="sched", strength=0.8, quality=0.5, reliability=0.5,
             source_records=None, source_objects=None, independent=True,
             timestamp=10 * NS, cutoff=12 * NS):
    return EvidenceObservationRecord(
        schema_version="1.0", evidence_id=evidence_id, timestamp_ns=timestamp,
        evidence_window_start_ns=9 * NS, evidence_window_end_ns=12 * NS,
        analysis_cutoff_ns=cutoff, cluster_id="cluster-a", namespace="ns",
        target_type=target_type, target_id=target_id, channel_id=channel,
        source_type="external_observer", normalized_strength=strength,
        observation_quality=quality, reliability_weight=reliability,
        source_record_ids=source_records or [f"record-{evidence_id}"],
        source_object_ids=source_objects or [f"object-{evidence_id}"],
        independent_from_residual=independent,
        provenance={"calibration_id": "cal-1"}, config_fingerprint="e" * 64,
    )


def joint(parent_value=2.0):
    values = p6.predictions(parent_value=parent_value)
    target = values[1]
    changed = replace(target.contributions[1], relation_ids=[IMPACT_ID])
    target = replace(target, contributions=[target.contributions[0], changed])
    return p6.build(metric_predictions=[values[0], target, *values[2:]])


def topology_store(second_present=False):
    def snapshot(snapshot_id, start, end, present):
        return TopologySnapshot(
            schema_version="1.0", snapshot_id=snapshot_id,
            valid_from_ns=start, valid_to_ns=end, cluster_id="cluster-a",
            services=["ns::api", "ns::db"],
            call_edges=([TopologyEdge("db", "api", "impact", "ns", "ns")]
                        if present else []),
            host_edges=[], resource_edges=[],
        )
    return TopologyStore([
        snapshot("top-a", 0, 5 * NS, True),
        snapshot("top-b", 5 * NS, 10 * NS, second_present),
    ])


def test_evidence_record_is_strict_and_round_trips():
    item = evidence()
    assert item.record_type == "evidence_observation"
    assert EvidenceObservationRecord.from_dict(item.to_dict()) == item


@pytest.mark.parametrize("field,value", [
    ("normalized_strength", -0.1), ("normalized_strength", 1.1),
    ("observation_quality", -0.1), ("observation_quality", 1.1),
    ("reliability_weight", -0.1), ("reliability_weight", 1.1),
])
def test_evidence_probability_bounds(field, value):
    with pytest.raises(ValueError):
        replace(evidence(), **{field: value})


def test_channel_max_then_noisy_or_is_exact_and_duplicate_idempotent():
    system = joint()
    records = [
        evidence(evidence_id="a", channel="one", strength=0.8, quality=0.5, reliability=0.5),
        evidence(evidence_id="b", channel="one", strength=1.0, quality=0.5, reliability=0.5),
        evidence(evidence_id="c", channel="two", strength=0.5, quality=1.0, reliability=1.0),
    ]
    result = aggregate_evidence(system, [*records, records[0]], config(), 12 * NS)
    expected = 1.0 - (1.0 - 0.25) * (1.0 - 0.5)
    assert result.node_h[1] == pytest.approx(expected)
    assert result.shock_h[0] == 0.0


@pytest.mark.parametrize("strength,quality,reliability", [
    (0.0, 1.0, 1.0), (0.1, 1.0, 1.0), (0.5, 1.0, 1.0),
    (1.0, 1.0, 1.0), (0.8, 0.5, 1.0), (0.8, 1.0, 0.5),
    (0.8, 0.5, 0.5), (0.3, 0.2, 0.9), (0.9, 0.1, 0.4),
    (0.25, 0.75, 0.8),
])
def test_single_channel_strength_matches_manual_product(strength, quality, reliability):
    item = evidence(strength=strength, quality=quality, reliability=reliability)
    result = aggregate_evidence(joint(), [item], config(), 12 * NS)
    assert result.node_h[1] == pytest.approx(strength * quality * reliability)


def test_conflicting_duplicate_evidence_id_fails():
    item = evidence()
    with pytest.raises(EvidenceConflictError):
        aggregate_evidence(joint(), [item, replace(item, normalized_strength=0.9)], config(), 12 * NS)


def test_same_source_object_across_channels_is_not_counted_as_independent():
    left = evidence(evidence_id="left", channel="a", strength=0.5,
                    quality=1.0, reliability=1.0, source_objects=["event-1"])
    right = evidence(evidence_id="right", channel="b", strength=0.8,
                     quality=1.0, reliability=1.0, source_objects=["event-1"])
    result = aggregate_evidence(joint(), [left, right], config(), 12 * NS)
    assert result.node_h[1] == pytest.approx(0.8)


def test_circular_and_non_independent_evidence_are_excluded_with_provenance():
    system = joint()
    circular = evidence(source_records=[system.node_row_refs[1].source_record_id])
    dependent = evidence(evidence_id="dependent", independent=False)
    result = aggregate_evidence(system, [circular, dependent], config(), 12 * NS)
    assert result.node_h[1] == 0.0
    assert {item["reason_code"] for item in result.excluded_evidence} == {
        "circular_evidence_excluded", "non_independent_evidence_excluded"
    }


def test_shock_evidence_does_not_come_from_edge_residual():
    result = aggregate_evidence(joint(), [], config(), 12 * NS)
    assert np.array_equal(result.shock_h, np.zeros(1))


def test_propagation_support_uses_positive_coefficient_and_topology_fraction():
    system = joint()
    result = compute_propagation_support(
        system, topology_store(), {p6.NODE_IDS[1]: [2 * NS, 7 * NS]}, config()
    )
    assert result.learned_support.tolist() == [1.0]
    assert result.topology_support.tolist() == [0.5]
    assert result.propagation_h.tolist() == [0.5]
    assert system.propagation_variable_refs[0].learned_coefficient == 0.25


@pytest.mark.parametrize("second_present,expected", [(True, 1.0), (False, 0.5)])
def test_topology_presence_extremes_are_exact(second_present, expected):
    result = compute_propagation_support(
        joint(), topology_store(second_present), {p6.NODE_IDS[1]: [2 * NS, 7 * NS]}, config()
    )
    assert result.topology_support[0] == expected


def test_zero_residual_uses_configured_scale_floor():
    system = joint(); object.__setattr__(system, "joint_residual", np.zeros_like(system.joint_residual))
    quality = build_observation_weights(system, config().quality)
    result = compute_penalties(system, np.zeros(4), np.zeros(1), np.zeros(1), quality, config().penalties)
    assert result.residual_scale_raw == 0.0
    assert result.residual_scale_used == config().penalties.residual_scale_floor


def test_mad_scale_matches_manual_value():
    system = joint(); quality = build_observation_weights(system, config().quality)
    result = compute_penalties(system, np.zeros(4), np.zeros(1), np.zeros(1), quality, config().penalties)
    values = system.joint_residual
    expected = 1.4826 * np.median(np.abs(values - np.median(values)))
    assert result.residual_scale_raw == pytest.approx(expected)


def test_negative_propagation_coefficient_has_zero_support():
    system = joint()
    ref = system.propagation_variable_refs[0]
    object.__setattr__(ref, "learned_coefficient", -0.25)
    result = compute_propagation_support(
        system, topology_store(True), {p6.NODE_IDS[1]: [2 * NS, 7 * NS]}, config()
    )
    assert result.learned_support.tolist() == [0.0]
    assert result.propagation_h.tolist() == [0.0]


def test_quality_weights_are_sparse_diagonal_and_do_not_change_residual():
    system = joint()
    before = system.joint_residual.copy()
    result = build_observation_weights(system, config().quality)
    assert sparse.isspmatrix_csr(result.W)
    assert result.W.shape == (5, 5)
    assert np.count_nonzero(result.W.toarray() - np.diag(np.diag(result.W.toarray()))) == 0
    assert result.node_weights.tolist() == [1.0, 1.0, 1.0, 1.0]
    assert result.edge_weights.tolist() == pytest.approx([0.72])
    assert np.array_equal(system.joint_residual, before)


def test_penalty_formulas_match_manual_values_and_are_positive():
    system = joint()
    quality = build_observation_weights(system, config().quality)
    node_h = np.asarray([0.0, 0.5, 0.0, 0.0])
    prop_h = np.asarray([0.5])
    shock_h = np.asarray([0.25])
    result = compute_penalties(system, node_h, prop_h, shock_h, quality, config().penalties)
    assert result.residual_scale_used >= 0.1
    assert np.all(result.lambda_u_effective > 0)
    assert np.all(result.lambda_delta_effective > 0)
    assert np.all(result.lambda_xi_effective > 0)
    assert result.lambda_node_group == pytest.approx(0.2 * result.lambda_u_base)
    assert result.lambda_propagation_group == pytest.approx(0.3 * result.lambda_delta_base)
    assert result.lambda_shock_group == pytest.approx(0.4 * result.lambda_xi_base)


def test_group_partitions_are_complete_nonoverlapping_and_deterministic():
    groups = build_group_partitions(joint())
    assert sorted(index for group in groups.node_groups for index in group.indices) == list(range(4))
    assert sorted(index for group in groups.propagation_groups for index in group.indices) == [0]
    assert sorted(index for group in groups.shock_groups for index in group.indices) == [0]
    assert all(group.indices == sorted(set(group.indices)) and group.indices for group in
               [*groups.node_groups, *groups.propagation_groups, *groups.shock_groups])
