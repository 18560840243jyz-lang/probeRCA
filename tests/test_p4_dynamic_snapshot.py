from __future__ import annotations

from dataclasses import replace

import numpy as np
import pytest

from proberca.propagation.service_rls import (
    PropagationTimeError,
    ServicePropagationLearner,
    TopologyModelMismatchError,
)

from test_p4_service_propagation import NS, gate, learner, state, topology


def window_states(window, values=None, **changes):
    values = values or {"api": 1.0, "db": 1.0, "peer": 1.0, "unrelated": 1.0}
    return [state(name, value, window, **changes) for name, value in values.items()]


def test_first_lag_windows_are_insufficient_history_without_predictions():
    model = learner(service_lags=[1, 2])
    result = model.process_window(window_states(0), gate(), topology())
    assert result.predictions == []
    assert {issue.reason_code for issue in result.issues} >= {"topology_reconfigured", "insufficient_history"}
    result = model.process_window(window_states(1), gate(), topology())
    assert result.predictions == []
    assert any(issue.reason_code == "insufficient_history" for issue in result.issues)


def test_missing_parent_skips_only_affected_targets_and_never_fills_zero():
    model = learner()
    missing_db = {"api": 2.0, "peer": 2.0, "unrelated": 2.0}
    model.process_window(window_states(0, missing_db), gate(), topology())
    result = model.process_window(window_states(1), gate(), topology())
    predicted_targets = {item.target_service_id for item in result.predictions}
    assert "cluster-a::ns::api" not in predicted_targets
    assert "cluster-a::ns::unrelated" in predicted_targets
    assert any(issue.target_service_id.endswith("::api") and issue.reason_code == "missing_parent_state"
               for issue in result.issues)


def test_low_quality_parent_and_target_do_not_train():
    model = learner(service_min_updates=1)
    low_parent = window_states(0)
    low_parent = [replace(item, observation_quality=0.1) if item.service_name == "db" else item
                  for item in low_parent]
    model.process_window(low_parent, gate(), topology())
    result = model.process_window(window_states(1), gate(), topology())
    assert "cluster-a::ns::api" not in {item.target_service_id for item in result.predictions}
    assert any(issue.reason_code == "low_observation_quality" for issue in result.issues)


def test_baseline_not_ready_prevents_update_but_keeps_prediction():
    model = learner(service_min_updates=1)
    model.process_window(window_states(0), gate(), topology())
    result = model.process_window(window_states(1, baseline_ready=False),
                                  gate(update=True, baseline=False), topology())
    prediction = next(item for item in result.predictions if item.target_service_id.endswith("::api"))
    assert not prediction.updated and prediction.skipped_reason == "baseline_not_ready"
    assert model.model_state(prediction.target_service_id).update_count == 0


def test_online_duplicate_and_out_of_order_windows_fail():
    model = learner()
    model.process_window(window_states(1), gate(), topology())
    with pytest.raises(PropagationTimeError):
        model.process_window(window_states(1), gate(), topology())
    with pytest.raises(PropagationTimeError):
        model.process_window(window_states(0), gate(), topology())


def test_large_gap_clears_history_and_requires_warmup():
    model = learner(service_max_gap_windows=2, service_min_updates=1)
    model.process_window(window_states(0), gate(), topology())
    model.process_window(window_states(1), gate(), topology())
    assert model.model_state("cluster-a::ns::api").ready
    result = model.process_window(window_states(5), gate(), topology())
    assert result.predictions == []
    assert any(issue.reason_code == "history_gap" for issue in result.issues)
    assert not model.model_state("cluster-a::ns::api").ready


def test_replay_sorts_only_at_shared_entry_and_records_reorder():
    model = learner()
    batches = [
        (window_states(1), gate(), topology()),
        (window_states(0), gate(), topology()),
    ]
    results = model.process_replay(batches)
    assert [item.timestamp_ns for item in results] == [NS, 2 * NS]
    assert results[0].reordered and results[1].reordered


def test_topology_add_remove_parent_preserves_shared_coefficients():
    model = learner(service_min_updates=1, topology_reconfigure_min_updates=2)
    model.process_window(window_states(0), gate(), topology(host=False, resource=False))
    model.process_window(window_states(1), gate(), topology(host=False, resource=False))
    before = model.model_state("cluster-a::ns::api")
    self_key = next(key for key in before.feature_keys if key.parent_service_id.endswith("::api"))
    self_value = before.theta[before.feature_keys.index(self_key)]
    changed = topology("top-2", host=True, resource=False)
    result = model.process_window(window_states(2), gate(update=False), changed)
    after = model.model_state("cluster-a::ns::api")
    assert result.topology_reconfigured and not after.ready
    assert after.theta[after.feature_keys.index(self_key)] == self_value
    peer_key = next(key for key in after.feature_keys if key.parent_service_id.endswith("::peer"))
    assert after.theta[after.feature_keys.index(peer_key)] == 0.0
    assert after.covariance[after.feature_keys.index(peer_key), after.feature_keys.index(peer_key)] == 100.0
    removed = topology("top-3", host=False, resource=False)
    model.process_window(window_states(3), gate(), removed)
    assert all(not key.parent_service_id.endswith("::peer")
               for key in model.feature_keys("cluster-a::ns::api"))


def test_relation_change_deduplicates_feature_and_retains_coefficient():
    model = learner(service_min_updates=1)
    both = topology(host=True, resource=True)
    model.process_window(window_states(0), gate(), both)
    model.process_window(window_states(1), gate(), both)
    before = model.model_state("cluster-a::ns::api")
    peer_keys = [key for key in before.feature_keys if key.parent_service_id.endswith("::peer")]
    assert len(peer_keys) == 1
    coefficient = before.theta[before.feature_keys.index(peer_keys[0])]
    host_only = topology("top-host", host=True, resource=False)
    model.process_window(window_states(2), gate(update=False), host_only)
    after = model.model_state("cluster-a::ns::api")
    peer_keys = [key for key in after.feature_keys if key.parent_service_id.endswith("::peer")]
    assert len(peer_keys) == 1
    assert after.theta[after.feature_keys.index(peer_keys[0])] == coefficient
    assert after.relation_types[peer_keys[0].parent_service_id] == ["host"]


def test_service_addition_and_removal_reconcile_active_index():
    model = learner()
    source = topology()
    model.process_window(window_states(0), gate(), source)
    reduced = replace(
        source,
        snapshot_id="reduced",
        services=["ns::api", "ns::db"],
        service_nodes=[],
        service_resources=[],
    )
    model.process_window(window_states(1, {"api": 1.0, "db": 1.0}),
                         gate(update=False), reduced)
    assert model.active_service_ids() == ["cluster-a::ns::api", "cluster-a::ns::db"]
    assert model.archived_service_ids() == ["cluster-a::ns::peer", "cluster-a::ns::unrelated"]


def test_deterministic_ar1_coefficient_recovery():
    model = learner(service_min_updates=5, rls_initial_covariance=1_000_000.0,
                    service_min_observation_quality=0.0)
    source = topology(host=False, resource=False)
    value = 1.0
    for window in range(80):
        values = {"api": value, "db": 0.0, "peer": 0.0, "unrelated": 0.0}
        model.process_window(window_states(window, values), gate(), source)
        value *= 0.8
    coefficient = next(item for item in model.export_sparse_coefficients()
                       if item.target_service_id.endswith("::api") and item.parent_service_id.endswith("::api"))
    assert coefficient.coefficient == pytest.approx(0.8, abs=1e-4)
    assert coefficient.ready


def test_multilag_feature_order_and_structural_zero():
    model = learner(service_lags=[1, 2, 3])
    model.process_window(window_states(0), gate(), topology())
    keys = model.feature_keys("cluster-a::ns::api")
    assert [(key.parent_service_id, key.lag) for key in keys] == sorted(
        (key.parent_service_id, key.lag) for key in keys
    )
    dense, services, lags = model.export_dense_matrices()
    unrelated = services.index("cluster-a::ns::unrelated")
    api = services.index("cluster-a::ns::api")
    assert lags == [1, 2, 3]
    assert np.all(dense[:, api, unrelated] == 0.0)


def test_snapshot_restore_prediction_and_continued_update_match(tmp_path):
    original = learner(service_min_updates=1)
    source = topology()
    original.process_window(window_states(0), gate(), source)
    original.process_window(window_states(1), gate(), source)
    snapshot_dir = tmp_path / "service-model"
    original.snapshot(snapshot_dir)
    restored = ServicePropagationLearner.restore(
        snapshot_dir, original.config, 1, [], False, topology_snapshot=source
    )
    left = original.process_window(window_states(2), gate(), source)
    right = restored.process_window(window_states(2), gate(), source)
    assert left == right
    assert np.array_equal(original.export_dense_matrices()[0], restored.export_dense_matrices()[0])


def test_snapshot_config_version_and_topology_mismatch_fail(tmp_path):
    model = learner()
    source = topology()
    model.process_window(window_states(0), gate(), source)
    path = tmp_path / "snapshot"
    model.snapshot(path)
    with pytest.raises(ValueError, match="configuration mismatch"):
        ServicePropagationLearner.restore(path, learner(service_lags=[1, 2]).config, 1, [], False,
                                          topology_snapshot=source)
    with pytest.raises(TopologyModelMismatchError):
        ServicePropagationLearner.restore(path, model.config, 1, [], False,
                                          topology_snapshot=topology("changed", host=False))
    metadata = path / "metadata.json"
    text = metadata.read_text(encoding="utf-8").replace('"format_version": "1"',
                                                         '"format_version": "unsupported"')
    metadata.write_text(text, encoding="utf-8")
    with pytest.raises(ValueError, match="version"):
        ServicePropagationLearner.restore(path, model.config, 1, [], False,
                                          topology_snapshot=source)


def test_snapshot_restores_history_for_lagged_prediction(tmp_path):
    model = learner(service_lags=[1, 2])
    source = topology()
    model.process_window(window_states(0), gate(), source)
    model.process_window(window_states(1), gate(), source)
    path = tmp_path / "snapshot"
    model.snapshot(path)
    restored = ServicePropagationLearner.restore(path, model.config, 1, [], False,
                                                  topology_snapshot=source)
    assert restored.process_window(window_states(2), gate(update=False), source).predictions
