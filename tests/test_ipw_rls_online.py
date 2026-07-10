from __future__ import annotations

import numpy as np

from proberca.propagation.ipw_rls_online import OnlineIPWMaskedRLS, RLSConfig


def test_online_rls_updates_theta_and_outputs_residuals() -> None:
    node_ids = ["svc.x"]
    parent_sets = {"svc.x": ["svc.x"]}
    sampling = {"sampling_probability_by_node": {"svc.x": 1.0}, "observed_by_node": {"svc.x": True}}
    learner = OnlineIPWMaskedRLS(node_ids, parent_sets, sampling, RLSConfig(ridge_init=10.0))
    values = [1.0]
    for _ in range(1, 12):
        values.append(0.8 * values[-1])
    z = np.asarray(values, dtype=float).reshape(-1, 1)
    mask = np.ones_like(z, dtype=bool)
    state = learner.run(z, mask, list(range(len(values))))
    assert state["total_updates"] > 0
    assert state["batch_ridge_used"] is False
    assert state["update_mode"] == "online_rls"
    assert abs(state["theta_by_node"]["svc.x"][0]) > 0.01
    assert learner.export_predictions()
    assert learner.export_residuals()


def test_ipw_weight_clips_low_probability() -> None:
    learner = OnlineIPWMaskedRLS(
        ["svc.a", "svc.b"],
        {"svc.a": ["svc.a"], "svc.b": ["svc.b"]},
        {"sampling_probability_by_node": {"svc.a": 1.0, "svc.b": 0.01}, "observed_by_node": {"svc.a": True, "svc.b": True}},
        RLSConfig(min_sampling_probability=0.05, max_ipw_weight=10.0),
    )
    assert learner._ipw("svc.b") > learner._ipw("svc.a")
    assert learner._ipw("svc.b") == 10.0


def test_missing_parent_is_not_used_in_phi() -> None:
    node_ids = ["svc.a", "svc.b", "svc.y"]
    parent_sets = {"svc.y": ["svc.a", "svc.b"], "svc.a": ["svc.a"], "svc.b": ["svc.b"]}
    sampling = {"sampling_probability_by_node": {node: 1.0 for node in node_ids}, "observed_by_node": {node: True for node in node_ids}}
    learner = OnlineIPWMaskedRLS(node_ids, parent_sets, sampling, RLSConfig(min_parent_observed=1))
    z = np.asarray([[1.0, 2.0, 0.0], [3.0, 4.0, 5.0]], dtype=float)
    mask = np.asarray([[False, True, True], [True, True, True]], dtype=bool)
    row = learner.update("svc.y", 1, z, mask, 1.0)
    assert row["observed_parent_count"] == 1
    assert row["updated"] is True
