from __future__ import annotations

import json
from dataclasses import replace

import numpy as np
import pytest

from proberca.propagation.metric_ridge import MetricPropagationLearner

from test_p5_history_rules import NS, candidate, gate
from test_p5_metric_ridge import alert, all_self_rules, geometric, train_learner, values_window


def ready_learner(**changes):
    return train_learner([geometric(0.5)] * 4, all_self_rules(),
                         metric_min_training_rows=3, **changes)


def test_identical_candidate_config_and_history_hits_cache():
    learner = ready_learner()
    first = learner.prepare_for_alert(alert(), candidate())
    second = learner.prepare_for_alert(alert(), candidate())
    assert not first.cache_hit and second.cache_hit
    assert first.info.model_snapshot_id == second.info.model_snapshot_id


def test_topology_and_candidate_id_changes_invalidate_cache():
    learner = ready_learner()
    learner.prepare_for_alert(alert(), candidate())
    learner.archive_soft_model()
    changed = replace(candidate(candidate_id="candidate-2"), topology_snapshot_id="top-2")
    result = learner.prepare_for_alert(alert(), changed)
    assert not result.cache_hit
    assert result.info.candidate_fingerprint != learner.cached_model_infos()[0].candidate_fingerprint


def test_history_cutoff_change_invalidates_cache():
    learner = ready_learner()
    learner.prepare_for_alert(alert(), candidate())
    learner.archive_soft_model()
    learner.ingest_healthy_window(values_window(10, (1, 1, 1, 1)), gate())
    later_alert = alert(timestamp=12 * NS)
    later_candidate = replace(candidate(), alert_timestamp_ns=12 * NS,
                              topology_valid_to_ns=20 * NS)
    result = learner.prepare_for_alert(later_alert, later_candidate)
    assert not result.cache_hit


def test_deterministic_cache_eviction_records_issue():
    learner = ready_learner(metric_model_cache_size=1)
    learner.prepare_for_alert(alert(), candidate())
    learner.archive_soft_model()
    result = learner.prepare_for_alert(alert(), candidate(candidate_id="candidate-2"))
    assert [item.reason_code for item in result.issues][-1] == "model_cache_eviction"
    assert len(learner.cached_model_infos()) == 1


def test_snapshot_restore_frozen_prediction_and_exports(tmp_path):
    learner = ready_learner()
    hard_candidate = replace(candidate(), alert_state="hard", rca_eligible=True,
                             alert_id="alert-hard")
    learner.freeze_for_hard(alert("hard"), hard_candidate)
    expected_predictions = learner.predict_window(11 * NS, None)
    expected_dense = learner.export_dense_matrices()
    path = tmp_path / "metric-model"
    learner.snapshot(path)
    assert (path / "metadata.json").exists() and (path / "arrays.npz").exists()
    restored = MetricPropagationLearner.restore(path, learner.config, 1,
                                                expected_candidate=hard_candidate)
    assert restored.predict_window(11 * NS, None) == expected_predictions
    actual_dense = restored.export_dense_matrices()
    assert np.array_equal(actual_dense[0], expected_dense[0])
    assert np.array_equal(actual_dense[1], expected_dense[1])
    assert restored.export_sparse_coefficients() == learner.export_sparse_coefficients()
    with pytest.raises(RuntimeError):
        restored.prepare_for_alert(alert(), candidate())


def test_snapshot_candidate_config_rules_and_version_mismatch(tmp_path):
    learner = ready_learner()
    learner.prepare_for_alert(alert(), candidate())
    path = tmp_path / "metric-model"
    learner.snapshot(path)
    with pytest.raises(ValueError, match="candidate"):
        MetricPropagationLearner.restore(path, learner.config, 1,
                                         expected_candidate=candidate(candidate_id="other"))
    altered = replace(learner.config, metric_ridge=0.2)
    with pytest.raises(ValueError, match="config"):
        MetricPropagationLearner.restore(path, altered, 1, expected_candidate=candidate())
    metadata = path / "metadata.json"
    payload = json.loads(metadata.read_text(encoding="utf-8"))
    payload["format_version"] = "unsupported"
    metadata.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="version"):
        MetricPropagationLearner.restore(path, learner.config, 1, expected_candidate=candidate())


def test_snapshot_node_index_and_topology_mismatch(tmp_path):
    learner = ready_learner()
    learner.prepare_for_alert(alert(), candidate())
    path = tmp_path / "metric-model"
    learner.snapshot(path)
    changed_topology = replace(candidate(), topology_snapshot_id="other-top")
    with pytest.raises(ValueError, match="topology"):
        MetricPropagationLearner.restore(path, learner.config, 1,
                                         expected_candidate=changed_topology)
