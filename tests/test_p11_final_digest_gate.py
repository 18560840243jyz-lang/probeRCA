from __future__ import annotations

import pytest


REQUIRED_SCENARIOS = (
    "discovery", "metrics", "three_window", "resume", "prometheus_down_up",
    "relist", "checkpoint_write_recovery", "output_write_recovery",
    "leader_delete_handoff", "rolling_restart", "retention", "health_metrics",
)


def test_final_digest_gate_requires_every_scenario_on_one_identity():
    from proberca.live.gate import FinalDigestGate

    gate = FinalDigestGate("f" * 64, "sha256:" + "d" * 64, REQUIRED_SCENARIOS)
    for scenario in REQUIRED_SCENARIOS:
        gate.record(scenario, "f" * 64, "sha256:" + "d" * 64, {"passed": True})
    assert gate.validate()["passed"] is True


def test_final_digest_gate_rejects_old_image_or_missing_scenario():
    from proberca.live.gate import FinalDigestGate, FinalDigestGateError

    gate = FinalDigestGate("f" * 64, "sha256:" + "d" * 64, REQUIRED_SCENARIOS)
    gate.record("discovery", "e" * 64, "sha256:" + "d" * 64, {"passed": True})
    with pytest.raises(FinalDigestGateError, match="identity"):
        gate.validate()

    gate = FinalDigestGate("f" * 64, "sha256:" + "d" * 64, REQUIRED_SCENARIOS)
    gate.record("discovery", "f" * 64, "sha256:" + "d" * 64, {"passed": True})
    with pytest.raises(FinalDigestGateError, match="missing"):
        gate.validate()


def test_final_digest_gate_invalidates_after_production_fingerprint_change():
    from proberca.live.gate import FinalDigestGate, FinalDigestGateError

    gate = FinalDigestGate("f" * 64, "sha256:" + "d" * 64, ("discovery",))
    gate.record("discovery", "f" * 64, "sha256:" + "d" * 64, {"passed": True})
    gate.assert_current_source("f" * 64)
    with pytest.raises(FinalDigestGateError, match="source"):
        gate.assert_current_source("a" * 64)
