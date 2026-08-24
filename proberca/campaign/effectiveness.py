"""Post-collection direct fault-effectiveness evaluation without RCA output."""

from __future__ import annotations

import statistics
from pathlib import Path
from typing import Any

from proberca.dataplane.archive import CollectionArchive
from proberca.dataplane.raw_archive import RawPrimitiveArchive

from .execution import evaluate_effectiveness_criterion
from .model import fingerprint
from .phases import FaultLifecycle, classify_window_phase


class EffectivenessError(RuntimeError):
    pass


_COUNTER_COMPONENTS = {
    "nr_throttled": ("cpu_nr_throttled_total",),
    "meaningful_operation": ("socket_ops_total",),
    "direct_drop_counter": (
        "node_nic_rx_drop_total", "node_nic_tx_drop_total",
        "node_nic_rx_error_total", "node_nic_tx_error_total",
        "node_qdisc_tx_drop_total",
    ),
}


def _record_matches(record: Any, coordinate: dict[str, Any]) -> bool:
    kind = coordinate["entity_kind"]
    if record.metric_name != coordinate["metric"]:
        return False
    if kind == "service":
        return getattr(record, "service_name", None) == coordinate["entity_id"] \
            and getattr(record, "scope", None) == "service"
    if kind == "host":
        return getattr(record, "node_name", None) == coordinate["entity_id"] \
            and getattr(record, "scope", None) == "node"
    if kind == "tcp_edge":
        source, destination = coordinate["entity_id"].split("->", 1)
        return (
            getattr(record, "src_service", None) == source
            and getattr(record, "dst_service", None) == destination
            and getattr(record, "protocol", None) == "tcp"
        )
    return False


def _sample_matches(sample: Any, coordinate: dict[str, Any]) -> bool:
    kind = coordinate["entity_kind"]
    if kind == "service":
        return sample.entity_type == "service" \
            and sample.service_name == coordinate["entity_id"]
    if kind == "host":
        return sample.entity_type == "host" \
            and sample.node_name == coordinate["entity_id"]
    if kind == "tcp_edge":
        source, destination = coordinate["entity_id"].split("->", 1)
        return sample.entity_type == "edge" and sample.src_service == source \
            and sample.dst_service == destination and sample.protocol == "tcp"
    return False


def _lifecycle(session: dict[str, Any]) -> FaultLifecycle:
    required = (
        "planned_apply_ns", "apply_command_start_ns", "apply_confirmed_ns",
        "planned_cleanup_ns", "cleanup_command_start_ns", "cleanup_confirmed_ns",
    )
    if any(not isinstance(session.get(name), int) for name in required):
        raise EffectivenessError("injection session has incomplete actual timing")
    return FaultLifecycle(
        planned_start_ns=session["planned_apply_ns"],
        apply_command_start_ns=session["apply_command_start_ns"],
        effect_confirmed_ns=session["apply_confirmed_ns"],
        planned_end_ns=session["planned_cleanup_ns"],
        cleanup_command_start_ns=session["cleanup_command_start_ns"],
        cleanup_confirmed_ns=session["cleanup_confirmed_ns"],
    )


def evaluate_fault_effectiveness(
    *,
    normal_root: Path,
    primitive_roots: list[Path],
    coordinate: dict[str, Any],
    profile: dict[str, Any],
    injection_session: dict[str, Any],
    contamination: dict[str, bool],
    minimum_phase_samples: int = 10,
) -> dict[str, Any]:
    """Use only measured metric/counter/filter evidence; never alert or RCA."""

    normal = CollectionArchive.load(normal_root)
    lifecycle = _lifecycle(injection_session)
    values = {"HEALTHY_PRE": [], "FAULT_ACTIVE": [], "RECOVERY": []}
    for window in normal.iter_windows():
        phase = classify_window_phase(
            window.window_start_ns, window.window_end_ns, lifecycle,
        )
        if phase not in values:
            continue
        records = [
            item for item in (*window.node_metrics, *window.edge_metrics)
            if _record_matches(item, coordinate)
        ]
        if len(records) != 1:
            raise EffectivenessError("target metric does not resolve uniquely")
        if records[0].valid:
            values[phase].append(float(records[0].value))
    if len(values["HEALTHY_PRE"]) < minimum_phase_samples \
            or len(values["FAULT_ACTIVE"]) < minimum_phase_samples:
        raise EffectivenessError("direct target metric lacks phase observations")
    baseline_value = statistics.median(values["HEALTHY_PRE"])
    active_value = statistics.median(values["FAULT_ACTIVE"])
    counter_series: dict[str, list[tuple[int, float]]] = {
        key: [] for key in _COUNTER_COMPONENTS
    }
    for root in primitive_roots:
        raw = RawPrimitiveArchive(root)
        if raw.manifest["dataset_id"] != normal.dataset_id:
            raise EffectivenessError("raw primitive Dataset ID mismatch")
        for window in raw.iter_windows():
            for key, components in _COUNTER_COMPONENTS.items():
                selected = [
                    item for item in window.samples
                    if item.component in components
                    and _sample_matches(item, coordinate)
                ]
                by_timestamp: dict[int, float] = {}
                for item in selected:
                    by_timestamp[item.timestamp_ns] = (
                        by_timestamp.get(item.timestamp_ns, 0.0) + item.value
                    )
                counter_series[key].extend(sorted(by_timestamp.items()))
    counters_before: dict[str, float] = {}
    counters_after: dict[str, float] = {}
    for key, samples in counter_series.items():
        samples.sort()
        before = [
            value for timestamp, value in samples
            if timestamp <= lifecycle.effect_confirmed_ns
        ]
        after = [
            value for timestamp, value in samples
            if timestamp <= lifecycle.cleanup_command_start_ns
        ]
        if before and after:
            counters_before[key] = before[-1]
            counters_after[key] = after[-1]
    tc = injection_session.get("cleanup_result", {}).get(
        "traffic_control_evidence", {}
    )
    filter_hit = tc.get("matched_filter_present") is True \
        and int(tc.get("packets", 0)) > 0
    lift = active_value > baseline_value
    conditions = {
        "direct_counter_lift_required": lift,
        "direct_psi_lift_required": lift,
        "filter_hit_and_latency_lift_required": filter_hit and lift,
        "filter_hit_and_failure_lift_required": filter_hit and lift,
    }
    baseline = {
        "metric": profile["effectiveness_criterion"]["metric"],
        "primary_value": baseline_value,
        "counters": counters_before,
    }
    active = {
        "metric": coordinate["metric"],
        "primary_value": active_value,
        "counters": counters_after,
        "conditions": conditions,
        "contamination": dict(contamination),
    }
    passed, failures = evaluate_effectiveness_criterion(
        profile["effectiveness_criterion"], baseline, active,
    )
    required_contamination = profile.get("contamination_checks", [])
    missing = sorted(set(required_contamination) - set(contamination))
    contaminated = sorted(
        key for key in required_contamination if contamination.get(key) is True
    )
    report = {
        "schema_version": "probeRCA-fault-effectiveness-report-v1",
        "dataset_id": normal.dataset_id,
        "coordinate": coordinate,
        "profile_id": profile["profile_id"],
        "criterion": profile["effectiveness_criterion"],
        "baseline_evidence": baseline,
        "active_evidence": active,
        "phase_valid_sample_counts": {
            key: len(item) for key, item in values.items()
        },
        "criterion_passed": passed,
        "criterion_failures": failures,
        "contamination_passed": not missing and not contaminated,
        "missing_contamination_checks": missing,
        "contaminated_dimensions": contaminated,
        "cleanup_passed": injection_session.get("cleanup_passed") is True,
    }
    report["accepted"] = (
        report["criterion_passed"]
        and report["contamination_passed"]
        and report["cleanup_passed"]
    )
    report["report_fingerprint"] = fingerprint(report)
    return report


def build_real_pilot_report(
    *,
    effectiveness_report: dict[str, Any],
    injection_session: dict[str, Any],
    profile: dict[str, Any],
    dataset_sha256: str,
) -> dict[str, Any]:
    """Convert sealed, post-collection evidence into registry-freeze input."""

    if len(dataset_sha256) != 64 or any(
        character not in "0123456789abcdef" for character in dataset_sha256
    ):
        raise EffectivenessError("Pilot dataset SHA-256 is invalid")
    if effectiveness_report.get("profile_id") != profile.get("profile_id"):
        raise EffectivenessError("Pilot effectiveness/profile mismatch")
    if effectiveness_report.get("dataset_id") != injection_session.get("dataset_id"):
        raise EffectivenessError("Pilot effectiveness/session Dataset ID mismatch")
    report = {
        "schema_version": "probeRCA-injector-pilot-evidence-v1",
        "profile_id": profile["profile_id"],
        "mechanism": profile["mechanism"],
        "target": injection_session.get("target"),
        "session_id": injection_session.get("session_id"),
        "dataset_id": effectiveness_report["dataset_id"],
        "sha256": dataset_sha256,
        "simulated": False,
        "applied_at_ns": injection_session.get("apply_confirmed_ns"),
        "cleaned_at_ns": injection_session.get("cleanup_confirmed_ns"),
        "baseline_state_fingerprint": injection_session.get(
            "baseline_state_fingerprint"
        ),
        "restored_state_fingerprint": injection_session.get(
            "restored_state_fingerprint"
        ),
        "effectiveness_passed": effectiveness_report.get("criterion_passed") is True,
        "effectiveness_failures": list(
            effectiveness_report.get("criterion_failures", ())
        ),
        "contamination_passed": (
            effectiveness_report.get("contamination_passed") is True
        ),
        "missing_contamination_checks": list(
            effectiveness_report.get("missing_contamination_checks", ())
        ),
        "contaminated_dimensions": list(
            effectiveness_report.get("contaminated_dimensions", ())
        ),
        "cleanup_passed": effectiveness_report.get("cleanup_passed") is True,
        "baseline_evidence": effectiveness_report.get("baseline_evidence"),
        "active_evidence": effectiveness_report.get("active_evidence"),
        "execution_error": injection_session.get("execution_error"),
    }
    report["accepted"] = effectiveness_report.get("accepted") is True \
        and report["execution_error"] is None
    report["report_fingerprint"] = fingerprint(report)
    return report
