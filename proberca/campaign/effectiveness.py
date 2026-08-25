"""Post-collection direct fault-effectiveness evaluation without RCA output."""

from __future__ import annotations

import hashlib
import json
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
    "nr_periods": ("cpu_nr_periods_total",),
    "meaningful_operation": ("socket_ops_total",),
    "direct_drop_counter": (
        "node_nic_rx_drop_total", "node_nic_tx_drop_total",
        "node_nic_rx_error_total", "node_nic_tx_error_total",
        "node_qdisc_tx_drop_total",
    ),
    "edge_request": ("edge_request_total",),
    "edge_error": ("edge_error_total",),
    "edge_timeout": ("edge_timeout_total",),
}


def _record_matches(
    record: Any, coordinate: dict[str, Any], *, metric_name: str | None = None,
) -> bool:
    kind = coordinate["entity_kind"]
    if record.metric_name != (metric_name or coordinate["metric"]):
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


def _quantile(values: list[float], fraction: float) -> float:
    """Return a deterministic linearly interpolated empirical quantile."""

    ordered = sorted(values)
    if not ordered:
        raise EffectivenessError("secondary signal reference is empty")
    if len(ordered) == 1:
        return ordered[0]
    position = fraction * (len(ordered) - 1)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _robust_upper_bound(
    values: list[float], *, scale_multiplier: float,
) -> tuple[float, float, float, float]:
    median = statistics.median(values)
    mad_scale = 1.4826 * statistics.median(
        abs(value - median) for value in values
    )
    iqr_scale = (_quantile(values, 0.75) - _quantile(values, 0.25)) / 1.349
    robust_scale = max(mad_scale, iqr_scale)
    return median + scale_multiplier * robust_scale, median, mad_scale, iqr_scale


def _longest_consecutive(sequences: list[int]) -> int:
    longest = current = 0
    previous = None
    for sequence in sorted(sequences):
        current = current + 1 if previous is not None and sequence == previous + 1 else 1
        longest = max(longest, current)
        previous = sequence
    return longest


def _load_intent_records(
    root: Path | None, *, dataset_id: str,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    if root is None:
        return None, []
    manifest_path = root / "manifest.json"
    ledger_path = root / "behavior-intents.jsonl"
    if not manifest_path.is_file() or not ledger_path.is_file():
        return None, []
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    encoded = ledger_path.read_bytes()
    if (
        manifest.get("schema_version") != "probeRCA-load-intent-manifest-v1"
        or manifest.get("dataset_id") != dataset_id
        or manifest.get("ledger_sha256") != hashlib.sha256(encoded).hexdigest()
    ):
        raise EffectivenessError("load-intent manifest integrity mismatch")
    records = [json.loads(line) for line in encoded.decode("utf-8").splitlines() if line]
    if len(records) != int(manifest.get("record_count", -1)):
        raise EffectivenessError("load-intent record count mismatch")
    return manifest, records


def _longest_recovery_run(
    rows: list[dict[str, Any]], *, healthy_failure_upper: float,
) -> int:
    eligible = []
    for item in rows:
        request = item["request_count"]
        if (
            request is not None and request > 0
            and item["valid"] and item["value"] <= healthy_failure_upper
        ):
            eligible.append(int(item["sequence"]))
    return _longest_consecutive(eligible)


def _evaluate_terminal_tcp_failure(
    *, policy: dict[str, Any], coordinate: dict[str, Any],
    rows: dict[str, list[dict[str, Any]]], lifecycle: FaultLifecycle,
    counters_before: dict[str, float], counters_after: dict[str, float],
    filter_hit: bool, load_intent_manifest: dict[str, Any] | None,
    load_intent_records: list[dict[str, Any]],
) -> dict[str, Any]:
    """Evaluate terminal connection failure without rewriting no-exposure data."""

    edge_id = str(coordinate["entity_id"])
    behaviors = policy["edge_behavior_intents"].get(edge_id)
    if not behaviors:
        raise EffectivenessError("terminal failure target has no demand-intent mapping")
    initial_start = lifecycle.effect_confirmed_ns + int(
        policy["drain_grace_seconds"]
    ) * 1_000_000_000
    initial_end = initial_start + int(
        policy["initial_confirmation_seconds"]
    ) * 1_000_000_000
    initial = [
        item for item in rows["FAULT_ACTIVE"]
        if item["window_start_ns"] >= initial_start
        and item["window_end_ns"] <= initial_end
    ]
    attempts = sum(
        item["request_count"] for item in initial
        if item["request_count"] is not None
    )
    estimated_failures = sum(
        item["request_count"] * item["value"] for item in initial
        if item["request_count"] is not None and item["valid"]
    )
    positive_sequences = [
        int(item["sequence"]) for item in initial
        if item["valid"] and item["value"] > 0
    ]
    failure_ratio = estimated_failures / attempts if attempts > 0 else 0.0
    direct_failure_delta = (
        float(counters_after.get("edge_error", 0.0))
        - float(counters_before.get("edge_error", 0.0))
        + float(counters_after.get("edge_timeout", 0.0))
        - float(counters_before.get("edge_timeout", 0.0))
    )

    demand_start = initial_end
    demand_end = lifecycle.cleanup_command_start_ns
    demand_intervals = [
        item for item in load_intent_records
        if item["interval_start_ns"] >= demand_start
        and item["interval_end_ns"] <= demand_end
    ]
    demand_counts = [
        sum(int(item["behavior_intents"].get(name, 0)) for name in behaviors)
        for item in demand_intervals
    ]
    expected_interval_ns = int(policy["demand_interval_seconds"]) * 1_000_000_000
    expected_demand_start = (
        (demand_start + expected_interval_ns - 1) // expected_interval_ns
    ) * expected_interval_ns
    expected_demand_end = (demand_end // expected_interval_ns) * expected_interval_ns
    demand_contiguous = bool(demand_intervals) and all(
        int(item["interval_end_ns"]) - int(item["interval_start_ns"])
        == expected_interval_ns
        for item in demand_intervals
    ) and all(
        left["interval_end_ns"] == right["interval_start_ns"]
        for left, right in zip(demand_intervals, demand_intervals[1:])
    ) and demand_intervals[0]["interval_start_ns"] == expected_demand_start \
        and demand_intervals[-1]["interval_end_ns"] == expected_demand_end
    demand_sufficient = (
        load_intent_manifest is not None and demand_contiguous
        and all(
            count >= int(policy["minimum_target_intents_per_interval"])
            for count in demand_counts
        )
    )
    unexpected_invalid = [
        item for item in rows["FAULT_ACTIVE"]
        if not item["valid"] and item["invalid_reason"] != "no_exposure"
    ]
    pre_values = [item["value"] for item in rows["HEALTHY_PRE"] if item["valid"]]
    healthy_failure_upper = max(pre_values) if pre_values else 0.0
    recovery_run = _longest_recovery_run(
        rows["RECOVERY"], healthy_failure_upper=healthy_failure_upper,
    )
    failures = []
    if not filter_hit:
        failures.append("terminal_rst_filter_not_hit")
    if direct_failure_delta < float(policy["direct_failure_delta_min"]):
        failures.append("terminal_direct_failure_counter_insufficient")
    if attempts < int(policy["initial_attempts_min"]):
        failures.append("terminal_initial_attempts_insufficient")
    if len(positive_sequences) < int(policy["initial_positive_windows_min"]):
        failures.append("terminal_initial_positive_windows_insufficient")
    if failure_ratio < float(policy["initial_failure_ratio_min"]):
        failures.append("terminal_initial_failure_ratio_insufficient")
    if unexpected_invalid:
        failures.append("terminal_unexpected_invalid_observation")
    if recovery_run < int(policy["recovery_consecutive_exposure_windows"]):
        failures.append("terminal_recovery_insufficient")
    not_evaluable = not demand_sufficient
    if not_evaluable:
        failures.append("terminal_target_demand_not_evaluable")
    return {
        "policy": dict(policy),
        "target_behaviors": list(behaviors),
        "drain_grace_end_ns": initial_start,
        "initial_confirmation_end_ns": initial_end,
        "initial_attempts": attempts,
        "initial_estimated_failures": estimated_failures,
        "initial_failure_ratio": failure_ratio,
        "initial_positive_windows": len(positive_sequences),
        "initial_longest_consecutive_positive_windows": _longest_consecutive(
            positive_sequences
        ),
        "direct_failure_counter_delta": direct_failure_delta,
        "rst_filter_hit": filter_hit,
        "backoff_no_exposure_windows": sum(
            not item["valid"] and item["invalid_reason"] == "no_exposure"
            for item in rows["FAULT_ACTIVE"]
        ),
        "unexpected_invalid_windows": len(unexpected_invalid),
        "demand_ledger_present": load_intent_manifest is not None,
        "demand_interval_count": len(demand_intervals),
        "target_intents_by_interval": demand_counts,
        "demand_sufficient": demand_sufficient,
        "healthy_pre_failure_upper": healthy_failure_upper,
        "recovery_consecutive_exposure_windows": recovery_run,
        "not_evaluable": not_evaluable,
        "passed": not failures,
        "failures": failures,
    }


def _evaluate_secondary_signal(
    *,
    policy: dict[str, Any],
    phase_records: dict[str, list[dict[str, Any]]],
    counter_deltas: dict[int, dict[str, float]],
    direct_mutation_controls: list[str],
    minimum_phase_samples: int,
) -> dict[str, Any]:
    reference = phase_records[policy["reference_phase"]]
    active = phase_records["FAULT_ACTIVE"]
    if len(reference) < minimum_phase_samples or len(active) < minimum_phase_samples:
        raise EffectivenessError("secondary signal lacks phase observations")
    upper, median, mad_scale, iqr_scale = _robust_upper_bound(
        [item["value"] for item in reference],
        scale_multiplier=float(policy["robust_scale_multiplier"]),
    )

    def excess(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [item for item in records if item["value"] > upper]

    reference_excess = excess(reference)
    active_excess = excess(active)
    reference_fraction = len(reference_excess) / len(reference)
    active_fraction = len(active_excess) / len(active)
    fraction_lift = active_fraction - reference_fraction
    longest = _longest_consecutive([
        int(item["sequence"]) for item in active_excess
    ])
    active_median = statistics.median(item["value"] for item in active)
    forbidden = set(policy["forbidden_direct_controls"])
    observed_forbidden = sorted(forbidden.intersection(direct_mutation_controls))
    reasons = []
    if observed_forbidden:
        reasons.append("forbidden_direct_mutation")
    if active_median > upper:
        reasons.append("active_median_above_healthy_upper")
    if longest > int(policy["max_consecutive_excess_windows"]):
        reasons.append("consecutive_excess_windows")
    if fraction_lift > float(policy["max_excess_fraction_lift"]):
        reasons.append("excess_fraction_lift")

    incidents = []
    for item in active_excess:
        sequence = int(item["sequence"])
        raw = counter_deltas.get(sequence, {})
        if "nr_throttled" not in raw or "nr_periods" not in raw:
            raise EffectivenessError(
                "secondary throttle incident lacks raw counter evidence"
            )
        if raw["nr_periods"] <= 0 or raw["nr_throttled"] < 0:
            raise EffectivenessError("secondary throttle raw counters are invalid")
        incidents.append({
            **item,
            "nr_throttled_delta": raw["nr_throttled"],
            "nr_periods_delta": raw["nr_periods"],
            "healthy_upper": upper,
        })
    return {
        "metric": policy["metric"],
        "reference_phase": policy["reference_phase"],
        "reference_valid_windows": len(reference),
        "active_valid_windows": len(active),
        "reference_median": median,
        "reference_mad_scale": mad_scale,
        "reference_iqr_scale": iqr_scale,
        "healthy_upper": upper,
        "active_median": active_median,
        "reference_excess_count": len(reference_excess),
        "active_excess_count": len(active_excess),
        "reference_excess_fraction": reference_fraction,
        "active_excess_fraction": active_fraction,
        "excess_fraction_lift": fraction_lift,
        "longest_consecutive_excess_windows": longest,
        "direct_mutation_controls": sorted(direct_mutation_controls),
        "forbidden_direct_controls_observed": observed_forbidden,
        "contaminated": bool(reasons),
        "contamination_reasons": reasons,
        "incidental": bool(active_excess) and not reasons,
        "incident_windows": incidents,
        "policy": dict(policy),
    }
def evaluate_fault_effectiveness(
    *,
    normal_root: Path,
    primitive_roots: list[Path],
    coordinate: dict[str, Any],
    profile: dict[str, Any],
    injection_session: dict[str, Any],
    contamination: dict[str, bool],
    minimum_phase_samples: int = 10,
    load_intent_root: Path | None = None,
) -> dict[str, Any]:
    """Use only measured metric/counter/filter evidence; never alert or RCA."""

    normal = CollectionArchive.load(normal_root)
    lifecycle = _lifecycle(injection_session)
    values = {"HEALTHY_PRE": [], "FAULT_ACTIVE": [], "RECOVERY": []}
    terminal_policy = profile.get("terminal_failure_policy")
    terminal_rows = {"HEALTHY_PRE": [], "FAULT_ACTIVE": [], "RECOVERY": []}
    policy = profile.get("secondary_signal_policy")
    secondary_records = {
        "HEALTHY_PRE": [], "FAULT_ACTIVE": [], "RECOVERY": [],
    }
    for fallback_sequence, window in enumerate(normal.iter_windows(), start=1):
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
        target_record = records[0]
        if target_record.valid:
            values[phase].append(float(target_record.value))
        if terminal_policy is not None:
            request_records = [
                item for item in window.edge_metrics
                if _record_matches(
                    item, coordinate, metric_name="edge_request_count",
                )
            ]
            if len(request_records) != 1:
                raise EffectivenessError(
                    "terminal failure request count does not resolve uniquely"
                )
            request = request_records[0]
            request_count = float(request.value) if request.valid else None
            terminal_rows[phase].append({
                "sequence": int(getattr(window, "sequence", fallback_sequence)),
                "window_start_ns": int(window.window_start_ns),
                "window_end_ns": int(window.window_end_ns),
                "valid": bool(target_record.valid),
                "value": (
                    float(target_record.value) if target_record.valid else None
                ),
                "invalid_reason": getattr(target_record, "invalid_reason", None),
                "request_count": request_count,
            })
        if policy is not None:
            secondary = [
                item for item in (*window.node_metrics, *window.edge_metrics)
                if _record_matches(
                    item, coordinate, metric_name=str(policy["metric"]),
                )
            ]
            if len(secondary) != 1:
                raise EffectivenessError("secondary metric does not resolve uniquely")
            if secondary[0].valid:
                secondary_records[phase].append({
                    "sequence": int(getattr(window, "sequence", fallback_sequence)),
                    "window_start_ns": int(window.window_start_ns),
                    "window_end_ns": int(window.window_end_ns),
                    "value": float(secondary[0].value),
                    "sample_count": int(secondary[0].sample_count),
                })
    if len(values["HEALTHY_PRE"]) < minimum_phase_samples or (
        terminal_policy is None
        and len(values["FAULT_ACTIVE"]) < minimum_phase_samples
    ):
        raise EffectivenessError("direct target metric lacks phase observations")
    baseline_value = statistics.median(values["HEALTHY_PRE"])
    active_value = (
        statistics.median(values["FAULT_ACTIVE"])
        if values["FAULT_ACTIVE"] else baseline_value
    )
    counter_series: dict[str, list[tuple[int, float]]] = {
        key: [] for key in _COUNTER_COMPONENTS
    }
    counter_deltas: dict[int, dict[str, float]] = {}
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
                if selected:
                    start = by_timestamp.get(window.window_start_ns)
                    end = by_timestamp.get(window.window_end_ns)
                    if start is None or end is None:
                        raise EffectivenessError(
                            "raw counter evidence lacks a window boundary"
                        )
                    delta = end - start
                    if delta < 0:
                        raise EffectivenessError("raw counter evidence reset")
                    per_window = counter_deltas.setdefault(window.sequence, {})
                    per_window[key] = per_window.get(key, 0.0) + delta
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
    cleanup_result = injection_session.get("cleanup_result", {})
    mutation_evidence = cleanup_result.get("packet_filter_evidence")
    if mutation_evidence is None:
        mutation_evidence = cleanup_result.get("traffic_control_evidence", {})
    filter_hit = mutation_evidence.get("matched_filter_present") is True \
        and int(mutation_evidence.get("packets", 0)) > 0
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
    effective_contamination = dict(contamination)
    secondary_signal = None
    if policy is not None:
        declared_controls = list(profile.get("direct_mutation_controls", ()))
        observed_controls = injection_session.get("apply_result", {}).get(
            "direct_mutation_controls"
        )
        if observed_controls is not None:
            if sorted(observed_controls) != sorted(declared_controls):
                raise EffectivenessError("direct mutation evidence/profile mismatch")
            direct_controls = list(observed_controls)
            mutation_evidence_source = "worker_apply_result"
        else:
            direct_controls = declared_controls
            mutation_evidence_source = "allow_listed_mechanism_contract"
        secondary_signal = _evaluate_secondary_signal(
            policy=policy, phase_records=secondary_records,
            counter_deltas=counter_deltas,
            direct_mutation_controls=direct_controls,
            minimum_phase_samples=minimum_phase_samples,
        )
        secondary_signal["direct_mutation_evidence_source"] = \
            mutation_evidence_source
        effective_contamination[str(policy["metric"])] = bool(
            secondary_signal["contaminated"]
        )
    active = {
        "metric": coordinate["metric"],
        "primary_value": active_value,
        "valid_window_count": len(values["FAULT_ACTIVE"]),
        "positive_window_count": sum(
            value > 0.0 for value in values["FAULT_ACTIVE"]
        ),
        "counters": counters_after,
        "conditions": conditions,
        "contamination": effective_contamination,
        "mutation_evidence": mutation_evidence,
    }
    passed, failures = evaluate_effectiveness_criterion(
        profile["effectiveness_criterion"], baseline, active,
    )
    terminal_failure = None
    if terminal_policy is not None:
        load_manifest, load_records = _load_intent_records(
            load_intent_root, dataset_id=normal.dataset_id,
        )
        terminal_failure = _evaluate_terminal_tcp_failure(
            policy=terminal_policy, coordinate=coordinate,
            rows=terminal_rows, lifecycle=lifecycle,
            counters_before=counters_before, counters_after=counters_after,
            filter_hit=filter_hit, load_intent_manifest=load_manifest,
            load_intent_records=load_records,
        )
        passed = passed and terminal_failure["passed"]
        failures = list(failures) + list(terminal_failure["failures"])
    required_contamination = profile.get("contamination_checks", [])
    missing = sorted(set(required_contamination) - set(effective_contamination))
    contaminated = sorted(
        key for key in required_contamination
        if effective_contamination.get(key) is True
    )
    report = {
        "schema_version": "probeRCA-fault-effectiveness-report-v2",
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
        "secondary_signal_evidence": secondary_signal,
        "terminal_failure_evidence": terminal_failure,
    }
    report["accepted"] = (
        report["criterion_passed"]
        and report["contamination_passed"]
        and report["cleanup_passed"]
    )
    if report["accepted"]:
        report["status"] = (
            terminal_policy["pass_status"]
            if terminal_failure is not None
            else policy["incidental_status"]
            if secondary_signal is not None and secondary_signal["incidental"]
            else "PASS"
        )
    elif terminal_failure is not None and terminal_failure["not_evaluable"]:
        report["status"] = "NOT_EVALUABLE_INSUFFICIENT_DEMAND"
    elif not report["criterion_passed"]:
        report["status"] = "INVALID_INEFFECTIVE"
    elif not report["contamination_passed"]:
        report["status"] = "INVALID_CONTAMINATED"
    else:
        report["status"] = "INVALID_CLEANUP"
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
        "schema_version": "probeRCA-injector-pilot-evidence-v2",
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
        "secondary_signal_evidence": effectiveness_report.get(
            "secondary_signal_evidence"
        ),
        "terminal_failure_evidence": effectiveness_report.get(
            "terminal_failure_evidence"
        ),
        "status": effectiveness_report.get("status"),
        "execution_error": injection_session.get("execution_error"),
    }
    report["accepted"] = effectiveness_report.get("accepted") is True \
        and report["execution_error"] is None
    report["report_fingerprint"] = fingerprint(report)
    return report
