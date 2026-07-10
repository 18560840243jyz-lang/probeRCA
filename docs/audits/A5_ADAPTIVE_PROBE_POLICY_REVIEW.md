# A5 Adaptive Probe Policy Review

## Scope

This review covers A5 Adaptive Probe Policy preview only.

- No real eBPF probe was activated.
- No fault reinjection was performed.
- No RCA pipeline was run.
- P1 scoring logic was not modified.
- A2 blind rerun results were not modified.
- I/O blind performance was not tuned or repaired.

## Files Changed

- `proberca/adapters/online_boutique/adaptive_probe_policy.py`
- `proberca/cli/build_adaptive_probe_policy.py`
- `proberca/cli/run_p2_probe_policy_preview.py`
- `proberca/cli/check_a5_probe_policy.py`
- `tests/test_online_boutique_adaptive_probe_policy.py`
- `docs/P2_ADAPTIVE_PROBE_POLICY.md`
- `docs/audits/A5_ADAPTIVE_PROBE_POLICY_REVIEW.md`
- `README.md`
- `experiments/README.md`
- `docs/DATA_SCHEMA.md`
- `docs/IMPLEMENTATION_PLAN.md`
- `docs/DECISIONS.md`

## Safety Checks

1. Probe policy does not use `root_service`, `root_metric`, or `root_type` for selection.
   - Verdict: PASS.
   - These fields are read only by `evaluate_probe_policy_for_debug` after policy generation.
2. Probe policy does not use `target_service`, `target_metric`, or `target_fault_type`.
   - Verdict: PASS.
3. Probe policy does not use `injected_path`.
   - Verdict: PASS.
4. Probe policy does not use incident `start_ts` or `end_ts`.
   - Verdict: PASS.
   - A5 consumes A3 alert windows, not incident windows.
5. `incidents.jsonl` is used only after policy generation for debug coverage.
   - Verdict: PASS.
6. No real eBPF probe was activated.
   - Verdict: PASS.
7. No `kubectl`, `tc`, `docker`, or real system-operation command is used by A5 code.
   - Verdict: PASS.
8. RCA pipeline was not run.
   - Verdict: PASS.
9. P1 scoring logic was not modified.
   - Verdict: PASS.
10. Debug root misses do not backfill or alter probe plans.
   - Verdict: PASS.

## Probe Policy Preview Results

- total_repeats: 20
- repeats_with_probe_plan: 20
- average_selected_probe_count: 4.0
- average_sampling_log_count: 13.9
- average_observation_mask_count: 50.1
- average_estimated_cost: 5.5
- debug_root_metric_family_selected_rate: 1.0
- debug_root_service_has_selected_probe_rate: 1.0

Per fault type:

- CPU: average_selected_probe_count=9.6, average_estimated_cost=11.0
- Network: average_selected_probe_count=2.4, average_estimated_cost=3.5
- I/O: average_selected_probe_count=2.0, average_estimated_cost=3.5
- Lock: average_selected_probe_count=2.0, average_estimated_cost=4.0

Debug coverage is debug-only and did not affect policy selection.

## Validation Results

- `python3 scripts/check_env.py`: PASS
- `python3 -m proberca.cli.check_project`: PASS
- `python3 -m proberca.cli.check_p0_freeze --freeze-dir docs/p0_freeze_snapshot`: PASS
- `python3 -m proberca.cli.check_p1_freeze --freeze-dir docs/p1_freeze_snapshot`: PASS
- `pytest -q`: PASS
- `python3 -m proberca.cli.build_adaptive_probe_policy ...`: PASS
- `python3 -m proberca.cli.run_p2_probe_policy_preview ...`: PASS
- `python3 -m proberca.cli.check_a5_probe_policy ...`: PASS

## Review Verdict

A5_review_passed: true

failed_checks: []

remaining_risks:

- A5 is a policy preview only; it does not activate real probes.
- A5 has no historical reward learning yet; `last_gain` is fixed at 0.
- Budgeting is per alert window; repeats with multiple alert windows can generate multiple plans.
- A6 still needs to consume `sampling_probability` and `observation_mask` for true IPW-masked RLS.
- A5 does not address A2 I/O blind RCA degradation.
