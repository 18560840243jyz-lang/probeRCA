# A6 IPW-masked RLS Review

## Scope

This review covers A6 True IPW-masked RLS propagation preview only.

- No RCA pipeline was run.
- No fault reinjection was performed.
- P1 scoring logic was not modified.
- No real probe was activated.
- A2/A3/A4/A5 outputs were consumed, not rewritten.
- I/O blind performance was not tuned or repaired.

## Files Changed

- `proberca/propagation/ipw_rls_online.py`
- `proberca/adapters/online_boutique/ipw_rls_preview.py`
- `proberca/cli/run_ipw_rls_preview.py`
- `proberca/cli/run_p2_ipw_rls_preview.py`
- `proberca/cli/check_a6_ipw_rls.py`
- `tests/test_ipw_rls_online.py`
- `tests/test_online_boutique_ipw_rls_preview.py`
- `docs/P2_IPW_MASKED_RLS.md`
- `docs/audits/A6_IPW_MASKED_RLS_REVIEW.md`
- `README.md`
- `experiments/README.md`
- `docs/DATA_SCHEMA.md`
- `docs/IMPLEMENTATION_PLAN.md`
- `docs/DECISIONS.md`

## Safety Checks

1. RLS learning does not use `root_service`, `root_metric`, or `root_type`.
   - Verdict: PASS.
   - These labels are read only by `evaluate_ipw_rls_debug` after learning.
2. RLS learning does not use `target_service`, `target_metric`, or `target_fault_type`.
   - Verdict: PASS.
3. RLS learning does not use `injected_path`.
   - Verdict: PASS.
4. RLS learning does not use incident `start_ts` or `end_ts`.
   - Verdict: PASS.
5. `incidents.jsonl` is used only after RLS output generation for debug residual-rank diagnostics.
   - Verdict: PASS.
6. A6 consumes A5 `sampling_probability`.
   - Verdict: PASS.
7. A6 consumes A5 `observation_mask`.
   - Verdict: PASS.
8. A6 uses online RLS updates with K/P/theta recursion.
   - Verdict: PASS.
9. A6 does not use batch ridge as a substitute.
   - Verdict: PASS.
10. RCA pipeline was not run.
   - Verdict: PASS.
11. P1 scoring logic was not modified.
   - Verdict: PASS.
12. Faults were not reinjected.
   - Verdict: PASS.

## RLS Preview Results

- total_repeats: 20
- repeats_completed: 20
- average_node_count: 21.75
- average_total_updates: 474.75
- average_skipped_updates: 317.25
- average_abs_residual: 8558154437811.943
- debug_root_metric_residual_rank_mean: 1.5
- debug_root_service_residual_rank_mean: 1.4

Per fault type:

- CPU: average_total_updates=1288.6, average_abs_residual=9042658476762.807
- Network: average_total_updates=243.0, average_abs_residual=2278363.3623193214
- I/O: average_total_updates=229.4, average_abs_residual=25186586131692.867
- Lock: average_total_updates=138.0, average_abs_residual=3370864428.7473154

Residual magnitudes are reported as observed. They are not used as an A6 pass/fail threshold.

## Validation Results

- `python3 scripts/check_env.py`: PASS
- `python3 -m proberca.cli.check_project`: PASS
- `python3 -m proberca.cli.check_p0_freeze --freeze-dir docs/p0_freeze_snapshot`: PASS
- `python3 -m proberca.cli.check_p1_freeze --freeze-dir docs/p1_freeze_snapshot`: PASS
- `pytest -q`: PASS
- `python3 -m proberca.cli.run_ipw_rls_preview ...`: PASS
- `python3 -m proberca.cli.run_p2_ipw_rls_preview ...`: PASS
- `python3 -m proberca.cli.check_a6_ipw_rls ...`: PASS

## Review Verdict

A6_review_passed: true

failed_checks: []

remaining_risks:

- A6 uses policy-preview expected observation masks, not a real randomized sampling stream.
- Robust normalization can produce very large residuals when prefix MAD is tiny; residual magnitude is not yet calibrated.
- Parent selection is local and label-free but still simplified; A7/A8 must decide how to consume these residuals.
- A6 is not wired into RCA scoring.
- A6 does not address A2 I/O blind RCA degradation.
