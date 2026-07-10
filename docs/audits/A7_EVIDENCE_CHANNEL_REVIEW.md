# A7 Evidence Channel Review

## Scope

This round only implements the `C h_t` evidence channel and residual calibration preview.

- No RCA pipeline was run.
- No real fault injection was run.
- P1 scoring logic was not modified.
- No real probe or eBPF activation was performed.
- I/O blind rerun behavior was not tuned or repaired.

## Files Changed

- `proberca/evidence/evidence_channel.py`
- `proberca/adapters/online_boutique/evidence_channel_preview.py`
- `proberca/cli/run_evidence_channel_preview.py`
- `proberca/cli/run_p2_evidence_channel_preview.py`
- `proberca/cli/check_a7_evidence_channel.py`
- `tests/test_evidence_channel.py`
- `tests/test_online_boutique_evidence_channel_preview.py`
- `docs/P2_EVIDENCE_CHANNEL.md`
- `docs/audits/A7_EVIDENCE_CHANNEL_REVIEW.md`
- `README.md`
- `experiments/README.md`
- `docs/DATA_SCHEMA.md`
- `docs/IMPLEMENTATION_PLAN.md`
- `docs/DECISIONS.md`

## Safety Checks

1. evidence channel uses `root_service` / `root_metric` / `root_type`: false for channel construction.
2. evidence channel uses `target_service` / `target_metric` / `target_fault_type`: false.
3. evidence channel uses `injected_path`: false.
4. evidence channel uses `incident.start_ts` / `incident.end_ts`: false.
5. `incidents.jsonl` use: debug-only after channel outputs are written.
6. consumes A2 blind evidence: true.
7. consumes A5 probe policy: true.
8. consumes A6 IPW RLS residuals: true.
9. generates `calibrated_residuals.jsonl`: true.
10. raw residual directly marked for A8 sparse inversion: false.
11. RCA pipeline run: false.
12. P1 scoring logic modified: false.
13. real fault injection run: false.

## Evidence Channel Preview Results

- total_repeats: 20
- repeats_completed: 20
- average_abs_raw_residual: 8558154437811.943
- average_abs_calibrated_residual: 2.9776101596814284
- max_abs_raw_residual: 8.974162431282006e+16
- max_abs_calibrated_residual: 10.0
- debug_root_metric_calibrated_residual_rank_mean: 9.0
- debug_root_service_calibrated_residual_rank_mean: 1.95

Per fault type:

| fault_type | average_abs_raw_residual | average_abs_calibrated_residual | max_abs_raw_residual | max_abs_calibrated_residual |
|---|---:|---:|---:|---:|
| CPU | 9042658476762.807 | 3.93522065706106 | 8.974162431282006e+16 | 10.0 |
| I/O | 25186586131692.867 | 1.5744579292861913 | 2013265920000000.0 | 10.0 |
| Lock | 3370864428.7473154 | 3.280671825212488 | 129786642865.90805 | 10.0 |
| Network | 2278363.3623193214 | 3.1200902271659765 | 1169433542.386664 | 10.0 |

## Test Results

- `python3 scripts/check_env.py`: passed.
- `python3 -m proberca.cli.check_project`: passed.
- `python3 -m proberca.cli.check_p0_freeze --freeze-dir docs/p0_freeze_snapshot`: passed.
- `python3 -m proberca.cli.check_p1_freeze --freeze-dir docs/p1_freeze_snapshot`: passed.
- `pytest -q`: reached 100% with no failures in terminal output.
- `python3 -m proberca.cli.run_evidence_channel_preview ...`: passed for CPU repeat 01.
- `python3 -m proberca.cli.run_p2_evidence_channel_preview ...`: passed for 20 repeats.
- `python3 -m proberca.cli.check_a7_evidence_channel ...`: passed.

## Review Verdict

A7_review_passed: true

failed_checks: []

remaining_risks:

- The current `C h_t` evidence effect is deterministic and interpretable, not learned from an online reward model.
- Debug root residual ranks are diagnostic only and were not used to construct the channel.
- A7 does not solve sparse root intervention `u_t`; A8 must consume `calibrated_residuals.jsonl`, not raw residuals.
- A7 uses A5 policy-preview probabilities, not a real randomized probe activation stream.
