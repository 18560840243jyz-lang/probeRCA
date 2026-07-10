# A9 Counterfactual Explanation Review

## Scope

This round only implements counterfactual explanation preview.

- It did not enter A10.
- It did not run the old P1 RCA pipeline.
- It did not reinject faults.
- It did not modify P1 scoring logic.
- It did not activate real probes.
- It did not modify the old A2 blind rerun result.

## Files Changed

- `proberca/explain/counterfactual_explanation.py`
- `proberca/adapters/online_boutique/counterfactual_preview.py`
- `proberca/cli/run_counterfactual_explanation.py`
- `proberca/cli/run_p2_counterfactual_preview.py`
- `proberca/cli/check_a9_counterfactual.py`
- `tests/test_counterfactual_explanation.py`
- `tests/test_online_boutique_counterfactual_preview.py`
- `docs/P2_COUNTERFACTUAL_EXPLANATION.md`
- `docs/audits/A9_COUNTERFACTUAL_EXPLANATION_REVIEW.md`

## Safety Checks

- counterfactual uses root_service/root_metric/root_type: false
- counterfactual uses target_service/target_metric/target_fault_type: false
- counterfactual uses injected_path: false
- counterfactual uses incident.start_ts/end_ts: false
- incidents.jsonl is used only after counterfactual outputs for debug ranking: true
- consumes A8R sparse interventions: true
- re-optimization with candidate removed: true
- fast approximation only: false
- runs old P1 RCA pipeline: false
- modifies P1 scoring logic: false
- reinjects faults: false

## Counterfactual Preview Results

- total_repeats = 20
- repeats_completed = 20
- average_metric_counterfactual_count = 9.25
- average_service_counterfactual_count = 2.75
- average_metric_delta_loss = 5.67402454923226
- average_service_delta_loss = 21.48256938786559
- debug_counterfactual_service_hit_at_1_overall = 0.95
- debug_counterfactual_metric_hit_at_3_overall = 0.75
- debug_counterfactual_root_type_accuracy_overall = 0.5

Per fault type:

| Fault | Metric CF count | Service CF count | Avg metric Delta L | Avg service Delta L | Debug service Hit@1 | Debug metric Hit@3 | Debug root type |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| CPU | 10.0 | 5.0 | 11.84296634201703 | 32.112082105724404 | 0.8 | 0.0 | 0.0 |
| Network | 9.0 | 2.0 | 3.42167624784246 | 17.90228043564489 | 1.0 | 1.0 | 1.0 |
| I/O | 9.0 | 2.0 | 4.5983553398099 | 17.608668436159768 | 1.0 | 1.0 | 1.0 |
| Lock | 9.0 | 2.0 | 2.8331002672596473 | 18.307246573933313 | 1.0 | 1.0 | 0.0 |

## Compare A8R vs A9 Debug

| Debug metric | A8R | A9 |
| --- | ---: | ---: |
| service Hit@1 | 0.95 | 0.95 |
| metric Hit@3 | 0.75 | 0.75 |
| root type accuracy | 0.75 | 0.5 |

These debug metrics are post-hoc diagnostics and are not formal P2E acceptance.

## Review Verdict

- A9_review_passed: true
- failed_checks: []
- remaining_risks:
  - CPU debug metric Hit@3 remains 0.0.
  - Counterfactual root type accuracy decreased from A8R 0.75 to A9 0.5.
  - A9 is still a preview and has not been integrated into a final online RCA output schema.
