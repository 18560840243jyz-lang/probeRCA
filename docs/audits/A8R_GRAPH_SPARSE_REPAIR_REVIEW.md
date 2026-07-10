# A8R Graph Sparse Repair Review

## Scope

This round only repairs A8 graph sparse inversion. It does not enter A9, does not run the old P1 RCA pipeline, does not reinject faults, does not modify P1 scoring logic, does not activate real probes, and does not modify the old A2 blind rerun result.

## Previous A8 Problems

- average_nonzero_intervention_count = 19.35
- average_node_count = 21.75
- nonzero_ratio = 0.8896551724137932
- CPU avg nonzero = 54.4
- CPU solver max_iter_reached = 5/5
- debug_service_hit_at_1_overall = 0.2
- debug_metric_hit_at_3_overall = 0.75

## A8R Changes

- Edge cap: metric-level expansion now avoids all-to-all service-edge expansion and caps per-node degree.
- positive_topk_mean residual signal: aggregation uses only A7 `calibrated_residual`, not raw residual.
- Family penalty: request/load symptom metrics are penalized so latency symptoms do not automatically dominate resource signals.
- Blind-evidence support boost: A7 h-value / blind evidence can boost signal without labels.
- Auto lambda: L1 and group regularization are derived from unlabeled residual signal distributions.
- Post sparsify: output interventions are capped by nonzero ratio and per-service metric limits.
- Adaptive rho / max_iter: ADMM now supports adaptive rho and defaults to 1000 iterations.

## Safety Checks

- sparse inversion uses root_service/root_metric/root_type: false
- sparse inversion uses target_service/target_metric/target_fault_type: false
- sparse inversion uses injected_path: false
- sparse inversion uses incident.start_ts/end_ts: false
- incidents.jsonl is used only after inversion for debug ranking: true
- consumes A7 calibrated_residuals: true
- directly consumes A6 raw_residuals: false
- uses ADMM graph sparse objective: true
- residual lift fallback used: false
- runs old P1 RCA pipeline: false
- modifies P1 scoring logic: false
- reinjects faults: false

## A8R Results

- total_repeats = 20
- repeats_completed = 20
- average_node_count = 21.75
- average_edge_count = 100.75
- average_nonzero_intervention_count = 7.55
- nonzero_ratio = 0.3471264367816092
- debug_service_hit_at_1_overall = 0.95
- debug_metric_hit_at_3_overall = 0.75
- debug_root_type_accuracy_overall = 0.75

Per fault type:

| Fault | Avg nonzero | Nonzero ratio | Solver status | Debug service Hit@1 | Debug metric Hit@3 |
| --- | ---: | ---: | --- | ---: | ---: |
| CPU | 19.8 | 0.33 | {'converged': 5} | 0.8 | 0.0 |
| Network | 4.0 | 0.4444444444444444 | {'converged': 5} | 1.0 | 1.0 |
| I/O | 3.2 | 0.35555555555555557 | {'converged': 5} | 1.0 | 1.0 |
| Lock | 3.2 | 0.35555555555555557 | {'converged': 5} | 1.0 | 1.0 |

## Compare A8 vs A8R

| Metric | A8 | A8R |
| --- | ---: | ---: |
| average_nonzero_intervention_count | 19.35 | 7.55 |
| nonzero_ratio | 0.8896551724137932 | 0.3471264367816092 |
| CPU avg nonzero | 54.4 | 19.8 |
| CPU max_iter_reached count | 5 | 0 |
| debug service Hit@1 | 0.2 | 0.95 |
| debug metric Hit@3 | 0.75 | 0.75 |

## Review Verdict

- A8R_review_passed: true
- failed_checks: []
- remaining_risks:
  - Debug metric Hit@3 remains 0.75; it is debug-only and not a P2E acceptance metric.
  - CPU debug metric Hit@3 remains 0.0; no root labels were used to tune it.
  - Lock debug root type accuracy is 0.0; this remains a debug-only limitation.
  - A8R is still a sparse inversion preview and has not been integrated into final online RCA output.
