# A8 Graph Sparse Inversion Review

## Scope

This round only implements graph sparse inversion preview.

- No old P1 RCA pipeline was run.
- No real fault injection was run.
- P1 scoring logic was not modified.
- No real probe was activated.
- I/O blind result was not tuned or repaired.

## Files Changed

- `proberca/inference/graph_sparse_inversion.py`
- `proberca/adapters/online_boutique/graph_sparse_preview.py`
- `proberca/cli/run_graph_sparse_inversion.py`
- `proberca/cli/run_p2_graph_sparse_preview.py`
- `proberca/cli/check_a8_graph_sparse.py`
- `tests/test_graph_sparse_inversion.py`
- `tests/test_online_boutique_graph_sparse_preview.py`
- `docs/P2_GRAPH_SPARSE_INVERSION.md`
- `docs/audits/A8_GRAPH_SPARSE_INVERSION_REVIEW.md`
- `README.md`
- `experiments/README.md`
- `docs/DATA_SCHEMA.md`
- `docs/IMPLEMENTATION_PLAN.md`
- `docs/DECISIONS.md`

## Safety Checks

1. sparse inversion uses `root_service` / `root_metric` / `root_type`: false for inversion; debug-only after outputs.
2. sparse inversion uses `target_service` / `target_metric` / `target_fault_type`: false.
3. sparse inversion uses `injected_path`: false.
4. sparse inversion uses `incident.start_ts` / `incident.end_ts`: false.
5. `incidents.jsonl` use: debug ranking only after inversion outputs are written.
6. consumes A7 calibrated residuals: true.
7. directly consumes A6 raw residuals: false.
8. uses ADMM graph sparse objective: true.
9. residual lift fallback used: false.
10. old P1 RCA pipeline run: false.
11. P1 scoring logic modified: false.
12. real fault injection run: false.

## Graph Sparse Preview Results

- total_repeats: 20
- repeats_completed: 20
- average_node_count: 21.75
- average_edge_count: 399.5
- average_nonzero_intervention_count: 19.35
- debug_service_hit_at_1_overall: 0.2
- debug_metric_hit_at_3_overall: 0.75
- debug_root_type_accuracy_overall: 0.75

Per fault type:

| fault_type | avg nodes | avg edges | avg nonzero | solver status | debug service Hit@1 | debug metric Hit@3 | debug root type accuracy |
|---|---:|---:|---:|---|---:|---:|---:|
| CPU | 60.0 | 1530.0 | 54.4 | max_iter_reached: 5 | 0.0 | 0.0 | 1.0 |
| I/O | 9.0 | 16.0 | 6.2 | converged: 5 | 0.0 | 1.0 | 1.0 |
| Lock | 9.0 | 36.0 | 8.4 | converged: 5 | 0.2 | 1.0 | 0.0 |
| Network | 9.0 | 16.0 | 8.4 | converged: 5 | 0.6 | 1.0 | 1.0 |

The debug hit rates are post-hoc diagnostics only and are not formal P2E results.

## Test Results

- `python3 scripts/check_env.py`: passed.
- `python3 -m proberca.cli.check_project`: passed.
- `python3 -m proberca.cli.check_p0_freeze --freeze-dir docs/p0_freeze_snapshot`: passed.
- `python3 -m proberca.cli.check_p1_freeze --freeze-dir docs/p1_freeze_snapshot`: passed.
- `pytest -q`: reached 100% with no failures in terminal output.
- `python3 -m proberca.cli.run_graph_sparse_inversion ...`: passed for CPU repeat 01.
- `python3 -m proberca.cli.run_p2_graph_sparse_preview ...`: passed for 20 repeats.
- `python3 -m proberca.cli.check_a8_graph_sparse ...`: passed.

## Review Verdict

A8_review_passed: true

failed_checks: []

remaining_risks:

- CPU candidate graphs are larger and all 5 CPU repeats reached `max_iter_reached`; this is not numeric failure, but convergence tuning remains future work.
- Debug service Hit@1 is low in A8 preview, especially CPU and I/O; this is reported as-is and not hidden.
- A8 is not yet connected to the final online RCA result schema.
- A8 debug metrics are post-hoc only and must not be presented as final P2E acceptance metrics.
- A9 still needs counterfactual explanation.
