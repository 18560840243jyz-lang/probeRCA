# A4 Candidate Subgraph Review

## Scope

This review covers A4 Candidate Subgraph Builder only.

- No fault reinjection was performed.
- No RCA pipeline was run.
- P1 scoring logic was not modified.
- A2 blind rerun results were not modified.
- I/O blind performance was not tuned or repaired.

## Files Changed

- `proberca/adapters/online_boutique/candidate_subgraph.py`
- `proberca/cli/build_candidate_subgraph.py`
- `proberca/cli/run_p2_candidate_preview.py`
- `proberca/cli/check_a4_candidate_subgraph.py`
- `tests/test_online_boutique_candidate_subgraph.py`
- `docs/P2_CANDIDATE_SUBGRAPH.md`
- `docs/audits/A4_CANDIDATE_SUBGRAPH_REVIEW.md`
- `README.md`
- `experiments/README.md`
- `docs/DATA_SCHEMA.md`
- `docs/IMPLEMENTATION_PLAN.md`
- `docs/DECISIONS.md`

## Safety Checks

1. Candidate graph building does not use `root_service`, `root_metric`, or `root_type`.
   - Verdict: PASS.
   - These labels are read only by `evaluate_candidate_subgraph_for_debug` after candidate graph generation.
2. Candidate graph building does not use `target_service`, `target_metric`, or `target_fault_type`.
   - Verdict: PASS.
   - The parser accepts generic service graph destination aliases such as `target`; this is not experiment target config.
3. Candidate graph building does not use `injected_path`.
   - Verdict: PASS.
4. Candidate graph building does not use incident `start_ts` or `end_ts`.
   - Verdict: PASS.
   - A4 consumes A3 alert window timestamps, not incident timestamps.
5. `incidents.jsonl` is used only after build for debug coverage.
   - Verdict: PASS.
   - Debug root coverage does not modify `candidate_services` or metric nodes.
6. Faults were not reinjected.
   - Verdict: PASS.
7. RCA pipeline was not run.
   - Verdict: PASS.
8. P1 scoring logic was not modified.
   - Verdict: PASS.
9. Debug root misses do not backfill candidate graph.
   - Verdict: PASS.

## Candidate Preview Results

- total_repeats: 20
- repeats_with_candidate_graph: 20
- average_candidate_service_count: 11.0
- average_candidate_metric_node_count: 21.75
- debug_root_service_candidate_hit_rate: 1.0
- debug_root_metric_candidate_hit_rate: 1.0

Per fault type:

- CPU: repeats_with_candidate_graph=5, average_candidate_service_count=11.0, average_candidate_metric_node_count=60.0
- Network: repeats_with_candidate_graph=5, average_candidate_service_count=11.0, average_candidate_metric_node_count=9.0
- I/O: repeats_with_candidate_graph=5, average_candidate_service_count=11.0, average_candidate_metric_node_count=9.0
- Lock: repeats_with_candidate_graph=5, average_candidate_service_count=11.0, average_candidate_metric_node_count=9.0

Debug coverage is debug-only and was not used for graph construction.

## Validation Results

- `python3 scripts/check_env.py`: PASS
- `python3 -m proberca.cli.check_project`: PASS
- `python3 -m proberca.cli.check_p0_freeze --freeze-dir docs/p0_freeze_snapshot`: PASS
- `python3 -m proberca.cli.check_p1_freeze --freeze-dir docs/p1_freeze_snapshot`: PASS
- `pytest -q`: PASS
- `python3 -m proberca.cli.build_candidate_subgraph ...`: PASS
- `python3 -m proberca.cli.run_p2_candidate_preview ...`: PASS
- `python3 -m proberca.cli.check_a4_candidate_subgraph ...`: PASS

## Review Verdict

A4_review_passed: true

failed_checks: []

remaining_risks:

- Graph direction is inferred from service graph conventions and recorded in metadata; future service graphs with different conventions should be checked.
- Resource neighbor expansion depends on node, pod, namespace, or host labels being present in raw metrics or metadata.
- A4 is not wired into RCA yet; it only produces candidate graph preview artifacts.
- A4 does not address I/O blind RCA performance from A2.
