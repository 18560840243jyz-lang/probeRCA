# B1R Integrated Blind RCA Review

## Scope

- 本轮只修复 B1 final result assembly。
- 没有进入 B2。
- 没有重新注入故障。
- 没有运行旧 P1 RCA pipeline。
- 没有修改 P1 scoring logic。
- 没有真实开启 probe。

## Previous B1 Problems

- `alert_windows_count=2` but `final_results_count=1`。
- `top1_service=adservice` but `top1_metric=redis-cart.memory.usage`。
- debug metrics all 0。
- `AGENTS.md` / `skills/proberca/SKILL.md` 状态文案仍写早期 P0-only，和 A10/B1 状态冲突。

## B1R Changes

- Added primary candidate abstraction.
- Added `metric_candidate_table`.
- Enforced top service / top metric consistency.
- Generated per-window RCA results.
- Added optional aggregate result with explicit `aggregation_mode`.
- Strengthened `check_b1_integrated_pipeline`.
- Updated `AGENTS.md` and `skills/proberca/SKILL.md` current status.

## Safety Checks

- root labels used for inference: false.
- target labels used for inference: false.
- injected path used for inference: false.
- incident start/end used for inference: false.
- legacy target-aware `evidence.jsonl` used: false.
- old P1 RCA pipeline run: false.
- fault reinjection: false.
- real eBPF probe activation: false.
- `incidents.jsonl` is used only for post-result debug evaluation.
- top1 service and top1 metric service are required to be consistent.
- per-window result count is required to match alert window count.

## Smoke Result

Executed B1R smoke run:

- `alert_windows_count`: 2
- `per_window_results_count`: 2
- `aggregate_result_count`: 1
- `top1_service`: redis-cart
- `top1_metric`: redis-cart.memory.usage
- `predicted_root_type`: memory
- `path_status`: found
- `path`: redis-cart -> cartservice -> frontend
- `top_service_metric_consistent`: true
- `per_window_results_match_alert_windows`: true
- `debug_service_hit_at_1`: 0.0, debug-only
- `debug_metric_hit_at_3`: 0.0, debug-only
- `debug_root_type_accuracy`: 0.0, debug-only
- `debug_path_fidelity`: 0.0, debug-only

The debug metrics remain poor for this CPU smoke case. B1R is a schema and assembly repair; it does not tune A8R/A9 ranking and does not use labels to repair the result.

## Review Verdict

- `B1R_review_passed`: true
- `failed_checks`: []
- `remaining_risks`:
  - B1R is only a single smoke repair, not B2 full replay.
  - Smoke debug quality may still be poor and must not be used as acceptance.
  - A5 remains policy preview, not real probe activation.
  - The system is still not production-ready.
