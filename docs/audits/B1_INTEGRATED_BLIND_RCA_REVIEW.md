# B1 Integrated Blind RCA Review

## Scope

本轮只实现端到端集成 pipeline 和单次 smoke。

- 没有进入 B2。
- 没有重新注入故障。
- 没有运行旧 P1 RCA pipeline。
- 没有修改 P1 scoring logic。
- 没有真实开启 probe。

## Files Changed

- `proberca/adapters/online_boutique/blind_evidence.py`
- `proberca/adapters/online_boutique/integrated_pipeline.py`
- `proberca/cli/run_integrated_blind_rca.py`
- `proberca/cli/check_b1_integrated_pipeline.py`
- `tests/test_online_boutique_integrated_pipeline.py`
- `docs/B1_INTEGRATED_BLIND_RCA_PIPELINE.md`
- `README.md`
- `experiments/README.md`
- `docs/DATA_SCHEMA.md`
- `docs/IMPLEMENTATION_PLAN.md`
- `docs/DECISIONS.md`
- `docs/audits/B1_INTEGRATED_BLIND_RCA_REVIEW.md`

## Pipeline Stages

- `01_alert_gate`: generated `alert_events.jsonl`, `alert_windows.jsonl`, `alert_gate_metadata.json`.
- `02_blind_evidence`: generated alert-window `blind_evidence.jsonl` and metadata.
- `03_candidate_subgraph`: generated repeat summary and per-window candidate services, metrics, and edges.
- `04_probe_policy`: generated probe plan, sampling log, observation mask, and metadata.
- `05_ipw_rls`: generated online RLS state, edges, predictions, residuals, and metadata.
- `06_evidence_channel`: generated evidence vectors, effects, calibrated residuals, and metadata.
- `07_graph_sparse`: generated sparse interventions, metric scores, service scores, objective trace, and metadata.
- `08_counterfactual`: generated metric/service counterfactual explanations and rankings.
- `09_final_result`: generated `integrated_rca_results.jsonl` and `integrated_rca_metadata.json`.

## Safety Checks

- root labels used for inference: false.
- target config used for inference: false.
- injected path used for inference: false.
- incident start/end used for evidence/window/candidate/RLS/sparse/counterfactual: false.
- legacy target-aware `evidence.jsonl` used: false.
- old P1 RCA pipeline run: false.
- fault reinjection: false.
- real eBPF probe activation: false.
- `incidents.jsonl` is used only for post-hoc debug evaluation when requested.

## Smoke Result

- `alert_windows_count`: 2
- `final_results_count`: 1
- `top1_service`: adservice
- `top1_metric`: redis-cart.memory.usage
- `predicted_root_type`: memory
- `path_status`: found
- debug-only `service_hit_at_1`: 0.0
- debug-only `metric_hit_at_3`: 0.0
- debug-only `root_type_accuracy`: 0.0
- debug-only `path_fidelity`: 0.0

These debug metrics are post-hoc diagnostics for the single CPU repeat smoke and are not a formal B2/P2E acceptance result.

## Review Verdict

- `B1_review_passed`: true
- `failed_checks`: []
- `remaining_risks`:
  - B1 is only a single smoke, not the 20-repeat B2 replay.
  - Smoke debug result is poor for CPU repeat_01 and must not be packaged as acceptance.
  - Integrated schema is new and still needs B2 replay quality analysis.
  - A5 remains policy preview, not real probe activation.
  - B1 does not make the system production-ready.
