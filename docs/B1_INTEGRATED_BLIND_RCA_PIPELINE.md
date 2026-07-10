# B1 Integrated Blind RCA Pipeline

## Goal

把 A3-A9 串成端到端 blind RCA pipeline。

## Input

- raw `metrics.jsonl`
- raw `service_graph.jsonl`

## Pipeline

alert gate -> alert-window blind evidence -> candidate subgraph -> adaptive probe policy -> IPW-RLS -> evidence channel -> graph sparse inversion -> counterfactual -> final RCA result

## Label Safety

B1 不使用 `root_service` / `root_metric` / `root_type`、target labels、`injected_path` 或 `incident.start_ts/end_ts` 参与推理。

`incidents.jsonl` 只能在 pipeline 完成后用于 debug evaluation，不能改变 final RCA result。

## Outputs

- `integrated_rca_results.jsonl`
- `integrated_rca_metadata.json`
- stage outputs under `01_alert_gate` through `09_final_result`

## Limitations

- B1 只做单次 smoke，不做全量 replay。
- B2 才跑 20 次已有 raw metrics。
- B3 才重新真实注入故障。
- 仍然不是真实 eBPF activation。
- 仍然不是生产系统。

## B1R Final Result Repair

B1R repairs final result assembly without changing A8R/A9 algorithms and without using root/target/injected labels.

- It fixes cross-service inconsistency between `top1_service` and `top1_metric`.
- It introduces `metric_candidate_table` as the primary candidate source.
- The primary candidate is selected from metric-level candidates.
- `top_services` is aggregated from metric candidates, not independently selected from raw service scores.
- Every alert window now gets a per-window RCA result.
- Optional repeat-level aggregate output is written separately as `integrated_rca_aggregate.json` with an explicit `aggregation_mode`.
- Path explanation starts from the primary candidate service and uses service-graph BFS only.
- Metadata records `top_service_metric_consistent`, `per_window_results_match_alert_windows`, and `primary_candidate_source`.

## B2R Integrated Ranking Repair

B2R repairs the integrated final candidate ranking after B2 showed that CPU repeats were consistently misranked to memory-family candidates. The repair is limited to label-free final ranking assembly and does not change A8R/A9 algorithms, old P1 scoring, or any historical A2-B2 outputs.

- `metric_candidate_table` now records static metric diagnostic specificity.
- `memory.usage` is treated as weak diagnostic evidence unless strong memory evidence such as `memory.events`, `memory.oom`, `memory.reclaim`, or `memory.pressure` is present in blind evidence.
- CPU throttling diagnostics such as `cpu.throttled_usec`, `cpu.throttled_periods`, and `cpu.throttle_ratio` receive high label-free diagnostic specificity.
- CPU throttling candidates with blind evidence receive a small semantic boost.
- `top_metrics` include score components for specificity, penalties, boosts, evidence, and counterfactual support.
- `predicted_root_type` remains derived from the primary metric family and records `root_type_uses_labels=false`.

These rules are static metric semantics and blind-evidence support only. They do not use `root_service`, `root_metric`, `root_type`, target labels, injected paths, or incident start/end times.

## B2S Service-first RCA Repair

B2R fixed CPU metric-family and root-type recognition, but CPU service Hit@1 remained 0.0. B2S changes the final integrated RCA schema from metric-first to service-first. The primary root service now comes from `service_candidate_table`; the primary root metric is selected only within that root service. Global metric ranking remains available only as `global_top_metrics_auxiliary` and is not a primary RCA result.

B2S adds service-conditioned evaluation fields: `service_conditioned_metric_hit_at_3`, `global_metric_hit_at_3_auxiliary`, and `service_metric_pair_hit_at_1`. Labels remain post-hoc evaluation only. B2S does not use root labels, target labels, injected paths, incident start/end timestamps, or legacy target-aware evidence for inference. B2S is still replay over existing raw metrics; B3 is the future real reinjection stage.

## B2M Service-Metric Ownership Mapping Repair

B2S already switched the integrated RCA output to a service-first hierarchy, but CPU service localization remained weak. B2M adds explicit service-metric ownership mapping so each service's own resource metric remains tied to that service throughout final assembly, for example `paymentservice.cpu.throttled_usec`, `adservice.cpu.throttled_usec`, and `checkoutservice.cpu.throttled_usec`.

Evidence support is now separated into node-level, service-family-level, and family-global-level support. Family-global evidence is kept as a weak fallback only, with `family_global_evidence_weight = 0.10`, so global CPU evidence cannot by itself make all CPU services equivalent. Primary RCA candidates must pass ownership checks, and final metadata records `service_local_support_used`, `global_family_support_weight_limited`, `ownership_invalid_count`, and `primary_candidate_ownership_valid`.

B2M remains an existing-raw-metrics replay. It does not use root labels, target labels, injected paths, or incident start/end during inference, and it is not B3 real re-injection.

## B2P Normal Propagation Audit and Repair

B2M ruled out service-metric ownership loss as the main CPU failure mode. CPU service localization remained weak, so B2P adds a stable-only structured multi-lag propagation support stage. This stage learns label-free parent sets and lagged propagation weights from existing raw metrics, alert windows, candidate nodes, and probe-policy sampling probabilities. It does not implement propagation drift.

The parent set is structure constrained rather than fully connected: self-lag, same-service resource -> request, same-service request -> request, callee resource/request -> caller request, and request-chain propagation. It does not use root labels, target labels, injected paths, or incident start/end.

The integrated pipeline now emits `05b_structured_propagation/` with structured parent sets, propagation edges, predictions, residuals, and metadata. Final service scoring consumes `structured_propagation_support`, `path_edge_support`, and `lag_support`; if structured support is unavailable, fallback is explicit in score components. B2P remains an existing-raw-metrics replay, not B3 real re-injection.

