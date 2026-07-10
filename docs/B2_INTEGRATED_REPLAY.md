# B2 Integrated Replay Existing Raw Metrics

## Goal

用已有 20 次真实 raw metrics 全量运行 B1R integrated blind RCA pipeline。

## Scope

- 不重新注入故障。
- 不运行旧 P1 RCA。
- 不修改 scoring。
- labels 只用于 final result 之后的 evaluation。

## Inputs

- existing raw `metrics.jsonl`
- `service_graph.jsonl`
- `incidents.jsonl` only for post-hoc evaluation

## Outputs

- `p2_integrated_replay_summary.json`
- `p2_integrated_replay_metadata.json`
- `p2_integrated_replay_failures.json`
- per-repeat stage outputs under `<fault_type>/repeat_XX/01_alert_gate` through `09_final_result`

## Interpretation

B2 是 integrated pipeline replay，不等同于重新真实注入。B3 才重新注入故障。

A2 和 B2 不是同一条 pipeline：A2 是 frozen P1 blind rerun；B2 是 B1R integrated blind RCA pipeline replay。

## Known Risk

B1R CPU smoke debug 为 0，所以 B2 结果可能较差，必须如实报告，不能为了指标修改算法或使用 labels。

## B2R Integrated Ranking Repair

B2 found that the integrated replay was structurally label-safe but CPU repeats all failed at service, metric, and root-type evaluation. Diagnosis showed that memory-family candidates, especially `memory.usage`, could outrank CPU throttling metrics because the final ranking lacked metric diagnostic specificity.

B2R repairs only the integrated final candidate ranking:

- Adds static label-free metric diagnostic specificity.
- Treats `memory.usage` as weak diagnostic evidence unless strong memory evidence exists.
- Gives CPU throttling metrics high diagnostic specificity.
- Applies a small CPU diagnostic boost when CPU blind evidence is present.
- Records transparent `score_components` in `metric_candidate_table` and final `top_metrics`.
- Adds `root_type_confidence`, with `root_type_source="primary_metric_family"` and `root_type_uses_labels=false`.

B2R replay still uses only existing raw metrics. It does not reinject faults, does not run the old P1 RCA pipeline, does not modify P1 scoring, and does not use incident labels for inference. Incident labels remain post-hoc evaluation only. B3 is the future real reinjection stage.

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

