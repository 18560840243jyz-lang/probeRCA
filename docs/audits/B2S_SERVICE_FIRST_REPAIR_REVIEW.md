# B2S Service-first Integrated RCA Repair Review

## Scope

- 本轮只修复 integrated final RCA 的 service-first hierarchy。
- 没有重新注入故障。
- 没有运行旧 P1 RCA pipeline。
- 没有修改 P1 scoring logic。
- 没有真实开启 probe。
- 没有进入 B3。

## Previous B2R Problems

- CPU service Hit@1 = 0.0
- CPU metric Hit@3 = 1.0
- CPU root type = 1.0
- 问题集中在 service localization，而不是 metric family/root type。
- 旧逻辑仍偏 metric-first，不适合作为 primary RCA schema。

## B2S Changes

- Added `service_candidate_table`.
- Added service-first root service selection.
- Added metric-within-service root metric selection.
- Added `global_top_metrics_auxiliary`.
- Added `service_conditioned_metric_hit_at_3`.
- Added `service_metric_pair_hit_at_1`.
- Enhanced B1/B2 check CLIs for service-first schema.

## Safety Checks

- uses root_service/root_metric/root_type for inference: false
- uses target_service/target_metric/target_fault_type: false
- uses injected_path for inference: false
- uses incident.start_ts/end_ts for inference: false
- uses legacy evidence: false
- runs old P1 RCA pipeline: false
- reinjects faults: false
- actual eBPF probe activation: false
- labels only after final result for evaluation: true
- primary root_metric selected only within root_service: true
- global_top_metrics auxiliary only: true

## B2S Results

- total_repeats = 20
- repeats_completed = 20
- repeats_failed = 0
- service_hit_at_1_overall = 0.8
- service_conditioned_metric_hit_at_3_overall = 0.8
- global_metric_hit_at_3_auxiliary_overall = 1.0
- service_metric_pair_hit_at_1_overall = 0.7
- root_type_accuracy_overall = 1.0
- path_fidelity_overall = 1.0
- auxiliary_metric_mrr_overall = 0.7416666666666667

### Per Fault Type

- cpu: service=0.2, service_conditioned_metric@3=0.2, global_aux_metric@3=1.0, pair@1=0.2, root_type=1.0, path=1.0, mrr=0.2
- network: service=1.0, service_conditioned_metric@3=1.0, global_aux_metric@3=1.0, pair@1=0.8, root_type=1.0, path=1.0, mrr=0.9
- io: service=1.0, service_conditioned_metric@3=1.0, global_aux_metric@3=1.0, pair@1=1.0, root_type=1.0, path=1.0, mrr=1.0
- lock: service=1.0, service_conditioned_metric@3=1.0, global_aux_metric@3=1.0, pair@1=0.8, root_type=1.0, path=1.0, mrr=0.8666666666666667

## B2R vs B2S Comparison

| Metric | B2R | B2S |
| --- | ---: | ---: |
| Overall service Hit@1 | 0.75 | 0.8 |
| Primary metric Hit@3 | global 1.0 | service-conditioned 0.8 |
| Root type accuracy | 1.0 | 1.0 |
| CPU service Hit@1 | 0.0 | 0.2 |
| CPU metric Hit@3 | global 1.0 | service-conditioned 0.2 |
| CPU root type accuracy | 1.0 | 1.0 |

## B3 Gate Recommendation

- b3_gate_recommended: false
- reason: B3 gate not recommended: service Hit@1 is 0.800 and CPU service Hit@1 is 0.200; service localization still needs repair before real reinjection.

## Review Verdict

- B2S_review_passed: true
- failed_checks: []
- remaining_risks: ["B2S service-first schema is structurally correct but service Hit@1 is 0.8, below the 0.9 B3 gate threshold.", "CPU service Hit@1 improved from 0.0 to 0.2 only; CPU service localization remains weak.", "B2S primary service-conditioned metric@3 is 0.8, while global auxiliary metric@3 remains 1.0; these must not be conflated.", "B2S is replay over existing raw metrics, not real reinjection."]
