# B2R Integrated Ranking Repair Review

## Scope

- 本轮只修复 integrated final candidate ranking。
- 没有重新注入故障。
- 没有运行旧 P1 RCA pipeline。
- 没有修改 P1 scoring logic。
- 没有真实开启 probe。
- 没有进入 B3。

## Previous B2 Problems

- CPU service/metric/root type 全部 0。
- CPU 被 misrank 到 memory-family candidates，尤其是 `memory.usage`。
- B2 overall service/metric/root type = 0.75。

## B2R Changes

- Added static label-free metric diagnostic specificity.
- Added weak `memory.usage` penalty unless strong memory evidence exists.
- Added CPU throttling diagnostic boost when CPU blind evidence exists.
- Added `root_type_confidence`, `root_type_source`, and `root_type_uses_labels=false`.
- Made `score_components` transparent in metric candidates and final top metrics.
- Enhanced B1/B2 check CLIs to require diagnostic specificity fields and root-type label-safety fields.

## Safety Checks

- sparse/integrated ranking uses root_service/root_metric/root_type for inference: false
- ranking uses target_service/target_metric/target_fault_type: false
- ranking uses injected_path for inference: false
- ranking uses incident.start_ts/end_ts for inference: false
- ranking uses legacy evidence: false
- runs old P1 RCA pipeline: false
- reinjects faults: false
- actual eBPF probe activation: false
- labels only after final result for evaluation: true
- diagnostic specificity is static label-free metric semantics: true

## B2R Results

- total_repeats = 20
- repeats_completed = 20
- repeats_failed = 0
- service_hit_at_1_overall = 0.75
- metric_hit_at_3_overall = 1.0
- root_type_accuracy_overall = 1.0
- path_fidelity_overall = 1.0
- auxiliary_metric_hit_at_1_overall = 0.8
- auxiliary_metric_mrr_overall = 0.8833333333333333

### Per Fault Type

- cpu: service=0.0, metric@3=1.0, root_type=1.0, path=1.0, metric@1=0.6, mrr=0.7666666666666666
- network: service=1.0, metric@3=1.0, root_type=1.0, path=1.0, metric@1=0.8, mrr=0.9
- io: service=1.0, metric@3=1.0, root_type=1.0, path=1.0, metric@1=1.0, mrr=1.0
- lock: service=1.0, metric@3=1.0, root_type=1.0, path=1.0, metric@1=0.8, mrr=0.8666666666666667

### CPU Before/After

- B2 CPU: service=0.0, metric@3=0.0, root_type=0.0.
- B2R CPU: service=0.0, metric@3=1.0, root_type=1.0.
- Interpretation: B2R fixed CPU metric-family/root-type misranking but did not fix CPU service localization; CPU service Hit@1 remains 0.0.

## B2 vs B2R Comparison

| Metric | B2 | B2R |
| --- | ---: | ---: |
| Overall service Hit@1 | 0.75 | 0.75 |
| Overall metric Hit@3 | 0.75 | 1 |
| Overall root type accuracy | 0.75 | 1 |
| CPU service Hit@1 | 0.0 | 0 |
| CPU metric Hit@3 | 0.0 | 1 |
| CPU root type accuracy | 0.0 | 1 |

## Review Verdict

- B2R_review_passed: true
- failed_checks: []
- remaining_risks: ["CPU service Hit@1 remains 0.0: ranking selects CPU-family metrics on neighboring services rather than paymentservice.", "B2R is replay over existing raw metrics, not real reinjection.", "Diagnostic specificity is a static semantic prior and should be validated in B3 rather than treated as production evidence.", "B2R should not be merged with A2 official blind rerun as a single acceptance claim."]

Labels are used only for post-hoc evaluation, not for inference or ranking repair.
