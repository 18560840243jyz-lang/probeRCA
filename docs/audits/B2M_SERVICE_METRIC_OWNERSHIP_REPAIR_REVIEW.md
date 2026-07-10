# B2M Service-Metric Ownership Mapping Repair Review

## Scope
- 本轮只修复 service-metric ownership mapping 和 service-local scoring。
- 没有重新注入故障。
- 没有运行旧 P1 RCA pipeline。
- 没有修改 P1 scoring logic。
- 没有真实开启 probe。
- 没有进入 B3。

## Previous B2S Problems
- CPU service Hit@1 = 0.2
- CPU global auxiliary metric@3 = 1.0
- CPU service-conditioned metric@3 = 0.2
- 说明真实 CPU 指标能被全局发现，但服务级归属/服务级排序仍弱。

## B2M Changes
- 新增 `proberca/adapters/online_boutique/service_metric_identity.py`。
- 新增 node_id/service/metric ownership validation and repair。
- blind evidence 和 evidence channel 新输出 `node_id`、`metric_family`、`ownership_valid`。
- integrated final candidate scoring 拆分 evidence support 为 node evidence、service-family evidence、family-global evidence。
- family-global evidence 限制为弱 fallback，`family_global_evidence_weight = 0.10`。
- primary candidate ownership check 已加入 B1/B2 check CLI。

## Ownership Audit Results
- CPU repeat ownership_invalid_count: [0, 0, 0, 0, 0]
- primary_candidate_ownership_valid across repeats: True
- paymentservice.cpu.* debug-only: all 5 CPU repeats preserve `service=paymentservice`, `ownership_valid=true`, and `service_matches_node_id=true` for paymentservice CPU rows, but B2M replay shows competing service-local CPU evidence remains stronger in several CPU repeats.
- 错误服务压过原因仍存在：CPU service candidates such as currencyservice/frontend/adservice can have high node/service-family CPU evidence and high service-local score, so ownership repair alone does not resolve service localization.

## Safety Checks
1. sparse/integrated inference 是否使用 root_service/root_metric/root_type：false。
2. 是否使用 target_service/target_metric/target_fault_type：false。
3. 是否使用 injected_path：false for inference; only post-hoc path_fidelity evaluation may read it。
4. 是否使用 incident.start_ts/end_ts：false for inference。
5. 是否使用 legacy evidence：false。
6. 是否运行旧 P1 RCA pipeline：false。
7. 是否重新注入故障：false。
8. 是否真实开启 eBPF probe：false。
9. labels 是否只在 final result 后用于 evaluation：true。
10. ownership repair 是否只用 node_id/service/metric 字段，不用 labels：true。
11. family-global evidence 是否仅为弱 fallback：true, weight 0.10。

## B2M Results
- total_repeats: 20
- repeats_completed: 20
- repeats_failed: 0
- service_hit_at_1_overall: 0.75
- service_conditioned_metric_hit_at_3_overall: 0.75
- global_metric_hit_at_3_auxiliary_overall: 0.95
- service_metric_pair_hit_at_1_overall: 0.7
- root_type_accuracy_overall: 1.0
- path_fidelity_overall: 1.0
- auxiliary metric_mrr: 0.7166666666666667
- per_fault_type:

```json
[
  {
    "auxiliary_metric_hit_at_1_mean": 0.0,
    "auxiliary_metric_mrr_mean": 0.0,
    "fault_type": "cpu",
    "global_metric_hit_at_3_auxiliary_mean": 0.8,
    "metric_hit_at_3_mean": 0.0,
    "path_fidelity_mean": 1.0,
    "primary_metric_hit_at_3_mean": 0.0,
    "repeats": 5,
    "repeats_completed": 5,
    "root_type_accuracy_mean": 1.0,
    "service_conditioned_metric_hit_at_1_mean": 0.0,
    "service_conditioned_metric_hit_at_3_mean": 0.0,
    "service_hit_at_1_mean": 0.0,
    "service_metric_pair_hit_at_1_mean": 0.0
  },
  {
    "auxiliary_metric_hit_at_1_mean": 1.0,
    "auxiliary_metric_mrr_mean": 1.0,
    "fault_type": "network",
    "global_metric_hit_at_3_auxiliary_mean": 1.0,
    "metric_hit_at_3_mean": 1.0,
    "path_fidelity_mean": 1.0,
    "primary_metric_hit_at_3_mean": 1.0,
    "repeats": 5,
    "repeats_completed": 5,
    "root_type_accuracy_mean": 1.0,
    "service_conditioned_metric_hit_at_1_mean": 1.0,
    "service_conditioned_metric_hit_at_3_mean": 1.0,
    "service_hit_at_1_mean": 1.0,
    "service_metric_pair_hit_at_1_mean": 1.0
  },
  {
    "auxiliary_metric_hit_at_1_mean": 1.0,
    "auxiliary_metric_mrr_mean": 1.0,
    "fault_type": "io",
    "global_metric_hit_at_3_auxiliary_mean": 1.0,
    "metric_hit_at_3_mean": 1.0,
    "path_fidelity_mean": 1.0,
    "primary_metric_hit_at_3_mean": 1.0,
    "repeats": 5,
    "repeats_completed": 5,
    "root_type_accuracy_mean": 1.0,
    "service_conditioned_metric_hit_at_1_mean": 1.0,
    "service_conditioned_metric_hit_at_3_mean": 1.0,
    "service_hit_at_1_mean": 1.0,
    "service_metric_pair_hit_at_1_mean": 1.0
  },
  {
    "auxiliary_metric_hit_at_1_mean": 0.8,
    "auxiliary_metric_mrr_mean": 0.8666666666666667,
    "fault_type": "lock",
    "global_metric_hit_at_3_auxiliary_mean": 1.0,
    "metric_hit_at_3_mean": 1.0,
    "path_fidelity_mean": 1.0,
    "primary_metric_hit_at_3_mean": 1.0,
    "repeats": 5,
    "repeats_completed": 5,
    "root_type_accuracy_mean": 1.0,
    "service_conditioned_metric_hit_at_1_mean": 0.8,
    "service_conditioned_metric_hit_at_3_mean": 1.0,
    "service_hit_at_1_mean": 1.0,
    "service_metric_pair_hit_at_1_mean": 0.8
  }
]
```

## B2S vs B2M Comparison
| Metric | B2S | B2M |
| --- | ---: | ---: |
| overall service | 0.8 | 0.75 |
| service-conditioned metric@3 | 0.8 | 0.75 |
| CPU service | 0.2 | 0.0 |
| CPU service-conditioned metric@3 | 0.2 | 0.0 |
| CPU root type | 1.0 | 1.0 |

## B3 Gate Recommendation
- b3_gate_recommended: false
- reason: B2M structural checks pass, but service accuracy and CPU service localization remain below B3 gate thresholds.

## Review Verdict
- B2M_review_passed: true
- failed_checks: []
- remaining_risks:
  - CPU service localization is still poor after ownership repair: service Hit@1 = 0.0.
  - Overall service Hit@1 decreased from B2S 0.8 to B2M 0.75.
  - Global auxiliary metric discovery remains better than service-conditioned primary metric, indicating service scoring still needs work before B3.
  - B3 gate is not recommended.
