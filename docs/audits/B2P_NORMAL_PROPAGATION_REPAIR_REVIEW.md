# B2P Normal Propagation Audit and Repair Review

## Scope
- 本轮只审计并修复正常传播学习。
- 没有重新注入故障。
- 没有运行旧 P1 RCA pipeline。
- 没有修改 P1 scoring logic。
- 没有真实开启 probe。
- 没有进入 B3。
- 未实现 propagation drift，坚持 stable-only。

## Previous B2M Problems
- CPU service Hit@1 = 0.0
- ownership_invalid_count = 0
- CPU root type = 1.0
- 说明问题从 ownership 转向 propagation support。

## Propagation Audit Results
- graph direction assumption: service_graph `src->dst` is caller->callee; impact/path explanation traverses callee->caller.
- parent set coverage: structured parent sets include self-lag, same-service resource/request, and cross-service resource/request to request relations.
- average parent_edge_count: 79.25
- average learned_edge_count: 237.75
- average cross_service_edge_count: 30.0
- average resource_to_request_edge_count: 36.25
- learned propagation weight summary: edges are learned by multi-lag ridge and exposed through effective_weight/path_edge_support.
- structured propagation support is now present in service score components.
- likely_failure_causes from audit: single_lag_too_simple, propagation_not_used_in_service_score, cross_service_edges_too_weak. B2P addresses the first two structurally, but CPU localization remains weak.

## B2P Changes
- `proberca/propagation/structured_multilag.py`
- structured parent sets
- multi-lag ridge propagation
- resource-to-request propagation
- service-to-symptom propagation support
- integrated service score 接入 `structured_propagation_support`

## Safety Checks
1. 是否使用 root_service/root_metric/root_type 参与 propagation learning：false。
2. 是否使用 target_service/target_metric/target_fault_type：false。
3. 是否使用 injected_path：false。
4. 是否使用 incident.start_ts/end_ts：false。
5. 是否使用 legacy evidence：false。
6. 是否运行旧 P1 RCA pipeline：false。
7. 是否重新注入故障：false。
8. 是否真实开启 eBPF probe：false。
9. labels 是否只在 final result 后用于 evaluation：true。
10. propagation drift 是否未使用：true。
11. parent set 是否不是全连接：true。
12. structured propagation 是否接入 service score：true。

## B2P Results
- total_repeats: 20
- repeats_completed: 20
- repeats_failed: 0
- service_hit_at_1_overall: 0.8
- service_conditioned_metric_hit_at_3_overall: 0.8
- global_metric_hit_at_3_auxiliary_overall: 0.95
- service_metric_pair_hit_at_1_overall: 0.7
- root_type_accuracy_overall: 1.0
- path_fidelity_overall: 1.0
- auxiliary metric_mrr: 0.7333333333333333
- per_fault_type:

```json
[
  {
    "auxiliary_metric_hit_at_1_mean": 0.0,
    "auxiliary_metric_mrr_mean": 0.06666666666666667,
    "fault_type": "cpu",
    "global_metric_hit_at_3_auxiliary_mean": 0.8,
    "metric_hit_at_3_mean": 0.2,
    "path_fidelity_mean": 1.0,
    "primary_metric_hit_at_3_mean": 0.2,
    "repeats": 5,
    "repeats_completed": 5,
    "root_type_accuracy_mean": 1.0,
    "service_conditioned_metric_hit_at_1_mean": 0.0,
    "service_conditioned_metric_hit_at_3_mean": 0.2,
    "service_hit_at_1_mean": 0.2,
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

## B2M vs B2P Comparison
| Metric | B2M | B2P |
| --- | ---: | ---: |
| overall service | 0.75 | 0.8 |
| service-conditioned metric@3 | 0.75 | 0.8 |
| CPU service | 0.0 | 0.2 |
| CPU service-conditioned metric@3 | 0.0 | 0.2 |
| CPU root type | 1.0 | 1.0 |

## B3 Gate Recommendation
- b3_gate_recommended: false
- reason: B2P structural checks pass, but overall service/metric and CPU service localization remain below B3 gate thresholds.

## Review Verdict
- B2P_review_passed: true
- failed_checks: []
- remaining_risks:
  - CPU service Hit@1 improved from B2M 0.0 to 0.2, but remains below the 0.6 B3 gate.
  - service-conditioned metric@3 overall is 0.8, below 0.9.
  - structured propagation support is now present, but CPU service candidates still compete closely along shared call paths.
  - B3 gate is not recommended yet.
