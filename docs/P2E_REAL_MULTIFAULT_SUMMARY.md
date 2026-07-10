# P2E Real Multi-Fault Summary

## Scope
- CPU / Network / I/O / Lock four real repeated experiments.
- Each fault type has 5 repeats, for 20 total repeats.
- All repeats are real injections in the Online Boutique single-VM kind cluster.
- This is not synthetic data.

## Primary Metrics
- service_hit_at_1_overall: 1
- metric_hit_at_3_overall: 1
- root_type_accuracy_overall: 1
- path_fidelity_overall: 1

## Auxiliary Metrics
- metric_hit_at_1_overall_auxiliary: 0.8
- metric_mrr_overall_auxiliary: 0.9

metric Hit@1 is auxiliary.
中文解释：指标级 Top1 是辅助指标，不作为 P2 真实实验通过门槛。

## Per Fault Type
### CPU
- repeats: 5
- target_service: paymentservice
- target_metric: cpu.throttled_usec
- service_hit_at_1: 1.0
- metric_hit_at_3: 1.0
- metric_hit_at_1 auxiliary: 0.2
- root_type_accuracy: 1.0
- path_fidelity: 1.0
- limitation: CPU exact metric Hit@1 is unstable; cpu.usage can rank above cpu.throttled_usec, but metric Hit@3 is stable.

### Network
- repeats: 5
- target_service: shippingservice
- target_metric: net.retrans
- service_hit_at_1: 1.0
- metric_hit_at_3: 1.0
- metric_hit_at_1 auxiliary: 1.0
- root_type_accuracy: 1.0
- path_fidelity: 1.0
- limitation: none

### I/O
- repeats: 5
- target_service: redis-cart
- target_metric: io.write_bytes
- service_hit_at_1: 1.0
- metric_hit_at_3: 1.0
- metric_hit_at_1 auxiliary: 1.0
- root_type_accuracy: 1.0
- path_fidelity: 1.0
- limitation: none

### Lock
- repeats: 5
- target_service: cartservice
- target_metric: lock.futex_wait_ms
- service_hit_at_1: 1.0
- metric_hit_at_3: 1.0
- metric_hit_at_1 auxiliary: 1.0
- root_type_accuracy: 1.0
- path_fidelity: 1.0
- limitation: Lock fault comes from a cartservice Pod sidecar lock-stress container, not an original cartservice business-code bug.; Baseline lock metrics are real idle sidecar measurements, not fake baseline zeros.; sidecar_lock_contention_not_original_cartservice_code_bug

## Known Limitations
- CPU exact metric Hit@1 is unstable: cpu.usage often ranks above cpu.throttled_usec, but metric Hit@3 is stable.
- Lock fault comes from sidecar lock-stress, not an original cartservice business-code bug.
- Current experiments do not use Prometheus/Beyla/ClickHouse.
- Current deployment is single-VM pseudo-distributed deployment.
  中文解释：单机伪分布式部署。

## Decision
P2E_REAL_MULTIFAULT_PASS
