# P2 Real Experiment Metrics

## Primary Metrics

P2 真实实验主指标：

- `service_hit_at_1` 中文解释：服务级 Top1 命中率。
- `metric_hit_at_3` 中文解释：指标级 Top3 命中率。
- `root_type_accuracy` 中文解释：根因类型准确率。
- `path_fidelity` 中文解释：路径解释命中率。

## Auxiliary Metrics

辅助指标：

- `metric_hit_at_1` 中文解释：指标级 Top1 命中率，只报告，不作为 P2 真实实验通过门槛。
- `metric_mrr` 中文解释：平均倒数排名，用于补充说明排序质量。

## Rationale

真实系统中同一故障机制常同时激活多个同服务同类型指标。例如 CPU throttling 会同时引起：

- `paymentservice.cpu.usage`
- `paymentservice.cpu.throttled_usec`
- `paymentservice.cpu.throttle_ratio`
- `paymentservice.cpu.throttled_periods`

因此 P2 真实实验采用 metric Hit@3 作为主要指标级定位口径，metric Hit@1 作为辅助指标报告。

## CPU Repeat Acceptance

当前 P2A-3R 受控 CPU 重复实验结果：

- `repeats_completed = 5`
- `service_hit_at_1_mean = 1.0`
- `service_hit_at_1_min = 1.0`
- `metric_hit_at_3_mean = 1.0`
- `metric_hit_at_3_min = 1.0`
- `root_type_accuracy_mean = 1.0`
- `root_type_accuracy_min = 1.0`
- `path_fidelity_mean = 1.0`
- `path_fidelity_min = 1.0`
- `metric_hit_at_1_mean = 0.2`
- `metric_mrr_mean = 0.6`

P2A-3R 不是 exact metric Top1 成功，而是 Top3 口径成功。
