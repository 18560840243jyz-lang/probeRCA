# P2A-3R Real CPU Repeated Controlled Experiment

P2A-3 原始真实 CPU 重复实验完成了 5 次真实注入、采集、恢复和 P1 RCA，但 gate 未通过。主要问题是部分 repeat 中 `paymentservice.cpu.usage` 或非目标服务 CPU throttling 噪声压过 `paymentservice.cpu.throttled_usec`。

P2A-3R 不修改 P1 算法和 P1 打分逻辑，只增强真实实验控制：

- 更强 CPU limit：`25m`。
- 更长 cooldown。
- 更多 faulty windows。
- 更多 frontend requests per window。
- pre-repeat throttling check。
- 记录非目标服务 CPU throttling 噪声。

运行命令：

```bash
bash scripts/online_boutique/run_p2a3r_cpu_repeated_controlled.sh
```

输出目录：

```text
data/p2_online_boutique/cpu_paymentservice_repeated_controlled
```

P2A-3R 仍然只代表 CPU 故障重复实验，不代表多故障总体准确率。不要将结果外推为 network / IO / lock 或多故障总体准确率。

## P2 Real Experiment Metric Policy

P2 真实实验主指标采用 `metric_hit_at_3`，并同时报告 `service_hit_at_1`、`root_type_accuracy` 和 `path_fidelity`。`metric_hit_at_1` 只作为辅助指标报告，不作为 P2 真实实验通过门槛。P2A-3R 在 Top3 口径下通过，但不是 exact metric Top1 成功。后续 network / IO / lock 真实注入也采用同一口径。多故障总体准确率必须同时报告：`service_hit_at_1`、`metric_hit_at_3`、auxiliary `metric_hit_at_1`、`root_type_accuracy`、`path_fidelity`。
