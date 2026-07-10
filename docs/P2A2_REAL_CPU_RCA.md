# P2A-2 Real CPU Injection Data to P1 RCA

P2A-2 将 P2A-1R 采集到的真实 Online Boutique CPU 注入数据接入已经冻结的 P1 RCA pipeline。

输入目录：

```text
data/p2_online_boutique/cpu_paymentservice_001_cadvisor
```

输出目录：

```text
data/p2_online_boutique/cpu_paymentservice_001_p1rca
```

本阶段使用 full-observation bridge：

- `observed_ratio=1.0`
- `sampling_probability=1.0`
- `partial_observation=false`

这表示真实已采集到的指标在本次离线接入中全量可见，不代表 P1A 自适应采样模拟。

## Boundary

- 不重新注入故障。
- 不修改 Online Boutique 部署。
- 不修改 P0/P1 冻结逻辑。
- 不修改 P1 打分逻辑。
- 不使用 root labels 参与打分。
- 不接 Prometheus、Beyla、ClickHouse。
- 不输出多故障总体准确率。

本结果只能作为 first real injection case（第一例真实注入案例）。

## Command

```bash
python3 -m proberca.cli.run_p2a2_real_cpu_rca \
  --input data/p2_online_boutique/cpu_paymentservice_001_cadvisor \
  --output data/p2_online_boutique/cpu_paymentservice_001_p1rca

python3 -m proberca.cli.check_p2a2_real_rca \
  --input data/p2_online_boutique/cpu_paymentservice_001_p1rca
```
