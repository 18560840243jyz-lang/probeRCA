# P2A-1 Online Boutique Real CPU Fault

本阶段做真实 CPU 故障注入和最小指标采集。

## Scope

- 目标服务：paymentservice。
- 注入方式：Kubernetes resource limit，将 paymentservice 容器 CPU limit 降到 50m。
- 采集方式：kubectl 状态、kind node crictl stats（如果当前用户可访问 Docker socket）、frontend curl latency。
- 输出目录：data/p2_online_boutique/cpu_paymentservice_001。

## Boundaries

- 不使用 Prometheus/Beyla/ClickHouse。
- 不运行 RCA pipeline。
- 不输出准确率。
- 不做 network / io / lock 故障。
- 不修改 P0/P1 冻结逻辑。

## Command

```bash
bash scripts/online_boutique/run_p2a1_cpu_fault.sh
```
