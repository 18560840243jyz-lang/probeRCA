# P2A-1R cAdvisor Metric Repair

P2A-1 已验证 Online Boutique 的 paymentservice CPU resource limit 故障注入和恢复可以成功执行，但最初只采集到了 frontend request 指标，缺少 root service 的 CPU 和 throttling 指标。

P2A-1R 使用 Kubernetes kubelet cAdvisor API 修复真实指标采集路径：

- kubelet summary: `/api/v1/nodes/<node>/proxy/stats/summary`
- cAdvisor metrics: `/api/v1/nodes/<node>/proxy/metrics/cadvisor`

本阶段不依赖 Docker socket，不使用 Prometheus、Beyla 或 ClickHouse，不运行 RCA pipeline，不输出真实故障注入准确率。

## Pass Criteria

数据质量合格标准：

- `paymentservice_cpu_metric_present=true`
- `paymentservice_throttled_metric_present=true`
- `root_service_metric_coverage_passed=true`
- `frontend_latency_metric_present=true`

## Command

```bash
bash scripts/online_boutique/run_p2a1r_cpu_fault_cadvisor.sh
```

输出目录：

```text
data/p2_online_boutique/cpu_paymentservice_001_cadvisor
```
