# P2A-0 Online Boutique Single-VM Deployment

本阶段目标是在单台 VM 上用 kind 运行 Google Online Boutique，形成 single-VM pseudo-distributed deployment。

中文解释：single-VM pseudo-distributed deployment 是单机伪分布式部署，即所有服务都在一台机器上，但每个服务仍然是独立容器或 Pod。

## Scope

- 本阶段只做部署和 smoke test。
- 不做真实故障注入。
- 不做准确率评估。
- 下一阶段 P2A-1 才做真实故障注入和指标采集。
- 使用 kind 在单 VM 上运行 Online Boutique。
- 当前不是 Prometheus/Beyla/ClickHouse 方案。
- 输出目录：data/p2_online_boutique/deploy_smoke

## Manual Command

```bash
bash scripts/online_boutique/run_p2a0_deploy_smoke.sh
```

该命令会检查环境、准备 Online Boutique 仓库、创建或复用 kind cluster、部署服务、对 frontend 做 HTTP smoke test，并写出服务拓扑 JSONL。

## Boundaries

P2A-0 不做故障注入，不采集实验指标，不运行 P1 RCA pipeline，不输出真实故障注入准确率。
