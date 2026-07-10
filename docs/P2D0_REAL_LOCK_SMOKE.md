# P2D-0 Real Lock Smoke

P2D-0 只验证真实 lock contention 注入可行。

目标服务是 `cartservice`。由于 Online Boutique 原始 `cartservice` 没有内置锁故障注入开关，本阶段使用 `cartservice` Pod 内临时 sidecar lock-stress container。sidecar 运行真实 Python 多线程锁竞争，并从 stdout JSON 行中采集真实 lock wait 指标。

重要限制：该故障是 `cartservice` Pod 内 sidecar 产生的真实锁竞争负载，不是 `cartservice` 原始业务代码内部 bug。

本阶段不运行 RCA pipeline，不输出准确率，不进入 repeated lock accuracy。

输出目录：

```bash
data/p2_online_boutique/lock_cartservice_smoke_001
```

运行命令：

```bash
bash scripts/online_boutique/run_p2d0_lock_smoke.sh
```

通过后才进入 P2D-1 repeated real lock contention injection。
