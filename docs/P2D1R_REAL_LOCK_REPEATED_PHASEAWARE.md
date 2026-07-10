# P2D-1R Real Lock Repeated Phase-Aware

P2D-1 原始 lock repeated 失败，因为 lock 指标只在 faulty 阶段出现，无法进入 P1 robust normalization 的 baseline 统计，因此 P1C 没有生成 `cartservice.lock.*` 候选。

P2D-1R 修复的是真实采集协议，不是 P1 打分逻辑。

phase-aware sidecar 在 baseline / faulty / recovery 全阶段都真实运行并真实上报 lock 指标：

- baseline: sidecar 运行但不激活锁竞争，输出真实 idle measurement。
- faulty: sidecar 激活真实 Python 多线程锁竞争，输出 lock wait 与 contention。
- recovery: sidecar 停止锁竞争，继续输出真实 idle measurement。

baseline lock metrics 是真实 idle sidecar measurement，不是 fake baseline 0。

重要限制：sidecar lock contention 仍然不是 `cartservice` 原始业务代码内部 bug，不能描述成原始业务代码的锁缺陷。

主指标按 [P2 Real Experiment Metrics](P2_REAL_EXPERIMENT_METRICS.md)：

- `service_hit_at_1`
- `metric_hit_at_3`
- `root_type_accuracy`
- `path_fidelity`

`metric_hit_at_1` 是 auxiliary metric，只报告，不作为 P2 真实实验通过门槛。

输出目录：

```bash
data/p2_online_boutique/lock_cartservice_repeated_phaseaware
```

运行命令：

```bash
bash scripts/online_boutique/run_p2d1r_lock_repeated.sh
```

P2D-1R 通过后，后续才能汇总多故障真实准确率。本阶段本身只代表 lock 故障重复实验，不代表多故障总体准确率。
