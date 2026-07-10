# P2D-1 Real Lock Repeated

P2D-1 是真实 lock contention 重复注入实验。

每次 repeat 都会临时给 `cartservice` Pod 添加 `proberca-lockstress` sidecar，sidecar 内运行真实 Python 多线程锁竞争，采集 sidecar stdout 中的 lock wait 指标，然后恢复 `cartservice` Deployment 并移除 sidecar。

这不是 synthetic data。每次 repeat 都重新注入、采集、恢复并接入 P1 RCA pipeline。

重要限制：sidecar lock contention 不是 `cartservice` 原始业务代码内部 bug，不能描述成原始业务代码的锁缺陷。

主指标按 [P2 Real Experiment Metrics](P2_REAL_EXPERIMENT_METRICS.md)：

- `service_hit_at_1`
- `metric_hit_at_3`
- `root_type_accuracy`
- `path_fidelity`

`metric_hit_at_1` 是 auxiliary metric，只报告，不作为 P2 真实实验通过门槛。

输出目录：

```bash
data/p2_online_boutique/lock_cartservice_repeated
```

运行命令：

```bash
bash scripts/online_boutique/run_p2d1_lock_repeated.sh
```

P2D-1 通过后，后续才能汇总多故障真实准确率。本阶段本身只代表 lock 故障重复实验，不代表多故障总体准确率。
