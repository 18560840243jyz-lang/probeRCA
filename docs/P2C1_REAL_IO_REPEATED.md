# P2C-1 Real IO Repeated

P2C-1 是真实 I/O 故障重复注入实验。

每次 repeat 都重新在 `redis-cart` Pod 内执行 I/O 写入压力、采集、恢复、RCA。它不是 synthetic data，也不是多故障总体准确率。

本阶段目标故障是 `redis-cart` 的 storage I/O 写入压力，候选根因指标包括 `redis-cart.io.write_bytes`、`redis-cart.io.write_ops` 和可用时的 `redis-cart.io.io_time_ms`。

主指标沿用 [P2 Real Experiment Metrics](P2_REAL_EXPERIMENT_METRICS.md)：

- `service_hit_at_1`
- `metric_hit_at_3`
- `root_type_accuracy`
- `path_fidelity`

`metric_hit_at_1` 是 auxiliary metric，只报告，不作为 P2 真实实验通过门槛。

输出目录：

```bash
data/p2_online_boutique/io_rediscart_repeated
```

运行命令：

```bash
bash scripts/online_boutique/run_p2c1_io_repeated.sh
```

通过后还需要 lock 真实注入实验，最后才能汇总多故障真实准确率。
