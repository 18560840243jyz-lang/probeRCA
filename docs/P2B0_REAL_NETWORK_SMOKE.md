# P2B-0 Real Network Fault Feasibility Smoke

P2B-0 只验证 Online Boutique 单机 kind 集群中的真实 network fault 注入可行性。

本阶段使用 `tc netem` 进入 `shippingservice` Pod network namespace，对 `eth0` 注入短时间 delay/loss。目标是验证：

- 可以定位 `shippingservice` Pod network namespace。
- kind 节点内 `nsenter`、`tc`、`crictl` 可用。
- 可以应用并恢复 `tc netem` qdisc。
- frontend smoke test 在恢复后正常。
- 可以采集 `/proc/net/snmp` 和 `ss -tin` 的网络观测。

本阶段不运行 RCA pipeline，不输出准确率，不进入 repeated network accuracy。

输出目录：

```text
data/p2_online_boutique/network_shippingservice_smoke_001
```

运行命令：

```bash
bash scripts/online_boutique/run_p2b0_network_smoke.sh
python3 -m proberca.cli.check_p2b0_network_smoke --input data/p2_online_boutique/network_shippingservice_smoke_001
```

通过后才进入 P2B-1 repeated real network fault injection。
