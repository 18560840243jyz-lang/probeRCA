# P2A-3 Real CPU Repeated Injection

P2A-3 是 Online Boutique paymentservice CPU throttling 的真实重复注入实验。

每次 repeat 都会重新执行：

1. 真实注入 Kubernetes CPU resource limit。
2. 真实采集 kubelet/cAdvisor metrics。
3. 真实恢复 paymentservice。
4. 将该次真实数据接入冻结的 P1 RCA pipeline。
5. 输出该次 RCA 结果。

本阶段不是 synthetic，不复用旧单次数据，不做 network / IO / lock 故障，也不代表多故障总体准确率。

输出目录：

```text
data/p2_online_boutique/cpu_paymentservice_repeated
```

运行命令：

```bash
bash scripts/online_boutique/run_p2a3_cpu_repeated.sh
```

后续 P2A-3 通过后，才考虑进入真实多故障注入实验。
