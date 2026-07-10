# probeRCA

当前项目是 probeRCA 单机虚拟机伪分布式验证版本。

## Current Stage

当前阶段：P0。

P0
中文解释：第一阶段最小闭环。

当前不依赖 GPU。

GPU
中文解释：图形处理器。

当前不依赖真实 eBPF。

eBPF
中文解释：Linux 内核观测技术。

当前不依赖 Kubernetes。

Kubernetes
中文解释：容器编排系统。

当前只建立本地项目规则文件。

下一步才是 project scaffold。

project scaffold
中文解释：项目代码骨架。

## Local and VM Workflow

Codex 可以在当前虚拟机目录修改代码。

如果 Windows 本机也维护代码，必须同步整个仓库。

单机虚拟机负责运行实验。

每次进入实验前，先确认项目规则文件存在。

不允许只同步部分代码导致项目规则丢失。

## Step 1 Project Scaffold

当前已创建 P0 所需的基础 Python 项目结构、schema、配置、检查命令和测试。

本阶段不实现 synthetic data generator、robust normalization、stable propagation、sparse inversion、semantic evidence scoring、path explanation 或任何真实系统接入。

## P0 Freeze

P0 已通过 G1 gate。

冻结快照保存在 docs/p0_freeze_snapshot。

进入 P1 前必须运行：

```bash
python3 -m proberca.cli.check_p0_freeze --freeze-dir docs/p0_freeze_snapshot
```

磁盘清理 dry-run：

```bash
python3 scripts/cleanup_p0_artifacts.py --base data/p0_single_vm
```

磁盘清理 apply：

```bash
python3 scripts/cleanup_p0_artifacts.py --base data/p0_single_vm --apply
```


## P1 Freeze

P1 已通过 P1 gate，冻结快照保存在 docs/p1_freeze_snapshot。

进入下一阶段前运行：

```bash
python3 -m proberca.cli.check_p1_freeze --freeze-dir docs/p1_freeze_snapshot
```

P1 清理 dry-run：

```bash
python3 scripts/cleanup_p1_artifacts.py --base data/p1_single_vm
```

P1 清理 apply：

```bash
python3 scripts/cleanup_p1_artifacts.py --base data/p1_single_vm --apply
```

注意：P1 cleanup 默认保留 p1_results、p1_audit_summary、p1_gate_decision 和各阶段 metadata/summary，只清理可再生成的大体量中间文件。

## P2A-0 Online Boutique Single-VM Smoke Test

P2A-0 用 kind 在单台 VM 上运行 Google Online Boutique，形成 single-VM pseudo-distributed deployment。

运行命令：

```bash
bash scripts/online_boutique/run_p2a0_deploy_smoke.sh
```

该命令只部署和 smoke test，不注入故障，不跑 RCA，不接 Prometheus/Beyla/ClickHouse。输出目录为 data/p2_online_boutique/deploy_smoke。

## P2A-1 Online Boutique Real CPU Fault

P2A-1 对 Online Boutique 的 paymentservice 做真实 CPU resource limit 故障注入，并采集最小指标数据。

```bash
bash scripts/online_boutique/run_p2a1_cpu_fault.sh
```

输出目录：data/p2_online_boutique/cpu_paymentservice_001。该步骤不运行 RCA pipeline，不输出准确率，不接 Prometheus/Beyla/ClickHouse。

## P2A-1R cAdvisor Metric Repair

P2A-1R repairs Online Boutique real metric collection with kubelet cAdvisor metrics so paymentservice CPU and CPU throttling metrics are present. It does not run the RCA pipeline and does not output accuracy.

```bash
bash scripts/online_boutique/run_p2a1r_cpu_fault_cadvisor.sh
```

## P2A-2 Real CPU Injection RCA

P2A-2 bridges the P2A-1R real Online Boutique CPU injection dataset into the frozen P1 RCA pipeline. It evaluates one real CPU injection case only, not multi-fault accuracy.

```bash
python3 -m proberca.cli.run_p2a2_real_cpu_rca --input data/p2_online_boutique/cpu_paymentservice_001_cadvisor --output data/p2_online_boutique/cpu_paymentservice_001_p1rca
python3 -m proberca.cli.check_p2a2_real_rca --input data/p2_online_boutique/cpu_paymentservice_001_p1rca
```

## P2A-3 Repeated Real CPU Injection

P2A-3 repeats the real Online Boutique paymentservice CPU throttling experiment five times. Each repeat re-injects the CPU fault, collects cAdvisor metrics, restores paymentservice, runs the frozen P1 RCA pipeline, and reports CPU-only repeated accuracy. This is not multi-fault accuracy.

```bash
bash scripts/online_boutique/run_p2a3_cpu_repeated.sh
```

## P2A-3R Controlled Real CPU Repeated Experiment

P2A-3R diagnoses failed real CPU repeats and reruns controlled paymentservice CPU throttling repeats without changing P1 scoring logic.

```bash
python3 -m proberca.cli.diagnose_p2a3_cpu_repeated --input data/p2_online_boutique/cpu_paymentservice_repeated
bash scripts/online_boutique/run_p2a3r_cpu_repeated_controlled.sh
```

This is CPU-only repeated real injection, not multi-fault overall accuracy.

## P2 Real Experiment Metric Policy

P2 真实实验主指标采用 `metric_hit_at_3`，并同时报告 `service_hit_at_1`、`root_type_accuracy` 和 `path_fidelity`。`metric_hit_at_1` 只作为辅助指标报告，不作为 P2 真实实验通过门槛。P2A-3R 在 Top3 口径下通过，但不是 exact metric Top1 成功。后续 network / IO / lock 真实注入也采用同一口径。多故障总体准确率必须同时报告：`service_hit_at_1`、`metric_hit_at_3`、auxiliary `metric_hit_at_1`、`root_type_accuracy`、`path_fidelity`。

## P2B-0 Real Network Fault Smoke

P2B-0 validates whether `tc netem` can inject and restore a real network delay/loss fault in the `shippingservice` Pod network namespace. It only performs feasibility smoke testing and does not run the RCA pipeline or report accuracy.

```bash
bash scripts/online_boutique/run_p2b0_network_smoke.sh
python3 -m proberca.cli.check_p2b0_network_smoke --input data/p2_online_boutique/network_shippingservice_smoke_001
```

## P2B-1 Real Network Repeated Injection

P2B-1 repeats real `shippingservice` network delay/loss injection with `tc netem`, collects real network metrics, restores qdisc, and runs the frozen P1 RCA pipeline per repeat.

```bash
bash scripts/online_boutique/run_p2b1_network_repeated.sh
```

This only reports repeated network-fault Top3 RCA results. It is not multi-fault overall accuracy. `metric_hit_at_1` is auxiliary.

## P2C-0 Real IO Fault Smoke

P2C-0 validates real `redis-cart` I/O write pressure with Pod-local `dd`, cAdvisor filesystem metrics, and frontend smoke latency. It does not run RCA and does not report accuracy.

```bash
bash scripts/online_boutique/run_p2c0_io_smoke.sh
```

## P2C-1 Real IO Repeated

P2C-1 repeats real `redis-cart` I/O write-pressure injection five times, collects kubelet/cAdvisor filesystem metrics, runs the frozen P1 RCA pipeline for each repeat, and reports Top3 RCA metrics. It is not multi-fault overall accuracy.

```bash
bash scripts/online_boutique/run_p2c1_io_repeated.sh
```

## P2D-0 Real Lock Smoke

P2D-0 validates real lock contention feasibility by temporarily adding a `proberca-lockstress` sidecar to the `cartservice` Pod. The sidecar runs real Python multithreaded lock contention and reports stdout lock wait metrics. This is not an original cartservice business-code bug and does not run RCA or report accuracy.

```bash
bash scripts/online_boutique/run_p2d0_lock_smoke.sh
```

## P2D-1 Real Lock Repeated

P2D-1 runs repeated real `cartservice` sidecar lock contention injections and evaluates them with the P2 Top3 policy. The sidecar lock contention is a real lock workload in the cartservice Pod, but it is not an original cartservice business-code bug.

```bash
bash scripts/online_boutique/run_p2d1_lock_repeated.sh
```

## P2D-1R Phase-Aware Real Lock Repeated

P2D-1R reruns repeated real `cartservice` sidecar lock contention with a phase-aware sidecar. Baseline and recovery lock metrics are real idle sidecar measurements, not fake baseline zeros. The sidecar workload is still not an original cartservice business-code bug.

```bash
bash scripts/online_boutique/run_p2d1r_lock_repeated.sh
```

## P2E Real Multi-Fault Summary

- CPU / Network / I/O / Lock real repeated experiments have been summarized under `data/p2_online_boutique/multifault_summary`.
- P2 primary metrics are service Hit@1, metric Hit@3, root type accuracy, and path fidelity.
- metric Hit@1 is an auxiliary metric and is reported, not used as a P2 real-experiment pass threshold.
- CPU exact Top1 instability is reported explicitly: CPU exact metric Hit@1 is unstable, while metric Hit@3 is stable.
- Lock sidecar limitation is reported explicitly: lock contention comes from a cartservice Pod sidecar and is not an original cartservice business-code bug.
- P2E passing does not imply Prometheus/Beyla/ClickHouse integration and does not imply multi-node production Kubernetes deployment.

## A1 Evidence De-leak

A1 Evidence De-leak implemented as blind evidence generation protocol.
中文解释：A1 已实现 blind evidence 生成协议，但尚未做 blind RCA rerun。

Legacy P2 `evidence.jsonl` remains target-aware evidence and must not be used for blind RCA claims. A1 writes separate `blind_evidence.jsonl` and `blind_evidence_metadata.json` from all observed service.metric lift without using root labels, target configuration, or injected paths.

## A2 Blind P2 Rerun

A2 Blind P2 Rerun uses existing real raw metrics and A1 blind evidence to rerun the frozen P1 RCA pipeline without new fault injection.
中文解释：A2 使用已有真实 raw metrics 和 A1 blind evidence 重跑冻结 P1 RCA pipeline，不重新注入故障。

A2 does not use legacy target-aware `evidence.jsonl` from raw experiment directories. It still uses `incident.start_ts` and `incident.end_ts` as the alert window; A3 will implement the true Alert Gate.

## A3 Alert Gate

A3 Alert Gate implements metrics-driven alert event detection and alert window construction.
中文解释：A3 实现基于 metrics 的告警事件检测和告警窗口构造。

A3 does not run RCA, does not reinject faults, does not modify P1 scoring, and does not use incident start/end or root labels for detection. Incidents may be used only after detection for debug overlap evaluation.


## A4 Candidate Subgraph Builder

A4 adds a label-safe candidate subgraph preview for P2 real experiments. It consumes A3 alert windows, raw metrics, and service graphs, then outputs candidate services, service-metric nodes, and candidate edges. It does not run RCA, does not re-inject faults, and does not modify P1 scoring logic.

## A5 Adaptive Probe Policy

A5 adds a label-safe adaptive probe policy preview. It consumes A3 alert windows, A4 candidate subgraphs, and optional blind evidence to emit `probe_plan.jsonl`, `sampling_log.jsonl`, and `observation_mask.jsonl`. It does not activate probes, run RCA, or modify P1 scoring logic.

## A6 IPW-masked RLS

A6 adds a true online IPW-masked RLS propagation preview. It consumes A5 sampling probabilities and observation masks, writes propagation parameters, predictions, and residuals, and does not run RCA or modify P1 scoring logic.

## A7 C h_t Evidence Channel

A7 adds a preview implementation of the fine-grained evidence channel. It consumes A2 blind evidence, A5 probe policy, and A6 IPW-masked RLS residuals, then writes calibrated residuals for later A8 graph sparse inversion. It does not run RCA, does not reinject faults, and does not modify P1 scoring logic.


## A8 Graph Sparse Inversion

A8 adds an ADMM graph-constrained sparse inversion preview. It consumes A4 candidate graphs and A7 calibrated residuals, writes sparse intervention rankings, and does not run the old P1 RCA pipeline or reinject faults.

## A8R Graph Sparse Inversion Repair

A8R repairs A8 sparse inversion by reducing metric-level edge explosion, using positive top-k calibrated residual aggregation, adding blind-evidence signal support, using automatic sparse regularization, applying post-sparsify, and improving ADMM convergence. The repair does not use root labels, target labels, injected paths, or incident start/end times for inversion. A8R remains a preview and is not a P2E acceptance result.

## A9 Counterfactual Explanation

A9 implements counterfactual explanation preview for A8R sparse candidates. For top metric and service candidates it re-optimizes graph sparse inversion with the candidate removed and reports `Delta L = L(u^{-v}) - L(u_hat)`. A9 does not use root labels, target labels, injected paths, or incident start/end times for explanation generation. It does not run old P1 RCA and does not reinject faults. Debug metrics are post-hoc diagnostics only, not P2E acceptance.

## B1 Integrated Blind RCA Pipeline

B1 integrates A3-A9 into a single end-to-end blind RCA smoke pipeline over existing raw metrics and service graph data. It uses A3 alert windows, alert-window blind evidence, A4 candidates, A5 policy preview, A6 IPW-masked RLS, A7 calibrated residuals, A8R graph sparse inversion, and A9 counterfactual explanation to write an integrated RCA result schema.

B1 does not reinject faults, does not run the old P1 RCA pipeline, does not modify P1 scoring logic, does not use legacy target-aware evidence, and does not use root/target labels or injected paths for inference. B1 is a single smoke integration step; B2 is the full 20-repeat replay and B3 is future real reinjection.

## B1R Integrated Final Result Repair

B1R repairs B1 final RCA result assembly. The final result now uses a metric-level `metric_candidate_table` as the primary candidate source, derives `top1_service`, `top1_metric`, and `predicted_root_type` from the same primary candidate, aggregates `top_services` from metric candidates, and writes one RCA result per alert window. B1R does not run B2 replay, does not reinject faults, does not run the old P1 RCA pipeline, and does not use root/target labels or legacy target-aware evidence.

## B2 Integrated Replay Existing Raw Metrics

B2 runs the B1R integrated blind RCA pipeline over the existing 20 Online Boutique raw-metric repeats. It does not reinject faults, does not run the old P1 RCA pipeline, and uses incident labels only after final results for post-hoc evaluation.
