# Experiments

当前 experiments 目录暂时只作为实验脚本入口。

P0 实验后续才实现。

当前不做真实 Kubernetes、真实 eBPF、Prometheus、Beyla、ClickHouse。

## P0 Synthetic Dataset

运行合成伪分布式数据生成器：

```bash
python -m proberca.cli.generate_synthetic --output data/p0_single_vm/demo --seed 7
```

检查输出文件：

```bash
find data/p0_single_vm/demo -maxdepth 2 -type f | sort
```

期望看到：

- metrics.jsonl
- evidence.jsonl
- incidents.jsonl
- service_graph.jsonl
- metadata.json

当前只生成 synthetic evidence（合成证据）和 synthetic pseudo-distributed data（合成伪分布式数据），不接真实 Kubernetes、真实 eBPF、Prometheus、Beyla、ClickHouse。

## Step 3 Robust Normalization

运行鲁棒归一化：

```bash
python3 -m proberca.cli.normalize_metrics --input data/p0_single_vm/demo
```

运行后会生成：

- normalized_metrics.jsonl
- robust_stats.jsonl
- normalization_metadata.json

当前只把原始 metric 指标转换成 robust deviation score（鲁棒异常分数），不实现 stable propagation、sparse inversion、semantic evidence scoring 或 path explanation。

## Step 4 Stable Propagation

运行稳定传播学习器：

```bash
python3 -m proberca.cli.train_stable_propagation --input data/p0_single_vm/demo
```

运行后会生成：

- stable_propagation_model.json
- stable_residuals.jsonl
- propagation_metadata.json

当前只学习 stable propagation（稳定传播）并计算 stable residuals（稳定传播残差），不实现 sparse inversion、semantic evidence scoring、path explanation 或最终 Top-K 根因输出。

## Step 5 Sparse Inversion

运行稀疏反演：

```bash
python3 -m proberca.cli.solve_sparse_inversion --input data/p0_single_vm/demo
```

运行后会生成：

- sparse_interventions.jsonl
- sparse_inversion_summary.json
- sparse_inversion_metadata.json

说明：当前输出只是 intervention candidates（干预候选），不是最终 RCA 输出；不包含语义证据打分、路径解释或最终 Top-K 根因结果。

## Step 6 Semantic Evidence Scoring

运行语义证据打分：

```bash
python3 -m proberca.cli.score_semantic_evidence --input data/p0_single_vm/demo
```

运行后会生成：

- semantic_interventions.jsonl
- semantic_type_scores.jsonl
- semantic_evidence_summary.json
- semantic_evidence_metadata.json

说明：当前输出只是 semantic candidates（语义候选），不是最终 RCAResult；不包含路径解释或最终 top_services / top_metrics 输出。

## Step 7 Path Explanation

运行路径解释：

```bash
python3 -m proberca.cli.explain_paths --input data/p0_single_vm/demo
```

运行后会生成：

- path_explanations.jsonl
- path_explanation_summary.json
- path_explanation_metadata.json

说明：当前输出只是 path explanations（路径解释），不是最终 RCAResult，不输出最终 top_services / top_metrics。

## Step 8 P0 End-to-End Experiment

运行 P0 单机伪分布式端到端实验：

```bash
python3 -m proberca.cli.run_p0_experiment --output data/p0_single_vm/demo --seed 7
```

运行后会生成：

- p0_results.jsonl
- p0_results_metadata.json
- p0_evaluation_summary.json
- p0_experiment_metadata.json

说明：当前是 single VM pseudo-distributed P0 experiment（单机伪分布式 P0 实验），不是真实分布式实验，不接真实 eBPF、Kubernetes、Prometheus、Beyla 或 ClickHouse。

## Step 8A P0 Sanity Audit

运行 P0 合理性审计：

```bash
python3 -m proberca.cli.run_p0_audit --output data/p0_single_vm/audit
```

快速审计：

```bash
python3 -m proberca.cli.run_p0_audit --output data/p0_single_vm/audit --quick
```

说明：这是 P0 sanity audit（合理性审计），用于检查 label leakage（标签泄漏）、multi-seed robustness（多 seed 鲁棒性）、semantic ablation（语义证据消融）和 noise sensitivity（噪声敏感性），不是 P1。

## Step 8B Full P0 Audit and G1 Gate

运行完整 P0 审计：

```bash
python3 -m proberca.cli.run_p0_audit --output data/p0_single_vm/audit_full
```

运行 G1 决策门：

```bash
python3 -m proberca.cli.run_g1_gate --audit-dir data/p0_single_vm/audit_full
```

说明：g1_decision.json 中文解释：G1 决策结果文件，用于判断 P0 是否可以冻结并进入 P1。当前步骤只做完整 P0 audit 和 G1 gate，不实现 P1 adaptive sampling、IPW-masked RLS 或 optional drift。

## Step 8C P0 Failure Diagnosis and Repair

分析旧 full audit 失败：

```bash
python3 -m proberca.cli.analyze_p0_failures --audit-dir data/p0_single_vm/audit_full
```

重新运行修复后的 full audit：

```bash
python3 -m proberca.cli.run_p0_audit --output data/p0_single_vm/audit_full_fix
python3 -m proberca.cli.run_g1_gate --audit-dir data/p0_single_vm/audit_full_fix
```

说明：metric_specificity_weight（指标特异性权重）用于在 semantic evidence scoring 阶段增强底层诊断指标、削弱 request latency 等症状指标。该先验不使用 root labels，不降低 G1 门槛，不进入 P1。

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

## P1A Adaptive Observation Simulator

进入 P1A 前先确认 P0 freeze：

```bash
python3 -m proberca.cli.check_p0_freeze --freeze-dir docs/p0_freeze_snapshot
```

运行 P1A 自适应观测流水线：

```bash
python3 -m proberca.cli.run_p1a_observation --output data/p1_single_vm/demo --seed 7
```

单独运行观测模拟器：

```bash
python3 -m proberca.cli.simulate_observation --input data/p1_single_vm/demo --seed 7
```

运行后会生成：

- observed_metrics.jsonl
- sampling_log.jsonl
- observation_mask.jsonl
- adaptive_observation_metadata.json

说明：当前是 P1A adaptive observation simulator（自适应观测模拟器），只生成 sampling_probability（采样概率）和 observation_mask（观测掩码），不实现 IPW-masked RLS、bandit adaptive probing、optional drift 或真实系统接入。

## P1B IPW-Masked Stable Propagation

运行 P1B IPW-masked stable propagation pipeline（逆概率加权掩码稳定传播流水线）：

```bash
python3 -m proberca.cli.run_p1b_ipw_propagation --output data/p1_single_vm/demo --seed 7
```

对已有 P1A 输出单独训练 IPW propagation：

```bash
python3 -m proberca.cli.train_ipw_propagation --input data/p1_single_vm/demo
```

no-IPW 对照：

```bash
python3 -m proberca.cli.train_ipw_propagation --input data/p1_single_vm/demo --no-ipw --output data/p1_single_vm/demo_no_ipw
```

运行后会生成：

- ipw_stable_propagation_model.json
- ipw_stable_residuals.jsonl
- ipw_propagation_metadata.json

说明：当前是 P1B IPW-masked stable propagation（逆概率加权掩码稳定传播），不实现 sparse inversion、semantic evidence scoring、path explanation 或最终 RCAResult。

## P1C IPW Residual Sparse Inversion

运行 P1C sparse inversion pipeline（稀疏反演流水线）：

```bash
python3 -m proberca.cli.run_p1c_sparse_inversion --output data/p1_single_vm/demo --seed 7
```

对已有 P1B 输出单独求解 IPW residual sparse inversion：

```bash
python3 -m proberca.cli.solve_ipw_sparse_inversion --input data/p1_single_vm/demo
```

no-IPW-weighted-mean 对照：

```bash
python3 -m proberca.cli.solve_ipw_sparse_inversion --input data/p1_single_vm/demo --no-ipw-weighted-mean --output data/p1_single_vm/demo_sparse_no_ipw_mean
```

运行后会生成：

- ipw_sparse_interventions.jsonl
- ipw_sparse_inversion_summary.json
- ipw_sparse_inversion_metadata.json

说明：当前是 P1C IPW residual sparse inversion（基于 IPW 残差的稀疏反演），不实现 P1D semantic evidence、path explanation 或最终 RCAResult。

## P1D IPW Semantic Evidence

运行 P1D semantic evidence pipeline（语义证据流水线）：

```bash
python3 -m proberca.cli.run_p1d_semantic_evidence --output data/p1_single_vm/demo --seed 7
```

对已有 P1C 输出单独运行 semantic evidence scoring：

```bash
python3 -m proberca.cli.score_ipw_semantic_evidence --input data/p1_single_vm/demo
```

specificity 消融：

```bash
python3 -m proberca.cli.score_ipw_semantic_evidence --input data/p1_single_vm/demo --output data/p1_single_vm/demo_semantic_no_specificity --disable-specificity
```

semantic anchor 消融：

```bash
python3 -m proberca.cli.score_ipw_semantic_evidence --input data/p1_single_vm/demo --output data/p1_single_vm/demo_semantic_no_anchor --disable-semantic-anchor
```

运行后会生成：

- ipw_semantic_interventions.jsonl
- ipw_semantic_type_scores.jsonl
- ipw_semantic_evidence_summary.json
- ipw_semantic_evidence_metadata.json

说明：当前是 P1D semantic evidence on IPW sparse candidates（在 IPW 稀疏候选上融合语义证据），不实现 path explanation 或最终 RCAResult。

## P1D-R Semantic Sibling Repair

中文解释：P1D-R 语义兄弟指标修复，用于诊断和修复同服务、同类型 sibling metric 压过更具体机制指标的问题。

运行 P1D-R 主流程：

```bash
python3 -m proberca.cli.run_p1d_semantic_evidence --output data/p1_single_vm/demo --seed 7
```

运行 sibling diagnosis：

```bash
python3 -m proberca.cli.diagnose_ipw_semantic --input data/p1_single_vm/demo
```

输出：

- ipw_semantic_sibling_diagnosis.json
  中文解释：P1D 兄弟指标错误诊断文件。

说明：diagnostic_priority_bonus（诊断优先级加分）和 metric_specificity_weight（指标特异性权重）不使用 root labels，不根据 incident_id 特判。当前仍不做 P1E path explanation，也不输出 final RCAResult。

## P1E IPW Semantic Path Explanation

中文解释：P1E 基于 IPW 语义候选生成路径解释。

运行 P1E pipeline：

```bash
python3 -m proberca.cli.run_p1e_path_explanation --output data/p1_single_vm/demo --seed 7
```

对已有 P1D 输出单独运行路径解释：

```bash
python3 -m proberca.cli.explain_ipw_paths --input data/p1_single_vm/demo
```

运行后生成：

- ipw_path_explanations.jsonl
  中文解释：P1E 路径解释记录。
- ipw_path_explanation_summary.json
  中文解释：P1E 路径解释摘要。
- ipw_path_explanation_metadata.json
  中文解释：P1E 路径解释元信息。

说明：当前是 P1E path explanation，不输出 final RCAResult，不实现 P1 gate。

## P1F End-to-End P1 RCA Result

中文解释：P1F 端到端 P1 根因结果生成与单 seed 评估。

运行 P1F：

```bash
python3 -m proberca.cli.run_p1f_result --output data/p1_single_vm/demo --seed 7
```

只基于已有 P1A-P1E 输出构建结果：

```bash
python3 -m proberca.cli.build_p1_results --input data/p1_single_vm/demo
```

输出：

- p1_results.jsonl
  中文解释：P1 端到端根因分析结果。
- p1_results_metadata.json
  中文解释：P1 结果元信息。
- p1_evaluation_summary.json
  中文解释：P1 单 seed 评估摘要。
- p1_experiment_metadata.json
  中文解释：P1F 实验元信息。

说明：当前是 P1F single-seed evaluation，不是 P1 gate，不做 multi-seed full audit。

## P1G Full P1 Audit And Gate

中文解释：P1G 完整 P1 审计和 P1 决策门。

quick audit：

```bash
python3 -m proberca.cli.run_p1_audit --output data/p1_single_vm/audit_quick --quick
python3 -m proberca.cli.run_p1_gate --audit-dir data/p1_single_vm/audit_quick
```

full audit：

```bash
python3 -m proberca.cli.run_p1_audit --output data/p1_single_vm/audit_full
python3 -m proberca.cli.run_p1_gate --audit-dir data/p1_single_vm/audit_full
```

输出：p1_audit_summary.json、p1_audit_metadata.json、p1_failure_analysis.json、p1_gate_decision.json。

说明：P1 是 partial observation（部分观测），P1 gate 同时检查 metric Hit@3、MRR、path fidelity 和 observed_ratio，不降低门槛，不进入 adaptive sampling bandit 或 optional drift。

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

Run the repaired real CPU metric collection experiment:

```bash
bash scripts/online_boutique/run_p2a1r_cpu_fault_cadvisor.sh
```

This step collects kubelet cAdvisor CPU, memory, and CPU throttling metrics for Online Boutique. It does not run RCA.

## P2A-2 Real CPU Injection Data to P1 RCA

Run the single real CPU injection case through the frozen P1 pipeline:

```bash
python3 -m proberca.cli.run_p2a2_real_cpu_rca --input data/p2_online_boutique/cpu_paymentservice_001_cadvisor --output data/p2_online_boutique/cpu_paymentservice_001_p1rca
python3 -m proberca.cli.check_p2a2_real_rca --input data/p2_online_boutique/cpu_paymentservice_001_p1rca
```

This does not re-inject faults and does not report multi-fault real accuracy.

## P2A-3 Repeated Real CPU Injection

Run five repeated real CPU throttling experiments:

```bash
bash scripts/online_boutique/run_p2a3_cpu_repeated.sh
```

Each repeat creates `repeat_XX/raw` and `repeat_XX/p1rca` under `data/p2_online_boutique/cpu_paymentservice_repeated`.

## P2A-3R Controlled CPU Repeat

Diagnose the original P2A-3 failures and rerun controlled real CPU repeats:

```bash
python3 -m proberca.cli.diagnose_p2a3_cpu_repeated --input data/p2_online_boutique/cpu_paymentservice_repeated
bash scripts/online_boutique/run_p2a3r_cpu_repeated_controlled.sh
```

The controlled run uses stronger CPU limit, longer cooldown, more faulty windows, more requests, and pre-repeat throttling checks. It does not modify P1 scoring.

## P2 Real Experiment Metric Policy

P2 真实实验主指标采用 `metric_hit_at_3`，并同时报告 `service_hit_at_1`、`root_type_accuracy` 和 `path_fidelity`。`metric_hit_at_1` 只作为辅助指标报告，不作为 P2 真实实验通过门槛。P2A-3R 在 Top3 口径下通过，但不是 exact metric Top1 成功。后续 network / IO / lock 真实注入也采用同一口径。多故障总体准确率必须同时报告：`service_hit_at_1`、`metric_hit_at_3`、auxiliary `metric_hit_at_1`、`root_type_accuracy`、`path_fidelity`。

## P2B-0 Real Network Fault Smoke

P2B-0 validates whether `tc netem` can inject and restore a real network delay/loss fault in the `shippingservice` Pod network namespace. It only performs feasibility smoke testing and does not run the RCA pipeline or report accuracy.

```bash
bash scripts/online_boutique/run_p2b0_network_smoke.sh
python3 -m proberca.cli.check_p2b0_network_smoke --input data/p2_online_boutique/network_shippingservice_smoke_001
```

## P2B-1 Real Network Repeated Injection

Run repeated real Online Boutique network fault experiments:

```bash
bash scripts/online_boutique/run_p2b1_network_repeated.sh
```

Outputs are under `data/p2_online_boutique/network_shippingservice_repeated`. Primary metrics are `service_hit_at_1`, `metric_hit_at_3`, `root_type_accuracy`, and `path_fidelity`; `metric_hit_at_1` is auxiliary.

## P2C-0 Real IO Fault Smoke

Run the real Online Boutique I/O feasibility smoke:

```bash
bash scripts/online_boutique/run_p2c0_io_smoke.sh
```

Outputs are under `data/p2_online_boutique/io_rediscart_smoke_001`. This stage only checks feasibility and cleanup.

## P2C-1 Real IO Repeated

Run repeated real I/O injection for `redis-cart` and evaluate with the P2 Top3 policy:

```bash
bash scripts/online_boutique/run_p2c1_io_repeated.sh
```

The output directory is `data/p2_online_boutique/io_rediscart_repeated`. `metric_hit_at_1` is reported as an auxiliary metric only.

## P2D-0 Real Lock Smoke

Run a feasibility smoke for cartservice Pod sidecar lock contention:

```bash
bash scripts/online_boutique/run_p2d0_lock_smoke.sh
```

The output directory is `data/p2_online_boutique/lock_cartservice_smoke_001`. This stage does not run RCA or report lock accuracy.

## P2D-1 Real Lock Repeated

Runs five real sidecar lock contention repeats for `cartservice`, restores the deployment after every repeat, and reports service Hit@1, metric Hit@3, root type accuracy, path fidelity, plus auxiliary metric Hit@1/MRR. This is not multi-fault accuracy.

## P2D-1R Phase-Aware Lock Repeated

Uses a phase-aware lock-stress sidecar so baseline/faulty/recovery all produce real lock measurements. This repairs the real collection protocol after P2D-1; it does not modify P1 scoring and does not represent multi-fault accuracy.

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


## A4 Candidate Subgraph Preview

A4 candidate preview artifacts are written under `data/p2_online_boutique/a4_candidate_preview`. They are generated from A3 alert windows and raw service graphs only. Incident labels are used only for optional debug coverage after graph construction.

## A5 Probe Policy Preview

A5 preview artifacts are stored under `data/p2_online_boutique/a5_probe_policy_preview`. They are planning artifacts only: no eBPF probes are activated and no RCA pipeline is run.

## A6 IPW-masked RLS Preview

A6 preview artifacts are stored under `data/p2_online_boutique/a6_ipw_rls_preview`. They are propagation-learning artifacts only and are not RCA scoring outputs.

## A7 Evidence Channel Preview

A7 preview outputs are written under `data/p2_online_boutique/a7_evidence_channel_preview`. They contain `evidence_vectors.jsonl`, `evidence_effects.jsonl`, `calibrated_residuals.jsonl`, and `evidence_channel_metadata.json` for each existing P2 repeat. Raw residuals are not used directly for sparse inversion.


## A8 Graph Sparse Inversion Preview

A8 preview outputs are written under `data/p2_online_boutique/a8_graph_sparse_preview`. They contain sparse interventions, metric scores, service scores, objective traces, and metadata for each existing P2 repeat.

## A8R Graph Sparse Inversion Repair

A8R repairs A8 sparse inversion by reducing metric-level edge explosion, using positive top-k calibrated residual aggregation, adding blind-evidence signal support, using automatic sparse regularization, applying post-sparsify, and improving ADMM convergence. The repair does not use root labels, target labels, injected paths, or incident start/end times for inversion. A8R remains a preview and is not a P2E acceptance result.

## A9 Counterfactual Explanation

A9 implements counterfactual explanation preview for A8R sparse candidates. For top metric and service candidates it re-optimizes graph sparse inversion with the candidate removed and reports `Delta L = L(u^{-v}) - L(u_hat)`. A9 does not use root labels, target labels, injected paths, or incident start/end times for explanation generation. It does not run old P1 RCA and does not reinject faults. Debug metrics are post-hoc diagnostics only, not P2E acceptance.

## B1 Integrated Blind RCA Pipeline

B1 integrates A3-A9 into a single end-to-end blind RCA smoke pipeline over existing raw metrics and service graph data. It uses A3 alert windows, alert-window blind evidence, A4 candidates, A5 policy preview, A6 IPW-masked RLS, A7 calibrated residuals, A8R graph sparse inversion, and A9 counterfactual explanation to write an integrated RCA result schema.

B1 does not reinject faults, does not run the old P1 RCA pipeline, does not modify P1 scoring logic, does not use legacy target-aware evidence, and does not use root/target labels or injected paths for inference. B1 is a single smoke integration step; B2 is the full 20-repeat replay and B3 is future real reinjection.

## B1R Integrated Final Result Repair

B1R repairs B1 final RCA result assembly. The final result now uses a metric-level `metric_candidate_table` as the primary candidate source, derives `top1_service`, `top1_metric`, and `predicted_root_type` from the same primary candidate, aggregates `top_services` from metric candidates, and writes one RCA result per alert window. B1R does not run B2 replay, does not reinject faults, does not run the old P1 RCA pipeline, and does not use root/target labels or legacy target-aware evidence.

## B2 Integrated Replay

B2 output lives under `data/p2_online_boutique/b2_integrated_replay`. It is a replay over existing raw metrics, not a new fault injection experiment.
