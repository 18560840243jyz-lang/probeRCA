# probeRCA Implementation Plan

## Step 0: local project memory

中文解释：创建本地项目记忆文件，也就是当前任务。

目标：固定项目规则，防止后续偏离方案。
输入：当前项目背景。
输出：AGENTS.md、skills/proberca/SKILL.md、docs/PROJECT_CONTEXT.md、docs/IMPLEMENTATION_PLAN.md、docs/DECISIONS.md。
当前不做：算法、测试、数据生成器、真实系统接入。

## Step 1: project scaffold

中文解释：创建项目代码骨架。

目标：创建 Python 包结构、配置、基础 schema、基础检查命令。
输入：项目规则文件。
输出：proberca/、configs/、tests/、scripts/ 等基础结构。
当前不做：复杂算法。

## Step 2: synthetic pseudo-distributed data generator

中文解释：合成伪分布式数据生成器。

目标：生成单机伪分布式数据。
输入：服务图、指标列表、故障配置。
输出：normal windows、faulty windows、incident labels。
当前不做：真实 Kubernetes、真实 eBPF。

## Step 3: robust normalization

中文解释：鲁棒归一化。

目标：把不同量纲的指标转成异常分数。
输入：正常窗口和故障窗口指标。
输出：z-score-like robust deviation scores。
当前不做：传播学习。

## Step 4: stable propagation learner

中文解释：稳定传播学习器。

目标：学习稳定传播矩阵 A0。
输入：鲁棒归一化后的时间序列和服务图。
输出：稳定传播矩阵。
当前不做：传播漂移。

## Step 5: sparse inversion solver

中文解释：稀疏反演求解器。

目标：从残差中恢复稀疏根因干预。
输入：稳定传播残差。
输出：root-cause intervention vector。
当前不做：Shapley。

## Step 6: semantic evidence scoring

中文解释：语义证据打分。

目标：用 CPU、网络、I/O、锁等证据修正根因类型。
输入：evidence table。
输出：root-cause type score。
当前不做：真实 eBPF。

## Step 7: path explanation

中文解释：路径解释。

目标：输出根因到症状服务的解释路径。
输入：服务图、传播矩阵、根因节点。
输出：explanation path。
当前不做：UI。

## Step 8: P0 experiments

中文解释：P0 实验。

目标：验证 CPU、网络、I/O、锁等故障。
输入：合成数据和 P0 算法。
输出：Hit@1、Hit@3、MRR、metric-Hit@3、root type accuracy。
当前不做：分布式实验。

## Step 9: P1 adaptive sampling and IPW-masked RLS

中文解释：P1 自适应采样和逆概率加权递归最小二乘。

adaptive sampling
中文解释：自适应采样。

IPW
中文解释：Inverse Propensity Weighting，逆概率加权。

RLS
中文解释：Recursive Least Squares，递归最小二乘。

目标：验证自适应观测下的传播学习。
输入：采样概率、观测掩码、时间序列。
输出：IPW-corrected stable propagation model。
当前不做：P0 未通过前不得实现。

## Step 10: optional drift only after P1 passes

中文解释：只有 P1 通过后，才考虑可选传播漂移。

optional drift
中文解释：可选传播漂移。

目标：验证传播真的变化时是否需要 drift。
输入：传播变化类故障。
输出：drift-gated RCA result。
当前不做：P1 未通过前不得实现。

## Step 9A: P1A adaptive observation simulator

中文解释：P1A 自适应观测模拟器。

目标：在单机伪分布式环境中模拟 adaptive observability（自适应可观测性），生成 observed metrics（实际可见指标）、sampling probability logging（采样概率日志）和 observation mask（观测掩码）。
输入：P0 synthetic pseudo-distributed data（合成伪分布式数据）和 normalized metrics（归一化指标）。
输出：observed_metrics.jsonl、sampling_log.jsonl、observation_mask.jsonl、adaptive_observation_metadata.json。
当前不做：IPW-masked RLS、bandit adaptive probing、optional drift、真实 eBPF、Kubernetes、Prometheus、Beyla、ClickHouse、Shapley、UI、GNN、Transformer、LLM。

## Step 9B: P1B IPW-masked stable propagation learner

中文解释：P1B 逆概率加权掩码稳定传播学习器。

目标：在 partial observation（部分观测）条件下，用 observation mask（观测掩码）和 sampling_probability（采样概率）学习稳定传播模型。
输入：observed_metrics.jsonl、sampling_log.jsonl、observation_mask.jsonl、incidents.jsonl、service_graph.jsonl。
输出：ipw_stable_propagation_model.json、ipw_stable_residuals.jsonl、ipw_propagation_metadata.json。
当前不做：sparse inversion、semantic evidence scoring、path explanation、final RCAResult、bandit adaptive probing、optional drift、真实 eBPF、Kubernetes、Prometheus、Beyla、ClickHouse、Shapley、UI、GNN、Transformer、LLM。

## Step 9C: P1C sparse inversion on IPW residuals

中文解释：P1C 在逆概率加权残差上做稀疏反演。

目标：在 partial observation（部分观测）条件下，基于 ipw_stable_residuals（逆概率加权稳定传播残差）输出 sparse intervention candidates（稀疏干预候选）。
输入：ipw_stable_residuals.jsonl、ipw_propagation_metadata.json、incidents.jsonl。
输出：ipw_sparse_interventions.jsonl、ipw_sparse_inversion_summary.json、ipw_sparse_inversion_metadata.json。
当前不做：semantic evidence scoring on P1 outputs、path explanation、final RCAResult、adaptive sampling bandit、optional drift、真实部署组件或任何深度学习模型。

## Step 9D: P1D semantic evidence on IPW sparse candidates

中文解释：P1D 在 IPW 稀疏候选上融合语义证据。

目标：对 IPW sparse candidates（IPW 稀疏候选）融合 semantic evidence（语义证据）、metric specificity prior（指标特异性先验）和 label-free semantic evidence anchor（无标签语义证据锚定）。
输入：ipw_sparse_interventions.jsonl、ipw_sparse_inversion_summary.json、evidence.jsonl、incidents.jsonl。
输出：ipw_semantic_interventions.jsonl、ipw_semantic_type_scores.jsonl、ipw_semantic_evidence_summary.json、ipw_semantic_evidence_metadata.json。
当前不做：path explanation、final RCAResult、adaptive sampling bandit、optional drift、真实部署组件或任何深度学习模型。

## Step 9D-R: P1D-R semantic sibling repair

中文解释：P1D-R 语义兄弟指标修复。

目标：诊断并修复 P1D 中同服务、同类型 sibling metric 压过更具体根因机制指标的问题。
输入：ipw_semantic_interventions.jsonl、ipw_semantic_evidence_summary.json、ipw_sparse_interventions.jsonl、incidents.jsonl。
输出：ipw_semantic_sibling_diagnosis.json，以及带 diagnostic_priority_bonus 的 ipw_semantic_interventions.jsonl。
当前不做：P1E path explanation、final RCAResult、adaptive sampling bandit、optional drift、真实部署组件或任何深度学习模型。

说明：metric_specificity_weight 和 diagnostic_priority_bonus 只使用 metric 名称、evidence_score 和当前 incident 的 sparse score 分布，不使用 root_service、root_metric、root_type，也不根据 incident_id 特判。

## Step 9E: P1E IPW semantic path explanation

中文解释：P1E 基于 IPW 语义候选的路径解释。

目标：为 IPW semantic candidates（IPW 语义候选）生成 service-level path explanation（服务级路径解释），并结合 IPW stable propagation model（IPW 稳定传播模型）计算 propagation-supported path score（传播支持路径分数）。
输入：ipw_semantic_interventions.jsonl、ipw_semantic_type_scores.jsonl、ipw_stable_propagation_model.json、service_graph.jsonl、incidents.jsonl。
输出：ipw_path_explanations.jsonl、ipw_path_explanation_summary.json、ipw_path_explanation_metadata.json。
当前不做：final RCAResult、P1 gate、adaptive sampling bandit、optional drift、真实部署组件或任何深度学习模型。

## Step 9F: P1F end-to-end P1 RCA result and single-seed evaluation

中文解释：P1F 端到端 P1 根因结果生成与单 seed 评估。

目标：串联 P1A 到 P1E 输出 final P1 RCAResult，并计算单 seed evaluation metrics（评估指标）。
输入：ipw_semantic_interventions.jsonl、ipw_semantic_type_scores.jsonl、ipw_path_explanations.jsonl、adaptive_observation_metadata.json、ipw_propagation_metadata.json、ipw_sparse_inversion_summary.json、incidents.jsonl。
输出：p1_results.jsonl、p1_results_metadata.json、p1_evaluation_summary.json、p1_experiment_metadata.json。
当前不做：P1 gate、multi-seed full audit、adaptive sampling bandit、optional drift、真实部署组件或任何深度学习模型。

## Step 9G: P1G full P1 audit and P1 gate

中文解释：P1G 完整 P1 审计和 P1 决策门。

目标：运行多 seed P1 audit（多随机种子审计）、label leakage scan（标签泄漏检查）、observation sanity check（观测合理性检查）和 P1 gate decision（P1 决策门）。
输入：P1F single-seed pipeline、seeds、P1 gate thresholds。
输出：p1_audit_summary.json、p1_audit_metadata.json、p1_failure_analysis.json、p1_gate_decision.json。
当前不做：adaptive sampling bandit、optional drift、真实部署组件或任何深度学习模型。

## Step 9H: P1H P1 freeze and cleanup

中文解释：P1 冻结记录与结果清理护栏。

目标：保存 docs/p1_freeze_snapshot，记录 P1 freeze report，提供 P1 freeze check 和 P1 artifact cleanup 工具。
输入：data/p1_single_vm/audit_full/p1_audit_summary.json、p1_audit_metadata.json、p1_failure_analysis.json、p1_gate_decision.json。
输出：docs/p1_freeze_snapshot、docs/P1_FREEZE_REPORT.md、proberca/cli/check_p1_freeze.py、scripts/cleanup_p1_artifacts.py。
当前不做：P2、adaptive sampling bandit、optional drift、真实部署组件或 P1 scoring logic 修改。

使用命令：

```bash
python3 -m proberca.cli.check_p1_freeze --freeze-dir docs/p1_freeze_snapshot
python3 scripts/cleanup_p1_artifacts.py --base data/p1_single_vm
python3 scripts/cleanup_p1_artifacts.py --base data/p1_single_vm --apply
```

## Step P2A-0: Online Boutique single-VM deployment smoke test

中文解释：Google Online Boutique 单机伪分布式部署与冒烟测试。

目标：用 kind 在单 VM 上部署 Online Boutique，记录 Pod/Service/Deployment 状态，并验证 frontend 可访问。
输入：Online Boutique upstream repository、kind、kubectl、Docker。
输出：data/p2_online_boutique/deploy_smoke 下的状态文件、frontend smoke test 结果和 service_graph.jsonl。
当前不做：真实故障注入、实验指标采集、P1 RCA pipeline、Prometheus、Beyla、ClickHouse、adaptive sampling bandit、optional drift 或深度学习模型。

## Step P2A-1: Online Boutique real CPU fault injection

中文解释：Online Boutique 真实 CPU 故障注入与最小指标采集。

目标：对 paymentservice 注入 Kubernetes CPU resource limit 故障，并生成 probeRCA schema 兼容的 metrics/evidence/incidents/service_graph/metadata。
输入：kind-proberca-ob 集群、online-boutique namespace、frontend curl、kubectl 状态、可用时的 crictl stats。
输出：data/p2_online_boutique/cpu_paymentservice_001。
当前不做：P1 RCA pipeline、准确率输出、多故障类型、Prometheus、Beyla、ClickHouse、adaptive sampling bandit、optional drift 或深度学习模型。

## P2A-1R Real Metric Collection Repair

P2A-1R repairs real Online Boutique metric collection by using kubelet cAdvisor APIs for paymentservice CPU and throttling counters. This remains a metric collection repair step only: no RCA pipeline, no accuracy output, no Prometheus/Beyla/ClickHouse integration.

## Step P2A-2: Real CPU Injection Data to P1 RCA

中文解释：把 P2A-1R 的真实 CPU 注入数据接入冻结的 P1 RCA pipeline。

目标：对真实 `metrics.jsonl` 做鲁棒归一化，构造 full-observation bridge，运行 P1B/P1C/P1D/P1E/P1F，并输出单个真实 CPU 注入事件的 RCAResult 和评估摘要。
当前不做：重新注入故障、修改 P1 打分逻辑、多故障总体准确率、network/io/lock 故障、Prometheus/Beyla/ClickHouse、adaptive sampling bandit、optional drift 或深度学习模型。

## Step P2A-3: Repeated Real CPU Fault Injection Experiments

中文解释：真实 CPU 故障重复注入实验。

目标：对 Online Boutique paymentservice 重复 5 次真实 CPU throttling 注入，每次重新采集 cAdvisor 指标、恢复服务、接入冻结的 P1 RCA pipeline，并汇总 CPU-only repeated accuracy。
当前不做：network / IO / lock 故障、多故障总体准确率、Prometheus/Beyla/ClickHouse、adaptive sampling bandit、optional drift 或深度学习模型。


## P2A-3R Controlled CPU Repeat

P2A-3R handles the failed P2A-3 gate by diagnosing real repeat outputs and rerunning controlled CPU repeats. It must not change P1 scoring, lower gates, use synthetic data, or enter multi-fault evaluation.

## P2 Real Experiment Metric Policy

P2 真实实验主指标采用 `metric_hit_at_3`，并同时报告 `service_hit_at_1`、`root_type_accuracy` 和 `path_fidelity`。`metric_hit_at_1` 只作为辅助指标报告，不作为 P2 真实实验通过门槛。P2A-3R 在 Top3 口径下通过，但不是 exact metric Top1 成功。后续 network / IO / lock 真实注入也采用同一口径。多故障总体准确率必须同时报告：`service_hit_at_1`、`metric_hit_at_3`、auxiliary `metric_hit_at_1`、`root_type_accuracy`、`path_fidelity`。

## P2B-0 Real Network Fault Smoke

P2B-0 validates whether `tc netem` can inject and restore a real network delay/loss fault in the `shippingservice` Pod network namespace. It only performs feasibility smoke testing and does not run the RCA pipeline or report accuracy.

```bash
bash scripts/online_boutique/run_p2b0_network_smoke.sh
python3 -m proberca.cli.check_p2b0_network_smoke --input data/p2_online_boutique/network_shippingservice_smoke_001
```

## P2B-1 Real Network Repeated Injection

P2B-1 repeats real `shippingservice` `tc netem` delay/loss injection, collects real network metrics, restores qdisc, and runs the frozen P1 RCA pipeline per repeat.

It does not modify P0/P1 frozen logic and does not change P1 scoring. It reports network-only repeated Top3 results and keeps `metric_hit_at_1` as an auxiliary metric.

## P2C-0 Real IO Fault Smoke

P2C-0 validates real `redis-cart` I/O write pressure using Pod-local `dd`, cAdvisor filesystem metrics, frontend smoke latency, and temp-file cleanup. It does not run P1 RCA and does not report I/O accuracy.


## P2C-1 Real IO Repeated

P2C-1 repeats real `redis-cart` I/O write-pressure injection. Each repeat runs real Pod `dd/sync`, collects cAdvisor filesystem counters, cleans temporary files, bridges the real metrics into the frozen P1 RCA pipeline, and reports Top3 metrics under the P2 real experiment policy. It does not modify P1 scoring logic and does not report multi-fault overall accuracy.


## P2D-0 Real Lock Smoke

P2D-0 validates lock contention feasibility with a temporary `cartservice` Pod sidecar. The sidecar runs real Python threading lock contention, emits stdout metrics, and is removed after the smoke. This stage does not run RCA and does not modify P1 scoring logic. It explicitly records that the fault is not an original cartservice business-code bug.

## P2D-1 Real Lock Repeated

P2D-1 repeats real sidecar lock contention injection for `cartservice`, collects real sidecar lock wait metrics, restores the deployment, runs the frozen P1 RCA pipeline, and evaluates with the P2 Top3 policy. It does not modify P0/P1 frozen logic or P1 scoring logic. The sidecar limitation must remain visible in summaries and reports.

## P2D-1R Phase-Aware Real Lock Repeated

P2D-1R repairs the real lock metric collection protocol by running the lock sidecar across baseline/faulty/recovery phases. It preserves frozen P0/P1 logic and does not modify P1 scoring. The output remains lock-only repeated real injection, not multi-fault accuracy.

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

A4 implements candidate subgraph construction from A3 alert windows, raw metrics, and service graphs. This is a preview-only step and is not wired into the RCA pipeline yet.

## A5 Adaptive Probe Policy

A5 implements a budgeted probe policy preview that emits sampling probabilities and observation masks for A6. It is not a real eBPF agent and does not run RCA.

## A6 True IPW-masked RLS

A6 implements online recursive propagation learning with IPW-masked parent features. It remains preview-only and is not wired into RCA scoring.

## A7 C h_t Evidence Channel

A7 is implemented as a preview module. It maps blind evidence and adaptive probe policy into `C h_t`, subtracts the evidence effect from A6 propagation residuals, and performs metric-family robust residual calibration. A7 does not run RCA and does not modify frozen P1 scoring.


## A8 Graph Sparse Inversion

A8 implements ADMM graph-constrained sparse inversion over A7 calibrated residuals. It does not use raw A6 residuals, does not run the old P1 RCA pipeline, and does not modify P1 scoring.

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

B2 replays the B1R integrated blind RCA pipeline over the existing 20 Online Boutique raw repeats. It does not modify P1 scoring, does not run old P1 RCA, and does not reinject faults.

## B2R Integrated Ranking Repair

B2R repairs only the integrated final candidate ranking. B2 showed CPU repeats were misranked toward memory-family candidates while network, I/O, and lock repeats were correct. B2R adds static label-free diagnostic specificity, weak `memory.usage` penalty unless strong memory evidence exists, CPU throttling specificity and boost, root-type confidence, and explicit score components. It keeps B2 as replay over existing raw metrics and does not enter B3 real reinjection.

## B2S Service-first RCA Repair

B2R fixed CPU metric-family and root-type recognition, but CPU service Hit@1 remained 0.0. B2S changes the final integrated RCA schema from metric-first to service-first. The primary root service now comes from `service_candidate_table`; the primary root metric is selected only within that root service. Global metric ranking remains available only as `global_top_metrics_auxiliary` and is not a primary RCA result.

B2S adds service-conditioned evaluation fields: `service_conditioned_metric_hit_at_3`, `global_metric_hit_at_3_auxiliary`, and `service_metric_pair_hit_at_1`. Labels remain post-hoc evaluation only. B2S does not use root labels, target labels, injected paths, incident start/end timestamps, or legacy target-aware evidence for inference. B2S is still replay over existing raw metrics; B3 is the future real reinjection stage.

## B2M Service-Metric Ownership Mapping Repair

B2S already switched the integrated RCA output to a service-first hierarchy, but CPU service localization remained weak. B2M adds explicit service-metric ownership mapping so each service's own resource metric remains tied to that service throughout final assembly, for example `paymentservice.cpu.throttled_usec`, `adservice.cpu.throttled_usec`, and `checkoutservice.cpu.throttled_usec`.

Evidence support is now separated into node-level, service-family-level, and family-global-level support. Family-global evidence is kept as a weak fallback only, with `family_global_evidence_weight = 0.10`, so global CPU evidence cannot by itself make all CPU services equivalent. Primary RCA candidates must pass ownership checks, and final metadata records `service_local_support_used`, `global_family_support_weight_limited`, `ownership_invalid_count`, and `primary_candidate_ownership_valid`.

B2M remains an existing-raw-metrics replay. It does not use root labels, target labels, injected paths, or incident start/end during inference, and it is not B3 real re-injection.

## B2P Normal Propagation Audit and Repair

B2M ruled out service-metric ownership loss as the main CPU failure mode. CPU service localization remained weak, so B2P adds a stable-only structured multi-lag propagation support stage. This stage learns label-free parent sets and lagged propagation weights from existing raw metrics, alert windows, candidate nodes, and probe-policy sampling probabilities. It does not implement propagation drift.

The parent set is structure constrained rather than fully connected: self-lag, same-service resource -> request, same-service request -> request, callee resource/request -> caller request, and request-chain propagation. It does not use root labels, target labels, injected paths, or incident start/end.

The integrated pipeline now emits `05b_structured_propagation/` with structured parent sets, propagation edges, predictions, residuals, and metadata. Final service scoring consumes `structured_propagation_support`, `path_edge_support`, and `lag_support`; if structured support is unavailable, fallback is explicit in score components. B2P remains an existing-raw-metrics replay, not B3 real re-injection.

## Final ProbeRCA-BPF Two-Plane Implementation

- [x] Introduce an algorithm-free `CollectedWindow` boundary and label-safety guard.
- [x] Add write-once sealed collection archives with contract and content fingerprints.
- [x] Enforce complete final-scheme service, host, TCP-edge, and DNS-edge metric sets.
- [x] Add an offline-only final control plane with Healthy `As`, Soft freeze/candidate pruning, healthy masked `Av`, signed cross-metric residuals, Burst penalty adjustment, and non-negative Sparse-Group FISTA.
- [x] Remove counterfactual re-solves from the final path.
- [x] Add separate seal/analyze commands and an adapter for existing engine windows.
- [x] Add end-to-end separation, integrity, metric completeness, evidence, and label-isolation tests.
- [ ] Migrate each real P13 collector metric name and aggregation scope to the final collection contract before the next real experiment run.
