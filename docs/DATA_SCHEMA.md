# probeRCA Data Schema

## MetricRecord

MetricRecord 中文解释：指标记录。

用于表示某个时间点、某个服务实例和节点上的一个指标观测值。

## EvidenceRecord

EvidenceRecord 中文解释：证据记录。

用于表示 CPU、网络、I/O、锁等语义证据的观测值。当前 P0 只定义 schema，不接真实 eBPF。

## IncidentRecord

IncidentRecord 中文解释：故障注入标签记录。

用于离线验证时记录故障编号、根因服务、根因指标、根因类型、症状服务、故障时间窗口和注入路径。

## RCAResult

RCAResult 中文解释：根因分析输出结果。

用于保存 Top-K 根因服务、Top-K 根因指标、根因类型、语义证据、解释路径和可选延迟。

## ServiceNode

ServiceNode 中文解释：服务节点。

用于描述服务图中的服务、实例和所在节点。

## GraphEdge

GraphEdge 中文解释：图边。

用于描述服务图中两个节点之间的关系。

edge_type 中文解释：边类型。

- call 中文解释：服务调用边。
- trace 中文解释：链路追踪边。
- cohost 中文解释：同主机共置边。
- resource 中文解释：共享资源边。
- synthetic 中文解释：伪分布式模拟边。

## Step 2 Generated Dataset Files

metrics.jsonl 中文解释：指标记录文件。用于 Step 3 robust normalization（鲁棒归一化）读取指标并计算异常分数。

evidence.jsonl 中文解释：证据记录文件。用于后续 semantic evidence scoring（语义证据打分）验证 CPU、网络、I/O、锁等根因类型证据。

incidents.jsonl 中文解释：故障标签记录文件。用于 P0 experiments（P0 实验）评估 Hit@1、Hit@3、MRR、metric-Hit@3 和 root type accuracy。

service_graph.jsonl 中文解释：服务图边记录文件。用于后续 stable propagation（稳定传播）和 path explanation（路径解释）的图结构输入。

metadata.json 中文解释：数据集元信息文件。用于记录 seed、services、nodes、窗口数量和各类记录计数，帮助复现实验。

## Step 3 Robust Normalization Files

normalized_metrics.jsonl 中文解释：归一化后的指标记录。每条记录保留原始 value，并新增 z_value（鲁棒异常分数）。

robust_stats.jsonl 中文解释：鲁棒统计记录，包括 median、MAD、scale 和 baseline_count。

normalization_metadata.json 中文解释：归一化元信息，记录 input_dir、output_dir、normalized_count、stats_count、eps 和 clip。

鲁棒归一化公式：

```text
z = (x - median) / (1.4826 * MAD + eps)
```

中文解释：x 是原始指标值；median 是正常窗口的中位数；MAD 是中位数绝对偏差；eps 是极小正数，防止分母为 0；z 是鲁棒异常分数。

## Step 4 Stable Propagation Files

stable_propagation_model.json 中文解释：稳定传播模型文件，保存每个 incident 的节点、传播系数、训练摘要。

stable_residuals.jsonl 中文解释：稳定传播残差文件，保存每个 service.metric 在每个时间窗口的 residual，后续 sparse inversion（稀疏反演）会使用它。

propagation_metadata.json 中文解释：传播学习元信息，记录 input_dir、output_dir、incidents_count、coefficients_count、residuals_count、ridge_lambda 和 coefficient_threshold。

稳定传播公式：

```text
z_t ≈ A0 z_{t-1}
```

中文解释：z_t 是当前窗口异常向量；A0 是稳定传播矩阵；z_{t-1} 是上一窗口异常向量。

ridge regression（岭回归）公式：

```text
beta = inv(X^T X + lambda I) X^T Y
```

中文解释：beta 是传播系数；X 是 parent 节点上一时刻异常分数；Y 是 target 节点当前异常分数；lambda 是岭回归正则系数；I 是单位矩阵。

## Step 5 Sparse Inversion Files

sparse_interventions.jsonl 中文解释：稀疏干预候选文件，保存每个 service.metric 节点的 intervention_score。

sparse_inversion_summary.json 中文解释：稀疏反演摘要文件，保存每个 incident 的候选统计、debug top candidates 和 synthetic true root debug 信息。

sparse_inversion_metadata.json 中文解释：稀疏反演元信息，记录 candidates_count、expected_candidates_count、candidates_count_matches_expected 和正则参数。

残差提升公式：

```text
residual_lift = max(0, mean(abs(residual_faulty)) - mean(abs(residual_baseline)))
```

中文解释：residual_faulty 是故障阶段残差；residual_baseline 是正常阶段残差；residual_lift 表示故障阶段残差相对正常阶段的提升。

soft thresholding（软阈值）公式：

```text
score = max(residual_lift - lambda, 0)
```

中文解释：lambda 是 L1 稀疏阈值；score 是稀疏化后的干预分数。

## Step 6 Semantic Evidence Files

semantic_interventions.jsonl 中文解释：语义证据修正后的干预候选文件。

semantic_type_scores.jsonl 中文解释：候选根因类型分数文件。

semantic_evidence_summary.json 中文解释：语义证据打分摘要文件。

semantic_evidence_metadata.json 中文解释：语义证据打分元信息。

语义融合公式：

```text
semantic_score = sparse_score × (1 + evidence_weight × evidence_score)
```

中文解释：sparse_score 是 Step 5 稀疏反演分数；evidence_weight 是证据权重；evidence_score 是语义证据归一化分数；semantic_score 是融合后的候选分数。

## Step 7 Path Explanation Files

path_explanations.jsonl 中文解释：路径解释文件，保存候选根因到症状服务的解释路径。

path_explanation_summary.json 中文解释：路径解释摘要文件。

path_explanation_metadata.json 中文解释：路径解释元信息。

路径评分公式：

```text
path_score = semantic_weight × semantic_score + propagation_weight × path_propagation + shorter_path_weight × shortness_bonus
```

中文解释：semantic_score 是语义候选分数；path_propagation 是路径传播强度；shortness_bonus 是短路径奖励；path_score 是路径分数。

## Step 8 P0 End-to-End Experiment Files

p0_results.jsonl 中文解释：P0 最终 RCAResult 文件，包含 top_services、top_metrics、root_type、evidence、path。

p0_results_metadata.json 中文解释：P0 RCAResult 元信息。

p0_evaluation_summary.json 中文解释：P0 评估指标文件。

p0_experiment_metadata.json 中文解释：P0 实验元信息。

Hit@K 中文解释：真实根因是否出现在前 K 名。

MRR 中文解释：Mean Reciprocal Rank，平均倒数排名，真实根因越靠前越高。

root_type_accuracy 中文解释：根因类型准确率。

path_fidelity 中文解释：解释路径是否命中注入路径。

## Step 8A P0 Sanity Audit Files

p0_audit_summary.json 中文解释：P0 审计总结果，包含标签泄漏检查、多 seed 鲁棒性、语义证据消融、噪声敏感性。

p0_audit_metadata.json 中文解释：P0 审计元信息。

## Step 8B G1 Gate Files

g1_decision.json 中文解释：G1 决策结果文件，用于判断 P0 是否可以冻结并进入 P1。

## Step 8C P0 Failure Analysis Files

p0_failure_analysis.json 中文解释：P0 失败分析文件，记录导致 full audit 指标级 Hit@1 下降的 seed、incident、top5 metrics 和 failure patterns。

metric_specificity_weight 中文解释：指标特异性权重，用于在语义证据阶段提高底层根因指标、降低纯症状 request 指标。

说明：metric_specificity_weight 是基于指标名称语义的通用先验，不使用 root_service、root_metric、root_type，不是标签泄漏。

## P1A Adaptive Observation Files

observed_metrics.jsonl 中文解释：经过自适应观测策略后实际可见的指标，保留 normalized metric（归一化指标）字段，并追加 observed、sampling_probability、observation_mode 和 reason。

sampling_log.jsonl 中文解释：采样日志，记录每条指标的 sampling_probability（采样概率）、是否观测到、observation_mode（观测模式）和 reason（原因）。

observation_mask.jsonl 中文解释：观测掩码，记录每个 incident、timestamp、service、metric 是否可见以及对应采样概率。

adaptive_observation_metadata.json 中文解释：自适应观测元信息，记录 total_records、observed_records、observed_ratio、各观测模式计数和 seed。

说明：P1A 不实现 IPW-masked RLS（逆概率加权掩码递归最小二乘），只生成 P1B 所需的 sampling_probability 和 observation_mask。

## P1B IPW-Masked Stable Propagation Files

ipw_stable_propagation_model.json 中文解释：逆概率加权掩码稳定传播模型，保存 IPW-masked stable propagation（逆概率加权掩码稳定传播）的配置、传播系数和每个 incident 的训练摘要。

ipw_stable_residuals.jsonl 中文解释：只对观测到的 target 输出的 IPW-masked 残差，未观测 target 不输出真实 residual。

ipw_propagation_metadata.json 中文解释：IPW 传播学习元信息，记录 coefficients_count、residuals_count、mean_sampling_probability、mean_ipw_weight 和 use_ipw。

逆概率权重公式：

```text
weight = 1 / max(sampling_probability, min_sampling_probability)
```

中文解释：weight 是逆概率权重；sampling_probability 是采样概率；min_sampling_probability 是最小采样概率。

说明：P1B 不实现 sparse inversion（稀疏反演）、semantic evidence scoring（语义证据打分）、path explanation（路径解释）或最终 RCAResult。

## P1C IPW Residual Sparse Inversion Files

ipw_sparse_interventions.jsonl 中文解释：基于 IPW 稳定传播残差得到的稀疏干预候选，保存每个 service.metric 的 residual_lift、intervention_score、confidence 和观测统计。

ipw_sparse_inversion_summary.json 中文解释：P1C 稀疏反演摘要，包含每个 incident 的候选数量、非零候选数量、top candidate 和 synthetic debug true_root_rank。

ipw_sparse_inversion_metadata.json 中文解释：P1C 稀疏反演元信息，记录 l1_lambda、use_ipw_weighted_mean、观测点数阈值和 IPW 权重参数。

残差抬升公式：

```text
residual_lift = max(0, faulty_abs_residual - baseline_abs_residual)
```

中文解释：residual_lift 是故障阶段残差相对正常阶段残差的抬升。

稀疏候选分数公式：

```text
score = max(residual_lift - l1_lambda, 0)
```

中文解释：score 是扣除稀疏惩罚后的候选根因分数。

说明：P1C 输出是 sparse intervention candidates（稀疏干预候选），不是 semantic evidence scoring、path explanation 或最终 RCAResult。

## P1D IPW Semantic Evidence Files

ipw_semantic_interventions.jsonl 中文解释：P1D 融合语义证据后的候选根因指标排序，保存 semantic_score、semantic_rank、evidence_type、specificity_weight 和 semantic_anchor_bonus。

ipw_semantic_type_scores.jsonl 中文解释：P1D 根因类型候选排序，保存 root_type_candidate、type_score、rank 和 supporting_nodes。

ipw_semantic_evidence_summary.json 中文解释：P1D 语义证据摘要，包含 mean_true_root_semantic_rank_debug 和每个 incident 的 semantic debug 信息。

ipw_semantic_evidence_metadata.json 中文解释：P1D 语义证据元信息，记录 evidence_weight、specificity_weight_enabled 和 semantic_anchor_enabled。

语义融合公式：

```text
semantic_score = sparse_score * (1 + evidence_weight * evidence_score) * specificity_weight + semantic_anchor_bonus
```

中文解释：semantic_score 是融合语义证据后的候选分数；sparse_score 是 P1C 稀疏反演分数；evidence_score 是语义证据分数；specificity_weight 是指标特异性权重；semantic_anchor_bonus 是无标签证据锚定加分。

说明：P1D 不实现 path explanation（路径解释）或最终 RCAResult。

## P1D-R Semantic Sibling Repair Files

ipw_semantic_sibling_diagnosis.json
中文解释：P1D 兄弟指标错误诊断文件，用于查看同服务、同类型 sibling metric 是否压过真实机制指标。

diagnostic_priority_bonus
中文解释：诊断优先级加分，用于区分更接近根因机制的指标和同类型症状/状态指标。

metric_specificity_weight
中文解释：指标特异性权重。P1D-R 将 cpu.throttled_usec、net.retrans、io.bio_latency_ms、lock.futex_wait_ms 视为强根因诊断指标；cpu.pressure、net.rtt_ms、io.queue_depth 视为同类型状态指标；request.* 视为弱症状指标。

说明：P1D-R 修复不使用 root_service、root_metric、root_type 参与打分，不根据 incident_id 特判，只使用 metric 名称、semantic evidence、sparse score 分布和 observation confidence。

## P1E IPW Semantic Path Explanation Files

ipw_path_explanations.jsonl
中文解释：P1E 路径解释记录，保存 IPW semantic candidate（IPW 语义候选）到 symptom service（症状服务）的服务级路径。

ipw_path_explanation_summary.json
中文解释：P1E 路径解释摘要，包含 path_records_count、paths_missing_count 和 path_fidelity_debug。

ipw_path_explanation_metadata.json
中文解释：P1E 路径解释元信息，记录 top_k_candidates、max_path_length、reverse edge 和 undirected fallback 配置。

path_score = semantic_score * (1 + propagation_support_weight * propagation_support) * (1 + confidence_weight * confidence)
中文解释：path_score 是路径解释分数；semantic_score 是 P1D 语义候选分数；propagation_support 是传播模型对路径的支持度；confidence 是候选置信度。

说明：P1E 输出不是 final RCAResult，不包含 top_services、top_metrics 或最终根因结果。

## P1F End-to-End P1 RCA Result Files

p1_results.jsonl
中文解释：P1 端到端根因分析结果，包含 top_services、top_metrics、root_type、evidence、path、observation 和 confidence。

p1_results_metadata.json
中文解释：P1 结果元信息，记录 observed_ratio、mean_sampling_probability 和 mean_ipw_weight。

p1_evaluation_summary.json
中文解释：P1 单 seed 评估摘要，包含 service Hit@K、metric Hit@K、MRR、root_type_accuracy、path_fidelity 和 observed_ratio。

p1_experiment_metadata.json
中文解释：P1F 实验元信息，记录 P1F single-seed pipeline 的步骤和生成文件。

说明：P1F 会输出 final P1 RCAResult，但不做 P1 gate。P1 gate 和 full multi-seed audit 留到 P1G。

## P1G Full P1 Audit And Gate Files

p1_audit_summary.json
中文解释：P1 完整审计摘要，包含多 seed 指标、标签泄漏检查、观测比例检查和 audit_passed。

p1_audit_metadata.json
中文解释：P1 审计元信息，记录 seeds、multi_seed 目录、清理报告和观测审计详情。

p1_failure_analysis.json
中文解释：P1 失败分析，记录失败 seed、失败 incident 和每个 seed 的关键指标。

p1_gate_decision.json
中文解释：P1 决策门结果，用于判断 P1 是否通过。

说明：P1G 才判断 P1 能否冻结。P1 是 partial observation（部分观测），所以 gate 不只看 metric Hit@1，还要看 Hit@3 和 MRR。

## P1 Freeze Snapshot And Cleanup

- docs/p1_freeze_snapshot/p1_audit_summary.json
  中文解释：P1 冻结审计摘要快照。
- docs/p1_freeze_snapshot/p1_audit_metadata.json
  中文解释：P1 冻结审计元信息快照。
- docs/p1_freeze_snapshot/p1_failure_analysis.json
  中文解释：P1 冻结失败分析快照。
- docs/p1_freeze_snapshot/p1_gate_decision.json
  中文解释：P1 决策门结果快照。

P1 freeze 检查命令：

```bash
python3 -m proberca.cli.check_p1_freeze --freeze-dir docs/p1_freeze_snapshot
```

P1 清理命令：

```bash
python3 scripts/cleanup_p1_artifacts.py --base data/p1_single_vm
python3 scripts/cleanup_p1_artifacts.py --base data/p1_single_vm --apply
```

cleanup dry-run 中文解释：试运行，只打印候选删除文件，不真正删除。
cleanup apply 中文解释：真正删除可再生成的大体量 P1 中间文件，但保留 P1 结果、审计摘要、决策门和关键 metadata。

## P2A-1 Real CPU Fault Dataset

- metrics.jsonl
  中文解释：Online Boutique 真实最小采集指标，包括 frontend request latency，以及可用时的 Pod CPU/memory 和 cgroup throttling。
- evidence.jsonl
  中文解释：CPU 故障弱语义证据，来自 Kubernetes resource limit 或可用的 CPU 指标。
- incidents.jsonl
  中文解释：paymentservice CPU throttling 真实故障标签，仅用于后续评估，不参与采集和打分。
- service_graph.jsonl
  中文解释：Online Boutique 服务调用图初始化。
- metadata.json
  中文解释：P2A-1 实验元信息。
- data_quality_report.json
  中文解释：最小采集数据质量报告。

## P2A-1R cAdvisor Metric Repair

`metrics.jsonl` may contain real Online Boutique resource metrics from kubelet cAdvisor with `source=kubelet_cadvisor`:

- `cpu.usage`: delta `container_cpu_usage_seconds_total / window_size_sec`, approximate CPU cores.
- `cpu.throttled_usec`: delta `container_cpu_cfs_throttled_seconds_total * 1_000_000`.
- `cpu.throttled_periods`: delta `container_cpu_cfs_throttled_periods_total`.
- `cpu.throttle_ratio`: throttled periods divided by total CFS periods in the window.
- `memory.usage`: cAdvisor working set or usage bytes.

`data_quality_report.json` includes `cadvisor_metrics_available`, `root_service_metric_coverage_passed`, and paymentservice CPU/throttling coverage flags.

## P2A-2 Real CPU RCA Outputs

`real_p1_rca_summary.json` stores the single real CPU injection P1 RCA summary, including single-incident metrics and debug labels only for evaluation.

`real_p1_rca_metadata.json` stores the P2A-2 pipeline metadata.

The full-observation bridge writes `observed_metrics.jsonl`, `sampling_log.jsonl`, `observation_mask.jsonl`, and `adaptive_observation_metadata.json` with `sampling_probability=1.0` for collected real metrics only. It does not synthesize missing metrics.

## P2A-3 Repeated CPU Outputs

`p2a3_cpu_repeat_summary.json` stores CPU-only repeated real injection aggregate metrics and per-repeat RCA summaries.

`p2a3_cpu_repeat_metadata.json` stores repeat experiment metadata.

`p2a3_cpu_repeat_failures.json` records failed repeats, if any.


## P2A-3R Files

`p2a3_failure_diagnosis.json` 中文解释：P2A-3 原始真实 CPU 重复实验失败诊断，包含 top5 候选、指标 lift、非目标服务 throttling 噪声和实验级建议。

`p2a3_cpu_repeat_summary.json` in `cpu_paymentservice_repeated_controlled` 中文解释：P2A-3R 受控真实 CPU 重复实验摘要，仍只代表 CPU 故障重复实验。

## P2 Real Experiment Metric Policy

P2 真实实验主指标采用 `metric_hit_at_3`，并同时报告 `service_hit_at_1`、`root_type_accuracy` 和 `path_fidelity`。`metric_hit_at_1` 只作为辅助指标报告，不作为 P2 真实实验通过门槛。P2A-3R 在 Top3 口径下通过，但不是 exact metric Top1 成功。后续 network / IO / lock 真实注入也采用同一口径。多故障总体准确率必须同时报告：`service_hit_at_1`、`metric_hit_at_3`、auxiliary `metric_hit_at_1`、`root_type_accuracy`、`path_fidelity`。

## P2B-0 Real Network Fault Smoke

P2B-0 validates whether `tc netem` can inject and restore a real network delay/loss fault in the `shippingservice` Pod network namespace. It only performs feasibility smoke testing and does not run the RCA pipeline or report accuracy.

```bash
bash scripts/online_boutique/run_p2b0_network_smoke.sh
python3 -m proberca.cli.check_p2b0_network_smoke --input data/p2_online_boutique/network_shippingservice_smoke_001
```

## P2B-1 Network Repeat Outputs

`p2b1_network_repeat_summary.json` 中文解释：P2B-1 真实 network 重复注入 Top3 汇总结果。

`p2b1_network_repeat_metadata.json` 中文解释：P2B-1 元信息。

`p2b1_network_repeat_failures.json` 中文解释：失败 repeat 记录。

Each repeat writes `raw/metrics.jsonl`, `raw/evidence.jsonl`, `raw/incidents.jsonl`, `raw/service_graph.jsonl`, `raw/data_quality_report.json`, and `p1rca/real_p1_rca_summary.json`.

Network MetricRecords use `source=real_tc_netem_collection` and include real `shippingservice.net.retrans`, `shippingservice.net.out_segs`, `shippingservice.net.in_segs`, optional `shippingservice.net.rtt_ms`, and frontend `request.*` latency metrics.

## P2C-0 IO Smoke Outputs

`p2c0_io_smoke_summary.json` 中文解释：P2C-0 真实 I/O 故障注入可行性摘要。

`io_metrics_before.json`, `io_metrics_during.json`, `io_metrics_after.json` 中文解释：基于 cAdvisor filesystem metrics 的注入前/中/后快照和增量。

`io_fault_log.json` and `io_restore_log.json` 中文解释：I/O 压力启动与临时文件清理记录。


## P2C-1 Real IO Repeated Outputs

- `p2c1_io_repeat_summary.json` 中文解释：P2C-1 真实 I/O 重复注入 Top3 评估摘要。
- `p2c1_io_repeat_metadata.json` 中文解释：P2C-1 重复实验元信息。
- `p2c1_io_repeat_failures.json` 中文解释：P2C-1 失败 repeat 记录。
- `repeat_XX/raw/metrics.jsonl` 中文解释：每次真实 I/O 注入采集得到的 cAdvisor filesystem 和 frontend request 指标。
- `repeat_XX/p1rca/p1_results.jsonl` 中文解释：每次真实 I/O 数据接入 P1 后的 RCAResult。

P2C-1 的主指标是 `service_hit_at_1`、`metric_hit_at_3`、`root_type_accuracy`、`path_fidelity`；`metric_hit_at_1` 只作为辅助指标报告。


## P2D-0 Real Lock Smoke Outputs

- `lock_fault_log.json` 中文解释：sidecar lock-stress 注入日志。
- `lock_restore_log.json` 中文解释：sidecar 移除和恢复日志。
- `lockstress_logs.txt` 中文解释：sidecar stdout 中真实 Python 多线程锁竞争 JSON 行。
- `lock_metrics_during.json` 中文解释：由 sidecar stdout 解析出的 lock wait 指标。
- `p2d0_lock_smoke_summary.json` 中文解释：P2D-0 锁竞争可行性冒烟摘要。
- `p2d0_lock_smoke_metadata.json` 中文解释：P2D-0 元信息和限制说明。

P2D-0 的 lock contention 来自 `cartservice` Pod 内临时 sidecar，不是原始 cartservice 业务代码内部 bug。

## P2D-1 Lock Repeat Outputs

- `p2d1_lock_repeat_summary.json` 中文解释：P2D-1 真实 lock 重复注入 Top3 评估摘要。
- `p2d1_lock_repeat_metadata.json` 中文解释：P2D-1 lock 重复实验元信息。
- `p2d1_lock_repeat_failures.json` 中文解释：P2D-1 失败 repeat 记录。
- `lock.futex_wait_ms` 中文解释：sidecar stdout 真实测量的锁等待总量，按 faulty 窗口分配。
- `lock.contention_count` 中文解释：sidecar stdout 真实测量的锁竞争次数，按 faulty 窗口分配。
- `lock_metrics_window_distribution=distributed_from_sidecar_total` 中文解释：lock 指标来自 sidecar 总量按故障窗口分配，不是每窗口原始采样。

## P2D-1R Phase-Aware Lock Outputs

- `p2d1r_lock_repeat_summary.json` 中文解释：P2D-1R phase-aware 真实 lock 重复注入 Top3 评估摘要。
- `p2d1r_lock_repeat_metadata.json` 中文解释：P2D-1R phase-aware lock 重复实验元信息。
- `p2d1r_lock_repeat_failures.json` 中文解释：P2D-1R 失败 repeat 记录。
- `lock.futex_wait_ms` 中文解释：sidecar 每个窗口真实上报的锁等待总量。
- `lock.wait_ms` 中文解释：sidecar 每个窗口真实上报的平均锁等待。
- `lock.wait_p95_ms` 中文解释：sidecar 每个窗口真实上报的 p95 锁等待。
- `baseline_lock_metrics_are_real_idle_sidecar_measurements=true` 中文解释：baseline lock 指标来自真实 idle sidecar measurement，不是 fake baseline 0。

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


## A4 Candidate Subgraph Artifacts

A4 writes `candidate_services.jsonl`, `candidate_metric_nodes.jsonl`, `candidate_edges.jsonl`, `candidate_subgraph_metadata.json`, and `repeat_candidate_summary.json`. Metadata records `uses_root_labels=false`, `uses_target_config=false`, `uses_injected_path=false`, and `uses_incident_start_end=false` for graph construction.

## A5 Adaptive Probe Policy Artifacts

A5 writes `probe_plan.jsonl`, `sampling_log.jsonl`, `observation_mask.jsonl`, and `adaptive_probe_metadata.json`. Metadata records `uses_root_labels=false`, `uses_target_config=false`, `uses_injected_path=false`, `uses_incident_start_end=false`, and `actual_probe_activation=false`.

## A6 IPW-masked RLS Artifacts

A6 writes `ipw_rls_state.json`, `ipw_rls_edges.jsonl`, `ipw_rls_residuals.jsonl`, `ipw_rls_predictions.jsonl`, and `ipw_rls_metadata.json`. Metadata records `consumes_sampling_probability=true`, `consumes_observation_mask=true`, `update_mode=online_rls`, and `batch_ridge_used=false`.

## A7 Evidence Channel Schema

Per repeat output directory:

- `evidence_vectors.jsonl`: one row per service-metric node with blind evidence score, service/family priors, probe sampling probability, and `h_value`.
- `evidence_effects.jsonl`: one row per A6 residual row with signed `evidence_effect`.
- `calibrated_residuals.jsonl`: one row per A6 residual row with `raw_residual`, `evidence_effect`, `raw_adjusted_residual`, and bounded `calibrated_residual`.
- `evidence_channel_metadata.json`: label-safety flags and residual calibration summary.


## A8 Graph Sparse Inversion Schema

Per repeat output directory:

- `sparse_interventions.jsonl`: nonzero graph sparse intervention candidates.
- `metric_scores.jsonl`: all candidate metric nodes with `u_value`, residual signal, evidence support, and rank.
- `service_scores.jsonl`: service-level group norms and rank.
- `graph_sparse_objective_trace.jsonl`: ADMM objective and residual trace.
- `graph_sparse_metadata.json`: solver status, objective terms, and label-safety flags.

## A8R Graph Sparse Inversion Repair

A8R repairs A8 sparse inversion by reducing metric-level edge explosion, using positive top-k calibrated residual aggregation, adding blind-evidence signal support, using automatic sparse regularization, applying post-sparsify, and improving ADMM convergence. The repair does not use root labels, target labels, injected paths, or incident start/end times for inversion. A8R remains a preview and is not a P2E acceptance result.

## A9 Counterfactual Explanation

A9 implements counterfactual explanation preview for A8R sparse candidates. For top metric and service candidates it re-optimizes graph sparse inversion with the candidate removed and reports `Delta L = L(u^{-v}) - L(u_hat)`. A9 does not use root labels, target labels, injected paths, or incident start/end times for explanation generation. It does not run old P1 RCA and does not reinject faults. Debug metrics are post-hoc diagnostics only, not P2E acceptance.

## B1 Integrated Blind RCA Pipeline

B1 integrates A3-A9 into a single end-to-end blind RCA smoke pipeline over existing raw metrics and service graph data. It uses A3 alert windows, alert-window blind evidence, A4 candidates, A5 policy preview, A6 IPW-masked RLS, A7 calibrated residuals, A8R graph sparse inversion, and A9 counterfactual explanation to write an integrated RCA result schema.

B1 does not reinject faults, does not run the old P1 RCA pipeline, does not modify P1 scoring logic, does not use legacy target-aware evidence, and does not use root/target labels or injected paths for inference. B1 is a single smoke integration step; B2 is the full 20-repeat replay and B3 is future real reinjection.

## B1R Integrated Final Result Repair

B1R repairs B1 final RCA result assembly. The final result now uses a metric-level `metric_candidate_table` as the primary candidate source, derives `top1_service`, `top1_metric`, and `predicted_root_type` from the same primary candidate, aggregates `top_services` from metric candidates, and writes one RCA result per alert window. B1R does not run B2 replay, does not reinject faults, does not run the old P1 RCA pipeline, and does not use root/target labels or legacy target-aware evidence.

## B2 Integrated Replay Schema

`p2_integrated_replay_summary.json` records 20-repeat integrated replay metrics, per-fault summaries, per-repeat selected results, and safety flags. `integrated_replay_evaluation.json` is post-hoc only and may read incident labels after final RCA result generation.

## B2R Integrated Ranking Repair Schema

B2R extends the B1R final result schema with transparent final-ranking fields:

- `metric_candidate_table.jsonl` includes `diagnostic_specificity`, `specificity_reason`, `symptom_penalty_applied`, `weak_memory_usage_penalty_applied`, `cpu_diagnostic_boost_applied`, `final_candidate_score_before_penalty`, and `final_candidate_score`.
- `top_metrics` entries are sourced from the same metric candidate table and include diagnostic specificity and score components.
- `integrated_rca_results.jsonl` includes `root_type_confidence`, `root_type_source="primary_metric_family"`, and `root_type_uses_labels=false`.
- `integrated_rca_metadata.json` preserves label-safety fields and still records that labels are not used for inference.

The B2R schema does not add root labels, target labels, injected paths, or incident start/end timestamps to inference outputs.

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

