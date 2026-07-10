# P1 Freeze Report

## Status

P1_PASS

中文解释：P1 通过 P1 决策门，可以冻结为下一阶段基础版本。

## Freeze Source

冻结来源：

- docs/p1_freeze_snapshot/p1_audit_summary.json
- docs/p1_freeze_snapshot/p1_gate_decision.json
- docs/p1_freeze_snapshot/p1_failure_analysis.json

## Frozen Scope

P1 包含：

- adaptive observation simulator
  中文解释：自适应观测模拟器。
- sampling probability logging
  中文解释：采样概率日志。
- observation mask
  中文解释：观测掩码。
- IPW-masked stable propagation
  中文解释：逆概率加权掩码稳定传播学习。
- sparse inversion on IPW residuals
  中文解释：在 IPW 残差上做稀疏反演。
- semantic evidence on IPW sparse candidates
  中文解释：在 IPW 稀疏候选上做语义证据增强。
- semantic sibling repair
  中文解释：语义兄弟指标修复。
- IPW semantic path explanation
  中文解释：基于 IPW 语义候选的路径解释。
- end-to-end P1 RCAResult
  中文解释：端到端 P1 根因分析结果。
- full P1 audit and P1 gate
  中文解释：完整 P1 审计与 P1 决策门。

## Frozen Metrics

以下结果读取自 docs/p1_freeze_snapshot/p1_audit_summary.json 和 docs/p1_freeze_snapshot/p1_gate_decision.json；缺失字段记为 unknown，不编造。

- label_leakage_passed: True
- multi_seed_mean_service_hit_at_1: 1.0
- multi_seed_min_service_hit_at_1: 1.0
- multi_seed_mean_metric_hit_at_1: 0.925
- multi_seed_min_metric_hit_at_1: 0.75
- multi_seed_mean_metric_hit_at_3: 0.975
- multi_seed_min_metric_hit_at_3: 0.75
- multi_seed_mean_metric_mrr: 0.9550000000000001
- multi_seed_min_metric_mrr: 0.8
- multi_seed_mean_root_type_accuracy: 1.0
- multi_seed_min_root_type_accuracy: 1.0
- multi_seed_mean_path_fidelity: 1.0
- multi_seed_min_path_fidelity: 1.0
- observed_ratio_mean: 0.6137192234848485
- observed_ratio_min: 0.6124289772727273
- observed_ratio_max: 0.6152107007575758
- observation_audit_passed: True
- audit_passed: True
- p1_gate_passed: True
- decision: P1_PASS

## Known Limitations

- P1 是 partial observation。
  中文解释：P1 是部分观测场景。
- P1 不要求每个 seed 的 metric Hit@1 都满分。
  中文解释：部分观测下指标级第一名可能被同类指标压过。
- P1G 中失败 seed 包括 3、7、17。
- 典型失败：
  - seed 3 的 CPU 故障中 cpu.pressure 压过 cpu.throttled_usec。
  - seed 7 和 seed 17 的网络故障中 net.rtt_ms 压过 net.retrans。
- 但 P1 gate 仍通过，因为 service-level、metric Hit@3、MRR、root type、path fidelity 均满足门槛。

## Boundary

- P1 没有实现 adaptive sampling bandit。
  中文解释：没有实现多臂老虎机式自适应采样控制器。
- P1 没有实现 optional drift。
  中文解释：没有实现可选传播漂移。
- P1 没有接真实部署组件。
  中文解释：没有接 eBPF、Kubernetes、Prometheus、Beyla、ClickHouse。
- P1 没有修改 P0 冻结逻辑。

## Next Phase

下一阶段可以选择：

- P2 real observation adapter
  中文解释：真实观测适配器。
- P2 deployment bridge
  中文解释：部署桥接层。
- 或者先做 P1 paper-ready ablation
  中文解释：论文级 P1 消融实验。

注意：不要在本次任务中实现这些内容。
