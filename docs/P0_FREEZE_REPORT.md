# P0 Freeze Report

## Status

G1_PASS

中文解释：P0 通过 G1 决策门，可以冻结为 P1 的基础版本。

## Freeze Source

冻结来源：

- docs/p0_freeze_snapshot/p0_audit_summary.json
- docs/p0_freeze_snapshot/g1_decision.json

说明：原始 data/p0_single_vm/audit_full_fix 已因磁盘清理删除，保留了关键冻结快照。

## Frozen Scope

P0 包含：

- synthetic pseudo-distributed data
  中文解释：合成伪分布式数据。
- robust normalization
  中文解释：鲁棒归一化。
- stable propagation
  中文解释：稳定传播。
- sparse inversion
  中文解释：稀疏反演。
- semantic evidence scoring
  中文解释：语义证据打分。
- path explanation
  中文解释：路径解释。
- P0 end-to-end evaluation
  中文解释：P0 端到端评估。
- P0 sanity audit
  中文解释：P0 合理性审计。
- G1 gate
  中文解释：G1 决策门。

## Frozen Metrics

以下结果读取自 docs/p0_freeze_snapshot/p0_audit_summary.json 和 docs/p0_freeze_snapshot/g1_decision.json；缺失字段记为 unknown，不编造。

- label_leakage_passed: True
- multi_seed_min_service_hit_at_1: 1.0
- multi_seed_min_metric_hit_at_1: 1.0
- full_metric_hit_at_1: 1.0
- no_semantic_metric_hit_at_1: 0.0
- audit_passed: True
- g1_passed: True
- decision: G1_PASS

## Important Fix

metric_specificity_weight(metric)
中文解释：指标特异性权重，用于提升底层诊断指标，降低纯症状 request 指标。

label-free semantic evidence anchor
中文解释：无标签语义证据锚定，不使用 root_service、root_metric、root_type，只根据当前 incident 的证据强度和 sparse score 尺度修正分数。

明确约束：

- 未使用 root labels 参与打分。
- 未根据 incident_id 特判。
- 未降低 G1 门槛。

## Disk Cleanup

Docker volumes 曾占用约 19G，已清理。

当前根目录磁盘约 43% 使用率，约 24G 可用。

后续跑 multi-seed audit 前必须检查磁盘空间。

## Next Phase

下一阶段才是 P1：

- adaptive sampling
  中文解释：自适应采样。
- IPW-masked RLS
  中文解释：逆概率加权掩码递归最小二乘。
- optional drift only after P1 passes
  中文解释：只有 P1 通过后才考虑可选传播漂移。
