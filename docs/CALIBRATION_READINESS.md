# ProbeRCA 校准与就绪门禁

本文档只描述最终数据面/控制面路径中的校准有效性规则。它不改变正式告警规则：

- Soft：同一实体的症状分数连续 3 个 1 秒窗口不低于 3。
- Hard：同一实体的症状分数连续 2 个 1 秒窗口不低于 5。

## 有效观测

数据面记录显式保存 `value`、`valid`、`invalid_reason`、覆盖率、映射质量、
样本数、事件损失率和来源记录。`valid=false` 时 `value` 必须为 `null`，
不得写入有限零值；`coverage`与`mapping_quality`不得相互混用。
控制面首先尊重数据面有效性，再逐指标追加自己的门禁判断：

- 原始采集覆盖为零：`invalid_reason=zero_coverage`，不进入基线、告警和 `A_v`。
- 完整采集但请求或操作计数为零：计数是有效零；没有分母或样本的P95/失败率
  使用 `invalid_reason=no_exposure`。
- 延迟 P95 的样本数低于 `latency_min_samples`：缺失。
- 失败率找不到请求/查询计数，或计数低于 `failure_min_requests`：缺失。
- 缺失值不得补零、前向填充、插值或复用上一窗口 P95。

`CalibrationReadinessReport.latest_observation_validity` 保存最新窗口中每个指标的
`raw_value`、`coverage`、`mapping_quality`、`sample_count`、`request_count`、`quality`、
`data_plane_invalid_reason`、`control_plane_invalid_reason` 和最终拒绝原因。

## 稳健尺度

`baseline_min_scale` 只作为浮点保护，不再充当统计尺度。对变换后的每个指标：

1. 正常 MAD 足够大时使用 `1.4826 * MAD`；
2. 否则使用 `IQR / 1.349`；
3. 两者均不足时使用该指标族冻结的尺度下限。

指标族为 `latency`、`ratio`、`count`、`psi`。每个结果保存
`mad_scale`、`iqr_scale`、`family_floor`、`final_scale` 和
`scale_source`。

正式配置中的指标族下限默认为 `null`。必须先用独立 Healthy Pilot 和采集精度
确定这些值，再写入并冻结正式控制配置；系统不会自行猜测下限。

第一次发现回放会在 `available_root_coordinates`、
`all_baseline_status` 和 `all_metric_model_status` 中列出全部坐标及状态。应根据
预先声明且与单次注入标签隔离的实验范围冻结
`calibration_required_root_coordinates` 和指标族下限，然后对同一只读 Healthy
档案重新校准。该范围不得按每次真实注入结果动态改变。

## 逐目标 `A_v` Readiness

Masked Ridge 按目标坐标分别拟合，不再要求整个候选矩阵同时完整。对目标坐标
`i`，只使用目标值及其允许父指标滞后值均有效的训练行。

默认最低训练行数为：

```text
N_i_min = max(metric_min_training_rows,
              ceil(metric_rows_per_feature * allowed_feature_count))
```

正式默认 `metric_rows_per_feature = 2`。每个目标输出：

- `allowed_feature_count`
- `valid_training_rows`
- `minimum_training_rows`
- `effective_rank`
- `condition_number`
- `ready`
- `not_ready_reason`

一条稀疏边不会阻止无关目标拟合，也不会进入 FISTA。正式计划故障范围内的
根因坐标必须通过 `calibration_required_root_coordinates` 显式冻结并全部
Ready。该范围只用于校准门禁，不能进入候选排序、残差或 FISTA。

## 状态和输出

控制面启动顺序为：

```text
STARTING -> CALIBRATING -> READY -> Healthy/Soft/Hard/Recovery
```

`CALIBRATING` 阶段不触发 Soft、Hard 或 FISTA。进入 `READY` 需要：

1. 必需指标拥有足够有效 Healthy 样本；
2. 所有尺度有效且指标族下限已经冻结；
3. `A_s` Ready；
4. 计划范围内所有根因坐标的 `A_v` Ready；
5. 拓扑和实体映射完整；
6. 连续健康验证窗口没有伪 Soft/Hard。

控制面输出目录包含：

- `control-run.json`
- `calibration-readiness.json`
- `rca-results.jsonl`

若 Hard 后候选模型意外失去就绪条件，输出 `RCA_NOT_READY` 及逐坐标原因，不运行
FISTA，也不生成伪根因。

## 故障注入门禁

最终单机故障矩阵入口必须显式接收独立 Healthy Pilot 的报告：

```bash
PYTHONPATH=. python3 scripts/run_final_fault_matrix.py \
  --calibration-readiness /path/to/calibration-readiness.json \
  --output /path/to/new-dataset
```

入口会校验报告 schema、指纹、全部 Ready 标志、健康验证窗口、控制配置指纹和
拓扑版本。任一条件不满足时，在创建数据集目录和注入故障之前拒绝启动。

## 固定回归

历史失败证据保存在：

- `artifacts/calibration-regressions/smoke_sparse_edge_coverage_failure`
- `artifacts/calibration-regressions/smoke_zero_mad_scale_failure`

`scripts/seal_calibration_regressions.py` 校验原始封存档案、盲测边界、旧配置、
来源日志和修复后回放结果。修复后回放必须保持 `CALIBRATING`、不产生 RCA
结果，并保留零覆盖和暴露不足为缺失。
