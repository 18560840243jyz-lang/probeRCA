# Final ProbeRCA-BPF Data/Control Plane

本文件描述当前论文版本的正式数据面/控制面边界。历史P/B阶段流程不覆盖本
文件；正式入口不得调用旧的混合`ProbeRCAEngine`。

## 两平面边界

```mermaid
flowchart LR
  C["Normal与低频Burst采集"] --> W["CollectedWindow"]
  W --> S["不可变封存归档"]
  S --> V["契约、哈希、拓扑与身份校验"]
  V --> B["Healthy基线与服务级A_s"]
  B --> A["按实体Soft/Hard Alert"]
  A --> G["冻结候选范围与Healthy A_v"]
  G --> R["带符号跨指标残差"]
  R --> P["Burst只调整匹配组惩罚"]
  P --> F["单次非负Sparse-Group FISTA"]
  F --> O["Top-K实体、根因大类与直接证据"]
```

- `proberca/dataplane/`只采集、聚合和封存，不调用告警或RCA算法。
- `proberca/controlplane/`只读已封存归档，不调用Collector，也不修改归档。
- Normal与Burst归档共享Dataset ID和窗口时间线，但保持独立来源与lineage。
- ground truth、注入目标、预期根因和事故标签不得跨入推断边界。

## 正式根因范围

正式根因实体只有：

1. 服务；
2. 主机；
3. 有向TCP通信边`(src_service -> dst_service, TCP)`。

正式常态指标契约为：

| 实体 | 指标数 | 内容 |
|---|---:|---|
| 服务 | 9 | 3个症状/上下文指标和6个服务根因指标 |
| 主机 | 4 | CPU、Memory、I/O、NIC |
| 有向TCP边 | 3 | count、latency_p95、failure_rate |

即正式`9/4/3`契约。TCP count是暴露量/上下文；TCP latency和failure是TCP
根因组坐标。

## 校准、Readiness与告警

状态机为：

```text
STARTING -> CALIBRATING -> READY -> Healthy / Soft / Hard / Recovery
```

在`CALIBRATING`期间不触发Soft/Hard、不运行FISTA，也不允许故障注入。进入
`READY`要求正式支持范围内的Baseline、服务级`A_s`和逐目标`A_v`全部Ready，
且独立Healthy验证段没有持续误报。

服务与有向TCP边分别维护连续告警计数，不同实体不能拼成连续异常：

- Soft：异常分数`>=3`，同一实体连续3个1秒窗口；
- Hard：异常分数`>=5`，同一实体连续2个1秒窗口。

## 有向TCP边完整路径

TCP边能力必须完整保留：

```text
TCP边独立告警
  -> 构造候选范围
  -> 使用Healthy A_v只扣除跨指标传播
  -> 提取TCP边latency/failure根因残差
  -> TCP Burst调整对应TCP组惩罚
  -> 单次非负Sparse-Group FISTA
  -> 输出(src_service -> dst_service, TCP)
```

Burst不得直接写入残差、不得成为额外排序项，也不得触发反事实重复求解。

## DNS实验边界

DNS不是当前论文版本的正式根因类别，也不是正式有向边实体。DNS：

- 不进入`required_candidate_scope`；
- 不进入Baseline或`A_v` Readiness分母；
- 不触发正式Soft/Hard Alert；
- 不生成根因残差、theta变量或FISTA候选组；
- 不提供正式Burst惩罚证据；
- 不进入故障矩阵、主实验、消融或论文评价。

DNS采集和事务诊断实现可作为`experimental / optional`历史能力保留，但默认
关闭。显式开启实验DNS不得改变正式`9/4/3`契约、正式Dataset ID或Readiness
分母。

旧v2/v3 DNS归档仍可兼容读取；其DNS坐标和Burst记录必须标记为
`excluded_from_formal_rca`，不能进入正式控制面计算。Replay结果应保留旧归档
原始契约指纹。

## 正式入口

先采集并封存：

```bash
proberca-collect-final \
  --source-config configs/final_live_collector.example.yaml \
  --collection-contract configs/final_collection_contract.yaml \
  --burst-config configs/final_live_burst.example.yaml \
  --output /path/to/normal-archive \
  --burst-output /path/to/burst-archive \
  --windows WINDOW_COUNT
```

再独立执行控制面：

```bash
proberca-analyze-collection \
  --archive /path/to/normal-archive \
  --config configs/final_control.yaml \
  --output /path/to/control-output
```
