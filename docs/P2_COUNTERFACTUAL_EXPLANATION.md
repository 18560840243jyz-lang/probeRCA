# P2 Counterfactual Explanation

## Goal

对 A8R 的候选根因做反事实解释，计算删除候选后解释损失增加多少。

## Formula

`Delta L_v = L(u^{-v}) - L(u_hat)`

中文解释：如果禁止候选 `v` 后模型损失明显增加，说明 `v` 是关键解释因素。

## Metric-level Counterfactual

禁止单个 metric node，并在剩余候选图上重新求解 graph sparse inversion。

## Service-level Counterfactual

禁止服务下所有 metric nodes，并在剩余候选图上重新求解 graph sparse inversion。

## Inputs

- A8R graph sparse outputs
- A4 candidate graph
- A7 evidence channel

## Outputs

- `counterfactual_metric_explanations.jsonl`
- `counterfactual_service_explanations.jsonl`
- `counterfactual_metric_ranking.jsonl`
- `counterfactual_service_ranking.jsonl`
- `counterfactual_metadata.json`

## Label Safety

Root labels are only allowed for post-hoc debug ranking after counterfactual outputs are written. They do not participate in candidate selection, re-optimization, or ranking.

## Limitations

- A9 is a preview; A10 performs final blind audit and summary.
- Counterfactual re-optimization is more expensive than a single sparse inversion.
- A9 does not reinject faults and does not run the old P1 RCA pipeline.
