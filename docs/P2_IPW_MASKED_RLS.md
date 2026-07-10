# P2 IPW-masked RLS

## Goal

A6 implements a true online stable propagation learner that consumes A5 `sampling_probability` and `observation_mask`. It replaces batch-ridge-style propagation previews with recursive online updates.

## Scope

A6 only produces propagation preview artifacts. It does not run RCA, does not reinject faults, does not modify P1 scoring logic, and does not fix I/O blind performance.

## Model

`z_{i,t} ~= phi_{i,t}^T theta_i`

中文解释：当前目标指标由上一时刻父节点指标线性传播解释。

`phi_{i,t} = M_{Pa(i),t-1} * Omega_{Pa(i),t-1} * z_{Pa(i),t-1}`

中文解释：只使用已观测父节点，并按采样概率做逆概率加权。

## RLS Update

`e_t = y_t - phi_t^T theta_{t-1}`

中文解释：预测误差。

`K_t = w_t P_{t-1} phi_t / (gamma + w_t phi_t^T P_{t-1} phi_t)`

中文解释：加权 RLS 增益。

`theta_t = theta_{t-1} + K_t e_t`

中文解释：参数递推更新。

`P_t = gamma^{-1}(P_{t-1} - K_t phi_t^T P_{t-1})`

中文解释：协方差递推更新。

## Inputs

- raw `metrics.jsonl`
- A4 candidate graph
- A5 `sampling_log.jsonl`
- A5 `observation_mask.jsonl`

## Outputs

- `ipw_rls_state.json`
- `ipw_rls_edges.jsonl`
- `ipw_rls_residuals.jsonl`
- `ipw_rls_predictions.jsonl`
- `ipw_rls_metadata.json`

## Label Safety

Root labels are allowed only for post-learning debug diagnostics. They do not participate in RLS updates, parent selection, normalization, or train/test splitting.

## Limitations

- A6 uses A5 policy-preview expected observation probabilities, not a real random sampling stream.
- A6 is not connected to RCA yet.
- A7 introduces the `C h_t` evidence channel.
- A8 performs graph-constrained sparse inversion.
