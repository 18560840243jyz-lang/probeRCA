# P2 Evidence Channel

## Goal

Map A2 blind evidence and A5 adaptive probe policy into a fine-grained `C h_t` evidence term, then produce calibrated residuals for A8 graph sparse inversion.

## Model

`z_t = A_t^0 z_{t-1} + C h_t + u_t + epsilon_t`

中文解释：`A_t^0 z_{t-1}` 是 A6 输出的传播解释项，`C h_t` 是 A7 输出的证据解释项，`u_t` 是 A8 要求解的稀疏根因干预。

A7 does not solve `u_t`, does not run RCA, and does not modify P1 scoring logic.

## Residual Calibration

A6 raw residual scale can be very large, so A7 does not pass raw residuals directly to sparse inversion. It first computes:

`raw_adjusted_residual = raw_residual - evidence_effect`

Then it calibrates residuals inside each metric family:

`calibrated = (raw_adjusted_residual - median) / (1.4826 * MAD + eps)`

中文解释：按指标族内部的中位数和 MAD 标定残差，避免 I/O bytes 这类大数值淹没 CPU、network、lock 等指标。

The calibrated residual is clipped to a bounded range before later modules can consume it.

## Inputs

- A2 `blind_evidence.jsonl`
- A5 `probe_plan.jsonl`, `sampling_log.jsonl`, `observation_mask.jsonl`
- A6 `ipw_rls_residuals.jsonl`

## Outputs

- `evidence_vectors.jsonl`
- `evidence_effects.jsonl`
- `calibrated_residuals.jsonl`
- `evidence_channel_metadata.json`

## Label Safety

`root_service`, `root_metric`, `root_type`, target config labels, `injected_path`, and incident start/end are not used to build `C h_t` or calibrate residuals. Root labels may only be read after output generation for debug-only rank evaluation.

## Limitations

- A7 does not identify root cause `u_t`.
- A7 does not run RCA.
- A8 performs graph sparse inversion using calibrated residuals.
- The current `C h_t` is an interpretable linear evidence effect, not a trained deep model.
