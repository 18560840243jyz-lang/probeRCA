# P2 Adaptive Probe Policy

## Goal

A5 implements an adaptive probe selection policy. Given alert windows and candidate subgraphs, it decides which probe families should be observed under a fixed budget and writes `sampling_probability` plus `observation_mask` artifacts for later A6 IPW-masked RLS.

## Scope

A5 only generates a policy preview. It does not activate real eBPF probes, does not run RCA, does not reinject faults, does not modify P1 scoring logic, and does not fix I/O blind performance.

## Inputs

- A3 `alert_windows.jsonl`
- A4 candidate subgraph artifacts
- A1/A2 `blind_evidence.jsonl` optional
- raw metrics optional through candidate metadata and metric availability

## Probe Layers

- `always_on`: low-cost request probe for symptom and available request metrics.
- `suspicious_burst`: CPU, memory, network, I/O, and lock probes selected by gain/cost under budget.
- `confirmation`: reserved for future confirmation probes.

## Budgeted Selection

A5 estimates probe gain from alert severity, alert intensity, blind evidence score, service centrality, metric family availability, uncertainty bonus, and cost penalty. It selects always-on probes first and then applies a gain/cost greedy policy for suspicious-burst probes while respecting the budget.

## Sampling Probability

For non-always-on probes, A5 uses:

`q = min_p + (max_p - min_p) * sigmoid(gain)`

Always-on probes have `q = 1.0`.

## Label Safety

`root_service`, `root_metric`, `root_type`, target config, injected paths, and incident start/end timestamps are not used for policy selection. `incidents.jsonl` is allowed only after policy generation for debug coverage.

## Outputs

- `probe_plan.jsonl`
- `sampling_log.jsonl`
- `observation_mask.jsonl`
- `adaptive_probe_metadata.json`
- `p2_probe_policy_preview_summary.json`

## Limitations

- A5 is a policy preview, not real probe activation.
- A5 does not include historical reward learning; `last_gain` is currently 0.
- A6 will consume `sampling_probability` and observation masks for IPW-masked RLS.
