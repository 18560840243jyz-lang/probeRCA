# P2 Blind Evidence Protocol

## Motivation

A0 audit found that legacy P2 `evidence.jsonl` generation has target-aware leakage risk. Legacy CPU, network, I/O, and lock evidence can be generated from experiment target knowledge and therefore must not be used to claim blind RCA.

## Rule

Blind evidence can only come from real statistical lift across all observed `service.metric` records. It must not come from `target_service`, `root_service`, `root_metric`, `root_type`, target metric configuration, injected paths, or incident-specific special cases.

For each incident window, A1 computes baseline and faulty means for every observed service and metric, derives positive lift, normalizes scores within each evidence type, and writes the highest scoring candidates per type.

## Current Scope

A1 only fixes the evidence generation protocol. It still uses `incident.start_ts` and `incident.end_ts` as a temporary alert window substitute.

This is window-aware blind evidence, not full alert-blind RCA. The real alert gate and automatic incident-window builder are reserved for A3.

## Outputs

A1 writes:

- `blind_evidence.jsonl`
- `blind_evidence_metadata.json`

The metadata records:

- `blind_evidence=true`
- `uses_root_labels=false`
- `uses_target_config=false`
- `uses_injected_path=false`
- `uses_alert_window_only=true`

## Legacy Evidence

Existing P2 `evidence.jsonl` files remain legacy target-aware evidence. They are retained for auditability and backward compatibility, but they must not be used for any blind RCA claim.

A2 will decide how to rerun the RCA bridge with `blind_evidence.jsonl` instead of legacy target-aware evidence. A1 does not rerun RCA and does not modify P1 scoring logic.
