# P2 Blind Rerun

## Goal

A2 reruns P2 RCA using existing real raw metrics and A1 blind evidence. The goal is to check localization ability after removing target-aware legacy evidence.

## Scope

- No fault injection is rerun.
- P1 scoring logic is not modified.
- Legacy target-aware `evidence.jsonl` from raw experiment directories is not used.
- A1 `blind_evidence.jsonl` is generated from all observed service.metric lift.
- A2 still uses `incident.start_ts` and `incident.end_ts` as the alert window.
- A3 is responsible for a true Alert Gate and alert-blind incident window discovery.

## Outputs

- `p2_blind_rerun_summary.json`
- `p2_blind_rerun_metadata.json`
- `p2_blind_rerun_failures.json`

Each repeat also stores a blind input directory with `blind_evidence.jsonl`, P1-compatible `evidence.jsonl` derived from blind evidence, and `blind_input_metadata.json`.

## Interpretation

If blind results drop, they must be reported as-is. A2 must not modify P1 scoring or fall back to legacy target-aware evidence to recover previous P2E numbers.
