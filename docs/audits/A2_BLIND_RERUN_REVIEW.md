# A2 Blind Rerun Review

## Scope

A2 uses existing real raw metrics, generates A1 blind evidence for every repeat, and reruns the frozen P1 RCA pipeline. It does not rerun fault injection, does not use legacy target-aware evidence, and does not modify P1 scoring logic.

## Files Changed

- `proberca/adapters/online_boutique/blind_evidence.py`
- `proberca/adapters/online_boutique/blind_rerun.py`
- `proberca/cli/run_p2_blind_rerun.py`
- `proberca/cli/check_p2_blind_rerun.py`
- `docs/P2_BLIND_RERUN.md`
- `README.md`
- `experiments/README.md`
- `docs/DATA_SCHEMA.md`
- `docs/IMPLEMENTATION_PLAN.md`
- `docs/DECISIONS.md`
- `tests/test_online_boutique_blind_rerun.py`
- `docs/audits/A2_BLIND_RERUN_REVIEW.md`
- `data/p2_online_boutique/blind_rerun/`

## Safety Checks

1. `run_p2_blind_rerun` does not read `raw_input_dir/evidence.jsonl` for scoring: `True`.
2. RCA input `evidence.jsonl` is derived from `blind_evidence.jsonl`: `True`.
3. Blind rerun does not use root labels to generate evidence: `True`.
4. Blind rerun does not use target config to generate evidence: `True`.
5. P1 scoring logic was not modified: `True`.
6. Faults were not reinjected: `True`.
7. A2 only uses incident `start_ts/end_ts` as the alert window: `True`.
8. Every repeat uses blind evidence and not legacy evidence: `True`.

## Result Summary

- `total_repeats`: `20`
- `total_completed`: `20`
- `total_successful_rca`: `20`
- `service_hit_at_1_overall`: `0.9`
- `metric_hit_at_3_overall`: `1.0`
- `root_type_accuracy_overall`: `0.9`
- `path_fidelity_overall`: `1.0`
- `auxiliary_metric_hit_at_1_overall`: `0.7`
- `auxiliary_metric_mrr_overall`: `0.8416666666666666`

Per fault type:

- `cpu`: service_hit_at_1_mean=`1.0`, metric_hit_at_3_mean=`1.0`, root_type_accuracy_mean=`1.0`, path_fidelity_mean=`1.0`, auxiliary_metric_hit_at_1_mean=`0.2`
- `io`: service_hit_at_1_mean=`0.6`, metric_hit_at_3_mean=`1.0`, root_type_accuracy_mean=`0.6`, path_fidelity_mean=`1.0`, auxiliary_metric_hit_at_1_mean=`0.6`
- `lock`: service_hit_at_1_mean=`1.0`, metric_hit_at_3_mean=`1.0`, root_type_accuracy_mean=`1.0`, path_fidelity_mean=`1.0`, auxiliary_metric_hit_at_1_mean=`1.0`
- `network`: service_hit_at_1_mean=`1.0`, metric_hit_at_3_mean=`1.0`, root_type_accuracy_mean=`1.0`, path_fidelity_mean=`1.0`, auxiliary_metric_hit_at_1_mean=`1.0`

## Review Verdict

- `A2_review_passed`: `true`
- `failed_checks`: `[]`
- `remaining_risks`: `["A2 still uses incident start_ts/end_ts as the alert window; true alert-blind operation remains A3 work.", "A2 evaluates against incidents.jsonl root labels only after scoring, so evaluation/debug still depends on labels by design.", "Blind I/O service_hit_at_1_mean and root_type_accuracy_mean dropped to 0.6; this is a real blind rerun limitation, not a structural failure.", "The bridge derives P1-compatible evidence_type labels from blind evidence metric prefixes because frozen P1 semantic scoring uses CPU/Net/IO/Lock labels."]`

A2 review passes on process cleanliness and structure. It does not claim that blind accuracy matches the previous target-aware P2E result.
