# A1 Evidence De-leak Review

## Scope

This review covers A1 Evidence De-leak only. A1 adds a blind evidence generation protocol from real metric lift. It does not rerun fault injection, does not rerun the RCA pipeline, and does not modify P1 scoring logic.

## Files Changed

- `proberca/adapters/online_boutique/blind_evidence.py`
- `proberca/cli/generate_blind_evidence.py`
- `docs/P2_BLIND_EVIDENCE_PROTOCOL.md`
- `README.md`
- `experiments/README.md`
- `docs/DATA_SCHEMA.md`
- `docs/IMPLEMENTATION_PLAN.md`
- `docs/DECISIONS.md`
- `tests/test_online_boutique_blind_evidence.py`
- `data/p2_online_boutique/a1_blind_evidence_preview/cpu_repeat_01/blind_evidence.jsonl`
- `data/p2_online_boutique/a1_blind_evidence_preview/cpu_repeat_01/blind_evidence_metadata.json`
- `docs/audits/A1_EVIDENCE_DELEAK_REVIEW.md`

## Safety Checks

1. `blind_evidence.py` does not use answer labels in evidence scoring: `True`.
2. `root_service`, `root_metric`, and `root_type` appear only in the forbidden-term safety list, not in scoring code.
3. `target_service`, `target_metric`, and `target_fault_type` appear only in the forbidden-term safety list, not in scoring code.
4. `injected_path` appears only in the forbidden-term safety list and metadata flag `uses_injected_path=false`.
5. Lift is computed across every observed `service.metric`; preview services include: `adservice, currencyservice, frontend, loadgenerator, paymentservice, redis-cart`.
6. Output is not limited to the known target service.
7. Legacy P2 `evidence.jsonl` risk is documented in `docs/P2_BLIND_EVIDENCE_PROTOCOL.md` and remains separate from `blind_evidence.jsonl`.
8. No P1 scoring files were modified.
9. No RCA pipeline was rerun; only `generate_blind_evidence` preview was executed.

## Test Results

- `python3 scripts/check_env.py`: passed.
- `python3 -m proberca.cli.check_project`: passed.
- `python3 -m proberca.cli.check_p0_freeze --freeze-dir docs/p0_freeze_snapshot`: passed.
- `python3 -m proberca.cli.check_p1_freeze --freeze-dir docs/p1_freeze_snapshot`: passed.
- `pytest -q`: completed at `[100%]` with no failure text in `/tmp/proberca_a1_validation.log`.
- `python3 -m proberca.cli.generate_blind_evidence --input ... --output ...`: succeeded.

Preview metadata:

```json
{
  "blind_evidence": true,
  "evidence_count": 8,
  "evidence_types": [
    "CPU",
    "load",
    "memory"
  ],
  "incidents_count": 1,
  "input_dir": "data/p2_online_boutique/cpu_paymentservice_repeated_controlled/repeat_01/raw",
  "metrics_count": 2332,
  "min_score": 0.05,
  "output_dir": "data/p2_online_boutique/a1_blind_evidence_preview/cpu_repeat_01",
  "top_k_per_type": 20,
  "uses_alert_window_only": true,
  "uses_injected_path": false,
  "uses_root_labels": false,
  "uses_target_config": false
}
```

Safety scan:

```json
{
  "allowed_lines": [
    {
      "line": 18,
      "terms": [
        "root_service"
      ],
      "text": "\"root_service\","
    },
    {
      "line": 19,
      "terms": [
        "root_metric"
      ],
      "text": "\"root_metric\","
    },
    {
      "line": 20,
      "terms": [
        "root_type"
      ],
      "text": "\"root_type\","
    },
    {
      "line": 21,
      "terms": [
        "target_service"
      ],
      "text": "\"target_service\","
    },
    {
      "line": 22,
      "terms": [
        "target_metric"
      ],
      "text": "\"target_metric\","
    },
    {
      "line": 23,
      "terms": [
        "target_fault_type"
      ],
      "text": "\"target_fault_type\","
    },
    {
      "line": 24,
      "terms": [
        "injected_path"
      ],
      "text": "\"injected_path\","
    },
    {
      "line": 286,
      "terms": [
        "injected_path"
      ],
      "text": "\"uses_injected_path\": False,"
    },
    {
      "line": 308,
      "terms": [
        "injected_path"
      ],
      "text": "\"uses_injected_path\","
    }
  ],
  "forbidden_terms": [
    "root_service",
    "root_metric",
    "root_type",
    "target_service",
    "target_metric",
    "target_fault_type",
    "injected_path"
  ],
  "passed": true,
  "suspicious_lines": []
}
```

## Review Verdict

- `A1_review_passed`: `true`
- `failed_checks`: `[]`
- `remaining_risks`: `["A1 still uses incident start_ts/end_ts as the temporary alert window; full alert-blind RCA remains A3 work.", "Legacy P2 evidence.jsonl files remain target-aware and must stay excluded from blind RCA claims until A2 rerun uses blind_evidence.jsonl.", "A1 only generates blind evidence preview and does not rerun RCA, so it does not update previous P2E accuracy claims."]`
