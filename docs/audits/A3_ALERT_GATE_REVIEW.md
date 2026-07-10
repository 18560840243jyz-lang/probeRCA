# A3 Alert Gate Review

## Scope

A3 implements metrics-only alert detection and alert window construction. It does not reinject faults, does not run the RCA pipeline, does not modify P1 scoring logic, and does not fix the A2 I/O blind result.

## Files Changed

- `proberca/adapters/online_boutique/alert_gate.py`
- `proberca/cli/detect_alert_windows.py`
- `proberca/cli/run_p2_alert_preview.py`
- `proberca/cli/check_a3_alert_gate.py`
- `docs/P2_ALERT_GATE.md`
- `README.md`
- `experiments/README.md`
- `docs/DATA_SCHEMA.md`
- `docs/IMPLEMENTATION_PLAN.md`
- `docs/DECISIONS.md`
- `tests/test_online_boutique_alert_gate.py`
- `data/p2_online_boutique/a3_alert_preview_single/cpu_repeat_01/`
- `data/p2_online_boutique/a3_alert_preview/`
- `docs/audits/A3_ALERT_GATE_REVIEW.md`

## Safety Checks

1. Alert detection does not use root labels: `True`.
2. Alert detection does not use target config: `True`.
3. Alert detection does not use injected paths: `True`.
4. Alert detection does not use incident start/end: `True`.
5. `incidents.jsonl` is used only after detection for debug overlap evaluation.
6. Faults were not reinjected: `True`.
7. RCA pipeline was not run: `True`.
8. P1 scoring files were not modified.

## Alert Preview Results

- `total_repeats`: `20`
- `repeats_with_alert_window`: `20`
- `alert_window_detection_rate`: `1.0`

Per fault type:

- `cpu`: detection_rate=`1.0`, repeats_with_alert_window=`5`, average_alert_windows=`1.6`, debug_incident_window_recall=`1.0`
- `io`: detection_rate=`1.0`, repeats_with_alert_window=`5`, average_alert_windows=`1.0`, debug_incident_window_recall=`1.0`
- `lock`: detection_rate=`1.0`, repeats_with_alert_window=`5`, average_alert_windows=`1.0`, debug_incident_window_recall=`1.0`
- `network`: detection_rate=`1.0`, repeats_with_alert_window=`5`, average_alert_windows=`1.2`, debug_incident_window_recall=`1.0`

## Review Verdict

- `A3_review_passed`: `true`
- `failed_checks`: `[]`
- `remaining_risks`: `["A3 uses prefix baseline, which assumes faults are not active in the first 30% of each metric series.", "A3 does not yet feed detected alert windows into A2 blind RCA inputs; that is future integration work.", "Debug incident overlap uses incidents.jsonl only after detection and remains an offline evaluation artifact.", "A3 does not address the A2 I/O blind RCA performance drop."]`

A3 passes as an alert-gate module and preview. It does not claim A2 RCA results have been rerun with detected windows.
