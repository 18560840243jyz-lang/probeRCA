# B2 Integrated Replay Review

## Scope

- 本轮只用已有 raw metrics 全量 replay。
- 没有重新注入故障。
- 没有运行旧 P1 RCA pipeline。
- 没有修改 P1 scoring logic。
- 没有真实开启 probe。
- 没有进入 B3。

## Files Changed

- `proberca/adapters/online_boutique/integrated_replay.py`
- `proberca/cli/run_p2_integrated_replay.py`
- `proberca/cli/check_b2_integrated_replay.py`
- `tests/test_online_boutique_integrated_replay.py`
- `docs/B2_INTEGRATED_REPLAY.md`
- `docs/audits/B2_INTEGRATED_REPLAY_REVIEW.md`
- `README.md`
- `experiments/README.md`
- `docs/DATA_SCHEMA.md`
- `docs/IMPLEMENTATION_PLAN.md`
- `docs/DECISIONS.md`

## Safety Checks

- root labels used for inference: false.
- target labels used for inference: false.
- injected path used for inference: false.
- incident start/end used for inference: false.
- legacy target-aware evidence used: false.
- old P1 RCA pipeline run: false.
- fault reinjection: false.
- real eBPF probe activation: false.
- labels are used only after final result for evaluation.
- every completed repeat final result keeps top service / top metric consistency.

## Integrated Replay Results

- `total_repeats`: 20
- `repeats_completed`: 20
- `repeats_failed`: 0
- `service_hit_at_1_overall`: 0.75
- `metric_hit_at_3_overall`: 0.75
- `root_type_accuracy_overall`: 0.75
- `path_fidelity_overall`: 1.0
- `auxiliary_metric_hit_at_1_overall`: 0.5
- `auxiliary_metric_mrr_overall`: 0.6166666666666667

Per fault type:

- CPU: service Hit@1 0.0, metric Hit@3 0.0, root type 0.0, path fidelity 1.0.
- Network: service Hit@1 1.0, metric Hit@3 1.0, root type 1.0, path fidelity 1.0.
- I/O: service Hit@1 1.0, metric Hit@3 1.0, root type 1.0, path fidelity 1.0.
- Lock: service Hit@1 1.0, metric Hit@3 1.0, root type 1.0, path fidelity 1.0.

CPU failure is not hidden: B2 integrated replay currently misranks CPU as memory-family candidates in all five CPU repeats.

## Compare With A2 Official Blind Rerun

A2 official blind rerun:

- service Hit@1 = 0.9
- metric Hit@3 = 1.0
- root type = 0.9
- path fidelity = 1.0

B2 integrated replay:

- service Hit@1 = 0.75
- metric Hit@3 = 0.75
- root type = 0.75
- path fidelity = 1.0

A2 is the frozen P1 pipeline blind rerun. B2 is the new integrated pipeline replay. They are not the same pipeline and must not be merged into one acceptance claim.

## Test Results

- `python3 scripts/check_env.py`: passed.
- `python3 -m proberca.cli.check_project`: passed.
- `python3 -m proberca.cli.check_p0_freeze --freeze-dir docs/p0_freeze_snapshot`: passed.
- `python3 -m proberca.cli.check_p1_freeze --freeze-dir docs/p1_freeze_snapshot`: passed.
- `pytest -q`: passed.
- `python3 -m proberca.cli.run_p2_integrated_replay --output data/p2_online_boutique/b2_integrated_replay --debug-evaluate-incidents`: completed 20 repeats with 0 failures.
- `python3 -m proberca.cli.check_b2_integrated_replay --input data/p2_online_boutique/b2_integrated_replay`: passed.

## Review Verdict

- `B2_review_passed`: true
- `failed_checks`: []
- `remaining_risks`:
  - B2 integrated replay is weaker than A2 official blind rerun.
  - CPU integrated replay fails service, metric, and root type post-hoc evaluation in all five repeats.
  - B2 is replay over existing raw metrics, not B3 real reinjection.
  - A5 remains policy preview and does not activate real probes.
  - Path fidelity is post-hoc debug and should not be overclaimed.
  - This is still not production-ready.
