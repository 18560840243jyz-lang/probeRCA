# P2 Alert Gate

## Goal

A3 implements metrics-driven alert detection so P2 no longer needs `incident.start_ts` and `incident.end_ts` as detection inputs.

## Scope

A3 only outputs `alert_events.jsonl` and `alert_windows.jsonl`.

It does not run RCA, does not reinject faults, does not change P1 scoring, and does not fix the A2 I/O blind performance drop.

## Baseline Strategy

A3 currently uses prefix baseline.

中文解释：prefix baseline 是用每条时间序列前 30% 的点作为无标签基线。

## Alert Rules

Soft alert:

- `request.p95_latency_ms` or `request.p99_latency_ms` robust z-score >= 3.0.

Hard alert:

- request latency robust z-score >= 6.0, or
- at least two consecutive request latency windows have z-score >= 3.0, or
- request latency soft alert is near a resource auxiliary metric with z-score >= 3.0.

Resource auxiliary metrics include CPU throttling, network retransmission, I/O write counters, lock wait counters, and memory usage. They help severity but do not define root service.

## Debug Evaluation

`incidents.jsonl` can only be used after detection for overlap debug evaluation. It must not influence alert events or alert windows.

## Limitations

- Prefix baseline assumes the fault is not already active in the first 30% of each metric series.
- A3 has not yet connected alert windows to RCA rerun inputs.
- A3 does not solve the A2 I/O blind accuracy drop.
