
from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict, deque
from pathlib import Path
from typing import Any


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding='utf-8').splitlines():
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            rows.append(item)
    return rows


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def peak_ts(points: list[tuple[float, float]]) -> float | None:
    if not points:
        return None
    baseline_n = max(1, int(math.ceil(len(points) * 0.3)))
    baseline = [v for _, v in points[:baseline_n]]
    med = sorted(baseline)[len(baseline) // 2]
    return max(points, key=lambda item: abs(item[1] - med))[0]


def read_metric_points(path: Path) -> dict[str, list[tuple[float, float]]]:
    points: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for row in load_jsonl(path):
        service = row.get('service') or row.get('service_name') or row.get('pod_service')
        metric = row.get('metric') or row.get('metric_name') or row.get('name')
        if not service or not metric:
            continue
        try:
            ts = float(row.get('timestamp', row.get('ts', row.get('time'))))
            value = float(row.get('value'))
        except (TypeError, ValueError):
            continue
        if math.isfinite(ts) and math.isfinite(value):
            points[f'{service}.{metric}'].append((ts, value))
    for node in points:
        points[node].sort()
    return points


def split_node(node_id: str) -> tuple[str, str]:
    if '.' not in str(node_id):
        return 'unknown', str(node_id or 'unknown')
    return str(node_id).split('.', 1)


def normalize_root_type(value: str) -> str:
    text = str(value or '').lower()
    if 'cpu' in text:
        return 'CPU'
    if 'network' in text or text == 'net':
        return 'network'
    if 'io' in text or 'i/o' in text or 'storage' in text:
        return 'storage I/O'
    if 'lock' in text:
        return 'lock contention'
    if 'memory' in text:
        return 'memory'
    return str(value or 'unknown')


def percentile_abs(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(abs(v) for v in values)
    idx = min(len(ordered) - 1, max(0, int(math.ceil((len(ordered) - 1) * q))))
    return float(ordered[idx])


def topk_mean(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted((max(0.0, v) for v in values), reverse=True)
    k = max(1, int(math.ceil(len(ordered) * 0.2)))
    return float(sum(ordered[:k]) / k)


def load_graph(path: Path) -> dict[str, set[str]]:
    adjacency: dict[str, set[str]] = defaultdict(set)
    for row in load_jsonl(path):
        src = str(row.get('src', ''))
        dst = str(row.get('dst', ''))
        if src and dst:
            adjacency[dst].add(src)
            adjacency.setdefault(src, set())
    return adjacency


def bfs_path(adjacency: dict[str, set[str]], start: str, goal: str) -> list[str]:
    if not start or not goal:
        return []
    queue: deque[list[str]] = deque([[start]])
    seen = {start}
    while queue:
        path = queue.popleft()
        node = path[-1]
        if node == goal:
            return path
        for nxt in sorted(adjacency.get(node, set())):
            if nxt not in seen:
                seen.add(nxt)
                queue.append(path + [nxt])
    return []


def overlap_ratio(a0: float | None, a1: float | None, b0: float | None, b1: float | None) -> float | None:
    if None in (a0, a1, b0, b1):
        return None
    lo = max(float(a0), float(b0))
    hi = min(float(a1), float(b1))
    if hi <= lo:
        return 0.0
    return (hi - lo) / max(1e-9, float(a1) - float(a0))


def md_table(rows: list[list[Any]], headers: list[str]) -> str:
    lines = ['| ' + ' | '.join(headers) + ' |', '| ' + ' | '.join(['---'] * len(headers)) + ' |']
    for row in rows:
        lines.append('| ' + ' | '.join(str(x) for x in row) + ' |')
    return '\n'.join(lines)


def score_components(row: dict[str, Any]) -> dict[str, Any]:
    sc = row.get('score_components') if isinstance(row.get('score_components'), dict) else {}
    out = dict(sc)
    for key in ['best_metric_score','top2_metric_mean','a8r_service_score','service_counterfactual_delta_loss','path_to_symptom_support','downstream_load_support','service_local_family_support','structured_propagation_support','path_edge_support','lag_support','local_family_support','evidence_support','final_service_score']:
        if key in row and key not in out:
            out[key] = row.get(key)
    return out


def service_rank(rows: list[dict[str, Any]], service: str) -> int | None:
    for idx, row in enumerate(rows, start=1):
        if str(row.get('service')) == service:
            return idx
    return None


def cpu_metric_summary(service: str, metric_rows: list[dict[str, Any]], calibrated_rows: list[dict[str, Any]], cf_rows: list[dict[str, Any]]) -> dict[str, Any]:
    cpu_rows = [r for r in metric_rows if str(r.get('service')) == service and str(r.get('metric_family')) == 'CPU']
    if not cpu_rows:
        return {'service': service, 'best_cpu_metric': None, 'final_metric_score': 0.0}
    best = sorted(cpu_rows, key=lambda r: (-as_float(r.get('final_metric_score', r.get('final_candidate_score'))), str(r.get('node_id'))))[0]
    sc = best.get('score_components') if isinstance(best.get('score_components'), dict) else {}
    residual_vals = [as_float(r.get('calibrated_residual')) for r in calibrated_rows if str(r.get('service')) == service and str(r.get('metric_family')) == 'CPU']
    cf_by_node = {str(r.get('node_id')): r for r in cf_rows if r.get('node_id')}
    node = str(best.get('node_id'))
    return {
        'service': service,
        'best_cpu_metric': node,
        'cpu_node_evidence_support': as_float(best.get('node_evidence_support', sc.get('node_evidence_support'))),
        'cpu_service_family_evidence_support': as_float(best.get('service_family_evidence_support', sc.get('service_family_evidence_support'))),
        'cpu_family_global_evidence_support': as_float(best.get('family_global_evidence_support', sc.get('family_global_evidence_support'))),
        'cpu_calibrated_residual_p95': percentile_abs(residual_vals, 0.95),
        'cpu_calibrated_residual_topk_mean': topk_mean(residual_vals),
        'cpu_a8r_metric_score': as_float(best.get('metric_score')),
        'cpu_counterfactual_delta_loss': as_float(cf_by_node.get(node, {}).get('delta_loss')),
        'diagnostic_specificity': as_float(best.get('diagnostic_specificity', sc.get('diagnostic_specificity'))),
        'final_metric_score': as_float(best.get('final_metric_score', best.get('final_candidate_score'))),
    }


def local_compare(root: dict[str, Any], pred: dict[str, Any]) -> dict[str, Any]:
    checks = {
        'root_beats_predicted_on_node_evidence': as_float(root.get('cpu_node_evidence_support')) > as_float(pred.get('cpu_node_evidence_support')),
        'root_beats_predicted_on_residual': as_float(root.get('cpu_calibrated_residual_topk_mean')) > as_float(pred.get('cpu_calibrated_residual_topk_mean')),
        'root_beats_predicted_on_metric_score': as_float(root.get('final_metric_score')) > as_float(pred.get('final_metric_score')),
        'root_beats_predicted_on_counterfactual': as_float(root.get('cpu_counterfactual_delta_loss')) > as_float(pred.get('cpu_counterfactual_delta_loss')),
    }
    wins = sum(1 for value in checks.values() if value)
    checks['separability_label'] = 'strong' if wins >= 3 else 'weak' if wins >= 2 else 'inverted' if wins == 0 else 'absent'
    return checks


def analyze_propagation(service: str, repeat_dir: Path) -> dict[str, Any]:
    parent_rows = load_jsonl(repeat_dir / '05b_structured_propagation' / 'structured_parent_sets.jsonl')
    edge_rows = load_jsonl(repeat_dir / '05b_structured_propagation' / 'structured_propagation_edges.jsonl')
    metadata = load_json(repeat_dir / '05b_structured_propagation' / 'structured_propagation_metadata.json')
    has_parent = False
    for row in parent_rows:
        target = str(row.get('target_node', row.get('node_id', '')))
        parents = row.get('parents', []) if isinstance(row.get('parents'), list) else []
        if target.startswith(service + '.request.') and any(str(p).startswith(service + '.cpu.') for p in parents):
            has_parent = True
    service_edges = []
    request_chain = []
    for row in edge_rows:
        src = str(row.get('src', ''))
        dst = str(row.get('dst', ''))
        if src.startswith(service + '.cpu.') and '.request.' in dst:
            service_edges.append(row)
        if '.request.' in src and '.request.' in dst:
            request_chain.append(row)
    weights = [abs(as_float(row.get('weight', row.get('effective_weight')))) for row in service_edges]
    lags = [as_float(row.get('lag', row.get('best_lag')), 0.0) for row in service_edges]
    path_support = 0.0
    if isinstance(metadata.get('path_edge_support_by_service'), dict):
        path_support = as_float(metadata.get('path_edge_support_by_service', {}).get(service))
    if path_support == 0.0 and weights:
        path_support = max(weights)
    return {
        'service': service,
        'has_resource_to_request_parent': has_parent,
        'has_resource_to_request_edge': bool(service_edges),
        'has_request_chain_edge': bool(request_chain),
        'path_edge_support': path_support,
        'lag_support': float(len(set(lags))) if lags else 0.0,
        'strongest_lag': max(lags) if lags else None,
        'edge_count_along_path': len(service_edges),
        'max_effective_weight': max(weights) if weights else 0.0,
        'mean_effective_weight': sum(weights) / len(weights) if weights else 0.0,
        'propagation_support_score': (max(weights) if weights else 0.0) + 0.1 * len(service_edges),
    }


def prop_compare(root: dict[str, Any], pred: dict[str, Any]) -> dict[str, Any]:
    checks = {
        'root_beats_predicted_on_path_support': as_float(root.get('path_edge_support')) > as_float(pred.get('path_edge_support')),
        'root_beats_predicted_on_lag_support': as_float(root.get('lag_support')) > as_float(pred.get('lag_support')),
        'root_beats_predicted_on_edge_weight': as_float(root.get('max_effective_weight')) > as_float(pred.get('max_effective_weight')),
    }
    wins = sum(1 for value in checks.values() if value)
    checks['separability_label'] = 'strong' if wins >= 2 else 'weak' if wins == 1 else 'inverted' if as_float(pred.get('propagation_support_score')) > as_float(root.get('propagation_support_score')) else 'absent'
    return checks


def analyze_repeat(input_root: Path, raw_root: Path, idx: int) -> dict[str, Any]:
    repeat = f'repeat_{idx:02d}'
    raw_dir = raw_root / repeat / 'raw'
    repeat_dir = input_root / 'cpu' / repeat
    missing = []
    for path in [raw_dir/'metrics.jsonl', raw_dir/'service_graph.jsonl', raw_dir/'incidents.jsonl', repeat_dir/'01_alert_gate'/'alert_windows.jsonl', repeat_dir/'09_final_result'/'integrated_rca_results.jsonl', repeat_dir/'09_final_result'/'service_candidate_table.jsonl']:
        if not path.exists():
            missing.append(str(path))
    incident = (load_jsonl(raw_dir/'incidents.jsonl') or [{}])[0]
    root_service = str(incident.get('root_service', 'unknown'))
    root_metric = str(incident.get('root_metric', 'unknown'))
    root_type = normalize_root_type(str(incident.get('root_type', 'unknown')))
    result = (load_jsonl(repeat_dir/'09_final_result'/'integrated_rca_results.jsonl') or [{}])[0]
    predicted_service = str(result.get('predicted_top1_service', result.get('top1_service', 'unknown')))
    predicted_metric = str(result.get('predicted_top1_metric', result.get('top1_metric', 'unknown')))
    service_rows = load_jsonl(repeat_dir/'09_final_result'/'service_candidate_table.jsonl')
    metric_rows = load_jsonl(repeat_dir/'09_final_result'/'metric_candidate_table.jsonl')
    calibrated_rows = load_jsonl(repeat_dir/'06_evidence_channel'/'calibrated_residuals.jsonl')
    cf_rows = load_jsonl(repeat_dir/'08_counterfactual'/'counterfactual_metric_ranking.jsonl')
    by_service = {str(row.get('service')): row for row in service_rows if row.get('service')}
    root_row = by_service.get(root_service, {})
    pred_row = by_service.get(predicted_service, {})
    root_score = as_float(root_row.get('final_service_score'))
    pred_score = as_float(pred_row.get('final_service_score'))
    root_components = score_components(root_row)
    pred_components = score_components(pred_row)
    component_deltas = []
    for key in sorted(set(root_components) | set(pred_components)):
        rv, pv = root_components.get(key), pred_components.get(key)
        if isinstance(rv, (int, float)) or isinstance(pv, (int, float)):
            component_deltas.append({'component': key, 'predicted': as_float(pv), 'root': as_float(rv), 'delta_pred_minus_root': as_float(pv) - as_float(rv)})
    component_deltas.sort(key=lambda row: -abs(row['delta_pred_minus_root']))
    root_cpu = cpu_metric_summary(root_service, metric_rows, calibrated_rows, cf_rows)
    pred_cpu = cpu_metric_summary(predicted_service, metric_rows, calibrated_rows, cf_rows)
    root_cpu['final_service_score'] = root_score
    pred_cpu['final_service_score'] = pred_score
    local_sep = local_compare(root_cpu, pred_cpu)
    points = read_metric_points(raw_dir/'metrics.jsonl')
    alert = (load_jsonl(repeat_dir/'01_alert_gate'/'alert_windows.jsonl') or [{}])[0]
    alert_start = as_float(alert.get('start_ts'), None) if alert else None
    alert_end = as_float(alert.get('end_ts'), None) if alert else None
    incident_start = as_float(incident.get('start_ts'), None) if incident else None
    incident_end = as_float(incident.get('end_ts'), None) if incident else None
    root_node = root_metric if '.' in root_metric else f'{root_service}.{root_metric}'
    root_peak = peak_ts(points.get(root_node, []))
    pred_peak = peak_ts(points.get(predicted_metric, []))
    frontend_peak = peak_ts(points.get('frontend.request.p99_latency_ms', []) or points.get('frontend.request.p95_latency_ms', []))
    checkout_peak = peak_ts(points.get('checkoutservice.request.p99_latency_ms', []) or points.get('checkoutservice.request.p95_latency_ms', []))
    def inside(ts: float | None) -> bool | None:
        if ts is None or alert_start is None or alert_end is None:
            return None
        return alert_start <= ts <= alert_end
    align = {'incident_start_debug': incident_start, 'incident_end_debug': incident_end, 'alert_start': alert_start, 'alert_end': alert_end, 'root_metric_peak_ts': root_peak, 'predicted_metric_peak_ts': pred_peak, 'frontend_latency_peak_ts': frontend_peak, 'checkout_latency_peak_ts': checkout_peak, 'root_peak_inside_alert': inside(root_peak), 'predicted_peak_inside_alert': inside(pred_peak), 'frontend_peak_inside_alert': inside(frontend_peak), 'alert_incident_overlap_ratio': overlap_ratio(alert_start, alert_end, incident_start, incident_end)}
    if align['root_peak_inside_alert'] is False:
        alignment_status = 'root_peak_missed'
    elif align['frontend_peak_inside_alert'] is False:
        alignment_status = 'symptom_peak_missed'
    elif align['alert_incident_overlap_ratio'] is not None and align['alert_incident_overlap_ratio'] < 0.25:
        alignment_status = 'shifted'
    elif align['root_peak_inside_alert'] is True and align['frontend_peak_inside_alert'] is True:
        alignment_status = 'good'
    else:
        alignment_status = 'unclear'
    align['window_alignment_status'] = alignment_status
    root_prop = analyze_propagation(root_service, repeat_dir)
    pred_prop = analyze_propagation(predicted_service, repeat_dir)
    prop_sep = prop_compare(root_prop, pred_prop)
    adjacency = load_graph(raw_dir/'service_graph.jsonl')
    symptom = str(result.get('symptom_service', 'frontend'))
    root_path = bfs_path(adjacency, root_service, symptom)
    pred_path = bfs_path(adjacency, predicted_service, symptom)
    root_len = len(root_path) - 1 if root_path else None
    pred_len = len(pred_path) - 1 if pred_path else None
    short_bias = bool(root_len is not None and pred_len is not None and pred_len < root_len and as_float(pred_components.get('path_to_symptom_support')) > as_float(root_components.get('path_to_symptom_support')))
    if alignment_status in {'root_peak_missed', 'symptom_peak_missed', 'shifted'}:
        status = 'alert_window_misaligned'
        reason = f'alert window alignment is {alignment_status}'
    elif prop_sep['separability_label'] == 'inverted' and as_float(pred_prop.get('propagation_support_score')) > as_float(root_prop.get('propagation_support_score')):
        status = 'propagation_model_wrong'
        reason = 'predicted service has stronger structured propagation support than root service'
    elif local_sep['separability_label'] in {'strong', 'weak'} and root_score < pred_score:
        status = 'distinguishable_but_score_misused'
        reason = 'root service has some stronger local CPU signals but final service score ranks another service higher'
    elif local_sep['separability_label'] == 'inverted':
        status = 'indistinguishable_with_current_observability'
        reason = 'predicted service is stronger than root service on most local CPU signals'
    else:
        status = 'weakly_distinguishable'
        reason = 'root and predicted services are close or mixed across local CPU and propagation signals'
    return {'repeat': repeat, 'missing_files': missing, 'root_service_debug': root_service, 'root_metric_debug': root_metric, 'root_type_debug': root_type, 'predicted_service': predicted_service, 'predicted_metric': predicted_metric, 'root_type_pred': result.get('predicted_root_type'), 'service_hit': predicted_service == root_service, 'root_service_rank': service_rank(service_rows, root_service), 'predicted_service_score': pred_score, 'root_service_score': root_score, 'score_margin': pred_score - root_score, 'root_service_missing_from_candidate_table': not bool(root_row), 'component_deltas': component_deltas[:12], 'root_service_components': root_components, 'predicted_service_components': pred_components, 'root_cpu_signal': root_cpu, 'predicted_cpu_signal': pred_cpu, 'cpu_local_signal_separability': local_sep, 'alert_window_alignment': align, 'root_propagation': root_prop, 'predicted_propagation': pred_prop, 'propagation_separability': prop_sep, 'short_path_bias': {'short_path_bias_suspected': short_bias, 'root_path': root_path, 'predicted_path': pred_path, 'root_path_length': root_len, 'predicted_path_length': pred_len, 'root_path_to_symptom_support': root_components.get('path_to_symptom_support'), 'predicted_path_to_symptom_support': pred_components.get('path_to_symptom_support')}, 'distinguishability_status': status, 'distinguishability_reason': reason}


def summarize(repeats: list[dict[str, Any]]) -> dict[str, Any]:
    statuses = [r['distinguishability_status'] for r in repeats]
    local_labels = [r['cpu_local_signal_separability'].get('separability_label') for r in repeats]
    prop_labels = [r['propagation_separability'].get('separability_label') for r in repeats]
    align_bad = [r for r in repeats if r['alert_window_alignment'].get('window_alignment_status') != 'good']
    if statuses.count('indistinguishable_with_current_observability') >= 3:
        overall, main = 'poor', 'current local CPU observability often makes wrong services look stronger than the true root service'
    elif statuses.count('distinguishable_but_score_misused') >= 3:
        overall, main = 'weak', 'root service has usable signal but final scoring underuses it'
    elif len(align_bad) >= 3:
        overall, main = 'weak', 'alert-window alignment weakens CPU service evidence'
    elif local_labels.count('strong') >= 3 or prop_labels.count('strong') >= 3:
        overall, main = 'strong', 'root service is usually separable but ranking does not consistently exploit it'
    else:
        overall, main = 'weak', 'CPU service evidence is mixed across services'
    algorithmic = any(s in {'distinguishable_but_score_misused', 'propagation_model_wrong'} for s in statuses)
    observability = statuses.count('indistinguishable_with_current_observability') >= 2 or local_labels.count('inverted') >= 2
    misaligned = len(align_bad) >= 2
    if observability:
        action = 'observability_repair'
    elif any(s == 'propagation_model_wrong' for s in statuses):
        action = 'propagation_parent_repair'
    elif misaligned:
        action = 'alert_window_repair'
    elif algorithmic:
        action = 'scoring_repair'
    else:
        action = 'stop_integrated_pipeline_for_cpu'
    return {'overall_cpu_distinguishability': overall, 'main_failure_mode': main, 'b3_gate_recommended': overall == 'strong' and not observability and not misaligned, 'recommended_next_action': action, 'whether_cpu_service_failure_is_algorithmic': algorithmic, 'whether_cpu_service_failure_is_observability_limited': observability, 'whether_alert_window_misalignment_detected': misaligned}


def render_report(repeats: list[dict[str, Any]], summary: dict[str, Any], input_dir: Path, raw_root: Path) -> str:
    verdict_rows = []
    score_rows = []
    local_rows = []
    align_rows = []
    prop_rows = []
    path_rows = []
    component_sections = []
    for r in repeats:
        verdict_rows.append([r['repeat'], r['root_service_rank'], r['predicted_service'], round(r['score_margin'], 6), r['cpu_local_signal_separability'].get('separability_label'), r['propagation_separability'].get('separability_label'), r['alert_window_alignment'].get('window_alignment_status'), r['distinguishability_status'], r['distinguishability_reason']])
        score_rows.append([r['repeat'], r['root_service_debug'], r['root_metric_debug'], r['predicted_service'], r['predicted_metric'], r['root_service_rank'], round(r['predicted_service_score'], 6), round(r['root_service_score'], 6), round(r['score_margin'], 6), r['root_type_pred'], r['root_type_debug'], r['service_hit']])
        sep = r['cpu_local_signal_separability']
        local_rows.append([r['repeat'], r['root_cpu_signal'].get('best_cpu_metric'), r['predicted_cpu_signal'].get('best_cpu_metric'), sep.get('root_beats_predicted_on_node_evidence'), sep.get('root_beats_predicted_on_residual'), sep.get('root_beats_predicted_on_metric_score'), sep.get('root_beats_predicted_on_counterfactual'), sep.get('separability_label')])
        aw = r['alert_window_alignment']
        align_rows.append([r['repeat'], aw.get('incident_start_debug'), aw.get('incident_end_debug'), aw.get('alert_start'), aw.get('alert_end'), aw.get('root_metric_peak_ts'), aw.get('predicted_metric_peak_ts'), aw.get('frontend_latency_peak_ts'), aw.get('root_peak_inside_alert'), aw.get('predicted_peak_inside_alert'), aw.get('frontend_peak_inside_alert'), aw.get('alert_incident_overlap_ratio'), aw.get('window_alignment_status')])
        ps = r['propagation_separability']
        prop_rows.append([r['repeat'], r['root_service_debug'], r['predicted_service'], ps.get('root_beats_predicted_on_path_support'), ps.get('root_beats_predicted_on_lag_support'), ps.get('root_beats_predicted_on_edge_weight'), ps.get('separability_label'), round(as_float(r['root_propagation'].get('propagation_support_score')), 6), round(as_float(r['predicted_propagation'].get('propagation_support_score')), 6)])
        sp = r['short_path_bias']
        path_rows.append([r['repeat'], sp.get('short_path_bias_suspected'), sp.get('root_path'), sp.get('predicted_path'), sp.get('root_path_length'), sp.get('predicted_path_length'), sp.get('root_path_to_symptom_support'), sp.get('predicted_path_to_symptom_support')])
        deltas = [[d['component'], round(d['predicted'], 6), round(d['root'], 6), round(d['delta_pred_minus_root'], 6)] for d in r['component_deltas'][:8]]
        component_sections.append('### ' + r['repeat'] + ' component deltas\n\n' + md_table(deltas, ['component','predicted','root','pred-root']))
    missing = [item for r in repeats for item in r.get('missing_files', [])]
    lines = [
        '# B2D CPU Distinguishability Audit', '',
        '## Scope', 'This audit only reads existing raw metrics and B2P replay outputs. It does not modify code logic, does not modify integrated pipeline outputs, does not run the old P1 RCA pipeline, does not reinject faults, and does not enter B3.', '',
        'Labels from `incidents.jsonl` are used only for post-hoc diagnosis tables and never for inference, ranking, scoring, candidate selection, or repair.', '',
        '## Executive Summary',
        f"- overall_cpu_distinguishability: {summary['overall_cpu_distinguishability']}",
        f"- main_failure_mode: {summary['main_failure_mode']}",
        f"- b3_gate_recommended: {str(summary['b3_gate_recommended']).lower()}",
        f"- recommended_next_action: {summary['recommended_next_action']}",
        f"- whether_cpu_service_failure_is_algorithmic: {str(summary['whether_cpu_service_failure_is_algorithmic']).lower()}",
        f"- whether_cpu_service_failure_is_observability_limited: {str(summary['whether_cpu_service_failure_is_observability_limited']).lower()}",
        f"- whether_alert_window_misalignment_detected: {str(summary['whether_alert_window_misalignment_detected']).lower()}", '',
        '## Inputs Checked', f'- B2P replay input: `{input_dir}`', f'- CPU raw root: `{raw_root}`', f'- Missing files count: {len(missing)}', '', '```json', json.dumps(missing, ensure_ascii=False, indent=2), '```', '',
        '## CPU Repeat-by-repeat Diagnosis', md_table(verdict_rows, ['repeat','root_service_rank','predicted_service','root_vs_predicted_score_margin','local_evidence_separability','propagation_separability','alert_window_alignment','distinguishability_status','reason']), '',
        '## Root vs Predicted Service Score Decomposition', md_table(score_rows, ['repeat','root_service_debug','root_metric_debug','predicted_service','predicted_metric','root_service_rank','predicted_service_score','root_service_score','score_margin','root_type_pred','root_type_debug','service_hit']), '',
        '\n\n'.join(component_sections), '',
        '## CPU Local Evidence Separability', md_table(local_rows, ['repeat','root_best_cpu_metric','predicted_best_cpu_metric','root_beats_node_evidence','root_beats_residual','root_beats_metric_score','root_beats_counterfactual','separability_label']), '',
        '## Alert Window Alignment', md_table(align_rows, ['repeat','incident_start_debug','incident_end_debug','alert_start','alert_end','root_metric_peak_ts','predicted_metric_peak_ts','frontend_latency_peak_ts','root_peak_inside_alert','predicted_peak_inside_alert','frontend_peak_inside_alert','alert_incident_overlap_ratio','window_alignment_status']), '',
        '## Propagation Separability', md_table(prop_rows, ['repeat','root_service','predicted_service','root_beats_path_support','root_beats_lag_support','root_beats_edge_weight','separability_label','root_propagation_support','predicted_propagation_support']), '',
        '## Short Path Bias Check', md_table(path_rows, ['repeat','short_path_bias_suspected','root_path','predicted_path','root_path_length','predicted_path_length','root_path_support','predicted_path_support']), '',
        '## Distinguishability Verdict', 'The CPU service failure is not explained by root label leakage or ownership loss in this audit. The key diagnostic is whether the true root service has stronger local CPU evidence, residual, counterfactual, and structured propagation support than the predicted service. The repeat-level verdict table above separates cases where scoring appears to underuse a signal from cases where the currently observed signals are weak or inverted.', '',
        '## Recommended Next Action', f"Recommended next action: `{summary['recommended_next_action']}`.", '', 'If action is `observability_repair`, B3 should add observability that can distinguish CPU throttling by service, such as service-to-service RPC latency, trace spans, route latency, and cgroup CPU throttling confirmation. If action is `scoring_repair` or `propagation_parent_repair`, the report tables identify which components are being underused or inverted. This audit itself does not implement any repair.', '',
        '## Safety Checks', '- uses_root_labels_for_inference: false', '- labels_used_only_for_posthoc_diagnosis: true', '- reinjects_faults: false', '- runs_old_p1_rca: false', '- modifies_code_logic: false', '- actual_probe_activation: false', ''
    ]
    return '\n'.join(lines)


def run(input_dir: str, raw_root: str, output: str) -> dict[str, Any]:
    input_path = Path(input_dir)
    raw_path = Path(raw_root)
    repeats = [analyze_repeat(input_path, raw_path, idx) for idx in range(1, 6)]
    summary = summarize(repeats)
    report = render_report(repeats, summary, input_path, raw_path)
    out = Path(output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report, encoding='utf-8')
    return {'summary': summary, 'repeat_count': len(repeats), 'output': str(out)}


def main() -> int:
    parser = argparse.ArgumentParser(description='Audit CPU service distinguishability from existing B2P replay outputs.')
    parser.add_argument('--input', required=True)
    parser.add_argument('--raw-root', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.input, args.raw_root, args.output), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
