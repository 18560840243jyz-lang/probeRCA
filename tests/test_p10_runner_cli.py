from __future__ import annotations
import ast, hashlib, json, subprocess, sys
from dataclasses import asdict
import yaml
import test_p1_data_contracts as p1
import test_p2_aggregation as p2agg
from test_p10_engine import NODE_ID, aggregation_plan, engine, raw_node, signal_specs, topology
from proberca.aggregation import AggregationPlan
from proberca.config import MetricSignalSpec, ProbeRCAConfig, dump_config_yaml
from proberca.data.io import write_records_jsonl, write_records_parquet
from proberca.replay import ReplayEvaluator, ReplayRecordReader, ReplayRunner
from proberca.replay.manifest import ReplayDatasetManifest

NS = 1_000_000_000
EDGE_ID = "cluster-a::observability::service-a->service-b::tcp::tcp.rtt_p95_ms"
EDGE_SERIES_ID = EDGE_ID + "::scope=service_pair::service_pair"

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def replay_engine():
    target = engine()
    edge_spec = p2agg.spec(
        method="last_same_series", kind="gauge", source="service_pair",
        target="service_pair", input_metric_ids=[EDGE_ID],
        input_series_ids=[EDGE_SERIES_ID],
        output_metric_name="tcp.rtt_p95_ms", output_metric_kind="gauge",
        output_unit="ms")
    edge_signal = MetricSignalSpec.from_dict({
        "record_type": "edge_metric", "metric_family": None,
        "metric_name": "tcp.rtt_p95_ms", "protocol": "tcp",
        "transform": "identity", "polarity": "increase_bad",
        "rare_event_threshold": None, "direct_hard": False, "z_cap": 6.0,
        "aggregation_output_id": EDGE_ID})
    return type(target)(
        target.config, aggregation_plan=AggregationPlan([
            *aggregation_plan().entries, (EDGE_ID, edge_spec)]),
        signal_specs=[*signal_specs(), edge_signal], baseline_config=target.baseline_config,
        score_config=target.score_aggregator.config,
        alert_state_config=target.alert_machine.config)

def dataset(root):
    root.mkdir()
    values = [1, 1, 1, 1, 1.01, .99, 1, 1.2, 1.4]
    nodes = [raw_node(i * NS - 1, value) for i, value in enumerate(values, 1)]
    edges = [p1.make_edge(timestamp_ns=i * NS - 1, window_sec=1, value=42.0)
             for i in range(1, 10)]
    write_records_parquet(root / "node.parquet", nodes)
    write_records_parquet(root / "edge.parquet", edges)
    write_records_jsonl(root / "topology.jsonl", [topology()])
    target = replay_engine()
    payload = target.config.to_dict()
    payload.update({
        "aggregation_specs": {key: item.to_dict()
                              for key, item in target.aggregation_plan.entries},
        "metric_signal_specs": [item.to_dict() for item in target.signal_specs],
        "baseline": asdict(target.baseline_config),
        "score": asdict(target.score_aggregator.config),
        "alert_state": asdict(target.alert_machine.config)})
    dump_config_yaml(root / "config.yaml", ProbeRCAConfig.from_dict(payload))
    write_records_jsonl(root / "labels.jsonl", [p1.make_label()])
    files = ["node.parquet", "edge.parquet", "topology.jsonl", "config.yaml", "labels.jsonl"]
    manifest = {
        "schema_version": "1.0", "dataset_id": "replay-a", "dataset_version": "1",
        "cluster_id": "cluster-a", "namespaces": ["observability"],
        "start_ns": 0, "end_ns": 10 * NS, "window_sec": 1,
        "node_metrics_file": files[0], "edge_metrics_file": files[1],
        "topology_file": files[2], "evidence_file": None, "labels_file": files[4],
        "config_file": files[3], "file_sha256": {name: sha(root / name) for name in files},
        "expected_schema_versions": ["1.0"],
        "allowed_record_types": ["node_metric", "edge_metric", "topology_snapshot"],
        "evidence_semantics": "normalized_only", "source_description": "raw deterministic",
        "created_at_ns": 1, "metadata": {}}
    (root / "manifest.yaml").write_text(yaml.safe_dump(manifest, sort_keys=True))
    return root

def test_runner_and_manual_window_execution_are_identical(tmp_path):
    root = dataset(tmp_path / "dataset")
    manual_engine = replay_engine()
    manual = [manual_engine.process_window(item) for item in
              ReplayRecordReader(root, ReplayDatasetManifest.load(root)).iter_windows()]
    run_manifest, replay = ReplayRunner(
        root, tmp_path / "output", engine=replay_engine()).run()
    assert [item.result_fingerprint for item in manual] == [item.result_fingerprint for item in replay]
    assert [r.report_fingerprint for x in manual for r in x.reports] == run_manifest.report_ids
    assert sorted(path.name for path in (tmp_path / "output").iterdir()) == [
        "alerts.jsonl", "failures.jsonl", "reports", "run_manifest.json"]

def test_repeated_replay_has_same_fingerprint_and_cli_runs(tmp_path):
    root = dataset(tmp_path / "dataset")
    one, _ = ReplayRunner(root, tmp_path / "one").run()
    two, _ = ReplayRunner(root, tmp_path / "two").run()
    assert one.run_fingerprint == two.run_fingerprint
    completed = subprocess.run([
        sys.executable, "-m", "proberca.cli.replay", "--dataset", str(root),
        "--output", str(tmp_path / "cli")], check=False)
    assert completed.returncode == 0
    evaluated = subprocess.run([
        sys.executable, "-m", "proberca.cli.replay", "--dataset", str(root),
        "--output", str(tmp_path / "evaluated"), "--evaluate-labels"], check=False)
    assert evaluated.returncode == 0
    assert (tmp_path / "evaluated" / "evaluation.json").is_file()

def test_runner_does_not_import_labels_or_late_stage_builders():
    tree = ast.parse(open("proberca/replay/runner.py", encoding="utf-8").read())
    imported = {alias.name for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
                for alias in node.names}
    assert "IncidentLabel" not in imported
    assert not {"build_joint_inversion_system", "build_weighted_joint_problem",
                "solve_weighted_joint_problem", "diagnose_weighted_solution"} & imported


def test_evaluator_keeps_failures_ambiguous_and_denominators_separate():
    report = p1.make_report()
    label = p1.make_label()
    result = ReplayEvaluator().evaluate([report], [object()], [label])
    assert result["incident_count"] == 1
    assert result["failed_pipeline_count"] == 1
    assert result["ambiguous_count"] == (report.primary_root.kind == "ambiguous")
    assert result["denominators"]["service"] == 1
