from __future__ import annotations

import hashlib
import ast
import json
import subprocess
import sys
from dataclasses import asdict, replace
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml

import test_p1_data_contracts as p1
from proberca.config import ProbeRCAConfig, dump_config_yaml
from proberca.data.io import write_records_jsonl, write_records_parquet
from proberca.replay import ReplayEvaluator, ReplayRunner
from test_p10_e2e_cases import (
    NS, case_engine, edge_engine, edge_window, propagation_engine,
    propagation_window, raw_window,
)


CASES = [
    ("cpu", "node", "cpu", "runtime.cpu_pressure", "ratio"),
    ("memory", "node", "memory", "runtime.memory_pressure", "ratio"),
    ("io", "node", "io", "runtime.block_delay", "ms"),
    ("lock", "node", "lock", "runtime.lock_wait", "ms"),
    ("tcp", "edge", "tcp.retrans_rate", "tcp", None),
    ("dns", "edge", "dns.timeout_rate", "dns", None),
    ("same-node", "propagation", "host", None, None),
    ("downstream", "propagation", "impact", None, None),
]


def sha(path: Path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def config_with_runtime(target):
    payload = target.config.to_dict()
    payload.update({
        "aggregation_specs": {
            output_id: spec.to_dict() for output_id, spec in target.aggregation_plan.entries},
        "metric_signal_specs": [item.to_dict() for item in target.signal_specs],
        "baseline": asdict(target.baseline_config),
        "score": asdict(target.score_aggregator.config),
        "alert_state": asdict(target.alert_machine.config),
    })
    return ProbeRCAConfig.from_dict(payload)


def scenario(case):
    name, kind, arg1, arg2, arg3 = case
    if kind == "node":
        target = case_engine(arg1, arg2, arg3)
        values = [1, 1, 1, 1, 1.01, .99, 1, 1.2, 1.4]
        windows = [raw_window(i, arg1, arg2, arg3, value)
                   for i, value in enumerate(values, 1)]
        truth = {"kind": "node", "fault_mode": "self", "service": "service-a",
                 "metric": arg2, "edge_subtype": None, "edge": None}
    elif kind == "edge":
        target = edge_engine(arg1, arg2)
        node_values = [1, 1, 1, 1, 1.01, .99, 1, 1.2, 1.4]
        edge_values = [.1, .1, .1, .1, .1, .1, .1, .2, 1.0]
        windows = [edge_window(i, arg1, arg2, node, edge)
                   for i, (node, edge) in enumerate(zip(node_values, edge_values), 1)]
        truth = {"kind": "edge", "fault_mode": "edge", "service": None,
                 "metric": None, "edge_subtype": "exogenous-edge-shock",
                 "edge": f"cluster-a::observability::service-a->service-b::{arg2}::{arg1}"}
    else:
        target = propagation_engine(arg1)
        a_values = [1, 1, 1, 1, 1.04, .96, 1.04, .96, 1.4]
        b_values = ([1, 1, 1, 1.04, .96, 1.04, .96, 1.2, 1.0]
                    if arg1 == "host" else
                    [1, 1, 1, 1.04, .96, 1.04, .96, 1.2, 1.4])
        windows = [propagation_window(i, arg1, a, b)
                   for i, (a, b) in enumerate(zip(a_values, b_values), 1)]
        truth = ({"kind": "edge", "fault_mode": "edge", "service": None,
                  "metric": None, "edge_subtype": "propagated-edge",
                  "edge": "configured-host-propagation"}
                 if arg1 == "host" else
                 {"kind": "node", "fault_mode": "self", "service": "service-b",
                  "metric": "runtime.pressure", "edge_subtype": None, "edge": None})
    return target, windows, truth


def write_empty_like(path: Path, sample):
    scratch = path.with_suffix(".sample.parquet")
    write_records_parquet(scratch, [sample])
    table = pq.read_table(scratch).slice(0, 0)
    pq.write_table(table, path)
    scratch.unlink()


def build_disk_dataset(root: Path, case):
    target, windows, truth = scenario(case)
    root.mkdir()
    nodes = [record for window in windows for record in window.node_metric_records]
    edges = [record for window in windows for record in window.edge_metric_records]
    topologies = [record for window in windows for record in window.topology_snapshot_events]
    write_records_parquet(root / "node_metrics_1s.parquet", nodes)
    if edges:
        write_records_parquet(root / "edge_metrics_1s.parquet", edges)
    else:
        write_empty_like(root / "edge_metrics_1s.parquet",
                         p1.make_edge(timestamp_ns=1, window_sec=1))
    write_records_jsonl(root / "topology_snapshots.jsonl", topologies)
    dump_config_yaml(root / "config.yaml", config_with_runtime(target))
    label = replace(
        p1.make_label(), incident_id=f"label-{case[0]}", start_ns=8 * NS,
        end_ns=10 * NS, fault_mode=truth["fault_mode"],
        edge_subtype=truth["edge_subtype"], root_service=truth["service"],
        root_metric=truth["metric"], root_edge=truth["edge"])
    write_records_jsonl(root / "incident_labels.jsonl", [label])
    files = ["node_metrics_1s.parquet", "edge_metrics_1s.parquet",
             "topology_snapshots.jsonl", "config.yaml", "incident_labels.jsonl"]
    manifest = {
        "schema_version": "1.0", "dataset_id": f"dataset-{case[0]}",
        "dataset_version": "1", "cluster_id": "cluster-a",
        "namespaces": ["observability"], "start_ns": 0, "end_ns": 10 * NS,
        "window_sec": 1, "node_metrics_file": files[0],
        "edge_metrics_file": files[1], "topology_file": files[2],
        "evidence_file": None, "labels_file": files[4], "config_file": files[3],
        "file_sha256": {name: sha(root / name) for name in files},
        "expected_schema_versions": ["1.0"],
        "allowed_record_types": ["node_metric", "edge_metric", "topology_snapshot"],
        "evidence_semantics": "normalized_only", "source_description": "P10.1 disk E2E",
        "created_at_ns": 1, "metadata": {"case": case[0]},
    }
    (root / "manifest.yaml").write_text(
        yaml.safe_dump(manifest, sort_keys=True), encoding="utf-8")
    return target, windows, truth, label


@pytest.mark.parametrize("case", CASES, ids=[item[0] for item in CASES])
def test_all_cases_use_manifest_parquet_jsonl_runner_and_evaluator(tmp_path, case):
    root = tmp_path / f"dataset-{case[0]}"
    manual_engine, windows, truth, label = build_disk_dataset(root, case)
    manual_results = [manual_engine.process_window(window) for window in windows]
    output = tmp_path / f"output-{case[0]}"
    run_manifest, replay_results = ReplayRunner(root, output).run()
    manual_reports = [report for result in manual_results for report in result.reports]
    replay_reports = [report for result in replay_results for report in result.reports]
    assert [item.alert_id for result in manual_results for item in result.alerts] == \
        [item.alert_id for result in replay_results for item in result.alerts]
    assert [item.report_fingerprint for item in manual_reports] == \
        [item.report_fingerprint for item in replay_reports]
    assert replay_reports[0].primary_root.kind == truth["kind"]
    assert replay_reports[0].primary_root.fault_mode == truth["fault_mode"]
    if truth["edge_subtype"]:
        assert replay_reports[0].primary_root.edge_subtype == truth["edge_subtype"]
    if case[0] == "same-node":
        assert "host" in replay_reports[0].primary_root.relation_types
    if case[0] == "downstream":
        assert replay_reports[0].symptoms
        assert replay_reports[0].primary_root.node_id not in {
            item["node_id"] for item in replay_reports[0].symptoms}
    evaluator = ReplayEvaluator()
    labels = evaluator.load_labels(
        root / "incident_labels.jsonl",
        sha(root / "incident_labels.jsonl"))
    evaluation = evaluator.evaluate(replay_reports, [], labels)
    assert evaluation["evaluated_count"] == 1
    assert run_manifest.report_ids == [replay_reports[0].report_fingerprint]


@pytest.mark.parametrize("case", CASES, ids=[item[0] for item in CASES])
def test_all_cases_execute_real_cli_normal_and_evaluation_modes(tmp_path, case):
    root = tmp_path / f"dataset-{case[0]}"
    build_disk_dataset(root, case)
    normal = subprocess.run([
        sys.executable, "-m", "proberca.cli.replay", "--dataset", str(root),
        "--output", str(tmp_path / f"normal-{case[0]}")], check=False)
    evaluated = subprocess.run([
        sys.executable, "-m", "proberca.cli.replay", "--dataset", str(root),
        "--output", str(tmp_path / f"evaluated-{case[0]}"), "--evaluate-labels"],
        check=False)
    assert normal.returncode == evaluated.returncode == 0
    assert not (tmp_path / f"normal-{case[0]}" / "evaluation.json").exists()
    assert (tmp_path / f"evaluated-{case[0]}" / "evaluation.json").is_file()


def run_cli(root, output, *extra):
    return subprocess.run([
        sys.executable, "-m", "proberca.cli.replay", "--dataset", str(root),
        "--output", str(output), *extra], check=False)


def rewrite_manifest_hash(root: Path, filename: str):
    manifest_path = root / "manifest.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    manifest["file_sha256"][filename] = sha(root / filename)
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=True), encoding="utf-8")


def test_cli_exit_code_2_for_input_failure(tmp_path):
    result = run_cli(tmp_path / "missing-dataset", tmp_path / "output")
    assert result.returncode == 2


def test_cli_exit_code_3_for_real_incident_failure(tmp_path):
    root = tmp_path / "dataset"
    build_disk_dataset(root, CASES[0])
    config_path = root / "config.yaml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["solver"]["max_iterations"] = 1
    config["solver"]["minimum_iterations"] = 1
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    rewrite_manifest_hash(root, "config.yaml")
    result = run_cli(root, tmp_path / "output")
    assert result.returncode == 3
    failures = (tmp_path / "output" / "failures.jsonl").read_text(encoding="utf-8")
    assert '"stage":"p8"' in failures


def test_cli_exit_code_4_for_checkpoint_failure(tmp_path):
    root = tmp_path / "dataset"
    build_disk_dataset(root, CASES[0])
    result = run_cli(
        root, tmp_path / "output", "--resume-from", str(tmp_path / "bad-checkpoint"))
    assert result.returncode == 4


def test_cli_exit_code_5_for_evaluation_failure(tmp_path):
    root = tmp_path / "dataset"
    build_disk_dataset(root, CASES[0])
    manifest_path = root / "manifest.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    manifest["labels_file"] = None
    manifest["file_sha256"].pop("incident_labels.jsonl")
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=True), encoding="utf-8")
    result = run_cli(root, tmp_path / "output", "--evaluate-labels")
    assert result.returncode == 5


def test_runner_rejects_manifest_config_window_mismatch(tmp_path):
    root = tmp_path / "dataset"
    build_disk_dataset(root, CASES[0])
    manifest_path = root / "manifest.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    manifest["window_sec"] = 2
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=True), encoding="utf-8")
    with pytest.raises(ValueError, match="window_sec"):
        ReplayRunner(root, tmp_path / "output")


def test_changing_independent_label_does_not_change_report_fingerprint(tmp_path):
    root = tmp_path / "dataset"
    build_disk_dataset(root, CASES[0])
    first, _ = ReplayRunner(root, tmp_path / "first").run()
    label_path = root / "incident_labels.jsonl"
    label_payload = json.loads(label_path.read_text(encoding="utf-8"))
    label_payload["root_service"] = "different-evaluation-only-service"
    label_path.write_text(
        json.dumps(label_payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8")
    rewrite_manifest_hash(root, "incident_labels.jsonl")
    second, _ = ReplayRunner(root, tmp_path / "second").run()
    assert second.report_ids == first.report_ids


def test_p10_production_has_no_hardcoding_labels_or_stage_bypass():
    production_files = sorted(Path("proberca/orchestration").glob("*.py")) + \
        sorted(Path("proberca/replay").glob("*.py"))
    inference_files = [path for path in production_files if path.name != "evaluator.py"]
    combined = "\n".join(path.read_text(encoding="utf-8") for path in production_files)
    inference = "\n".join(path.read_text(encoding="utf-8") for path in inference_files)
    for forbidden in (
            "paymentservice", "checkoutservice", "Online Boutique",
            "graph_sparse_admm", "TODO", "pytest.skip", "pytest.xfail"):
        assert forbidden not in combined
    assert "IncidentLabel" not in inference
    runner_tree = ast.parse(Path("proberca/replay/runner.py").read_text(encoding="utf-8"))
    imported = {alias.name for node in ast.walk(runner_tree)
                if isinstance(node, ast.ImportFrom) for alias in node.names}
    assert not {"build_joint_inversion_system", "build_weighted_joint_problem",
                "solve_weighted_joint_problem", "diagnose_weighted_solution"} & imported
