from __future__ import annotations

import ast
from pathlib import Path

from proberca.data.schema import TopologySnapshot
from proberca.k8s.topology_builder import LiveTopologyBuilder

from test_p11_mapping_topology import inventory_with_backends


def test_live_production_sources_have_no_labels_hardcoding_kubectl_or_legacy_solver():
    roots = [Path("proberca/k8s"), Path("proberca/metrics"), Path("proberca/live")]
    sources = "\n".join(
        path.read_text(encoding="utf-8") for root in roots for path in root.glob("*.py"))
    for forbidden in (
            "IncidentLabel", "paymentservice", "checkoutservice", "Online Boutique",
            "graph_sparse_admm", "evidence_channel", "subprocess", "kubectl ",
            "pytest.skip", "pytest.xfail", "TODO"):
        assert forbidden not in sources


def test_live_runner_does_not_import_or_call_p2_to_p9_internals():
    path = Path("proberca/live/runner.py")
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported = {node.module for node in ast.walk(tree)
                if isinstance(node, ast.ImportFrom) and node.module}
    forbidden = {"aggregation", "baseline", "alerting", "candidates", "propagation",
                 "inversion", "evidence", "diagnosis"}
    assert not any(any(part in module.split(".") for part in forbidden) for module in imported)
    calls = {node.func.attr for node in ast.walk(tree)
             if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)}
    assert "process_window" in calls
    assert not {"solve_weighted_joint_problem", "diagnose_weighted_solution"} & calls


def test_live_topology_is_p3_round_trip_compatible_and_order_deterministic():
    revision = inventory_with_backends(second_service=True).freeze(2)
    builder = LiveTopologyBuilder("cluster-a")
    first = builder.build(1, 2, revision, ())
    restored = TopologySnapshot.from_dict(first.to_dict())
    second = builder.build(1, 2, revision, ())
    assert restored == first == second
    assert first.snapshot_id == second.snapshot_id
    adjacent = builder.build(2, 3, revision, ())
    assert first.inventory_revision_id == revision.revision_id
    assert first.resource_version_vector
    assert first.structure_fingerprint == adjacent.structure_fingerprint
    assert first.snapshot_id != adjacent.snapshot_id


def test_p11_contains_no_ebpf_implementation_or_daemonset():
    paths = [path.as_posix() for path in Path("deploy/kubernetes").rglob("*") if path.is_file()]
    assert not any("daemonset" in path.lower() or "ebpf" in path.lower() for path in paths)
