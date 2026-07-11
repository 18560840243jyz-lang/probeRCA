from __future__ import annotations

import ast
import json
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest

import test_p1_data_contracts as p1
import test_p3_candidates as cand
import test_p3_contracts as contracts
import test_p3_topology as topo
from proberca.candidates import (
    AmbiguousMetricSelectionError,
    CandidateOverflowError,
    CandidateSerializationError,
    CandidateSubgraphBuilder,
    CandidateValidationError,
)
from proberca.config import CandidateGraphConfig, ImpactDerivationRule
from proberca.data.schema import CandidateProvenance, CandidateSubgraph, ServiceNodePlacement, TopologyEdge
from proberca.topology import TopologyStore, TopologyValidationError, build_topology_graph


def candidate_payload(**changes):
    payload = {
        "upstream_hops": 2, "downstream_hops": 1, "include_cohost": True,
        "include_shared_resource": True, "allow_cross_namespace": False,
        "allowed_namespaces": ["ns"], "max_candidate_services": 10,
        "max_candidate_node_metrics": 20, "max_candidate_physical_edges": 10,
        "fail_on_candidate_overflow": True, "include_trigger_edge_endpoints": True,
        "include_all_provenance_paths": True, "max_provenance_paths_per_object": 5,
    }
    payload.update(changes)
    return payload


@pytest.mark.parametrize("changes", [
    {"upstream_hops": 0}, {"downstream_hops": 0}, {"max_candidate_services": 0},
    {"max_candidate_node_metrics": -1}, {"max_candidate_physical_edges": 0},
    {"max_provenance_paths_per_object": 0}, {"include_cohost": 1},
    {"allow_cross_namespace": "yes"}, {"allowed_namespaces": ["ns", "ns"]},
    {"allowed_namespaces": [""]}, {"unknown": 1},
])
def test_candidate_config_is_strict(changes):
    with pytest.raises((ValueError, TypeError)):
        CandidateGraphConfig.from_dict(candidate_payload(**changes))


@pytest.mark.parametrize("changes", [
    {"direction": "auto"}, {"source_relation_type": "trace"}, {"enabled": 1},
    {"rule_id": ""}, {"rule_id": "bad::id"}, {"provenance_label": ""},
])
def test_impact_rule_is_strict(changes):
    payload = {"rule_id": "rule", "source_relation_type": "call", "protocol": None,
               "direction": "reverse", "enabled": True, "provenance_label": "configured"}
    payload.update(changes)
    with pytest.raises((ValueError, TypeError)):
        ImpactDerivationRule.from_dict(payload)


@pytest.mark.parametrize("mutation", [
    "unknown_placement", "duplicate_placement", "ambiguous_endpoint", "self_loop",
])
def test_topology_snapshot_rejects_invalid_structure(mutation):
    base = topo.snapshot()
    if mutation == "unknown_placement":
        kwargs = {"service_nodes": [ServiceNodePlacement("ns", "missing", "node", "pod")]}
    elif mutation == "duplicate_placement":
        item = base.service_nodes[0]; kwargs = {"service_nodes": [item, item]}
    elif mutation == "ambiguous_endpoint":
        kwargs = {"services": [*base.services, "other::api"],
                  "call_edges": [TopologyEdge("api", "worker", "call", protocol="http")]}
    else:
        kwargs = {"call_edges": [topo.edge("api", "api")]}
    with pytest.raises(ValueError): replace(base, **kwargs)


def test_topology_store_rejects_bad_version_and_multinamespace_unscoped(tmp_path):
    cross = replace(topo.snapshot(), services=[*topo.snapshot().services, "other::remote"])
    store = TopologyStore([cross])
    with pytest.raises(TopologyValidationError): store.query("cluster-a", 1)
    path = tmp_path / "store.json"; store.save_json(path)
    payload = json.loads(path.read_text()); payload["format_version"] = "bad"; path.write_text(json.dumps(payload))
    with pytest.raises(TopologyValidationError): TopologyStore.load_json(path)


def test_multiple_protocols_create_distinct_physical_edges():
    source = replace(topo.snapshot(), call_edges=[topo.edge("api", "worker", protocol="http"),
                                                  topo.edge("api", "worker", protocol="grpc")])
    graph = build_topology_graph(source, [], False)
    assert [item.relation_id for item in graph.physical_edges] == [
        "cluster-a::ns::api->worker::grpc", "cluster-a::ns::api->worker::http"
    ]
    assert len(graph.call_edges) == 1 and graph.call_edges[0].detail["protocols"] == ["grpc", "http"]


@pytest.mark.parametrize("allowed,expected", [(False, False), (True, True)])
def test_cross_namespace_candidate_expansion_is_configured(allowed, expected):
    cross_edge = TopologyEdge("worker", "remote", "call", src_namespace="ns", dst_namespace="other", protocol="http")
    source = replace(cand.source_snapshot(), services=[*cand.source_snapshot().services, "other::remote"],
                     call_edges=[*cand.source_snapshot().call_edges, cross_edge])
    nodes, edges, specs = cand.build_inputs()
    cfg = cand.config(allow_cross_namespace=allowed, allowed_namespaces=["ns", "other"])
    result = CandidateSubgraphBuilder(cfg, specs).prepare(cand.alert(), TopologyStore([source]), nodes, edges)
    assert (cand.service_id("remote", "other") in result.candidate_services) is expected


def test_trigger_namespace_outside_allowlist_is_stale():
    source = replace(cand.source_snapshot(), services=[*cand.source_snapshot().services, "other::remote"])
    nodes, edges, specs = cand.build_inputs()
    with pytest.raises(ValueError, match="allowed_namespaces"):
        CandidateSubgraphBuilder(cand.config(), specs).prepare(
            cand.alert(services=[cand.service_id("remote", "other")]), TopologyStore([source]), nodes, edges)


@pytest.mark.parametrize("limit_name", [
    "max_candidate_services", "max_candidate_node_metrics", "max_candidate_physical_edges",
])
def test_all_candidate_limits_fail_without_topk(limit_name):
    nodes, edges, specs = cand.build_inputs()
    cfg = cand.config(**{limit_name: 1})
    trigger = cand.alert(services=[cand.service_id("api"), cand.service_id("worker")]) if limit_name == "max_candidate_physical_edges" else cand.alert()
    with pytest.raises(CandidateOverflowError, match="truncation is forbidden"):
        CandidateSubgraphBuilder(cfg, specs).prepare(
            trigger, TopologyStore([cand.source_snapshot()]), nodes, edges)


def test_cross_cluster_metrics_and_ambiguous_signal_specs_fail():
    nodes, edges, specs = cand.build_inputs()
    foreign = replace(nodes[0], cluster_id="cluster-b")
    with pytest.raises(CandidateValidationError):
        CandidateSubgraphBuilder(cand.config(), specs).prepare(
            cand.alert(), TopologyStore([cand.source_snapshot()]), [foreign, *nodes[1:]], edges)
    with pytest.raises(AmbiguousMetricSelectionError):
        CandidateSubgraphBuilder(cand.config(), [specs[0], specs[0]])


def test_impact_edges_never_invent_physical_edges_and_unconfigured_shocks():
    nodes, edges, specs = cand.build_inputs()
    result = CandidateSubgraphBuilder(cand.config(), specs).prepare(
        cand.alert(), TopologyStore([cand.source_snapshot()]), nodes, edges)
    assert not any(item["src_service_id"].endswith("root") for item in result.physical_edges)
    templates = {key: value for key, value in cand.config().shock_templates.items() if key != "edge.signal"}
    no_shock_config = replace(cand.config(), shock_templates=templates)
    no_shock = CandidateSubgraphBuilder(no_shock_config, specs).prepare(
        cand.alert(), TopologyStore([cand.source_snapshot()]), nodes, edges)
    assert no_shock.candidate_edge_metric_ids and no_shock.candidate_shock_ids == []


@pytest.mark.parametrize("reason,detail", [
    ("cohost", {}), ("shared_resource", {}),
    ("trigger_service", {}),
])
def test_provenance_rejects_invalid_reason_semantics(reason, detail):
    kwargs = dict(object_id="object", object_type="service", reason_code=reason,
                  source_object_id="source", hop_count=1, relation_path=["a", "b"],
                  relation_ids=["relation"], snapshot_id="snapshot", alert_id="alert", detail=detail)
    with pytest.raises(ValueError): CandidateProvenance(**kwargs)


@pytest.mark.parametrize("field,value", [
    ("object_type", "root"), ("reason_code", "top_k"), ("hop_count", -1),
])
def test_provenance_rejects_invalid_enums_and_bounds(field, value):
    payload = dict(object_id="object", object_type="service", reason_code="trigger_service",
                   source_object_id="source", hop_count=0, relation_path=["object"], relation_ids=[],
                   snapshot_id="snapshot", alert_id="alert", detail={})
    payload[field] = value
    with pytest.raises(ValueError): CandidateProvenance(**payload)


@pytest.mark.parametrize("relation_field", ["call_edges", "impact_edges", "host_relations", "resource_relations", "physical_edges"])
def test_candidate_rejects_duplicate_relation_objects(relation_field):
    payload = contracts.valid_candidate().to_dict()
    assert payload[relation_field], f"fixture must exercise {relation_field}"
    payload[relation_field].append(dict(payload[relation_field][0]))
    with pytest.raises(ValueError): CandidateSubgraph.from_dict(payload)


def test_candidate_error_types_are_explicit():
    assert issubclass(CandidateSerializationError, ValueError)
    assert issubclass(CandidateValidationError, ValueError)
    assert issubclass(AmbiguousMetricSelectionError, ValueError)


P3_FILES = [Path("proberca/topology/core.py"), Path("proberca/candidates/builder.py"),
            Path("proberca/config.py"), Path("proberca/data/schema.py")]


@pytest.mark.parametrize("path", P3_FILES)
def test_p3_production_has_no_fixed_services_labels_or_name_heuristics(path):
    text = path.read_text(encoding="utf-8").lower()
    forbidden = ("paymentservice", "checkoutservice", "online boutique",
                 '"p99"', ".endswith(", ".startswith(")
    assert not any(item in text for item in forbidden)


def test_online_p3_modules_do_not_import_incident_labels():
    for path in P3_FILES[:2]:
        assert "incidentlabel" not in path.read_text(encoding="utf-8").lower()


@pytest.mark.parametrize("path", P3_FILES[:2])
def test_p3_production_has_no_todo_or_pass(path):
    source = path.read_text(encoding="utf-8")
    assert "TODO" not in source and "FIXME" not in source
    assert not any(isinstance(node, ast.Pass) for node in ast.walk(ast.parse(source)))


def test_p3_does_not_modify_forbidden_modules():
    changed = subprocess.run(["git", "diff", "--name-only", "HEAD"], check=True, text=True,
                             capture_output=True).stdout.splitlines()
    assert not any(path.startswith(("proberca/aggregation/", "proberca/baseline/", "proberca/alerting/",
                                    "proberca/propagation/", "proberca/inference/", "proberca/evidence/",
                                    "experiments/", "bpf/")) for path in changed)


def test_p3_tests_have_no_skip_or_xfail_markers():
    forbidden = ["pytest." + "skip", "pytest.mark." + "skip", "pytest.mark." + "xfail"]
    for path in Path("tests").glob("test_p3_*.py"):
        assert not any(item in path.read_text(encoding="utf-8") for item in forbidden)
