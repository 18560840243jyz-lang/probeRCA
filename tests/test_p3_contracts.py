from __future__ import annotations

from dataclasses import replace

import pytest

import test_p3_candidates as cand
import test_p3_topology as topo
from proberca.candidates import CandidateSubgraphBuilder
from proberca.data.io import (
    read_record_json,
    read_records_jsonl,
    read_records_parquet,
    write_record_json,
    write_records_jsonl,
    write_records_parquet,
)
from proberca.data.schema import CandidateProvenance, CandidateSubgraph
from proberca.topology import TopologyStore


def valid_candidate(state="soft", ts=10, snapshot=None, builder=None):
    nodes, edges, specs = cand.build_inputs()
    snapshot = snapshot or cand.source_snapshot()
    builder = builder or CandidateSubgraphBuilder(cand.config(), specs, clock_ns=iter((0, 100)).__next__)
    return builder.prepare(cand.alert(state, ts), TopologyStore([snapshot]), nodes, edges)


def test_candidate_json_jsonl_and_parquet_round_trip(tmp_path):
    candidate = valid_candidate()
    json_path = tmp_path / "candidate.json"
    jsonl_path = tmp_path / "candidate.jsonl"
    parquet_path = tmp_path / "candidate.parquet"
    write_record_json(json_path, candidate)
    write_records_jsonl(jsonl_path, [candidate])
    write_records_parquet(parquet_path, [candidate])
    assert read_record_json(json_path) == candidate
    assert read_records_jsonl(jsonl_path) == [candidate]
    assert read_records_parquet(parquet_path) == [candidate]


def test_candidate_record_type_and_unknown_fields_fail():
    payload = valid_candidate().to_dict()
    payload["record_type"] = "rca_report"
    with pytest.raises(ValueError): CandidateSubgraph.from_dict(payload)
    payload = valid_candidate().to_dict(); payload["unknown"] = 1
    with pytest.raises(ValueError): CandidateSubgraph.from_dict(payload)


@pytest.mark.parametrize("mutation", [
    "edge_endpoint", "node_service", "shock_physical", "seed", "duplicate_service",
    "soft_eligible", "hard_ineligible", "missing_provenance",
])
def test_candidate_structural_validation_fails(mutation):
    base = valid_candidate("hard" if mutation == "hard_ineligible" else "soft").to_dict()
    if mutation == "edge_endpoint": base["call_edges"][0]["src_service_id"] = "cluster-a::ns::missing"
    elif mutation == "node_service": base["candidate_node_ids"][0] = "cluster-a::ns::missing::cpu.signal"
    elif mutation == "shock_physical": base["candidate_shock_ids"][0] = "cluster-a::ns::x->y::tcp::shock::metric"
    elif mutation == "seed": base["seed_services"] = ["cluster-a::ns::missing"]
    elif mutation == "duplicate_service": base["candidate_services"].append(base["candidate_services"][0])
    elif mutation == "soft_eligible": base["rca_eligible"] = True
    elif mutation == "hard_ineligible": base["rca_eligible"] = False
    elif mutation == "missing_provenance": base["provenance"] = [
        item for item in base["provenance"] if item["object_id"] != base["candidate_services"][0]
    ]
    with pytest.raises(ValueError): CandidateSubgraph.from_dict(base)


def test_provenance_contract_enforces_path_and_context():
    common = dict(object_id="service", object_type="service", source_object_id="seed",
                  snapshot_id="snapshot", alert_id="alert", detail={})
    with pytest.raises(ValueError):
        CandidateProvenance(reason_code="impact_ancestor", hop_count=2,
                            relation_path=["a", "b"], relation_ids=["edge"], **common)
    with pytest.raises(ValueError):
        CandidateProvenance(reason_code="cohost", hop_count=1,
                            relation_path=["a", "b"], relation_ids=["host"], **common)
    with pytest.raises(ValueError):
        CandidateProvenance(reason_code="shared_resource", hop_count=1,
                            relation_path=["a", "b"], relation_ids=["resource"], **common)


def test_provenance_is_sorted_and_truncated_deterministically():
    source = cand.source_snapshot()
    # root reaches worker both through db and through an additional explicit impact path.
    source = replace(source, call_edges=[*source.call_edges,
                     topo.edge("root", "worker", "impact", protocol=None)])
    nodes, edges, specs = cand.build_inputs()
    cfg = cand.config(include_all_provenance_paths=False, max_provenance_paths_per_object=1)
    result = CandidateSubgraphBuilder(cfg, specs).prepare(
        cand.alert(), TopologyStore([source]), nodes, edges)
    assert any(item["reason_code"] == "provenance_truncated" for item in result.quality_issues)
    grouped = [item for item in result.provenance if item.object_id == cand.service_id("root")]
    assert len(grouped) == 1


def test_soft_hard_rebuild_and_topology_change_issue():
    first = cand.source_snapshot("soft-snapshot", 0, 100)
    second = cand.source_snapshot("hard-snapshot", 100, 200)
    nodes, edges, specs = cand.build_inputs()
    builder = CandidateSubgraphBuilder(cand.config(), specs)
    soft = builder.prepare(cand.alert("soft", 10), TopologyStore([first, second]), nodes, edges)
    hard = builder.prepare(cand.alert("hard", 110), TopologyStore([first, second]), nodes, edges)
    assert soft.topology_snapshot_id == "soft-snapshot" and hard.topology_snapshot_id == "hard-snapshot"
    assert any(item["reason_code"] == "topology_changed_since_soft" for item in hard.quality_issues)
    assert soft.candidate_services == hard.candidate_services


def test_config_and_metric_fingerprints_force_distinct_candidate_ids():
    nodes, edges, specs = cand.build_inputs()
    first = CandidateSubgraphBuilder(cand.config(), specs).prepare(
        cand.alert(), TopologyStore([cand.source_snapshot()]), nodes, edges)
    changed = CandidateSubgraphBuilder(cand.config(downstream_hops=2), specs).prepare(
        cand.alert(), TopologyStore([cand.source_snapshot()]), nodes, edges)
    assert first.config_fingerprint != changed.config_fingerprint
    assert first.candidate_id != changed.candidate_id
    fewer_metrics = CandidateSubgraphBuilder(cand.config(), specs).prepare(
        cand.alert(), TopologyStore([cand.source_snapshot()]), nodes[:-1], edges)
    assert first.candidate_id != fewer_metrics.candidate_id
