from dataclasses import replace
import pytest
import test_p2_aggregation as p2agg
from test_p10_engine import NS, core_config, topology
from proberca.aggregation import AggregationPlan
from proberca.config import (
    AlertStateConfig, BaselineConfig, MetricSignalSpec, ProbeRCAConfig, ScoreConfig,
)
from proberca.orchestration import EngineWindowInput, ProbeRCAEngine
from proberca.data.schema import ServiceNodePlacement, TopologyEdge
import test_p1_data_contracts as p1

def case_engine(family, metric_name, unit):
    node_id = f"cluster-a::observability::service-a::{metric_name}"
    payload = core_config().to_dict()
    payload["rca_metric_families"] = [family]
    rule = payload["propagation"]["metric_parent_rules"][0]
    rule.update({"target_family": family, "parent_family": family,
                 "target_metric_names": [metric_name],
                 "parent_metric_names": [metric_name]})
    config = ProbeRCAConfig.from_dict(payload)
    aggregation = p2agg.spec(
        method="median_max", kind="gauge", source="pod", target="service",
        input_metric_ids=[node_id], output_metric_name=metric_name,
        output_metric_kind="gauge", output_unit=unit, median_weight=1.0)
    signal = MetricSignalSpec.from_dict({
        "record_type": "node_metric", "metric_family": family,
        "metric_name": metric_name, "protocol": None, "transform": "identity",
        "polarity": "increase_bad", "rare_event_threshold": None,
        "direct_hard": False, "z_cap": 6.0, "aggregation_output_id": node_id})
    weights = {name: float(name == family)
               for name in ("request", "cpu", "memory", "io", "net_local", "lock")}
    return ProbeRCAEngine(
        config, aggregation_plan=AggregationPlan([(node_id, aggregation)]),
        signal_specs=[signal], baseline_config=BaselineConfig(30, 3, .1, 6),
        score_config=ScoreConfig(weights, True, 1, 1),
        alert_state_config=AlertStateConfig(.5, 1, 1, 2, 1, .25, 1, 1, 1))

def raw_window(index, family, metric_name, unit, value):
    end = index * NS
    record = p1.make_node(
        timestamp_ns=end - 1, window_sec=1, metric_kind="gauge", scope="pod",
        metric_family=family, metric_name=metric_name, value=value, unit=unit,
        sample_count=1, coverage=1.0, event_loss_rate=0.0)
    return EngineWindowInput(
        end, end - NS, end, [record], [], [topology()] if index == 1 else [], [],
        [f"node:{index}"], index, [])

@pytest.mark.parametrize("family,metric_name,unit", [
    ("cpu", "runtime.cpu_pressure", "ratio"),
    ("memory", "runtime.memory_pressure", "ratio"),
    ("io", "runtime.block_delay", "ms"),
    ("lock", "runtime.lock_wait", "ms"),
])
def test_raw_node_incident_cases_reach_real_self_diagnosis(family, metric_name, unit):
    target = case_engine(family, metric_name, unit)
    values = [1, 1, 1, 1, 1.01, .99, 1, 1.2, 1.4]
    results = [target.process_window(raw_window(i, family, metric_name, unit, value))
               for i, value in enumerate(values, 1)]
    report = results[-1].reports[0]
    assert report.primary_root.kind == "node"
    assert report.primary_root.fault_mode == "self"
    assert report.primary_root.metric_name == metric_name


def edge_engine(metric_name, protocol):
    node_name = "request.latency"
    node_id = f"cluster-a::observability::service-a::{node_name}"
    edge_id = f"cluster-a::observability::service-a->service-b::{protocol}::{metric_name}"
    edge_series = edge_id + "::scope=service_pair::service_pair"
    payload = core_config().to_dict()
    payload["rca_metric_families"] = ["request"]
    payload["propagation"]["metric_parent_rules"][0].update({
        "target_family": "request", "parent_family": "request",
        "target_metric_names": [node_name], "parent_metric_names": [node_name]})
    payload["shock_projection_templates"] = [{
        "template_id": "edge-request", "enabled": True,
        "edge_metric_name": metric_name, "protocol": protocol,
        "projections": [{"endpoint_role": "source", "metric_family": "request",
                         "metric_names": [node_name], "raw_weight": 1.0}]}]
    config = ProbeRCAConfig.from_dict(payload)
    node_spec = p2agg.spec(
        method="median_max", kind="gauge", source="pod", target="service",
        input_metric_ids=[node_id], output_metric_name=node_name,
        output_metric_kind="gauge", output_unit="ms", median_weight=1.0)
    edge_spec = p2agg.spec(
        method="last_same_series", kind="gauge", source="service_pair",
        target="service_pair", input_metric_ids=[edge_id], input_series_ids=[edge_series],
        output_metric_name=metric_name, output_metric_kind="gauge", output_unit="ratio")
    signals = [
        MetricSignalSpec("node_metric", "request", node_name, None, "identity",
                         "increase_bad", None, False, 6, node_id),
        MetricSignalSpec("edge_metric", None, metric_name, protocol, "identity",
                         "increase_bad", None, True, 6, edge_id)]
    return ProbeRCAEngine(
        config, aggregation_plan=AggregationPlan([(node_id, node_spec), (edge_id, edge_spec)]),
        signal_specs=signals, baseline_config=BaselineConfig(30, 3, .1, 6),
        score_config=ScoreConfig({"request": 1, "cpu": 0, "memory": 0, "io": 0,
                                  "net_local": 0, "lock": 0}, True, 1, 1),
        alert_state_config=AlertStateConfig(.5, 1, 1, 2, 1, .25, 1, 1, 1))

def edge_window(index, metric_name, protocol, node_value, edge_value):
    end = index * NS
    node = p1.make_node(
        timestamp_ns=end - 1, window_sec=1, metric_kind="gauge", scope="pod",
        metric_family="request", metric_name="request.latency", value=node_value,
        unit="ms", sample_count=1, coverage=1, event_loss_rate=0)
    edge = replace(p1.make_edge(timestamp_ns=end - 1, window_sec=1),
                   metric_name=metric_name, protocol=protocol,
                   value=edge_value, unit="ratio")
    top = topology()
    top = replace(top, call_edges=[replace(top.call_edges[0], protocol=protocol)])
    return EngineWindowInput(end, end - NS, end, [node], [edge], [top] if index == 1 else [],
                             [], [f"node:{index}", f"edge:{index}"], index, [])

@pytest.mark.parametrize("metric_name,protocol", [
    ("tcp.retrans_rate", "tcp"), ("dns.timeout_rate", "dns")])
def test_raw_edge_shock_cases_reach_real_edge_diagnosis(metric_name, protocol):
    target = edge_engine(metric_name, protocol)
    node_values = [1, 1, 1, 1, 1.01, .99, 1, 1.2, 1.4]
    edge_values = [.1, .1, .1, .1, .1, .1, .1, .2, 1.0]
    results = [target.process_window(edge_window(i, metric_name, protocol, node, edge))
               for i, (node, edge) in enumerate(zip(node_values, edge_values), 1)]
    assert results[-1].reports
    report = results[-1].reports[0]
    assert report.primary_root.kind == "edge"
    assert report.primary_root.edge_subtype == "exogenous-edge-shock"


def propagation_engine(relation_type):
    metric_name = "runtime.pressure"
    ids = [f"cluster-a::observability::{service}::{metric_name}"
           for service in ("service-a", "service-b")]
    payload = core_config().to_dict()
    payload["candidate_graph"].update({"include_cohost": True,
                                       "include_shared_resource": True})
    payload["propagation"]["metric_parent_rules"] = [
        {"rule_id": "self", "enabled": True, "target_family": "cpu",
         "target_metric_names": [metric_name], "relation_type": "self_history",
         "parent_family": "cpu", "parent_metric_names": [metric_name], "lags": [1],
         "require_signal_spec": True, "provenance_label": "self"},
        {"rule_id": relation_type, "enabled": True, "target_family": "cpu",
         "target_metric_names": [metric_name], "relation_type": relation_type,
         "parent_family": "cpu", "parent_metric_names": [metric_name], "lags": [1],
         "require_signal_spec": True, "provenance_label": relation_type}]
    if relation_type == "impact":
        payload["penalties"]["c_delta"] = 0.5
        payload["diagnosis"]["propagated_explained_ratio_threshold"] = 0.2
    config = ProbeRCAConfig.from_dict(payload)
    entries, signals = [], []
    for node_id in ids:
        entries.append((node_id, p2agg.spec(
            method="median_max", kind="gauge", source="pod", target="service",
            input_metric_ids=[node_id], output_metric_name=metric_name,
            output_metric_kind="gauge", output_unit="ratio", median_weight=1.0)))
        signals.append(MetricSignalSpec(
            "node_metric", "cpu", metric_name, None, "identity", "increase_bad",
            None, False, 6, node_id))
    return ProbeRCAEngine(
        config, aggregation_plan=AggregationPlan(entries), signal_specs=signals,
        baseline_config=BaselineConfig(30, 3, .1, 6),
        score_config=ScoreConfig({"request": 0, "cpu": 1, "memory": 0, "io": 0,
                                  "net_local": 0, "lock": 0}, True, 1, 1),
        alert_state_config=AlertStateConfig(.5, 1, 1, 2, 1, .25, 1, 1, 1))

def propagation_window(index, relation_type, a_value, b_value):
    end = index * NS
    records = [replace(
        p1.make_node(timestamp_ns=end - 1, window_sec=1, metric_kind="gauge",
                     scope="pod", metric_family="cpu", metric_name="runtime.pressure",
                     value=value, unit="ratio", sample_count=1, coverage=1,
                     event_loss_rate=0), service_name=service,
        pod_uid=f"pod-{service[-1]}")
        for service, value in (("service-a", a_value), ("service-b", b_value))]
    top = topology()
    relation = TopologyEdge("service-b", "service-a", relation_type,
                            directed=relation_type == "impact")
    if relation_type == "host":
        top = replace(top, host_edges=[relation], service_nodes=[
            ServiceNodePlacement("observability", "service-a", "worker-shared", "pod-a"),
            ServiceNodePlacement("observability", "service-b", "worker-shared", "pod-b")])
    else:
        top = replace(top, call_edges=[relation])
    return EngineWindowInput(end, end - NS, end, records, [], [top] if index == 1 else [],
                             [], [f"node:a:{index}", f"node:b:{index}"], index, [])

def test_same_node_pressure_uses_host_structural_candidate_without_top_k():
    target = propagation_engine("host")
    a_values = [1, 1, 1, 1, 1.04, .96, 1.04, .96, 1.4]
    b_values = [1, 1, 1, 1.04, .96, 1.04, .96, 1.2, 1.0]
    results = [target.process_window(propagation_window(i, "host", a, b))
               for i, (a, b) in enumerate(zip(a_values, b_values), 1)]
    candidate = results[-1].candidate_subgraph
    assert candidate.host_relations
    assert {item.reason_code for item in candidate.provenance} >= {"cohost"}
    assert results[-1].reports

def test_propagated_downstream_symptom_is_not_promoted_over_current_root():
    target = propagation_engine("impact")
    a_values = [1, 1, 1, 1, 1.04, .96, 1.04, .96, 1.4]
    b_values = [1, 1, 1, 1.04, .96, 1.04, .96, 1.2, 1.4]
    results = [target.process_window(propagation_window(i, "impact", a, b))
               for i, (a, b) in enumerate(zip(a_values, b_values), 1)]
    report = results[-1].reports[0]
    assert report.primary_root.kind == "node"
    assert report.symptoms
    assert all(item["node_id"] != report.primary_root.node_id for item in report.symptoms)
    assert all(item.get("object_type") != "propagated" for item in report.ranked_candidates)
