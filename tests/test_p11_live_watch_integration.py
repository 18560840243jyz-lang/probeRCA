from __future__ import annotations

import ast
import inspect
from types import SimpleNamespace

import pytest

from proberca.live.coordinator import LiveCoordinatorState
from proberca.live.runner import ProbeRCALiveRunner


class Supervisor:
    def __init__(self):
        self.started = 0
        self.waited = 0
        self.frozen = []
        self.stopped = 0
        self.joined = 0

    def start(self):
        self.started += 1

    def wait_until_synchronized(self, timeout_sec):
        self.waited += 1
        return True

    def freeze_revision(self, observed_at_ns):
        self.frozen.append(observed_at_ns)
        return SimpleNamespace(ready=True, revision_id=str(observed_at_ns))

    def stop(self):
        self.stopped += 1

    def join(self, timeout_sec):
        self.joined += 1

    def health_snapshot(self):
        return {
            "synchronized": True,
            "states": {},
            "fatal": False,
            "reconnect_count": 0,
            "relist_count": 0,
        }


class Coordinator:
    def __init__(self, process):
        self.state = LiveCoordinatorState.LEADER_ACTIVE
        self.process = process
        self.commits = []

    def begin_window(self, start_ns, end_ns, attempt_index=1):
        return SimpleNamespace(
            sequence=len(self.commits) + 1,
            working_engine=SimpleNamespace(),
            engine_result=None,
        )

    def run_engine(self, context, value):
        context.engine_result = self.process(value)
        return context.engine_result

    def prepare_generation(self, context, **payload):
        return payload

    def commit(self, context, generation):
        self.commits.append(context.sequence)


def _payload(*_):
    return {
        "engine_state": {},
        "output_ledger": {},
        "output_bundle": {},
        "config_fingerprint": "c" * 64,
        "code_schema_version": "generation_v5",
    }


def test_runner_owns_one_supervisor_lifecycle_and_freezes_each_window():
    supervisor = Supervisor()
    calls = []
    coordinator = Coordinator(lambda value: calls.append(value) or value)
    runner = ProbeRCALiveRunner(
        coordinator=coordinator,
        watch_supervisor=supervisor,
        topology_builder=lambda window, revision: revision,
        metric_collector=lambda window, revision: (
            [window.end_ns],
            [revision.revision_id],
        ),
        window_adapter=lambda window, topology, nodes, edges: (
            window.end_ns,
            nodes,
            edges,
        ),
        commit_payload_builder=_payload,
    )
    runner.start(sync_timeout_sec=1.0)
    runner.process_window(SimpleNamespace(start_ns=0, end_ns=10, sequence=90))
    runner.process_window(SimpleNamespace(start_ns=10, end_ns=20, sequence=91))
    runner.stop(join_timeout_sec=1.0)
    assert supervisor.started == supervisor.waited == 1
    assert supervisor.frozen == [10, 20]
    assert len(calls) == 2
    assert coordinator.commits == [1, 2]
    assert supervisor.stopped == supervisor.joined == 1


def test_live_runner_and_window_loop_do_not_call_kubernetes_list_or_replay():
    import proberca.cli.live as live_cli
    import proberca.live.runner as live_runner

    source = inspect.getsource(live_cli) + inspect.getsource(live_runner)
    tree = ast.parse(source)
    forbidden = {
        "discover_once",
        "list_namespaced_pod",
        "list_pod_for_all_namespaces",
        "list_namespaced_service",
        "list_namespaced_endpoint_slice",
        "list_node",
    }
    called = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert not called.intersection(forbidden)
    assert "ReplayRunner" not in source


def test_unsynchronized_supervisor_prevents_runner_start():
    supervisor = Supervisor()
    supervisor.wait_until_synchronized = lambda timeout: False
    runner = ProbeRCALiveRunner(
        coordinator=Coordinator(lambda value: value),
        watch_supervisor=supervisor,
        topology_builder=lambda *_: None,
        metric_collector=lambda *_: ([], []),
        window_adapter=lambda *_: None,
        commit_payload_builder=_payload,
    )
    with pytest.raises(Exception, match="synchronization timed out"):
        runner.start(sync_timeout_sec=0.01)


def test_metric_failure_does_not_call_engine_or_prepare_generation():
    called = []
    coordinator = Coordinator(
        lambda value: called.append("engine") or value,
    )
    runner = ProbeRCALiveRunner(
        coordinator=coordinator,
        inventory=SimpleNamespace(
            ready=True,
            freeze=lambda end: SimpleNamespace(ready=True),
        ),
        topology_builder=lambda *_: object(),
        metric_collector=lambda *_: (_ for _ in ()).throw(
            RuntimeError("metrics failed"),
        ),
        window_adapter=lambda *_: object(),
        commit_payload_builder=lambda *_: called.append("prepare"),
    )
    with pytest.raises(RuntimeError, match="metrics failed"):
        runner.process_window(SimpleNamespace(start_ns=0, end_ns=1, sequence=9))
    assert called == []
    assert coordinator.commits == []


@pytest.mark.parametrize("forbidden", [
    "graph_sparse_admm",
    "ReplayRunner",
    "IncidentLabel",
    "BurstEventRecord",
    "list_namespaced_pod",
    "list_namespaced_service",
])
def test_live_runner_has_no_offline_or_direct_discovery_dependency(forbidden):
    import proberca.live.runner as live_runner

    assert forbidden not in inspect.getsource(live_runner)


def test_live_source_record_id_distinguishes_rolling_pod_records():
    from types import SimpleNamespace

    from proberca.cli.live import _source_record_id

    first = SimpleNamespace(to_dict=lambda: {
        "record_type": "node_metric", "timestamp_ns": 10,
        "service_name": "service", "pod_uid": "pod-a", "value": 1.0,
    })
    second = SimpleNamespace(to_dict=lambda: {
        "record_type": "node_metric", "timestamp_ns": 10,
        "service_name": "service", "pod_uid": "pod-b", "value": 1.0,
    })
    assert _source_record_id(first) != _source_record_id(second)
    assert _source_record_id(first) == _source_record_id(first)
