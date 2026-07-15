from dataclasses import asdict

import pytest

from proberca.config import LiveLivenessConfig
from proberca.live.progress import LiveStage


def test_live_liveness_config_covers_every_window_stage_with_positive_deadline():
    config = LiveLivenessConfig()
    config.validate()
    mapping = config.stage_timeouts()
    required = {
        LiveStage.BEGIN_WINDOW,
        LiveStage.FREEZE_REVISION,
        LiveStage.BUILD_TOPOLOGY,
        LiveStage.COLLECT_CALL_EDGES,
        LiveStage.COLLECT_NODE_METRICS,
        LiveStage.COLLECT_EDGE_METRICS,
        LiveStage.ADAPT_NODE_RECORDS,
        LiveStage.ADAPT_EDGE_RECORDS,
        LiveStage.BUILD_ENGINE_INPUT,
        LiveStage.ENGINE_PROCESS,
        LiveStage.PREPARE_GENERATION,
        LiveStage.COMMIT_RUN_STATE,
        LiveStage.PROJECT_OUTPUT,
        LiveStage.RETENTION,
    }
    assert required <= set(mapping)
    assert all(value > 0 for value in mapping.values())


@pytest.mark.parametrize(
    "updates",
    [
        {"engine_process_timeout_sec": 0},
        {"transient_retry_max_attempts": 0},
        {"transient_retry_initial_backoff_sec": 3,
         "transient_retry_max_backoff_sec": 2},
        {"watchdog_poll_interval_sec": 10, "progress_timeout_sec": 5},
        {"watchdog_dump_grace_sec": 10, "watchdog_exit_grace_sec": 5},
        {"backlog_not_ready_threshold": 5, "backlog_fatal_threshold": 4},
        {"maximum_stage_event_history": 0},
        {"fail_stop_on_unrecoverable_stall": False},
        {"controlled_stage_delay_enabled": True,
         "controlled_stage_delay_stage": "WINDOW_COMPLETE",
         "controlled_stage_delay_sec": 1.0},
    ],
)
def test_live_liveness_config_rejects_unbounded_or_inconsistent_values(updates):
    payload = asdict(LiveLivenessConfig())
    payload.update(updates)
    with pytest.raises((TypeError, ValueError)):
        LiveLivenessConfig.from_dict(payload)


def test_liveness_config_has_no_metric_service_namespace_or_sequence_exception():
    fields = set(LiveLivenessConfig.__dataclass_fields__)
    forbidden = {
        "metric_name", "service_name", "namespace_name", "sequence_id",
        "allow_empty", "exception",
    }
    assert not any(any(word in field for word in forbidden) for field in fields)


def test_runner_freeze_timeout_never_reaches_engine_or_commit():
    import threading
    from types import SimpleNamespace

    from proberca.live.coordinator import LiveCoordinatorState
    from proberca.live.executor import LiveStageTimeoutError
    from proberca.live.runner import ProbeRCALiveRunner

    release = threading.Event()

    class Coordinator:
        state = LiveCoordinatorState.LEADER_ACTIVE

        def __init__(self):
            self.engine_calls = 0
            self.commit_calls = 0

        def begin_window(self, *_):
            return SimpleNamespace(sequence=24)

        def run_engine(self, *_):
            self.engine_calls += 1

        def commit(self, *_):
            self.commit_calls += 1

    coordinator = Coordinator()
    config = LiveLivenessConfig(
        freeze_revision_timeout_sec=0.01,
    )
    runner = ProbeRCALiveRunner(
        coordinator=coordinator,
        inventory=SimpleNamespace(
            ready=True,
            freeze=lambda _: release.wait(5.0),
        ),
        topology_builder=lambda *_: object(),
        metric_collector=lambda *_: ([object()], [object()]),
        window_adapter=lambda *_: object(),
        commit_payload_builder=lambda *_: {},
        liveness_config=config,
    )
    with pytest.raises(LiveStageTimeoutError) as caught:
        runner.process_window(SimpleNamespace(start_ns=10, end_ns=20))
    release.set()
    assert caught.value.stage is LiveStage.FREEZE_REVISION
    assert coordinator.engine_calls == 0
    assert coordinator.commit_calls == 0


def test_runner_uses_separate_node_and_edge_collection_stages():
    from types import SimpleNamespace

    from proberca.live.coordinator import LiveCoordinatorState
    from proberca.live.runner import ProbeRCALiveRunner

    calls = []

    class Coordinator:
        state = LiveCoordinatorState.LEADER_ACTIVE

        def begin_window(self, *_):
            return SimpleNamespace(sequence=1, working_engine=object())

        def run_engine(self, context, value):
            context.engine_result = value
            return value

        def prepare_generation(self, _context, **payload):
            return payload

        def commit(self, *_):
            return None

    runner = ProbeRCALiveRunner(
        coordinator=Coordinator(),
        inventory=SimpleNamespace(ready=True, freeze=lambda _: object()),
        topology_builder=lambda *_: object(),
        node_metric_collector=lambda *_: calls.append("node") or [object()],
        edge_metric_collector=lambda *_: calls.append("edge") or [object()],
        window_adapter=lambda *_: object(),
        commit_payload_builder=lambda *_: {},
        liveness_config=LiveLivenessConfig(),
    )
    runner.process_window(SimpleNamespace(start_ns=10, end_ns=20))
    assert calls == ["node", "edge"]


def test_inventory_freeze_lock_has_bounded_timeout_and_owner_context():
    import threading

    from proberca.k8s.inventory import (
        InventoryLockTimeout,
        KubernetesInventory,
    )

    inventory = KubernetesInventory(
        "cluster",
        required_kinds=(),
        stale_after_sec=60,
        lock_timeout_sec=0.02,
    )
    acquired = threading.Event()
    release = threading.Event()

    def hold_lock():
        with inventory._lock:
            inventory._lock_holder = "watch-writer"
            acquired.set()
            release.wait(1.0)

    worker = threading.Thread(target=hold_lock)
    worker.start()
    assert acquired.wait(0.5)
    try:
        with pytest.raises(InventoryLockTimeout) as caught:
            inventory.freeze(100)
        assert caught.value.lock_name == "inventory-freeze"
        assert caught.value.holder_thread == "watch-writer"
        assert caught.value.wait_duration_sec >= 0.02
    finally:
        release.set()
        worker.join(1.0)


def test_kubernetes_lease_api_uses_native_request_timeout():
    from proberca.live.leader import KubernetesLeaseAPI, LeaseState

    class API:
        def __init__(self):
            self.timeouts = []

        def read_namespaced_lease(self, _name, _namespace, **kwargs):
            self.timeouts.append(kwargs["_request_timeout"])
            raise __import__("kubernetes").client.exceptions.ApiException(404)

        def create_namespaced_lease(self, _namespace, body, **kwargs):
            self.timeouts.append(kwargs["_request_timeout"])
            body.metadata.resource_version = "1"
            body.metadata.uid = "lease-uid"
            return body

    api = API()
    adapter = KubernetesLeaseAPI(api, request_timeout_sec=2.5)
    assert adapter.read("ns", "lease") is None
    adapter.create_or_replace(
        "ns",
        "lease",
        LeaseState("holder", 1.0, 15.0, ""),
    )
    assert api.timeouts == [(2.5, 2.5), (2.5, 2.5)]


def test_run_state_lock_wait_has_bounded_timeout_and_owner_context():
    import threading

    from proberca.live.run_state import (
        KubernetesLeaseRunStateStore,
        LeaseRunStateLockTimeout,
        LeaseRunStateRecord,
    )

    record = LeaseRunStateRecord.initial(
        run_id="run",
        cluster_id="cluster",
        namespace_scope=("ns",),
        config_fingerprint="a" * 64,
        code_schema_version="1",
    )
    store = KubernetesLeaseRunStateStore(
        object(),
        namespace="ns",
        name="lease",
        initial_record=record,
        lease_duration_sec=15.0,
        clock=lambda: 1.0,
        annotation_max_bytes=4096,
        lock_timeout_sec=0.02,
    )
    acquired = threading.Event()
    release = threading.Event()

    def hold_lock():
        with store._lock:
            store._lock_holder = "lease-worker"
            acquired.set()
            release.wait(1.0)

    worker = threading.Thread(target=hold_lock)
    worker.start()
    assert acquired.wait(0.5)
    try:
        with pytest.raises(LeaseRunStateLockTimeout) as caught:
            store.read()
        assert caught.value.lock_name == "run-state-cas"
        assert caught.value.holder_thread == "lease-worker"
        assert caught.value.wait_duration_sec >= 0.02
    finally:
        release.set()
        worker.join(1.0)
        assert not worker.is_alive()


def test_live_cli_wires_run_state_lock_timeout_from_liveness_config():
    import inspect

    from proberca.cli import live

    source = "".join(inspect.getsource(live._run_live).split())
    assert (
        "lock_timeout_sec=(config.live_liveness.run_state_commit_timeout_sec)"
        in source
    )
    assert (
        "backlog_fatal_threshold=config.live_liveness.backlog_fatal_threshold"
        in source
    )


def test_runner_wires_critical_lock_timeouts_from_liveness_config():
    import inspect

    from proberca.live.runner import ProbeRCALiveRunner

    source = "".join(inspect.getsource(ProbeRCALiveRunner.__init__).split())
    assert "lock_timeout_sec=self.liveness_config.record_adaptation_timeout_sec" in source
    assert "lock_timeout_sec=self.liveness_config.retention_timeout_sec" in source
    assert "lock_timeout_sec=self.liveness_config.engine_process_timeout_sec" in source


def test_smoke_config_declares_every_live_liveness_field():
    from pathlib import Path

    import yaml

    path = Path("deploy/kubernetes/test/p11-smoke/proberca-live-configmap.yaml")
    manifest = yaml.safe_load(path.read_text(encoding="utf-8"))
    config = yaml.safe_load(manifest["data"]["config.yaml"])
    assert set(config["live_liveness"]) == set(LiveLivenessConfig.__dataclass_fields__)
    LiveLivenessConfig.from_dict(config["live_liveness"])


def test_controlled_stage_delay_is_generic_and_disabled_by_default():
    default = LiveLivenessConfig()
    assert not default.controlled_stage_delay_enabled
    assert default.controlled_stage_delay_stage == ""
    assert default.controlled_stage_delay_sec == 0.0

    configured = LiveLivenessConfig(
        freeze_revision_timeout_sec=0.01,
        controlled_stage_delay_enabled=True,
        controlled_stage_delay_stage="FREEZE_REVISION",
        controlled_stage_delay_sec=0.05,
    )
    configured.validate()
    assert configured.controlled_stage_delay_stage == LiveStage.FREEZE_REVISION.value


def test_controlled_stage_delay_uses_canonical_stage_timeout():
    from types import SimpleNamespace

    from proberca.live.coordinator import LiveCoordinatorState
    from proberca.live.executor import LiveStageTimeoutError
    from proberca.live.runner import ProbeRCALiveRunner

    class Coordinator:
        state = LiveCoordinatorState.LEADER_ACTIVE

        def begin_window(self, *_):
            return SimpleNamespace(sequence=24, working_engine=object())

    runner = ProbeRCALiveRunner(
        coordinator=Coordinator(),
        inventory=SimpleNamespace(ready=True, freeze=lambda _: object()),
        topology_builder=lambda *_: object(),
        metric_collector=lambda *_: ([object()], [object()]),
        window_adapter=lambda *_: object(),
        commit_payload_builder=lambda *_: {},
        liveness_config=LiveLivenessConfig(
            freeze_revision_timeout_sec=0.01,
            controlled_stage_delay_enabled=True,
            controlled_stage_delay_stage="FREEZE_REVISION",
            controlled_stage_delay_sec=0.05,
        ),
    )
    with pytest.raises(LiveStageTimeoutError) as caught:
        runner.process_window(SimpleNamespace(start_ns=10, end_ns=20))
    assert caught.value.stage is LiveStage.FREEZE_REVISION


def test_inventory_watcher_write_lock_has_bounded_timeout_and_owner_context():
    import threading

    from proberca.k8s.inventory import InventoryLockTimeout, KubernetesInventory

    inventory = KubernetesInventory(
        "cluster", required_kinds=("Pod",), stale_after_sec=60,
        lock_timeout_sec=0.02,
    )
    acquired = threading.Event()
    release = threading.Event()

    def hold_lock():
        with inventory._lock:
            inventory._lock_holder = "blocked-writer"
            acquired.set()
            release.wait(1.0)

    worker = threading.Thread(target=hold_lock)
    worker.start()
    assert acquired.wait(0.5)
    try:
        with pytest.raises(InventoryLockTimeout) as caught:
            inventory.mark_relisting("Pod", 100)
        assert caught.value.lock_name == "inventory-write"
        assert caught.value.holder_thread == "blocked-writer"
        assert caught.value.wait_duration_sec >= 0.02
    finally:
        release.set()
        worker.join(1.0)


def test_generation_payload_serialization_is_inside_prepare_deadline():
    import threading
    from types import SimpleNamespace

    from proberca.live.coordinator import LiveCoordinatorState
    from proberca.live.executor import LiveStageTimeoutError
    from proberca.live.runner import ProbeRCALiveRunner

    release = threading.Event()

    class Coordinator:
        state = LiveCoordinatorState.LEADER_ACTIVE

        def __init__(self):
            self.commits = []

        def begin_window(self, *_):
            return SimpleNamespace(sequence=24, working_engine=object())

        def run_engine(self, context, value):
            context.engine_result = value
            return value

        def prepare_generation(self, _context, **payload):
            return payload

        def commit(self, context, _generation):
            self.commits.append(context.sequence)

    coordinator = Coordinator()
    runner = ProbeRCALiveRunner(
        coordinator=coordinator,
        inventory=SimpleNamespace(ready=True, freeze=lambda _: object()),
        topology_builder=lambda *_: object(),
        metric_collector=lambda *_: ([object()], [object()]),
        window_adapter=lambda *_: object(),
        commit_payload_builder=lambda *_: release.wait(1.0) or {},
        liveness_config=LiveLivenessConfig(generation_prepare_timeout_sec=0.02),
    )
    try:
        with pytest.raises(LiveStageTimeoutError) as caught:
            runner.process_window(SimpleNamespace(start_ns=10, end_ns=20))
        assert caught.value.stage is LiveStage.PREPARE_GENERATION
        assert coordinator.commits == []
    finally:
        release.set()


def test_controlled_liveness_hooks_do_not_change_durable_config_identity():
    from dataclasses import replace

    from proberca.cli.live import _fingerprint
    from pathlib import Path

    import yaml

    from proberca.config import ProbeRCAConfig

    manifest = yaml.safe_load(Path(
        "deploy/kubernetes/test/p11-smoke/proberca-live-configmap.yaml",
    ).read_text(encoding="utf-8"))
    base = ProbeRCAConfig.from_dict(
        yaml.safe_load(manifest["data"]["config.yaml"]),
    )
    with_hook = replace(
        base,
        live_liveness=replace(
            base.live_liveness,
            controlled_stage_delay_enabled=True,
            controlled_stage_delay_stage="ENGINE_PROCESS",
            controlled_stage_delay_sec=1.0,
        ),
    )
    assert _fingerprint(base) == _fingerprint(with_hook)
    changed_timeout = replace(
        base,
        live_liveness=replace(
            base.live_liveness,
            engine_process_timeout_sec=base.live_liveness.engine_process_timeout_sec + 1,
        ),
    )
    assert _fingerprint(base) != _fingerprint(changed_timeout)
