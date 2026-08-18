from __future__ import annotations

from collections import deque
from concurrent.futures import Future
from types import SimpleNamespace
import threading
import time

import pytest

from proberca.dataplane.primitive_exporter import FinalPrimitiveExporter
from proberca.dataplane.collector import FinalLiveCollectionRunner
from proberca.dataplane.raw import RawCollectionError
from proberca.dataplane.sources import PrometheusPrimitiveSource


def _pipeline_exporter(*, max_pending: int = 4):
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter.config = SimpleNamespace(
        snapshot_period_sec=1,
        acquisition_max_pending=max_pending,
        publish_queue_max_pending=max_pending,
        publish_visibility_sec=0.25,
    )
    exporter._stop = threading.Event()
    exporter._lock = threading.Lock()
    exporter._snapshot = ""
    exporter._snapshot_ns = 0
    exporter._last_error = None
    exporter.wall_clock_ns = time.time_ns
    exporter._snapshot_deadline_misses_total = 0
    exporter._ensure_pipeline_state()
    return exporter


def _pending(target_ns: int, inventory: object | None = None):
    return SimpleNamespace(context=SimpleNamespace(
        target_timestamp_ns=target_ns,
        inventory=inventory,
        started_perf_ns=time.perf_counter_ns(),
        acquisition_lag_ns=0,
    ))


def test_ordered_assembly_waits_for_slow_earlier_target():
    exporter = _pipeline_exporter()
    first = _pending(1_000_000_000)
    second = _pending(2_000_000_000)
    gates = {
        first.context.target_timestamp_ns: threading.Event(),
        second.context.target_timestamp_ns: threading.Event(),
    }
    published = []

    def complete(item):
        assert gates[item.context.target_timestamp_ns].wait(timeout=2)
        return SimpleNamespace(context=item.context)

    exporter._complete_raw_acquisition = complete
    exporter._assemble_raw_acquisition = lambda raw: (
        str(raw.context.target_timestamp_ns), {}
    )

    def publish(**kwargs):
        published.append(kwargs["target_ns"])
        if len(published) == 2:
            exporter._stop.set()

    exporter._publish_ordered = publish
    exporter._pending_acquisitions.extend((first, second))
    assembly = threading.Thread(target=exporter._assembly_loop)
    publication = threading.Thread(target=exporter._publication_loop)
    assembly.start()
    publication.start()
    gates[2_000_000_000].set()
    time.sleep(0.05)
    assert published == []
    gates[1_000_000_000].set()
    assembly.join(timeout=2)
    publication.join(timeout=2)
    assert not assembly.is_alive()
    assert not publication.is_alive()
    assert published == [1_000_000_000, 2_000_000_000]


def test_scheduler_fails_closed_at_bounded_pending_capacity():
    clock = {"ns": 100_000_000}

    class Stop:
        def is_set(self):
            return False

        def wait(self, seconds):
            clock["ns"] += int(seconds * 1_000_000_000)
            return False

    exporter = _pipeline_exporter(max_pending=2)
    exporter._stop = Stop()
    exporter.wall_clock_ns = lambda: clock["ns"]
    exporter._pending_acquisitions = deque((object(), object()))
    exporter._snapshot_loop()
    assert exporter._acquisition_backpressure_total == 1
    assert exporter._pipeline_failed.is_set()
    assert exporter._last_error.startswith(
        "primitive_acquisition_backpressure:"
    )


def test_assembly_fails_closed_at_bounded_publish_capacity():
    exporter = _pipeline_exporter(max_pending=2)
    pending = _pending(1_000_000_000)
    exporter._pending_acquisitions.append(pending)
    exporter._publish_queue.extend((object(), object()))
    exporter._complete_raw_acquisition = lambda item: SimpleNamespace(
        context=item.context
    )
    exporter._assemble_raw_acquisition = lambda _raw: ("snapshot", {})
    exporter._assembly_loop()
    assert exporter._pipeline_failed.is_set()
    assert "primitive_publish_backpressure" in exporter._last_error


def test_scheduler_fails_closed_when_an_epoch_target_was_really_missed():
    clock = {"ns": 100_000_000}

    class Stop:
        def is_set(self):
            return False

        def wait(self, seconds):
            # Simulate host scheduling that wakes more than one whole period
            # late; unlike a slow source Future, this target was never started.
            clock["ns"] += int(seconds * 1_000_000_000) + 1_000_000_000
            return False

    exporter = _pipeline_exporter()
    exporter._stop = Stop()
    exporter.wall_clock_ns = lambda: clock["ns"]
    exporter._launch_target = lambda _target: pytest.fail(
        "a truly missed target must not be fabricated"
    )
    exporter._snapshot_loop()
    assert exporter._missing_targets_total == 1
    assert exporter._snapshot_deadline_misses_total == 1
    assert exporter._pipeline_failed.is_set()


def test_launch_starts_beyla_and_all_other_raw_sources_immediately():
    exporter = _pipeline_exporter()
    calls = []

    class RecordingExecutor:
        def __init__(self, channel):
            self.channel = channel

        def submit(self, function, *args):
            calls.append((self.channel, function, args))
            return Future()

    exporter._beyla_executor = RecordingExecutor("beyla")
    exporter._raw_source_executor = RecordingExecutor("raw")
    exporter.config = SimpleNamespace(
        snapshot_period_sec=1,
        acquisition_max_pending=4,
        publish_queue_max_pending=4,
        publish_visibility_sec=0.25,
        node_exporter_url="http://node/metrics",
    )
    pod = SimpleNamespace(container_id="dns")
    inventory = SimpleNamespace(coredns_pods=(pod,))
    context = SimpleNamespace(
        inventory=inventory,
        cgroup_paths=(("container", SimpleNamespace()),),
        cgroup_identity=((123, SimpleNamespace()),),
    )
    pending = exporter._launch_raw_acquisition(context)
    assert pending.context is context
    assert [channel for channel, _function, _args in calls].count("beyla") == 1
    assert [channel for channel, _function, _args in calls].count("raw") == 5


def test_late_target_keeps_its_frozen_inventory_during_assembly():
    exporter = _pipeline_exporter()
    inventory_t = object()
    inventory_next = object()
    first = _pending(1_000_000_000, inventory_t)
    second = _pending(2_000_000_000, inventory_next)
    observed = []
    exporter._complete_raw_acquisition = lambda item: SimpleNamespace(
        context=item.context
    )

    def assemble(raw):
        observed.append(raw.context.inventory)
        return "snapshot", {}

    exporter._assemble_raw_acquisition = assemble

    def publish(**_kwargs):
        if len(observed) == 2:
            exporter._stop.set()

    exporter._publish_ordered = publish
    exporter._pending_acquisitions.extend((first, second))
    assembly = threading.Thread(target=exporter._assembly_loop)
    publication = threading.Thread(target=exporter._publication_loop)
    assembly.start()
    publication.start()
    assembly.join(timeout=2)
    publication.join(timeout=2)
    assert observed == [inventory_t, inventory_next]


def test_publication_enforces_visibility_before_overwrite():
    exporter = _pipeline_exporter()
    exporter.wall_clock_ns = lambda: 3_000_000_000
    raw = SimpleNamespace(context=SimpleNamespace(acquisition_lag_ns=0,
                                                  started_perf_ns=0),
                          completed_perf_ns=0)
    started = time.perf_counter()
    exporter._publish_ordered(
        target_ns=1_000_000_000,
        rendered="first",
        raw=raw,
        stage_durations_ns={},
    )
    exporter._publish_ordered(
        target_ns=2_000_000_000,
        rendered="second",
        raw=raw,
        stage_durations_ns={},
    )
    assert time.perf_counter() - started >= 0.24
    assert exporter._snapshot == "second"
    assert exporter._snapshot_ns == 2_000_000_000


def test_publication_rejects_out_of_order_target():
    exporter = _pipeline_exporter()
    exporter._last_published_target_ns = 2_000_000_000
    raw = SimpleNamespace(context=SimpleNamespace(acquisition_lag_ns=0,
                                                  started_perf_ns=0),
                          completed_perf_ns=0)
    with pytest.raises(RawCollectionError, match="target order"):
        exporter._publish_ordered(
            target_ns=1_000_000_000,
            rendered="old",
            raw=raw,
            stage_durations_ns={},
        )


def test_raw_acquisition_completion_does_not_mutate_persistent_state():
    exporter = _pipeline_exporter()
    exporter._active_task_ns = {"container": 3.0}
    exporter._active_thread_ns = {"container": 5.0}
    exporter._futex_wait_ns = {"container": 7.0}
    exporter._qdisc_drop_state = {"veth0": (2.0, 4.0)}
    exporter._service_sample_high_water = {"service": object()}
    exporter._edge_sample_high_water = {"edge": object()}
    before = (
        dict(exporter._active_task_ns),
        dict(exporter._active_thread_ns),
        dict(exporter._futex_wait_ns),
        dict(exporter._qdisc_drop_state),
        dict(exporter._service_sample_high_water),
        dict(exporter._edge_sample_high_water),
    )
    futures = []
    for result in ((), (), (), (), ()):
        future = Future()
        future.set_result((result, 1))
        futures.append(future)
    pending = SimpleNamespace(
        context=_pending(1_000_000_000).context,
        beyla_future=futures[0], bpf_future=futures[1],
        node_future=futures[2], cgroup_future=futures[3],
        qdisc_future=futures[4], coredns_futures=(),
    )
    FinalPrimitiveExporter._complete_raw_acquisition(pending)
    after = (
        exporter._active_task_ns,
        exporter._active_thread_ns,
        exporter._futex_wait_ns,
        exporter._qdisc_drop_state,
        exporter._service_sample_high_water,
        exporter._edge_sample_high_water,
    )
    assert after == before


def test_prometheus_final_target_wait_requires_exact_sentinel_timestamp():
    target_ns = 10_000_000_000

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [{
                        "metric": {"cluster_id": "cluster"},
                        "values": [[10.0, "1"]],
                    }],
                },
            }

    calls = []
    source = PrometheusPrimitiveSource.__new__(PrometheusPrimitiveSource)
    source.config = SimpleNamespace(
        base_url="http://prometheus",
        timeout_sec=1.0,
        reject_warnings=True,
        final_target_wait_timeout_sec=1.0,
        sentinel_poll_interval_sec=0.05,
    )
    source.session = SimpleNamespace(
        get=lambda url, **kwargs: calls.append((url, kwargs)) or Response()
    )
    source.last_sentinel_wait_stats = {}
    source.wait_for_target_timestamp(
        target_timestamp_ns=target_ns, cluster_id="cluster"
    )
    assert calls[0][1]["params"]["start"] == "10.000000000"
    assert calls[0][1]["params"]["end"] == "10.000000000"
    assert source.last_sentinel_wait_stats["target_timestamp_ns"] == target_ns


def test_live_runner_passes_exact_final_boundary_to_primitive_waiter():
    calls = []
    runner = FinalLiveCollectionRunner.__new__(FinalLiveCollectionRunner)
    runner.config = SimpleNamespace(cluster_id="cluster")
    runner.primitive_source = SimpleNamespace(
        wait_for_target_timestamp=lambda **kwargs: calls.append(kwargs)
    )
    runner._wait_for_primitive_target(12_000_000_000)
    assert calls == [{
        "target_timestamp_ns": 12_000_000_000,
        "cluster_id": "cluster",
    }]
