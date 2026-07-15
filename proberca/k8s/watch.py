"""List-then-watch state machine with opaque resourceVersion handling."""
from __future__ import annotations

import time
import threading

from .contracts import KubernetesWatchEvent


class WatchExpiredError(RuntimeError):
    """Kubernetes returned 410 Gone and requires a relist."""


class KubernetesListWatcher:
    def __init__(self, resource_kind, inventory, list_call, watch_call, *,
                 reconnect_initial_sec=1.0, reconnect_max_sec=30.0,
                 clock_ns=time.time_ns, sleep=time.sleep):
        self.resource_kind = resource_kind
        self.inventory = inventory
        self.list_call = list_call
        self.watch_call = watch_call
        self.resource_version: str | None = None
        self.relist_generation = 0
        self.stream_id = f"{resource_kind}-watch-0"
        self.reconnect_initial_sec = reconnect_initial_sec
        self.reconnect_max_sec = reconnect_max_sec
        self.clock_ns = clock_ns
        self.sleep = sleep
        self._active_stream = None
        self._relist_requested = threading.Event()
        self._relist_reason = None

    def synchronize(self, observed_at_ns: int) -> None:
        objects, resource_version = self.list_call()
        self.inventory.replace_kind(
            self.resource_kind, list(objects), str(resource_version), observed_at_ns,
            self.stream_id)
        self.resource_version = str(resource_version)

    def _apply_raw_event(self, raw_event, observed_at_ns: int) -> None:
        if raw_event.get("type") == "BOOKMARK":
            metadata = (raw_event.get("object") or {}).get("metadata") or {}
            resource_version = str(metadata.get("resourceVersion") or "")
            self.inventory.apply_bookmark(
                self.resource_kind, resource_version, observed_at_ns,
                self.stream_id,
            )
            self.resource_version = resource_version
            return
        event = KubernetesWatchEvent.from_raw(
            raw_event["type"], raw_event["object"], observed_at_ns,
            self.stream_id, self.relist_generation)
        self.inventory.apply_event(event)
        self.resource_version = event.object_ref.resource_version

    def consume_once(self, observed_at_ns: int) -> None:
        if self.resource_version is None:
            self.synchronize(observed_at_ns)
        try:
            events = self.watch_call(self.resource_version)
            for raw_event in events:
                self._apply_raw_event(raw_event, observed_at_ns)
        except WatchExpiredError:
            self.inventory.mark_relisting(self.resource_kind, observed_at_ns)
            self.relist_generation += 1
            self.stream_id = f"{self.resource_kind}-watch-{self.relist_generation}"
            self.synchronize(observed_at_ns)

    def run(self, stop_event, state_callback) -> None:
        self.synchronize(self.clock_ns())
        state_callback(self.resource_kind, "synchronized", None)
        delay = self.reconnect_initial_sec
        while not stop_event.is_set():
            try:
                events = self.watch_call(self.resource_version)
                self._active_stream = events
                for raw_event in events:
                    if stop_event.is_set():
                        break
                    self._apply_raw_event(raw_event, self.clock_ns())
                if self._relist_requested.is_set() and not stop_event.is_set():
                    self._perform_relist(state_callback)
                    delay = self.reconnect_initial_sec
                    continue
                delay = self.reconnect_initial_sec
                if not stop_event.is_set():
                    state_callback(self.resource_kind, "reconnecting", None)
                    state_callback(self.resource_kind, "synchronized", None)
            except WatchExpiredError:
                self._relist_reason = "resource_version_expired"
                self._perform_relist(state_callback)
                delay = self.reconnect_initial_sec
            except (ValueError, TypeError) as error:
                state_callback(self.resource_kind, "fatal", error)
                raise
            except Exception as error:
                if stop_event.is_set():
                    break
                state_callback(self.resource_kind, "reconnecting", error)
                stop_event.wait(delay)
                delay = min(self.reconnect_max_sec, max(delay * 2, self.reconnect_initial_sec))
        state_callback(self.resource_kind, "stopped", None)

    def _perform_relist(self, state_callback) -> None:
        state_callback(self.resource_kind, "relisting", None)
        self.inventory.mark_relisting(self.resource_kind, self.clock_ns())
        self.relist_generation += 1
        self.stream_id = f"{self.resource_kind}-watch-{self.relist_generation}"
        self.synchronize(self.clock_ns())
        self._relist_requested.clear()
        self._relist_reason = None
        state_callback(self.resource_kind, "synchronized", None)

    def request_relist(self, reason) -> None:
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("relist reason is required")
        self._relist_reason = reason
        self._relist_requested.set()
        stream = self._active_stream
        stop = getattr(stream, "stop", None)
        if stop is not None:
            stop()

    def stop(self) -> None:
        stream = self._active_stream
        stop = getattr(stream, "stop", None)
        if stop is not None:
            stop()
