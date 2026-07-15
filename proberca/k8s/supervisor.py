"""Managed, persistent Kubernetes list/watch lifecycle."""
from __future__ import annotations

import threading
import time


class WatchSupervisorError(RuntimeError):
    """The inventory cannot provide a coherent live revision."""


class KubernetesWatchSupervisor:
    def __init__(self, inventory, watchers, *, clock_ns=time.time_ns):
        self.inventory = inventory
        self.watchers = tuple(watchers)
        self.clock_ns = clock_ns
        self._stop = threading.Event()
        self._condition = threading.Condition()
        self._states = {watcher.resource_kind: "starting" for watcher in self.watchers}
        self._errors = {}
        self._threads = []
        self._relist_count = 0
        self._reconnect_count = 0
        self._state_listeners = []

    def start(self):
        with self._condition:
            if self._threads:
                raise WatchSupervisorError("watch supervisor is already started")
            for watcher in self.watchers:
                thread = threading.Thread(
                    target=self._run_watcher, args=(watcher,),
                    name=f"proberca-watch-{watcher.resource_kind}", daemon=False)
                self._threads.append(thread)
                thread.start()

    def _run_watcher(self, watcher):
        try:
            watcher.run(self._stop, self.update_watcher_state)
        except Exception as error:
            self.update_watcher_state(watcher.resource_kind, "fatal", error)

    def update_watcher_state(self, kind, state, error=None):
        if state not in {"starting", "synchronized", "relisting", "reconnecting", "fatal", "stopped"}:
            raise WatchSupervisorError(f"invalid watcher state={state}")
        with self._condition:
            previous = self._states.get(kind)
            self._states[kind] = state
            if state == "reconnecting":
                self._reconnect_count += 1
            if state == "relisting" and previous != "relisting":
                self._relist_count += 1
            if state == "fatal" and error is not None:
                self._errors[kind] = error
            self._condition.notify_all()
            listeners = tuple(self._state_listeners)
        for listener in listeners:
            listener(kind, state)

    def add_state_listener(self, listener):
        if not callable(listener):
            raise TypeError("watch state listener must be callable")
        with self._condition:
            self._state_listeners.append(listener)

    def wait_until_synchronized(self, timeout_sec):
        deadline = time.monotonic() + timeout_sec
        with self._condition:
            while True:
                if self._errors:
                    detail = ",".join(
                        f"{kind}={type(error).__name__}:{error}"
                        for kind, error in sorted(self._errors.items()))
                    raise WatchSupervisorError("watcher fatal: " + detail)
                if self._states and all(value == "synchronized" for value in self._states.values()):
                    return True
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)

    def freeze_revision(self, observed_at_ns):
        with self._condition:
            if self._errors or any(value == "fatal" for value in self._states.values()):
                raise WatchSupervisorError("watch supervisor is fatal")
            relisting = sorted(kind for kind, value in self._states.items()
                               if value in {"starting", "relisting"})
            if relisting:
                raise WatchSupervisorError(f"watch supervisor relisting: {relisting}")
        revision = self.inventory.freeze(observed_at_ns)
        if not revision.synchronized:
            raise WatchSupervisorError("inventory is not synchronized")
        if revision.stale:
            raise WatchSupervisorError("inventory is stale")
        return revision

    def health_snapshot(self):
        with self._condition:
            return {
                "synchronized": bool(self._states) and all(
                    state == "synchronized" for state in self._states.values()),
                "fatal": bool(self._errors),
                "states": dict(sorted(self._states.items())),
                "thread_count": sum(thread.is_alive() for thread in self._threads),
                "relist_count": self._relist_count,
                "reconnect_count": self._reconnect_count,
            }

    def request_relist(self, resource_kind, reason):
        if not isinstance(reason, str) or not reason.strip():
            raise WatchSupervisorError("relist reason is required")
        matches = [watcher for watcher in self.watchers
                   if watcher.resource_kind == resource_kind]
        if len(matches) != 1:
            raise WatchSupervisorError(
                f"unknown or duplicate watcher kind={resource_kind}")
        self.update_watcher_state(resource_kind, "relisting", None)
        matches[0].request_relist(reason)

    def stop(self):
        self._stop.set()
        for watcher in self.watchers:
            cancel = getattr(watcher, "stop", None)
            if cancel is not None:
                cancel()

    def join(self, timeout_sec):
        deadline = time.monotonic() + timeout_sec
        for thread in self._threads:
            thread.join(max(0.0, deadline - time.monotonic()))
        alive = [thread.name for thread in self._threads if thread.is_alive()]
        if alive:
            raise WatchSupervisorError(f"watcher threads did not stop: {alive}")
