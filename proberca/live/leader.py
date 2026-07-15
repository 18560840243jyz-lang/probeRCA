"""Lease coordination with stable leadership epochs and write fencing."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum


class LeadershipFenceError(RuntimeError):
    """A durable operation was attempted without the active Lease epoch."""


class LeadershipState(str, Enum):
    STANDBY = "STANDBY"
    ACQUIRING = "ACQUIRING"
    LEADER_INITIALIZING = "LEADER_INITIALIZING"
    LEADER_ACTIVE = "LEADER_ACTIVE"
    LEADER_DRAINING = "LEADER_DRAINING"
    LOST = "LOST"


@dataclass
class LeaseState:
    holder: str
    renew_time: float
    duration: float
    resource_version: str
    lease_uid: str = ""
    lease_transition: int = 0
    annotations: dict[str, str] = field(default_factory=dict)
    acquire_time: float = 0.0


@dataclass(frozen=True)
class LeaseFenceToken:
    lease_namespace: str
    lease_name: str
    lease_uid: str
    holder_identity_fingerprint: str
    lease_transition: int
    acquire_time: float
    acquisition_resource_version: str
    token_fingerprint: str

    @classmethod
    def create(cls, *, namespace, name, lease_uid, holder_identity,
               lease_transition, acquire_time, resource_version):
        holder_fingerprint = hashlib.sha256(holder_identity.encode()).hexdigest()[:24]
        payload = {
            "lease_namespace": namespace,
            "lease_name": name,
            "lease_uid": lease_uid,
            "holder_identity_fingerprint": holder_fingerprint,
            "lease_transition": int(lease_transition),
            "acquire_time": float(acquire_time),
            "acquisition_resource_version": str(resource_version),
        }
        fingerprint = hashlib.sha256(json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        ).encode()).hexdigest()
        return cls(**payload, token_fingerprint=fingerprint)


class InMemoryLeaseAPI:
    """Deterministic Lease semantics used by tests, not a production fallback."""
    def __init__(self):
        self.value: LeaseState | None = None
        self.version = 0
        self.uid = "in-memory-lease"

    def read(self, namespace, name):
        return self.value

    def create_or_replace(self, namespace, name, value, expected_version=None):
        if expected_version is not None and self.value is not None \
                and self.value.resource_version != expected_version:
            raise RuntimeError("lease resourceVersion conflict")
        self.version += 1
        value.resource_version = str(self.version)
        value.lease_uid = self.value.lease_uid if self.value else self.uid
        self.value = value
        return value


class KubernetesLeaseAPI:
    """coordination.k8s.io/v1 Lease adapter using resourceVersion fencing."""
    def __init__(self, api, *, request_timeout_sec: float = 30.0):
        if request_timeout_sec <= 0:
            raise ValueError("Lease API request timeout must be positive")
        self.api = api
        self.request_timeout_sec = float(request_timeout_sec)
        self._request_timeout = (
            self.request_timeout_sec, self.request_timeout_sec)

    @classmethod
    def from_kubernetes_config(cls, settings):
        from urllib.parse import urlparse
        from kubernetes import client, config
        if settings.in_cluster:
            config.load_incluster_config()
        else:
            config.load_kube_config(
                config_file=settings.kubeconfig_path, context=settings.context)
        configuration = client.Configuration.get_default_copy()
        host = urlparse(configuration.host).hostname
        no_proxy = [item for item in (configuration.no_proxy or "").split(",") if item]
        if host and host not in no_proxy:
            no_proxy.append(host)
        configuration.no_proxy = ",".join(no_proxy)
        return cls(
            client.CoordinationV1Api(client.ApiClient(configuration)),
            request_timeout_sec=settings.watch_timeout_sec,
        )

    def read(self, namespace, name):
        from kubernetes.client.exceptions import ApiException
        try:
            value = self.api.read_namespaced_lease(
                name, namespace, _request_timeout=self._request_timeout,
            )
        except ApiException as error:
            if error.status == 404:
                return None
            raise
        spec = value.spec
        renew = spec.renew_time.timestamp() if spec.renew_time else 0.0
        return LeaseState(
            spec.holder_identity or "", renew,
            float(spec.lease_duration_seconds or 0),
            value.metadata.resource_version,
            str(value.metadata.uid or ""), int(spec.lease_transitions or 0),
            dict(value.metadata.annotations or {}),
            spec.acquire_time.timestamp() if spec.acquire_time else 0.0)

    def create_or_replace(self, namespace, name, value, expected_version=None):
        from datetime import datetime, timezone
        from kubernetes import client
        body = client.V1Lease(
            metadata=client.V1ObjectMeta(
                name=name, namespace=namespace, resource_version=expected_version,
                annotations=dict(value.annotations)),
            spec=client.V1LeaseSpec(
                holder_identity=value.holder,
                lease_duration_seconds=int(value.duration),
                lease_transitions=int(value.lease_transition),
                renew_time=datetime.fromtimestamp(value.renew_time, timezone.utc),
                acquire_time=(datetime.fromtimestamp(value.acquire_time, timezone.utc)
                              if value.acquire_time else None)))
        if expected_version is None:
            result = self.api.create_namespaced_lease(
                namespace, body, _request_timeout=self._request_timeout,
            )
        else:
            result = self.api.replace_namespaced_lease(
                name, namespace, body,
                _request_timeout=self._request_timeout,
            )
        value.resource_version = result.metadata.resource_version
        value.lease_uid = str(result.metadata.uid or value.lease_uid)
        value.lease_transition = int(result.spec.lease_transitions or 0)
        return value


class LeaseCoordinator:
    WRITE_OPERATIONS = frozenset({
        "engine_begin", "engine_complete", "output_publish",
        "generation_prepare", "generation_publish", "current_replace",
        "sequence_commit", "retention_cleanup",
    })

    def __init__(self, api, config, identity: str, *, clock):
        config.validate()
        if not identity:
            raise ValueError("unique Lease identity is required")
        self.api = api
        self.config = config
        self.identity = identity
        self.clock = clock
        self.state = LeadershipState.STANDBY
        self.fence_token: LeaseFenceToken | None = None
        self.loss_reason: str | None = None

    @property
    def is_leader(self):
        return self.state == LeadershipState.LEADER_ACTIVE

    @property
    def can_commit(self):
        return self.state == LeadershipState.LEADER_ACTIVE

    def _current_matches(self, lease, token):
        return bool(
            lease is not None and token is not None
            and lease.holder == self.identity
            and lease.lease_uid == token.lease_uid
            and lease.lease_transition == token.lease_transition
            and self.clock() < lease.renew_time + lease.duration)

    def acquire(self):
        if not self.config.enabled:
            self.state = LeadershipState.LEADER_INITIALIZING
            self.fence_token = LeaseFenceToken.create(
                namespace="local", name="single-instance", lease_uid="local",
                holder_identity=self.identity, lease_transition=1,
                acquire_time=self.clock(), resource_version="1")
            return self.fence_token
        self.state = LeadershipState.ACQUIRING
        now = self.clock()
        lease = self.api.read(self.config.lease_namespace, self.config.lease_name)
        available = lease is None or lease.holder == self.identity or \
            now >= lease.renew_time + lease.duration
        if not available:
            self.state = LeadershipState.STANDBY
            self.fence_token = None
            return None
        same_epoch = lease is not None and lease.holder == self.identity \
            and now < lease.renew_time + lease.duration
        transition = (lease.lease_transition if same_epoch else
                      ((lease.lease_transition if lease else 0) + 1))
        expected = lease.resource_version if lease else None
        value = LeaseState(
            self.identity, now, self.config.lease_duration_sec, expected or "",
            lease.lease_uid if lease else "", transition)
        try:
            value = self.api.create_or_replace(
                self.config.lease_namespace, self.config.lease_name, value, expected)
        except Exception:
            self.state = LeadershipState.STANDBY
            self.fence_token = None
            return None
        if same_epoch and self.fence_token is not None:
            token = self.fence_token
        else:
            token = LeaseFenceToken.create(
                namespace=self.config.lease_namespace,
                name=self.config.lease_name, lease_uid=value.lease_uid,
                holder_identity=self.identity, lease_transition=value.lease_transition,
                acquire_time=now, resource_version=value.resource_version)
        self.fence_token = token
        self.state = LeadershipState.LEADER_INITIALIZING
        self.loss_reason = None
        return token

    def activate(self, token):
        if self.state != LeadershipState.LEADER_INITIALIZING or \
                token != self.fence_token or not self.validate_fence(
                    token, require_active=False):
            self.lose("activation_fence_invalid")
            raise LeadershipFenceError("Lease fence is invalid during activation")
        self.state = LeadershipState.LEADER_ACTIVE

    def try_acquire(self) -> bool:
        if self.state == LeadershipState.LEADER_ACTIVE:
            return self.renew()
        token = self.acquire()
        if token is None:
            return False
        self.activate(token)
        return True

    def renew(self) -> bool:
        token = self.fence_token
        if self.state not in {
                LeadershipState.LEADER_ACTIVE,
                LeadershipState.LEADER_INITIALIZING} or token is None:
            return False
        lease = self.api.read(self.config.lease_namespace, self.config.lease_name)
        if not self._current_matches(lease, token):
            self.lose("lease_epoch_changed")
            return False
        value = LeaseState(
            self.identity, self.clock(), self.config.lease_duration_sec,
            lease.resource_version, lease.lease_uid, lease.lease_transition)
        try:
            self.api.create_or_replace(
                self.config.lease_namespace, self.config.lease_name,
                value, lease.resource_version)
        except Exception:
            self.lose("lease_renew_failed")
            return False
        return True

    def validate_fence(self, token, *, require_active=True):
        if token is None or token != self.fence_token:
            return False
        if require_active and self.state != LeadershipState.LEADER_ACTIVE:
            return False
        if not self.config.enabled:
            return self.state in {
                LeadershipState.LEADER_INITIALIZING,
                LeadershipState.LEADER_ACTIVE,
                LeadershipState.LEADER_DRAINING,
            }
        lease = self.api.read(self.config.lease_namespace, self.config.lease_name)
        return self._current_matches(lease, token)

    def authorize(self, operation, token):
        if operation not in self.WRITE_OPERATIONS:
            raise LeadershipFenceError(f"unknown fenced operation: {operation}")
        if not self.validate_fence(token, require_active=True):
            raise LeadershipFenceError(
                f"active Lease fence required for operation={operation}")

    def begin_draining(self):
        if self.state == LeadershipState.LEADER_ACTIVE:
            self.state = LeadershipState.LEADER_DRAINING

    def lose(self, reason="lease_lost") -> None:
        self.state = LeadershipState.LOST
        self.loss_reason = str(reason)
        self.fence_token = None
