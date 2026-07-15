"""Transactional live RunState stored atomically with Kubernetes Lease ownership."""

from __future__ import annotations

import contextlib
import hashlib
import json
import threading
import time
from dataclasses import dataclass, replace


RUN_STATE_SCHEMA_VERSION = "1.0"
RUN_STATE_RECORD_TYPE = "lease_run_state"
_SHA256_LENGTH = 64


class LeaseRunStateError(ValueError):
    """Lease-backed RunState is malformed or violates its commit contract."""


class LeaseRunStateConflict(LeaseRunStateError):
    """A Lease CAS context no longer owns the current transactional RunState."""


class LeaseRunStateLockTimeout(LeaseRunStateError):
    """The named RunState CAS lock was not acquired before its deadline."""

    def __init__(self, lock_name, holder_thread, wait_duration_sec):
        self.lock_name = str(lock_name)
        self.holder_thread = str(holder_thread or "unknown")
        self.wait_duration_sec = float(wait_duration_sec)
        super().__init__(
            f"lock timeout lock={self.lock_name} holder={self.holder_thread} "
            f"wait_sec={self.wait_duration_sec:.6f}"
        )


def _canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha(value) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def encode_run_state_annotation(
    record: "LeaseRunStateRecord", *, max_bytes: int,
) -> str:
    if not isinstance(record, LeaseRunStateRecord):
        raise TypeError("record must be LeaseRunStateRecord")
    if not isinstance(max_bytes, int) or max_bytes <= 0:
        raise LeaseRunStateError("annotation byte limit must be positive")
    payload = _canonical(record.to_dict())
    if len(payload.encode("utf-8")) > max_bytes:
        raise LeaseRunStateError("RunState annotation exceeds byte limit")
    return payload


def decode_run_state_annotation(
    payload: str, *, max_bytes: int,
) -> "LeaseRunStateRecord":
    if not isinstance(payload, str):
        raise LeaseRunStateError("RunState annotation must be text")
    if not isinstance(max_bytes, int) or max_bytes <= 0:
        raise LeaseRunStateError("annotation byte limit must be positive")
    if len(payload.encode("utf-8")) > max_bytes:
        raise LeaseRunStateError("RunState annotation exceeds byte limit")
    try:
        value = json.loads(payload)
    except Exception as error:
        raise LeaseRunStateError(f"RunState annotation is invalid: {error}") from error
    if not isinstance(value, dict):
        raise LeaseRunStateError("RunState annotation must contain an object")
    return LeaseRunStateRecord.from_dict(value)


def _fingerprint(value: str | None, field: str, *, nullable: bool = False) -> None:
    if value is None and nullable:
        return
    if not isinstance(value, str) or len(value) != _SHA256_LENGTH or any(
            item not in "0123456789abcdef" for item in value):
        raise LeaseRunStateError(f"{field} must be a SHA-256 fingerprint")


@dataclass(frozen=True)
class LeaseRunStateRecord:
    schema_version: str
    record_type: str
    run_id: str
    cluster_id: str
    namespace_scope: tuple[str, ...]
    leadership_epoch: int
    holder_fingerprint: str | None
    committed_sequence: int
    current_generation_id: str | None
    current_generation_fingerprint: str | None
    output_ledger_fingerprint: str | None
    output_bundle_fingerprint: str | None
    last_window_start_ns: int | None
    last_window_end_ns: int | None
    last_engine_state_fingerprint: str | None
    commit_fingerprint: str | None
    config_fingerprint: str
    code_schema_version: str
    record_fingerprint: str

    @classmethod
    def initial(cls, *, run_id: str, cluster_id: str, namespace_scope,
                config_fingerprint: str, code_schema_version: str) -> "LeaseRunStateRecord":
        record = cls(
            schema_version=RUN_STATE_SCHEMA_VERSION, record_type=RUN_STATE_RECORD_TYPE,
            run_id=str(run_id), cluster_id=str(cluster_id),
            namespace_scope=tuple(sorted(set(namespace_scope))), leadership_epoch=0,
            holder_fingerprint=None, committed_sequence=0, current_generation_id=None,
            current_generation_fingerprint=None, output_ledger_fingerprint=None,
            output_bundle_fingerprint=None, last_window_start_ns=None,
            last_window_end_ns=None, last_engine_state_fingerprint=None,
            commit_fingerprint=None, config_fingerprint=str(config_fingerprint),
            code_schema_version=str(code_schema_version), record_fingerprint="",
        )
        return record._validated_with_fingerprint()

    def _payload(self, *, include_record_fingerprint: bool) -> dict:
        payload = {
            "schema_version": self.schema_version, "record_type": self.record_type,
            "run_id": self.run_id, "cluster_id": self.cluster_id,
            "namespace_scope": list(self.namespace_scope),
            "leadership_epoch": self.leadership_epoch,
            "holder_fingerprint": self.holder_fingerprint,
            "committed_sequence": self.committed_sequence,
            "current_generation_id": self.current_generation_id,
            "current_generation_fingerprint": self.current_generation_fingerprint,
            "output_ledger_fingerprint": self.output_ledger_fingerprint,
            "output_bundle_fingerprint": self.output_bundle_fingerprint,
            "last_window_start_ns": self.last_window_start_ns,
            "last_window_end_ns": self.last_window_end_ns,
            "last_engine_state_fingerprint": self.last_engine_state_fingerprint,
            "commit_fingerprint": self.commit_fingerprint,
            "config_fingerprint": self.config_fingerprint,
            "code_schema_version": self.code_schema_version,
        }
        if include_record_fingerprint:
            payload["record_fingerprint"] = self.record_fingerprint
        return payload

    def _validated_with_fingerprint(self) -> "LeaseRunStateRecord":
        if self.schema_version != RUN_STATE_SCHEMA_VERSION or self.record_type != RUN_STATE_RECORD_TYPE:
            raise LeaseRunStateError("RunState schema or record type is invalid")
        if not self.run_id or not self.cluster_id or not self.namespace_scope:
            raise LeaseRunStateError("RunState identity is incomplete")
        if tuple(sorted(set(self.namespace_scope))) != self.namespace_scope:
            raise LeaseRunStateError("namespace_scope must be unique and sorted")
        if self.leadership_epoch < 0 or self.committed_sequence < 0:
            raise LeaseRunStateError("epoch and committed sequence must be non-negative")
        _fingerprint(self.config_fingerprint, "config_fingerprint")
        if not self.code_schema_version:
            raise LeaseRunStateError("code_schema_version is required")
        committed = self.committed_sequence > 0
        dependent = (
            self.current_generation_id, self.current_generation_fingerprint,
            self.output_ledger_fingerprint, self.output_bundle_fingerprint,
            self.last_engine_state_fingerprint, self.commit_fingerprint,
            self.last_window_start_ns, self.last_window_end_ns,
        )
        if committed and (not self.holder_fingerprint or any(item is None for item in dependent)):
            raise LeaseRunStateError("committed sequence requires complete generation identity")
        if not committed and any(item is not None for item in dependent):
            raise LeaseRunStateError("uncommitted RunState cannot reference generation data")
        if self.holder_fingerprint is not None:
            _fingerprint(self.holder_fingerprint, "holder_fingerprint")
        for name in (
                "current_generation_fingerprint", "output_ledger_fingerprint",
                "output_bundle_fingerprint", "last_engine_state_fingerprint",
                "commit_fingerprint"):
            _fingerprint(getattr(self, name), name, nullable=True)
        if committed and (self.last_window_start_ns is None or self.last_window_end_ns is None or
                          self.last_window_start_ns >= self.last_window_end_ns):
            raise LeaseRunStateError("committed RunState has invalid window bounds")
        expected = _sha(self._payload(include_record_fingerprint=False))
        if self.record_fingerprint and self.record_fingerprint != expected:
            raise LeaseRunStateError("RunState record fingerprint mismatch")
        return replace(self, record_fingerprint=expected)

    def with_holder(self, holder_fingerprint: str, leadership_epoch: int) -> "LeaseRunStateRecord":
        _fingerprint(holder_fingerprint, "holder_fingerprint")
        if leadership_epoch < self.leadership_epoch:
            raise LeaseRunStateError("leadership epoch cannot regress")
        return replace(self, holder_fingerprint=holder_fingerprint,
                       leadership_epoch=leadership_epoch, record_fingerprint="")._validated_with_fingerprint()

    def with_commit(self, *, holder_fingerprint: str, leadership_epoch: int, sequence: int,
                    generation_id: str, generation_fingerprint: str,
                    output_ledger_fingerprint: str, output_bundle_fingerprint: str,
                    engine_state_fingerprint: str, window_start_ns: int,
                    window_end_ns: int) -> "LeaseRunStateRecord":
        if sequence != self.committed_sequence + 1:
            raise LeaseRunStateError("committed sequence must advance exactly once")
        if leadership_epoch != self.leadership_epoch:
            raise LeaseRunStateError("commit leadership epoch does not match RunState")
        if holder_fingerprint != self.holder_fingerprint:
            raise LeaseRunStateError("commit holder does not match RunState")
        if not generation_id:
            raise LeaseRunStateError("generation_id is required")
        commit_fingerprint = _sha({
            "previous": self.record_fingerprint, "epoch": leadership_epoch,
            "holder": holder_fingerprint, "sequence": sequence,
            "generation": generation_id, "generation_fingerprint": generation_fingerprint,
            "ledger": output_ledger_fingerprint, "bundle": output_bundle_fingerprint,
            "engine": engine_state_fingerprint, "window": [window_start_ns, window_end_ns],
        })
        return replace(
            self, committed_sequence=sequence, current_generation_id=generation_id,
            current_generation_fingerprint=generation_fingerprint,
            output_ledger_fingerprint=output_ledger_fingerprint,
            output_bundle_fingerprint=output_bundle_fingerprint,
            last_engine_state_fingerprint=engine_state_fingerprint,
            last_window_start_ns=window_start_ns, last_window_end_ns=window_end_ns,
            commit_fingerprint=commit_fingerprint, record_fingerprint="",
        )._validated_with_fingerprint()

    def to_dict(self) -> dict:
        return self._payload(include_record_fingerprint=True)

    @classmethod
    def from_dict(cls, payload: dict) -> "LeaseRunStateRecord":
        fields = set(cls.__dataclass_fields__)
        if set(payload) != fields:
            raise LeaseRunStateError("RunState fields are invalid")
        result = cls(
            **{**payload, "namespace_scope": tuple(payload["namespace_scope"])})
        return result._validated_with_fingerprint()


@dataclass(frozen=True)
class LeaseFenceToken:
    holder_fingerprint: str
    leadership_epoch: int
    resource_version: str
    token_fingerprint: str
    lease_uid: str = ""
    lease_namespace: str = ""
    lease_name: str = ""
    expires_at: float = 0.0


@dataclass(frozen=True)
class LeaseRunStateSnapshot:
    record: LeaseRunStateRecord
    resource_version: str


@dataclass(frozen=True)
class CommitCASContext:
    token: LeaseFenceToken
    expected_sequence: int
    expected_generation_id: str | None
    expected_resource_version: str


class InMemoryLeaseRunStateStore:
    """Deterministic LeaseRunState CAS model used by transactional live tests."""

    def __init__(self, record: LeaseRunStateRecord):
        self._record = record
        self._resource_version = 0

    def read(self) -> LeaseRunStateSnapshot:
        return LeaseRunStateSnapshot(self._record, str(self._resource_version))

    @staticmethod
    def _holder(value: str) -> str:
        return hashlib.sha256(str(value).encode("utf-8")).hexdigest()

    def try_acquire(self, instance_fingerprint: str) -> LeaseFenceToken:
        holder = self._holder(instance_fingerprint)
        epoch = self._record.leadership_epoch
        if self._record.holder_fingerprint != holder:
            epoch += 1
            self._record = self._record.with_holder(holder, epoch)
            self._resource_version += 1
        token = LeaseFenceToken(
            holder_fingerprint=holder, leadership_epoch=epoch,
            resource_version=str(self._resource_version),
            token_fingerprint=_sha({"holder": holder, "epoch": epoch,
                                    "resource_version": self._resource_version}),
        )
        return token

    def renew(self, token: LeaseFenceToken) -> LeaseFenceToken:
        self._validate_token(token)
        self._resource_version += 1
        return LeaseFenceToken(
            holder_fingerprint=token.holder_fingerprint,
            leadership_epoch=token.leadership_epoch,
            resource_version=str(self._resource_version),
            token_fingerprint=token.token_fingerprint,
            lease_uid=token.lease_uid,
            lease_namespace=token.lease_namespace,
            lease_name=token.lease_name,
            expires_at=token.expires_at,
        )

    def _validate_token(self, token: LeaseFenceToken) -> None:
        if token.holder_fingerprint != self._record.holder_fingerprint or                 token.leadership_epoch != self._record.leadership_epoch:
            raise LeaseRunStateConflict("stale Lease epoch")

    def prepare_commit(self, token: LeaseFenceToken, *, expected_sequence: int,
                       expected_generation_id: str | None) -> CommitCASContext:
        self._validate_token(token)
        if expected_sequence != self._record.committed_sequence + 1:
            raise LeaseRunStateConflict("expected sequence is stale")
        if expected_generation_id != self._record.current_generation_id:
            raise LeaseRunStateConflict("expected generation is stale")
        return CommitCASContext(token, expected_sequence, expected_generation_id,
                                str(self._resource_version))

    def commit_generation(self, context: CommitCASContext,
                          candidate: LeaseRunStateRecord) -> LeaseRunStateSnapshot:
        self._validate_token(context.token)
        validated = LeaseRunStateRecord.from_dict(candidate.to_dict())
        if validated.record_fingerprint == self._record.record_fingerprint:
            return self.read()
        if validated.committed_sequence <= self._record.committed_sequence:
            raise LeaseRunStateConflict("conflicting duplicate commit")
        if context.expected_resource_version != str(self._resource_version):
            raise LeaseRunStateConflict("stale Lease resourceVersion")
        if context.expected_sequence != self._record.committed_sequence + 1 or                 context.expected_generation_id != self._record.current_generation_id:
            raise LeaseRunStateConflict("RunState changed before commit")
        if validated.committed_sequence != context.expected_sequence:
            raise LeaseRunStateConflict("candidate sequence mismatch")
        self._record = validated
        self._resource_version += 1
        return self.read()

    def force_takeover_for_test(self, instance_fingerprint: str) -> LeaseFenceToken:
        return self.try_acquire(instance_fingerprint)


RUN_STATE_SCHEMA_ANNOTATION = "proberca.io/run-state-schema"
RUN_STATE_ANNOTATION = "proberca.io/run-state"


class KubernetesLeaseRunStateStore:
    """Lease-backed owner, epoch, sequence and generation CAS authority."""

    def __init__(
        self,
        api,
        *,
        namespace: str,
        name: str,
        initial_record: LeaseRunStateRecord,
        lease_duration_sec: float,
        clock,
        annotation_max_bytes: int,
        lock_timeout_sec: float = 10.0,
    ):
        if not namespace or not name:
            raise LeaseRunStateError("Lease namespace and name are required")
        if lease_duration_sec <= 0:
            raise LeaseRunStateError("Lease duration must be positive")
        if annotation_max_bytes <= 0:
            raise LeaseRunStateError("annotation byte limit must be positive")
        self.api = api
        self.namespace = namespace
        self.name = name
        self.initial_record = LeaseRunStateRecord.from_dict(
            initial_record.to_dict(),
        )
        self.lease_duration_sec = float(lease_duration_sec)
        self.clock = clock
        self.annotation_max_bytes = int(annotation_max_bytes)
        if lock_timeout_sec <= 0:
            raise LeaseRunStateError("RunState lock timeout must be positive")
        self.lock_timeout_sec = float(lock_timeout_sec)
        self._lock = threading.RLock()
        self._lock_holder = None

    @contextlib.contextmanager
    def _locked(self, lock_name="run-state-cas"):
        started = time.monotonic()
        acquired = self._lock.acquire(timeout=self.lock_timeout_sec)
        waited = time.monotonic() - started
        if not acquired:
            raise LeaseRunStateLockTimeout(
                lock_name, self._lock_holder, waited,
            )
        previous_holder = self._lock_holder
        self._lock_holder = threading.current_thread().name
        try:
            yield
        finally:
            self._lock_holder = previous_holder
            self._lock.release()

    @staticmethod
    def _holder(value: str) -> str:
        return hashlib.sha256(str(value).encode("utf-8")).hexdigest()

    def _decode(self, lease) -> LeaseRunStateRecord:
        if lease is None:
            return self.initial_record
        annotations = dict(getattr(lease, "annotations", {}) or {})
        if annotations.get(RUN_STATE_SCHEMA_ANNOTATION) != RUN_STATE_SCHEMA_VERSION:
            raise LeaseRunStateError("Lease RunState schema annotation is invalid")
        payload = annotations.get(RUN_STATE_ANNOTATION)
        if payload is None:
            raise LeaseRunStateError("Lease RunState annotation is missing")
        record = decode_run_state_annotation(
            payload, max_bytes=self.annotation_max_bytes,
        )
        initial = self.initial_record
        if (
            record.run_id != initial.run_id
            or record.cluster_id != initial.cluster_id
            or record.namespace_scope != initial.namespace_scope
            or record.config_fingerprint != initial.config_fingerprint
            or record.code_schema_version != initial.code_schema_version
        ):
            raise LeaseRunStateError("Lease RunState identity mismatch")
        return record

    def _annotations(self, lease, record):
        annotations = dict(getattr(lease, "annotations", {}) or {})
        annotations[RUN_STATE_SCHEMA_ANNOTATION] = RUN_STATE_SCHEMA_VERSION
        annotations[RUN_STATE_ANNOTATION] = encode_run_state_annotation(
            record, max_bytes=self.annotation_max_bytes,
        )
        return annotations

    def _token(self, lease, record) -> LeaseFenceToken:
        fingerprint = _sha({
            "lease_uid": lease.lease_uid,
            "namespace": self.namespace,
            "name": self.name,
            "holder": record.holder_fingerprint,
            "epoch": record.leadership_epoch,
        })
        return LeaseFenceToken(
            holder_fingerprint=record.holder_fingerprint,
            leadership_epoch=record.leadership_epoch,
            resource_version=str(lease.resource_version),
            token_fingerprint=fingerprint,
            lease_uid=str(lease.lease_uid),
            lease_namespace=self.namespace,
            lease_name=self.name,
            expires_at=float(lease.renew_time + lease.duration),
        )

    def _validate(self, token, lease, record) -> None:
        if lease is None:
            raise LeaseRunStateConflict("Lease does not exist")
        if (
            token.lease_namespace != self.namespace
            or token.lease_name != self.name
            or token.lease_uid != str(lease.lease_uid)
            or token.holder_fingerprint != record.holder_fingerprint
            or token.leadership_epoch != record.leadership_epoch
            or self._holder(lease.holder) != token.holder_fingerprint
            or self.clock() >= lease.renew_time + lease.duration
        ):
            raise LeaseRunStateConflict("stale Lease epoch")

    def read(self) -> LeaseRunStateSnapshot:
        with self._locked():
            lease = self.api.read(self.namespace, self.name)
            record = self._decode(lease)
            return LeaseRunStateSnapshot(
                record,
                str(lease.resource_version) if lease is not None else "",
            )

    def try_acquire(self, instance_fingerprint: str) -> LeaseFenceToken:
        from .leader import LeaseState

        if not instance_fingerprint:
            raise LeaseRunStateError("instance fingerprint is required")
        with self._locked():
            lease = self.api.read(self.namespace, self.name)
            record = self._decode(lease)
            now = float(self.clock())
            available = (
                lease is None
                or lease.holder == instance_fingerprint
                or now >= lease.renew_time + lease.duration
            )
            if not available:
                raise LeaseRunStateConflict("Lease is held by another instance")
            same_epoch = (
                lease is not None
                and lease.holder == instance_fingerprint
                and now < lease.renew_time + lease.duration
            )
            epoch = record.leadership_epoch if same_epoch else record.leadership_epoch + 1
            holder = self._holder(instance_fingerprint)
            updated = record.with_holder(holder, epoch)
            transition = (
                lease.lease_transition if same_epoch
                else (lease.lease_transition if lease is not None else 0) + 1
            )
            acquire_time = lease.acquire_time if same_epoch else now
            candidate = LeaseState(
                instance_fingerprint,
                now,
                self.lease_duration_sec,
                lease.resource_version if lease is not None else "",
                lease.lease_uid if lease is not None else "",
                transition,
                self._annotations(lease, updated),
                acquire_time,
            )
            try:
                committed = self.api.create_or_replace(
                    self.namespace,
                    self.name,
                    candidate,
                    lease.resource_version if lease is not None else None,
                )
            except Exception as error:
                raise LeaseRunStateConflict(
                    "Lease acquisition resourceVersion conflict",
                ) from error
            return self._token(committed, updated)

    def renew(self, token: LeaseFenceToken) -> LeaseFenceToken:
        from .leader import LeaseState

        with self._locked():
            lease = self.api.read(self.namespace, self.name)
            record = self._decode(lease)
            self._validate(token, lease, record)
            candidate = LeaseState(
                lease.holder,
                float(self.clock()),
                self.lease_duration_sec,
                lease.resource_version,
                lease.lease_uid,
                lease.lease_transition,
                self._annotations(lease, record),
                lease.acquire_time,
            )
            try:
                committed = self.api.create_or_replace(
                    self.namespace,
                    self.name,
                    candidate,
                    lease.resource_version,
                )
            except Exception as error:
                raise LeaseRunStateConflict(
                    "Lease renew resourceVersion conflict",
                ) from error
            return self._token(committed, record)

    def prepare_commit(
        self,
        token: LeaseFenceToken,
        *,
        expected_sequence: int,
        expected_generation_id: str | None,
    ) -> CommitCASContext:
        with self._locked():
            lease = self.api.read(self.namespace, self.name)
            record = self._decode(lease)
            self._validate(token, lease, record)
            if expected_sequence != record.committed_sequence + 1:
                raise LeaseRunStateConflict("expected sequence is stale")
            if expected_generation_id != record.current_generation_id:
                raise LeaseRunStateConflict("expected generation is stale")
            return CommitCASContext(
                token,
                expected_sequence,
                expected_generation_id,
                str(lease.resource_version),
            )

    def commit_generation(
        self,
        context: CommitCASContext,
        candidate: LeaseRunStateRecord,
    ) -> LeaseRunStateSnapshot:
        from .leader import LeaseState

        with self._locked():
            lease = self.api.read(self.namespace, self.name)
            record = self._decode(lease)
            self._validate(context.token, lease, record)
            validated = LeaseRunStateRecord.from_dict(candidate.to_dict())
            if validated.record_fingerprint == record.record_fingerprint:
                return LeaseRunStateSnapshot(record, str(lease.resource_version))
            if validated.committed_sequence <= record.committed_sequence:
                raise LeaseRunStateConflict("conflicting duplicate commit")
            if str(lease.resource_version) != context.expected_resource_version:
                raise LeaseRunStateConflict("stale Lease resourceVersion")
            if (
                context.expected_sequence != record.committed_sequence + 1
                or context.expected_generation_id != record.current_generation_id
                or validated.committed_sequence != context.expected_sequence
            ):
                raise LeaseRunStateConflict("RunState changed before commit")
            updated_lease = LeaseState(
                lease.holder,
                float(self.clock()),
                self.lease_duration_sec,
                lease.resource_version,
                lease.lease_uid,
                lease.lease_transition,
                self._annotations(lease, validated),
                lease.acquire_time,
            )
            try:
                committed = self.api.create_or_replace(
                    self.namespace,
                    self.name,
                    updated_lease,
                    lease.resource_version,
                )
            except Exception as error:
                raise LeaseRunStateConflict(
                    "Lease commit resourceVersion conflict",
                ) from error
            return LeaseRunStateSnapshot(
                validated, str(committed.resource_version),
            )

    def release(self, token: LeaseFenceToken) -> None:
        from .leader import LeaseState

        with self._locked():
            lease = self.api.read(self.namespace, self.name)
            record = self._decode(lease)
            self._validate(token, lease, record)
            candidate = LeaseState(
                lease.holder,
                max(0.0, float(self.clock()) - self.lease_duration_sec - 1.0),
                self.lease_duration_sec,
                lease.resource_version,
                lease.lease_uid,
                lease.lease_transition,
                self._annotations(lease, record),
                lease.acquire_time,
            )
            try:
                self.api.create_or_replace(
                    self.namespace,
                    self.name,
                    candidate,
                    lease.resource_version,
                )
            except Exception as error:
                raise LeaseRunStateConflict(
                    "Lease release resourceVersion conflict",
                ) from error
