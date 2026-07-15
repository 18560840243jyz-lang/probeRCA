"""Single transactional coordinator for the P11 live commit path."""

from __future__ import annotations

import hashlib
import json
import secrets
from dataclasses import dataclass
from enum import Enum

from .generation import ImmutableGeneration, ImmutableGenerationStore
from .run_state import (
    CommitCASContext,
    LeaseRunStateConflict,
    LeaseRunStateSnapshot,
)


class LiveCoordinatorState(str, Enum):
    STANDBY = "STANDBY"
    ACQUIRING = "ACQUIRING"
    LEADER_RECOVERING = "LEADER_RECOVERING"
    LEADER_ACTIVE = "LEADER_ACTIVE"
    LEADER_DRAINING = "LEADER_DRAINING"
    COMMITTED_OUTPUT_DEGRADED = "COMMITTED_OUTPUT_DEGRADED"
    LOST = "LOST"
    FATAL = "FATAL"


class CommittedOutputDegradedError(RuntimeError):
    """RunState committed successfully but its derived output view failed."""


@dataclass(frozen=True)
class WindowAttemptIdentity:
    sequence: int
    window_start_ns: int
    window_end_ns: int
    attempt_index: int
    leadership_epoch_fingerprint: str
    runner_instance_fingerprint: str
    previous_generation_fingerprint: str | None

    def __post_init__(self) -> None:
        if self.sequence < 1:
            raise ValueError("attempt sequence must be positive")
        if self.window_start_ns >= self.window_end_ns:
            raise ValueError("attempt window bounds are invalid")
        if self.attempt_index < 1:
            raise ValueError("attempt index must be positive")
        if not self.leadership_epoch_fingerprint:
            raise ValueError("leadership epoch fingerprint is required")
        if not self.runner_instance_fingerprint:
            raise ValueError("runner instance fingerprint is required")

    @property
    def transaction_id(self) -> str:
        payload = {
            "schema_version": "live-transaction-v2",
            "leadership_epoch_fingerprint": self.leadership_epoch_fingerprint,
            "runner_instance_fingerprint": self.runner_instance_fingerprint,
            "sequence": self.sequence,
            "window_start_ns": self.window_start_ns,
            "window_end_ns": self.window_end_ns,
            "previous_generation_fingerprint": (
                self.previous_generation_fingerprint
            ),
            "attempt_index": self.attempt_index,
        }
        canonical = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    def _resource_fingerprint(self, resource_role: str) -> str:
        canonical = json.dumps(
            {
                "schema_version": "live-attempt-resource-v1",
                "transaction_id": self.transaction_id,
                "resource_role": str(resource_role),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    @property
    def working_engine_fingerprint(self) -> str:
        return self._resource_fingerprint("working-engine")

    @property
    def generation_staging_fingerprint(self) -> str:
        return self._resource_fingerprint("generation-staging")


@dataclass
class LiveWindowContext:
    sequence: int
    window_start_ns: int
    window_end_ns: int
    expected_generation_id: str | None
    token: object
    run_state_snapshot: LeaseRunStateSnapshot
    attempt_identity: WindowAttemptIdentity | None = None
    transaction_id: str = ""
    working_engine_fingerprint: str = ""
    generation_staging_fingerprint: str = ""
    attempt_state: str = "active"
    abort_reason: str | None = None
    working_engine: object | None = None
    engine_result: object | None = None
    cas_context: CommitCASContext | None = None
    generation: ImmutableGeneration | None = None


class LiveCommitCoordinator:
    """Own Lease authority, staged Engine state and the only live commit point."""

    def __init__(
        self,
        authority,
        generation_store: ImmutableGenerationStore,
        instance_fingerprint: str,
        *,
        output_projector=None,
        retention_config=None,
        clock=None,
        progress_tracker=None,
        runner_instance_fingerprint=None,
    ):
        self.authority = authority
        self.generation_store = generation_store
        self.instance_fingerprint = str(instance_fingerprint)
        if not self.instance_fingerprint:
            raise ValueError("instance fingerprint is required")
        if runner_instance_fingerprint is None:
            runner_instance_fingerprint = hashlib.sha256(json.dumps(
                {
                    "schema_version": "live-runner-instance-v1",
                    "holder_fingerprint": self.instance_fingerprint,
                    "process_nonce": secrets.token_hex(32),
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")).hexdigest()
        self.runner_instance_fingerprint = str(runner_instance_fingerprint)
        if not self.runner_instance_fingerprint:
            raise ValueError("runner instance fingerprint is required")
        self.output_projector = output_projector
        self.retention_config = retention_config
        self.clock = clock
        self.progress_tracker = progress_tracker
        self.retention_issues = []
        self.state = LiveCoordinatorState.STANDBY
        self.token = None
        self.active_engine = None

    def acquire_and_recover(self, *, active_engine=None):
        if self.state not in {
            LiveCoordinatorState.STANDBY,
            LiveCoordinatorState.LOST,
        }:
            raise RuntimeError("coordinator is not available for acquisition")
        self.state = LiveCoordinatorState.ACQUIRING
        try:
            token = self.authority.try_acquire(self.instance_fingerprint)
        except LeaseRunStateConflict:
            self.state = LiveCoordinatorState.STANDBY
            self.token = None
            raise
        self.state = LiveCoordinatorState.LEADER_RECOVERING
        self.token = token
        if active_engine is not None:
            self.active_engine = active_engine
        return token

    def renew(self):
        if self.token is None or self.state not in {
            LiveCoordinatorState.LEADER_RECOVERING,
            LiveCoordinatorState.LEADER_ACTIVE,
            LiveCoordinatorState.COMMITTED_OUTPUT_DEGRADED,
        }:
            raise RuntimeError("transactional leadership is not renewable")
        try:
            self.token = self.authority.renew(self.token)
        except LeaseRunStateConflict:
            self.state = LiveCoordinatorState.LOST
            self.token = None
            raise
        return self.token

    def begin_window(
        self,
        window_start_ns: int,
        window_end_ns: int,
        attempt_index: int = 1,
    ) -> LiveWindowContext:
        if (
            self.state is not LiveCoordinatorState.LEADER_ACTIVE
            or self.token is None
        ):
            raise RuntimeError("active transactional leadership is required")
        if window_start_ns >= window_end_ns:
            raise ValueError("live window bounds are invalid")
        snapshot = self.authority.read()
        attempt_identity = WindowAttemptIdentity(
            sequence=snapshot.record.committed_sequence + 1,
            window_start_ns=window_start_ns,
            window_end_ns=window_end_ns,
            attempt_index=attempt_index,
            leadership_epoch_fingerprint=self.token.token_fingerprint,
            runner_instance_fingerprint=self.runner_instance_fingerprint,
            previous_generation_fingerprint=(
                snapshot.record.current_generation_id
            ),
        )
        return LiveWindowContext(
            sequence=attempt_identity.sequence,
            window_start_ns=window_start_ns,
            window_end_ns=window_end_ns,
            expected_generation_id=snapshot.record.current_generation_id,
            token=self.token,
            run_state_snapshot=snapshot,
            attempt_identity=attempt_identity,
            transaction_id=attempt_identity.transaction_id,
            working_engine_fingerprint=(
                attempt_identity.working_engine_fingerprint
            ),
            generation_staging_fingerprint=hashlib.sha256(
                (
                    f".pending.{self.instance_fingerprint}."
                    f"{attempt_identity.transaction_id}.tmp"
                ).encode("utf-8")
            ).hexdigest(),
        )

    def working_engine(self, context: LiveWindowContext):
        self._require_active_context(context)
        if (
            self.active_engine is None
            or not hasattr(self.active_engine, "fork_for_window")
        ):
            raise RuntimeError(
                "active engine does not support transactional staging",
            )
        if context.working_engine is None:
            context.working_engine = self.active_engine.fork_for_window()
        return context.working_engine

    def run_engine(self, context: LiveWindowContext, engine_input):
        working = self.working_engine(context)
        context.engine_result = working.process_window(engine_input)
        return context.engine_result

    def prepare_generation(
        self,
        context: LiveWindowContext,
        *,
        engine_state,
        output_ledger,
        output_bundle,
        config_fingerprint: str,
        code_schema_version: str,
    ) -> ImmutableGeneration:
        self._require_active_context(context)
        if context.working_engine is None or context.engine_result is None:
            raise RuntimeError("working Engine must complete before generation")
        context.cas_context = self.authority.prepare_commit(
            context.token,
            expected_sequence=context.sequence,
            expected_generation_id=context.expected_generation_id,
        )
        if (self.progress_tracker is not None
                and self.progress_tracker.snapshot().get(
                    "generation_staging_fingerprint"
                )):
            self.progress_tracker.materialize_generation_staging()
        generation = self.generation_store.prepare(
            previous_generation_id=context.expected_generation_id,
            proposed_sequence=context.sequence,
            window_start_ns=context.window_start_ns,
            window_end_ns=context.window_end_ns,
            leadership_epoch=context.token.leadership_epoch,
            holder_fingerprint=context.token.holder_fingerprint,
            engine_state=engine_state,
            output_ledger=output_ledger,
            output_bundle=output_bundle,
            config_fingerprint=config_fingerprint,
            code_schema_version=code_schema_version,
            transaction_id=context.transaction_id,
            instance_fingerprint=self.instance_fingerprint,
        )
        context.generation = generation
        return generation

    def _validate_prepared_generation(
        self,
        context: LiveWindowContext,
        generation: ImmutableGeneration,
    ) -> None:
        self._require_active_context(context)
        if (
            context.cas_context is None
            or context.generation != generation
            or context.working_engine is None
        ):
            raise RuntimeError("generation was not prepared by this transaction")
        manifest = generation.manifest
        if (
            manifest["proposed_sequence"] != context.sequence
            or manifest["previous_generation_id"] != context.expected_generation_id
            or manifest["leadership_epoch"] != context.token.leadership_epoch
            or manifest["holder_fingerprint"] != context.token.holder_fingerprint
        ):
            raise RuntimeError("generation identity does not match transaction")

    def commit_run_state(
        self,
        context: LiveWindowContext,
        generation: ImmutableGeneration,
    ) -> LeaseRunStateSnapshot:
        """Perform the only durable commit: the fenced Lease RunState CAS."""
        self._validate_prepared_generation(context, generation)
        manifest = generation.manifest
        candidate = context.run_state_snapshot.record.with_commit(
            holder_fingerprint=context.token.holder_fingerprint,
            leadership_epoch=context.token.leadership_epoch,
            sequence=context.sequence,
            generation_id=generation.generation_id,
            generation_fingerprint=generation.generation_fingerprint,
            output_ledger_fingerprint=manifest["output_ledger_fingerprint"],
            output_bundle_fingerprint=manifest["output_bundle_fingerprint"],
            engine_state_fingerprint=manifest["engine_state_fingerprint"],
            window_start_ns=context.window_start_ns,
            window_end_ns=context.window_end_ns,
        )
        try:
            snapshot = self.authority.commit_generation(
                context.cas_context, candidate,
            )
        except LeaseRunStateConflict:
            self.state = LiveCoordinatorState.LOST
            raise
        self.active_engine.adopt_committed_working_engine(
            context.working_engine,
        )
        context.attempt_state = "committed"
        if (self.progress_tracker is not None
                and self.progress_tracker.snapshot().get("transaction_id")):
            self.progress_tracker.commit_attempt(context.sequence)
        self.state = LiveCoordinatorState.LEADER_ACTIVE
        return snapshot

    def project_output(
        self,
        context: LiveWindowContext,
        generation: ImmutableGeneration,
    ) -> None:
        """Project a replaceable view after RunState already committed."""
        if context.generation != generation:
            raise RuntimeError("output generation does not match transaction")
        if self.output_projector is None:
            return
        try:
            self.output_projector.project(generation.generation_id)
        except Exception as error:
            self.state = LiveCoordinatorState.COMMITTED_OUTPUT_DEGRADED
            raise CommittedOutputDegradedError(
                "RunState committed but output projection failed",
            ) from error

    def apply_retention(self, generation: ImmutableGeneration) -> list:
        """Apply non-transactional retention after commit and output projection."""
        if self.retention_config is None:
            self.retention_issues = []
            return []
        import time
        now = self.clock() if self.clock is not None else time.time()
        self.retention_issues = self.generation_store.apply_retention(
            generation.generation_id,
            keep_generations=self.retention_config.checkpoint_generations,
            minimum_age_sec=self.retention_config.checkpoint_min_age_sec,
            now=now,
        )
        return list(self.retention_issues)

    def commit(
        self,
        context: LiveWindowContext,
        generation: ImmutableGeneration,
    ) -> LeaseRunStateSnapshot:
        """Compatibility composition around the explicit three commit phases."""
        snapshot = self.commit_run_state(context, generation)
        self.project_output(context, generation)
        self.apply_retention(generation)
        return snapshot

    def recover_current(self, *, engine_loader):
        if self.state not in {
            LiveCoordinatorState.LEADER_RECOVERING,
            LiveCoordinatorState.LEADER_ACTIVE,
            LiveCoordinatorState.COMMITTED_OUTPUT_DEGRADED,
        }:
            raise RuntimeError("coordinator does not own recovery authority")
        snapshot = self.authority.read()
        record = snapshot.record
        if record.committed_sequence == 0:
            self.generation_store.initialize_root()
            if self.output_projector is not None:
                self.output_projector.initialize_empty_view()
            self.state = LiveCoordinatorState.LEADER_ACTIVE
            return self.active_engine
        generation = self.generation_store.load(record.current_generation_id)
        manifest = generation.manifest
        identity_checks = (
            (generation.generation_fingerprint,
             record.current_generation_fingerprint, "generation fingerprint"),
            (manifest["proposed_sequence"], record.committed_sequence,
             "committed sequence"),
            (manifest["window_start_ns"], record.last_window_start_ns,
             "window start"),
            (manifest["window_end_ns"], record.last_window_end_ns,
             "window end"),
            (manifest["engine_state_fingerprint"],
             record.last_engine_state_fingerprint, "engine state"),
            (manifest["output_ledger_fingerprint"],
             record.output_ledger_fingerprint, "output ledger"),
            (manifest["output_bundle_fingerprint"],
             record.output_bundle_fingerprint, "output bundle"),
            (manifest["config_fingerprint"], record.config_fingerprint,
             "config"),
            (manifest["code_schema_version"], record.code_schema_version,
             "code schema"),
        )
        for actual, expected, label in identity_checks:
            if actual != expected:
                raise RuntimeError(f"RunState {label} mismatch")
        engine_directory = generation.path / "engine_state"
        state_file = engine_directory / "state.json"
        source = (
            json.loads(state_file.read_text(encoding="utf-8"))
            if state_file.exists()
            else engine_directory
        )
        restored = engine_loader(source)
        from proberca.orchestration.state import OutputLedger
        ledger = OutputLedger.from_dict(json.loads(
            (generation.path / "output_ledger.json").read_text(
                encoding="utf-8",
            ),
        ))
        if ledger.ledger_fingerprint != record.output_ledger_fingerprint:
            raise RuntimeError("RunState output ledger fingerprint mismatch")
        restored._output_ledger = ledger
        restored._previous_output_ledger = None
        self.active_engine = restored
        if self.output_projector is not None:
            self.output_projector.project(generation.generation_id)
        self.state = LiveCoordinatorState.LEADER_ACTIVE
        return restored

    def drain(self) -> None:
        if self.state is LiveCoordinatorState.LEADER_ACTIVE:
            self.state = LiveCoordinatorState.LEADER_DRAINING

    def release(self) -> None:
        if self.token is not None and hasattr(self.authority, "release"):
            self.authority.release(self.token)
        self.token = None
        self.state = LiveCoordinatorState.STANDBY

    def _require_active_context(self, context: LiveWindowContext) -> None:
        if (
            self.state is not LiveCoordinatorState.LEADER_ACTIVE
            or self.token is None
            or context.token != self.token
        ):
            raise RuntimeError("active transactional leadership is required")
