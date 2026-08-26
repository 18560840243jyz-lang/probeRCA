"""Crash-safe execution skeleton bound to the frozen public manifest."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .state import CampaignState


class CampaignExecutionError(RuntimeError):
    pass


@dataclass(frozen=True)
class SealedCaseResult:
    dataset_id: str
    sha256: str


class CampaignExecutor:
    """Execute one deterministic campaign case at a time.

    The generic executor fails closed. An operational controller may explicitly
    defer a failed attempt only after separately proving cleanup and runtime
    restoration, using :meth:`CampaignState.defer`.
    """

    def __init__(
        self, public_manifest: dict[str, Any], state: CampaignState,
    ) -> None:
        if public_manifest.get("executable") is not True:
            raise CampaignExecutionError("campaign manifest is not executable")
        expected = [item["case_id"] for item in public_manifest.get("cases", ())]
        if tuple(expected) != state.case_ids:
            raise CampaignExecutionError("campaign state does not match manifest order")
        if public_manifest.get("manifest_fingerprint") != state.manifest_fingerprint:
            raise CampaignExecutionError("campaign state is bound to another manifest")
        self.public_manifest = public_manifest
        self.state = state
        self.by_id = {item["case_id"]: item for item in public_manifest["cases"]}

    def run_next(
        self, handler: Callable[[dict[str, Any], int], SealedCaseResult],
    ) -> str | None:
        case_id = self.state.next_case_id()
        if case_id is None:
            return None
        attempt = self.state.start(case_id)
        try:
            result = handler(self.by_id[case_id], attempt)
            if not isinstance(result, SealedCaseResult):
                raise CampaignExecutionError("case handler returned no sealed result")
            self.state.finish(
                case_id, dataset_id=result.dataset_id, sha256=result.sha256,
            )
        except Exception as error:
            self.state.fail(case_id, str(error))
            raise
        return case_id
