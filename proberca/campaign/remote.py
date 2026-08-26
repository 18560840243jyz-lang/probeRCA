"""Strict SSH transport for the allow-listed campaign worker agent."""

from __future__ import annotations

import json
import subprocess
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .model import canonical_json, fingerprint


class RemoteAgentError(RuntimeError):
    pass


@dataclass(frozen=True)
class RemoteNode:
    node_id: str
    host: str
    user: str
    port: int
    identity_file: Path


class SSHAgentClient:
    """Invoke one JSON operation without a remote shell or command template."""

    allowed_actions = frozenset({
        "preflight", "resolve", "snapshot", "apply", "cleanup",
    })

    def __init__(
        self,
        nodes: list[RemoteNode],
        *,
        known_hosts_file: Path,
        agent_path: str = "/opt/proberca/current/scripts/multinode_fault_agent.py",
        timeout_seconds: int = 30,
        runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("remote agent timeout must be positive")
        self.nodes = {item.node_id: item for item in nodes}
        if len(self.nodes) != len(nodes):
            raise ValueError("remote node IDs must be unique")
        self.known_hosts_file = Path(known_hosts_file).resolve()
        self.agent_path = agent_path
        self.timeout_seconds = timeout_seconds
        self.runner = runner

    def invoke(
        self, node_id: str, action: str, payload: dict[str, Any],
    ) -> dict[str, Any]:
        if action not in self.allowed_actions:
            raise RemoteAgentError("remote action is not allow-listed")
        node = self.nodes.get(node_id)
        if node is None:
            raise RemoteAgentError("remote node is not in the frozen inventory")
        if not self.known_hosts_file.is_file():
            raise RemoteAgentError("pinned known_hosts file is missing")
        if not Path(node.identity_file).is_file():
            raise RemoteAgentError("SSH identity file is missing")
        request_core = {
            "schema_version": "probeRCA-worker-agent-request-v1",
            "request_id": uuid.uuid4().hex,
            "node_id": node_id,
            "action": action,
            "payload": payload,
        }
        request = dict(request_core)
        request["request_fingerprint"] = fingerprint(request_core)
        command = [
            "ssh",
            "-T",
            "-p", str(node.port),
            "-i", str(Path(node.identity_file).resolve()),
            "-o", "BatchMode=yes",
            "-o", "IdentitiesOnly=yes",
            "-o", "StrictHostKeyChecking=yes",
            "-o", f"UserKnownHostsFile={self.known_hosts_file}",
            f"{node.user}@{node.host}",
            "sudo", "-n", "env", "PYTHONPATH=/opt/proberca/current",
            f"PROBERCA_NODE_ID={node_id}",
            "python3", self.agent_path, "--request-stdin",
        ]
        try:
            completed = self.runner(
                command,
                input=(canonical_json(request) + "\n").encode("utf-8"),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=self.timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as error:
            raise RemoteAgentError("worker agent operation timed out") from error
        if completed.returncode != 0:
            message = completed.stderr.decode("utf-8", errors="replace").strip()
            if not message:
                try:
                    failed_response = json.loads(completed.stdout.decode("utf-8"))
                    message = str(failed_response.get("error", "")).strip()
                except (UnicodeDecodeError, json.JSONDecodeError):
                    pass
            raise RemoteAgentError(f"worker agent failed closed: {message}")
        try:
            response = json.loads(completed.stdout.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RemoteAgentError("worker agent returned invalid JSON") from error
        if response.get("request_id") != request["request_id"]:
            raise RemoteAgentError("worker agent response request ID mismatch")
        core = {key: value for key, value in response.items() if key != "response_fingerprint"}
        if response.get("response_fingerprint") != fingerprint(core):
            raise RemoteAgentError("worker agent response fingerprint mismatch")
        if response.get("ok") is not True:
            raise RemoteAgentError(
                f"worker agent rejected operation: {response.get('error', 'unknown')}"
            )
        result = response.get("result")
        if not isinstance(result, dict):
            raise RemoteAgentError("worker agent returned no structured result")
        return result
