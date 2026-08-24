#!/usr/bin/env python3
"""Root entry point for one strict multi-node fault-agent JSON request."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[1]
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from proberca.campaign.worker_agent import LinuxWorkerBackend, response_for_request


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request-stdin", action="store_true")
    parser.add_argument("--node-id", default=os.environ.get("PROBERCA_NODE_ID"))
    parser.add_argument(
        "--state-root", type=Path,
        default=Path("/var/lib/proberca-campaign/agent-sessions"),
    )
    parser.add_argument(
        "--work-root", type=Path,
        default=Path("/var/lib/proberca-campaign/fault-work"),
    )
    parser.add_argument(
        "--actor-path", type=Path,
        default=Path("/opt/proberca/current/scripts/multinode_fault_actor.py"),
    )
    arguments = parser.parse_args()
    if not arguments.request_stdin:
        raise SystemExit("exactly one --request-stdin operation is required")
    if not arguments.node_id:
        raise SystemExit("PROBERCA_NODE_ID or --node-id is required")
    try:
        request = json.loads(sys.stdin.buffer.read())
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SystemExit(f"invalid worker-agent JSON: {error}")
    if not isinstance(request, dict):
        raise SystemExit("worker-agent request must be an object")
    backend = LinuxWorkerBackend(
        node_id=arguments.node_id,
        state_root=arguments.state_root,
        work_root=arguments.work_root,
        actor_path=arguments.actor_path,
    )
    response = response_for_request(request, backend)
    print(json.dumps(response, sort_keys=True, separators=(",", ":")))
    return 0 if response["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
