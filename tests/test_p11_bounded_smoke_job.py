from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_bounded_smoke_uses_non_restarting_job_not_deployment_wrapper():
    path = ROOT / "deploy/kubernetes/test/p11-smoke/proberca-bounded-smoke-job.yaml"
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert payload["kind"] == "Job"
    assert payload["spec"]["backoffLimit"] == 0
    pod_spec = payload["spec"]["template"]["spec"]
    assert pod_spec["restartPolicy"] == "Never"
    container = pod_spec["containers"][0]
    assert "--max-windows" in container["args"]
    index = container["args"].index("--max-windows")
    assert container["args"][index + 1] == "3"
    assert "sleep" not in " ".join(container.get("command", []) + container["args"])
    assert "livenessProbe" not in container


def test_bounded_smoke_job_has_creation_time_run_labels():
    path = ROOT / "deploy/kubernetes/test/p11-smoke/proberca-bounded-smoke-job.yaml"
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    required = {
        "app.kubernetes.io/managed-by",
        "proberca.io/smoke-run-id",
        "proberca.io/smoke-purpose",
    }
    assert required <= set(payload["metadata"]["labels"])
    assert required <= set(payload["spec"]["template"]["metadata"]["labels"])


def test_live_deployment_does_not_contain_bounded_wrapper():
    source = (ROOT / "deploy/kubernetes/test/p11-smoke/proberca-live-deployment.yaml").read_text(encoding="utf-8")
    assert "--max-windows" not in source
    assert "/diag/runner.py" not in source
    assert "sleep" not in source


def test_runner_max_windows_three_processes_exactly_three_once():
    import threading
    from types import SimpleNamespace

    from proberca.live.runner import ProbeRCALiveRunner

    windows = [
        SimpleNamespace(sequence=value) for value in range(1, 6)
    ]

    class Scheduler:
        def __init__(self):
            self.advanced = []

        def eligible_windows(self, _now):
            return windows

        def advance(self, window):
            self.advanced.append(window.sequence)

    calls = []
    runner = object.__new__(ProbeRCALiveRunner)
    runner.process_window = lambda window: calls.append(window.sequence)
    scheduler = Scheduler()
    processed = runner.run_forever(
        scheduler,
        now_ns=lambda: 1,
        stop=threading.Event(),
        max_windows=3,
    )
    assert processed == 3
    assert calls == [1, 2, 3]
    assert scheduler.advanced == [1, 2, 3]
