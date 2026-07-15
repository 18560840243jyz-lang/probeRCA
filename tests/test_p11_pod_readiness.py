from __future__ import annotations


def test_follower_is_pod_ready_but_not_active_processor_ready():
    from proberca.live.health import LiveHealthState

    state = LiveHealthState()
    state.update(
        kubernetes_connected=True, watchers_synchronized=True,
        inventory_stale=False, leader=False, catchup_exceeded=False,
    )
    assert state.pod_ready is True
    assert state.ready is False
    assert state.pod_reason_codes() == []
    assert "standby" in state.reason_codes()


def test_health_server_exposes_distinct_podreadyz_endpoint():
    import threading
    from urllib.request import urlopen

    from proberca.live.health import LiveHealthState, serve_health

    state = LiveHealthState()
    state.update(kubernetes_connected=True, watchers_synchronized=True,
                 inventory_stale=False, leader=False, catchup_exceeded=False)
    server = serve_health(state, "127.0.0.1:0")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with urlopen(f"http://127.0.0.1:{server.server_port}/podreadyz") as response:
            assert response.status == 200
    finally:
        server.shutdown(); server.server_close(); thread.join()
