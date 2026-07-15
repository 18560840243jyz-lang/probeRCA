from __future__ import annotations


def test_working_engine_isolated_until_transaction_commit():
    from test_p10_engine import engine, window

    active = engine()
    working = active.fork_for_window()
    working.process_window(window(1, 1.0, include_topology=True))

    assert active._last_timestamp is None
    assert working._last_timestamp is not None
    active.adopt_committed_working_engine(working)
    assert active._last_timestamp == working._last_timestamp
