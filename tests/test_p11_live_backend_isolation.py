from __future__ import annotations

import inspect


def test_live_runner_has_only_transactional_commit_coordinator():
    import proberca.live.runner as runner

    source = inspect.getsource(runner)
    for legacy in (
        "output_writer",
        "save_engine_checkpoint",
        "restore_engine_checkpoint",
        "checkpoint_writer",
        "transaction_committer",
        "current_generation",
        "ledger_fingerprint",
        "LiveWindowTransaction",
    ):
        assert legacy not in source
    assert "LiveCommitCoordinator" in source


def test_live_cli_does_not_use_p10_current_journal_or_direct_output_writer():
    import proberca.cli.live as live_cli

    source = inspect.getsource(live_cli)
    for legacy in (
        "save_engine_checkpoint",
        "restore_engine_checkpoint",
        "LiveSequenceJournal",
        "ReplayOutputWriter",
        'Path(checkpoint_dir, "CURRENT")',
        "scheduler.commit",
    ):
        assert legacy not in source


def test_scheduler_sequence_is_not_a_live_commit_authority():
    import proberca.live.runner as runner

    source = inspect.getsource(runner.ProbeRCALiveRunner.process_window)
    assert "window.sequence" not in source
    assert "begin_window" in source


def test_kubernetes_live_authority_does_not_import_local_checkpoint_backend():
    import proberca.live.commit_authority as authority

    source = inspect.getsource(authority)
    assert "checkpoint" not in source
    assert "LocalAtomicCommitAuthority" not in source
    assert "KubernetesLeaseRunStateStore" in source


def test_p10_local_authority_is_explicitly_separate():
    from proberca.orchestration.commit_authority import LocalAtomicCommitAuthority

    assert LocalAtomicCommitAuthority.backend_name == "local_atomic_current"
