from proberca.live.collection import CollectionOutcome, WindowCollectionRetrier
from proberca.live.transaction import CommitPhase, LiveWindowTransactionState


def test_committed_23_retries_and_commits_exactly_24_from_five_samples():
    sequences = []
    attempts = []
    retrier = WindowCollectionRetrier(
        max_attempts=2,
        initial_backoff_sec=0.001,
        max_backoff_sec=0.001,
        sleep=lambda _: None,
    )
    responses = [
        (CollectionOutcome.TRANSIENT_EMPTY, None),
        (CollectionOutcome.SUCCESS, tuple(range(5))),
    ]
    samples = retrier.run(
        sequence=24,
        new_context=lambda attempt: {"sequence": 24, "attempt": attempt},
        collect=lambda context, attempt: attempts.append(dict(context)) or responses[attempt - 1],
        cleanup=lambda context: context.clear(),
    )
    state = LiveWindowTransactionState(expected_sequence=24, committed_sequence=23)
    assert len(samples) == 5
    state.enter_phase(CommitPhase.PRE_COMMIT)
    state.mark_run_state_committed()
    sequences.append(state.committed_sequence)

    assert sequences == [24]
    assert state.next_sequence == 25
    assert [item["sequence"] for item in attempts] == [24, 24]


def test_progression_23_to_26_has_no_gap_duplicate_or_special_case():
    state = LiveWindowTransactionState(expected_sequence=24, committed_sequence=23)
    committed = []
    for sequence in (24, 25, 26):
        assert state.next_sequence == sequence
        state = LiveWindowTransactionState(
            expected_sequence=sequence,
            committed_sequence=state.committed_sequence,
        )
        state.mark_run_state_committed()
        committed.append(state.committed_sequence)
    assert committed == [24, 25, 26]
    assert len(committed) == len(set(committed))
    assert all(right - left == 1 for left, right in zip(committed, committed[1:]))


def test_production_liveness_modules_have_no_sequence_or_metric_special_case():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "proberca/live"
    source = "\n".join(path.read_text(encoding="utf-8") for path in root.glob("*.py"))
    assert "sequence == 24" not in source
    assert "sequence=24" not in source
    assert "smoke-call-count" not in source
