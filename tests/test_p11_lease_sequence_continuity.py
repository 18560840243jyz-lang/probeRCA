from __future__ import annotations


def test_sequence_continuity_reports_no_gap_or_duplicate():
    from proberca.live.sequence import validate_sequence_continuity

    result = validate_sequence_continuity([
        {"sequence": 8, "holder_fingerprint": "one"},
        {"sequence": 9, "holder_fingerprint": "one"},
        {"sequence": 10, "holder_fingerprint": "two"},
        {"sequence": 11, "holder_fingerprint": "two"},
    ])
    assert result.sequence_range == (8, 11)
    assert result.sequence_count == 4
    assert result.gap_count == result.duplicate_count == 0
    assert result.max_holders_per_sequence == 1


def test_sequence_continuity_detects_gap_duplicate_and_dual_holder():
    from proberca.live.sequence import validate_sequence_continuity

    result = validate_sequence_continuity([
        {"sequence": 1, "holder_fingerprint": "one"},
        {"sequence": 1, "holder_fingerprint": "two"},
        {"sequence": 3, "holder_fingerprint": "two"},
    ])
    assert result.gap_count == 1
    assert result.duplicate_count == 1
    assert result.max_holders_per_sequence == 2


def test_commit_journal_is_atomic_and_round_trips(tmp_path):
    from proberca.live.sequence import LiveSequenceJournal

    journal = LiveSequenceJournal(tmp_path / "sequence.jsonl")
    journal.commit(1, "holder-a", "checkpoint-a")
    journal.commit(2, "holder-a", "checkpoint-b")
    assert [item["sequence"] for item in journal.read()] == [1, 2]
    assert not list(tmp_path.glob("*.tmp"))


def test_commit_journal_rejects_noncontinuous_or_conflicting_commit(tmp_path):
    from proberca.live.sequence import LiveSequenceJournal, SequenceContinuityError
    import pytest

    journal = LiveSequenceJournal(tmp_path / "sequence.jsonl")
    journal.commit(4, "holder-a", "checkpoint-a")
    with pytest.raises(SequenceContinuityError):
        journal.commit(6, "holder-a", "checkpoint-c")
    with pytest.raises(SequenceContinuityError):
        journal.commit(4, "holder-b", "checkpoint-z")
