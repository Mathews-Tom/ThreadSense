from __future__ import annotations

from pathlib import Path

from threadsense.evaluation import load_golden_dataset, validate_against_golden
from threadsense.models.canonical import load_canonical_thread
from threadsense.pipeline.analyze import analyze_thread


def test_validate_against_golden_matches_fixture_expectations() -> None:
    dataset = load_golden_dataset(Path("tests/golden/developer_tools/reddit_feedback_thread.json"))
    fixture_path = Path(dataset.thread_fixture)
    analysis = analyze_thread(load_canonical_thread(fixture_path), fixture_path)

    result = validate_against_golden(analysis, dataset)

    assert result.metrics.theme_precision == 1.0
    assert result.metrics.theme_recall == 1.0
    assert result.metrics.duplicate_recall == 1.0
    assert result.missing_themes == []
    assert result.missing_evidence_comment_ids == []
