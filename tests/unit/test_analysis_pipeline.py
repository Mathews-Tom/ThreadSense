from __future__ import annotations

import json
from pathlib import Path

import pytest

from threadsense.errors import AnalysisBoundaryError
from threadsense.models.analysis import load_analysis_artifact_file
from threadsense.models.canonical import load_canonical_thread
from threadsense.pipeline.analyze import (
    analyze_thread,
    analyze_thread_file,
)
from threadsense.pipeline.strategies.keyword_heuristic import (
    are_near_duplicates,
    build_comment_signal,
    canonicalize_text,
    clean_text,
    extract_top_phrases,
    select_representative_quotes,
)


def load_canonical_fixture() -> Path:
    return Path("tests/fixtures/analysis/canonical_feedback_thread.json")


def test_text_normalization_preserves_token_order() -> None:
    assert clean_text("  docs\n\nneed   help  ") == "docs need help"
    assert canonicalize_text("Docs need help!") == "docs need help"


def test_duplicate_detection_marks_exact_duplicate_comments() -> None:
    thread = load_canonical_thread(load_canonical_fixture())
    left = build_comment_signal(thread.comments[0])
    right = build_comment_signal(thread.comments[1])

    assert are_near_duplicates(left, right, threshold=0.88) is True


def test_quote_selection_prefers_high_signal_comments() -> None:
    thread = load_canonical_thread(load_canonical_fixture())
    signals = [build_comment_signal(comment) for comment in thread.comments]

    quotes = select_representative_quotes(signals, limit=2)

    assert [quote.comment_id for quote in quotes] == ["reddit:c1", "reddit:c3"]


def test_extract_top_phrases_returns_ranked_bigrams() -> None:
    thread = load_canonical_thread(load_canonical_fixture())
    signals = [build_comment_signal(comment) for comment in thread.comments]

    phrases = extract_top_phrases(signals, limit=3)

    assert "add quickstart" in phrases
    assert "confusing incomplete" in phrases


def test_analyze_thread_groups_findings_and_duplicates(tmp_path: Path) -> None:
    fixture_path = load_canonical_fixture()
    thread = load_canonical_thread(fixture_path)

    analysis = analyze_thread(thread, fixture_path)
    analysis_path = tmp_path / "analysis.json"
    analysis_path.write_text(json.dumps(analysis.to_dict()), encoding="utf-8")
    reloaded = load_analysis_artifact_file(analysis_path)

    assert analysis.total_comments == 7
    assert analysis.distinct_comment_count == 5
    assert analysis.duplicate_group_count == 2
    assert analysis.findings[0].theme_key == "performance"
    assert reloaded.findings[0].quotes


def test_analyze_thread_file_rejects_empty_analysis_text(tmp_path: Path) -> None:
    fixture_path = tmp_path / "blank.json"
    fixture_path.write_text(
        json.dumps(
            {
                "artifact_kind": "canonical_thread",
                "schema_version": 1,
                "normalization_version": "reddit-to-canonical-v1",
                "thread": {
                    "thread_id": "reddit:test",
                    "source": {
                        "source_name": "reddit",
                        "community": "test",
                        "source_thread_id": "test",
                        "thread_url": "https://example.com/thread",
                    },
                    "title": "blank",
                    "permalink": "https://example.com/thread",
                    "author": {"username": "op", "source_author_id": None},
                    "comments": [
                        {
                            "thread_id": "reddit:test",
                            "comment_id": "reddit:c1",
                            "parent_comment_id": None,
                            "author": {"username": "user", "source_author_id": None},
                            "body": "!!!",
                            "score": 1,
                            "created_utc": 1.0,
                            "depth": 0,
                            "permalink": "https://example.com/thread/c1",
                        }
                    ],
                    "comment_count": 1,
                    "provenance": {
                        "raw_artifact_path": "/tmp/raw.json",
                        "raw_sha256": "sha",
                        "retrieved_at_utc": 1.0,
                        "normalized_at_utc": 2.0,
                        "schema_version": 1,
                        "normalization_version": "reddit-to-canonical-v1",
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(AnalysisBoundaryError):
        analyze_thread_file(fixture_path)
