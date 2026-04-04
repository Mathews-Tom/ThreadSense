from __future__ import annotations

import json
from pathlib import Path

import pytest

from threadsense.config import AnalysisConfig
from threadsense.domains import load_domain_vocabulary
from threadsense.errors import AnalysisBoundaryError
from threadsense.models.analysis import load_analysis_artifact_file
from threadsense.models.canonical import (
    AuthorRef,
    Comment,
    ProvenanceMetadata,
    SourceRef,
    Thread,
    load_canonical_thread,
)
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

    assert left is not None
    assert right is not None
    assert are_near_duplicates(left, right, threshold=0.88) is True


def test_quote_selection_prefers_high_signal_comments() -> None:
    thread = load_canonical_thread(load_canonical_fixture())
    signals = [signal for comment in thread.comments if (signal := build_comment_signal(comment))]

    quotes = select_representative_quotes(signals, limit=2)

    assert [quote.comment_id for quote in quotes] == ["reddit:c1", "reddit:c3"]


def test_extract_top_phrases_returns_ranked_bigrams() -> None:
    thread = load_canonical_thread(load_canonical_fixture())
    signals = [signal for comment in thread.comments if (signal := build_comment_signal(comment))]

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
    assert analysis.filtered_comment_count == 0
    assert analysis.distinct_comment_count == 5
    assert analysis.duplicate_group_count == 2
    assert analysis.findings[0].theme_key == "performance"
    assert analysis.conversation_structure.max_depth == 0
    assert analysis.conversation_structure.top_level_count == 7
    assert reloaded.findings[0].quotes


def test_analyze_thread_file_filters_empty_analysis_text(tmp_path: Path) -> None:
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

    analysis = analyze_thread_file(fixture_path)

    assert analysis.total_comments == 1
    assert analysis.filtered_comment_count == 1
    assert analysis.distinct_comment_count == 0
    assert analysis.findings == []
    assert analysis.top_quotes == []


def test_load_domain_vocabulary_matches_default_developer_tools_rules() -> None:
    vocabulary = load_domain_vocabulary("developer_tools")

    assert vocabulary.theme_rules["documentation"] == (
        "doc",
        "docs",
        "guide",
        "guides",
        "onboarding",
        "quickstart",
        "tutorial",
    )
    assert vocabulary.issue_markers[:3] == ("bug", "broken", "confusing")
    assert vocabulary.request_fallback_theme == "workflow"


def test_analyze_thread_uses_requested_domain_vocabulary() -> None:
    thread = Thread(
        thread_id="reddit:hiring",
        source=SourceRef(
            source_name="reddit",
            community="jobs",
            source_thread_id="hiring",
            thread_url="https://example.com/hiring",
        ),
        title="Hiring thread",
        permalink="https://example.com/hiring",
        author=AuthorRef(username="op", source_author_id=None),
        comments=[
            Comment(
                thread_id="reddit:hiring",
                comment_id="reddit:h1",
                parent_comment_id=None,
                author=AuthorRef(username="user1", source_author_id=None),
                body=(
                    "The interview process had a system design round and the recruiter ghosted me."
                ),
                score=4,
                created_utc=1.0,
                depth=0,
                permalink="https://example.com/hiring/h1",
            ),
            Comment(
                thread_id="reddit:hiring",
                comment_id="reddit:h2",
                parent_comment_id=None,
                author=AuthorRef(username="user2", source_author_id=None),
                body="Compensation looked low and the offer included almost no bonus.",
                score=3,
                created_utc=2.0,
                depth=0,
                permalink="https://example.com/hiring/h2",
            ),
        ],
        comment_count=2,
        provenance=ProvenanceMetadata(
            raw_artifact_path="/tmp/raw.json",
            raw_sha256="sha",
            retrieved_at_utc=1.0,
            normalized_at_utc=2.0,
            schema_version=1,
            normalization_version="reddit-to-canonical-v1",
        ),
    )

    analysis = analyze_thread(
        thread,
        Path("tests/fixtures/analysis/canonical_feedback_thread.json"),
        config=AnalysisConfig(domain="hiring_careers"),
    )

    assert [finding.theme_key for finding in analysis.findings] == ["process", "compensation"]


def test_analyze_thread_rejects_unknown_domain() -> None:
    thread = load_canonical_thread(load_canonical_fixture())

    with pytest.raises(AnalysisBoundaryError) as exc_info:
        analyze_thread(
            thread,
            load_canonical_fixture(),
            config=AnalysisConfig(domain="missing_domain"),
        )

    assert "domain vocabulary definition does not exist" in str(exc_info.value)
