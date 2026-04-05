from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from threadsense.config import AnalysisConfig
from threadsense.contracts import AbstractionLevel, AnalysisContract
from threadsense.domains import load_domain_vocabulary
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
    CommentSignal,
    are_near_duplicates,
    build_comment_signal,
    canonicalize_text,
    clean_text,
    decompose_catch_all,
    extract_top_phrases,
    is_noise_signal,
    score_severity,
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
    assert analysis.provenance.contract["domain"] == "developer_tools"
    assert analysis.provenance.contract["objective"] == "general_survey"
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
        "readme",
        "wiki",
    )
    assert "architecture" in vocabulary.theme_rules
    assert "tooling" in vocabulary.theme_rules
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

    with pytest.raises(ValidationError) as exc_info:
        analyze_thread(
            thread,
            load_canonical_fixture(),
            config=AnalysisConfig(domain="missing_domain"),
        )

    assert "Input should be" in str(exc_info.value)


def test_abstraction_level_changes_finding_granularity() -> None:
    thread = load_canonical_thread(load_canonical_fixture())
    fixture_path = load_canonical_fixture()

    operational = analyze_thread(
        thread,
        fixture_path,
        config=AnalysisConfig(abstraction_level=AbstractionLevel.OPERATIONAL),
    )
    architectural = analyze_thread(
        thread,
        fixture_path,
        config=AnalysisConfig(abstraction_level=AbstractionLevel.ARCHITECTURAL),
    )
    strategic = analyze_thread(
        thread,
        fixture_path,
        config=AnalysisConfig(abstraction_level=AbstractionLevel.STRATEGIC),
    )

    assert len(operational.findings) >= len(architectural.findings)
    assert len(architectural.findings) >= len(strategic.findings)
    assert all(finding.severity == "high" for finding in strategic.findings)


# ---------------------------------------------------------------------------
# Text preprocessing: URL stripping
# ---------------------------------------------------------------------------


def test_clean_text_strips_urls() -> None:
    assert clean_text("check https://example.com/path for info") == "check for info"
    assert clean_text("see www.example.com/foo or ask") == "see or ask"
    assert clean_text("https://a.io https://b.io") == ""


def test_clean_text_strips_urls_preserving_surrounding_text() -> None:
    text = "I use https://github.com/user/repo and it works great"
    assert clean_text(text) == "I use and it works great"


def test_url_tokens_excluded_from_signal() -> None:
    comment = Comment(
        thread_id="t",
        comment_id="c1",
        parent_comment_id=None,
        author=AuthorRef(username="u", source_author_id=None),
        body="Check https://www.reddit.com/r/test for the slow performance bug",
        score=5,
        created_utc=1.0,
        depth=0,
        permalink="https://example.com",
    )
    signal = build_comment_signal(comment)
    assert signal is not None
    assert "reddit" not in signal.tokens
    assert "www" not in signal.tokens
    assert "com" not in signal.tokens
    assert "slow" in signal.tokens
    assert "performance" in signal.tokens


# ---------------------------------------------------------------------------
# Text preprocessing: platform noise stopwords
# ---------------------------------------------------------------------------


def test_platform_noise_excluded_from_phrases() -> None:
    comments = [
        Comment(
            thread_id="t",
            comment_id=f"c{i}",
            parent_comment_id=None,
            author=AuthorRef(username="u", source_author_id=None),
            body=f"lol edit tbh the performance is really slow here topic{i}",
            score=3,
            created_utc=float(i),
            depth=0,
            permalink="https://example.com",
        )
        for i in range(5)
    ]
    signals = [s for c in comments if (s := build_comment_signal(c)) is not None]
    phrases = extract_top_phrases(signals, limit=10)
    assert not any("lol" in phrase for phrase in phrases)
    assert not any("edit" in phrase.split() for phrase in phrases)
    assert not any("tbh" in phrase for phrase in phrases)


# ---------------------------------------------------------------------------
# Phrase extraction: unigram support
# ---------------------------------------------------------------------------


def test_extract_top_phrases_includes_frequent_unigrams() -> None:
    comments = [
        Comment(
            thread_id="t",
            comment_id=f"c{i}",
            parent_comment_id=None,
            author=AuthorRef(username=f"u{i}", source_author_id=None),
            body=f"obsidian vault is great for embedding notes about topic{i}",
            score=5,
            created_utc=float(i),
            depth=0,
            permalink="https://example.com",
        )
        for i in range(5)
    ]
    signals = [s for c in comments if (s := build_comment_signal(c)) is not None]
    phrases = extract_top_phrases(signals, limit=15)
    assert "obsidian" in phrases
    assert "vault" in phrases
    assert "great" in phrases
    assert "embedding" in phrases


def test_extract_top_phrases_unigrams_require_min_frequency() -> None:
    comments = [
        Comment(
            thread_id="t",
            comment_id=f"c{i}",
            parent_comment_id=None,
            author=AuthorRef(username=f"u{i}", source_author_id=None),
            body=f"unique{i} obsidian embedding setup for knowledge",
            score=3,
            created_utc=float(i),
            depth=0,
            permalink="https://example.com",
        )
        for i in range(2)
    ]
    signals = [s for c in comments if (s := build_comment_signal(c)) is not None]
    phrases = extract_top_phrases(signals, limit=10)
    # "obsidian" appears only 2 times — below the minimum frequency of 3
    assert "obsidian" not in phrases


# ---------------------------------------------------------------------------
# Severity scoring: density normalization
# ---------------------------------------------------------------------------


def _make_signal(comment_id: str, body: str, score: int) -> CommentSignal:
    comment = Comment(
        thread_id="t",
        comment_id=comment_id,
        parent_comment_id=None,
        author=AuthorRef(username="u", source_author_id=None),
        body=body,
        score=score,
        created_utc=1.0,
        depth=0,
        permalink="https://example.com",
    )
    signal = build_comment_signal(comment)
    assert signal is not None
    return signal


def test_severity_density_normalization_penalizes_low_signal_volume() -> None:
    contract = AnalysisContract.from_dict(
        {
            "domain": "developer_tools",
            "objective": "general_survey",
            "abstraction_level": "operational",
            "schema_version": "1.0",
            "created_at_utc": 1.0,
        }
    )
    # 20 generic comments with 1 upvote each, no markers
    high_volume_signals = [
        _make_signal(f"c{i}", f"this is a generic comment number {i}", 1) for i in range(20)
    ]
    # raw_score = 0*3 + 0 + 20 = 20, old formula: severity = high (>= 15)
    # density = 20/20 = 1.0, weighted = 1.0 * 10 = 10, severity = medium (>= 6, < 15)
    severity = score_severity(high_volume_signals, 0, 0, contract=contract)
    assert severity == "medium"


def test_severity_density_normalization_preserves_focused_findings() -> None:
    contract = AnalysisContract.from_dict(
        {
            "domain": "developer_tools",
            "objective": "general_survey",
            "abstraction_level": "operational",
            "schema_version": "1.0",
            "created_at_utc": 1.0,
        }
    )
    # 3 focused comments with issue markers and upvotes
    focused_signals = [
        _make_signal("c1", "the error crash is broken", 5),
        _make_signal("c2", "slow failure on large threads", 5),
        _make_signal("c3", "bug in the export workflow", 5),
    ]
    # issue_markers=6, request_markers=0, upvotes=15
    # raw_score = 6*3 + 0 + 15 = 33, density = 33/3 = 11, weighted = 11 * 3 = 33
    severity = score_severity(focused_signals, 6, 0, contract=contract)
    assert severity == "high"


# ---------------------------------------------------------------------------
# Sub-clustering: decompose_catch_all
# ---------------------------------------------------------------------------


def test_decompose_catch_all_skips_when_below_ratio() -> None:
    signals = [_make_signal(f"c{i}", f"generic comment about things {i}", 1) for i in range(3)]
    result = decompose_catch_all(signals, total_signal_count=10, default_theme="general_feedback")

    assert len(result) == 1
    assert result[0][0] == "general_feedback"
    assert len(result[0][1]) == 3


def test_decompose_catch_all_creates_sub_clusters_when_dominant() -> None:
    cluster_a = [
        _make_signal(f"a{i}", f"obsidian vault embedding vector notes topic {i}", 3)
        for i in range(4)
    ]
    cluster_b = [
        _make_signal(f"b{i}", f"performance slow latency memory gpu speed issue {i}", 3)
        for i in range(4)
    ]
    outlier = _make_signal("out1", "random unrelated standalone comment here", 1)
    all_signals = cluster_a + cluster_b + [outlier]

    result = decompose_catch_all(
        all_signals,
        total_signal_count=len(all_signals),
        default_theme="general_feedback",
    )

    sub_keys = [key for key, _ in result]
    assert any(key.startswith("general_feedback.") for key in sub_keys)
    total_assigned = sum(len(sigs) for _, sigs in result)
    assert total_assigned == len(all_signals)


def test_decompose_catch_all_preserves_remainder_in_default() -> None:
    clustered = [
        _make_signal(f"c{i}", f"obsidian vault notes embedding knowledge {i}", 3) for i in range(4)
    ]
    loners = [
        _make_signal("lone0", "the weather is nice today outside", 1),
        _make_signal("lone1", "basketball playoffs happening soon", 1),
    ]
    all_signals = clustered + loners

    result = decompose_catch_all(
        all_signals,
        total_signal_count=len(all_signals),
        default_theme="general_feedback",
    )

    has_remainder = any(key == "general_feedback" for key, _ in result)
    assert has_remainder


# ---------------------------------------------------------------------------
# Noise filter
# ---------------------------------------------------------------------------


def test_noise_filter_excludes_short_empty_comments() -> None:
    signal = _make_signal("c1", "ok", 1)
    assert is_noise_signal(signal) is True


def test_noise_filter_preserves_short_comments_with_markers() -> None:
    signal = _make_signal("c1", "bug in export", 1)
    assert is_noise_signal(signal) is False


def test_noise_filter_detects_reminder_bots() -> None:
    signal = _make_signal("c1", "RemindMe! 30 days to check this thread", 1)
    assert is_noise_signal(signal) is True


def test_noise_filter_detects_bot_accounts() -> None:
    signal = _make_signal("c1", "This is a bot account", 1)
    assert is_noise_signal(signal) is True


def test_noise_filter_detects_acknowledgements() -> None:
    signal = _make_signal("c1", "thanks", 1)
    assert is_noise_signal(signal) is True


def test_noise_filter_preserves_substantive_comments() -> None:
    signal = _make_signal(
        "c1",
        "I have been running something close to what you describe for about a year "
        "with two Obsidian vaults and a self-hosted Paperless instance",
        5,
    )
    assert is_noise_signal(signal) is False
