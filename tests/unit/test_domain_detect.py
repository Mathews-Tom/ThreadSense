from __future__ import annotations

from threadsense.contracts import DomainType
from threadsense.models.canonical import (
    AuthorRef,
    Comment,
    ProvenanceMetadata,
    SourceRef,
    Thread,
)
from threadsense.pipeline.domain_detect import (
    detect_domain,
)


def _thread(comments: list[Comment]) -> Thread:
    return Thread(
        thread_id="reddit:test",
        source=SourceRef(
            source_name="reddit",
            community="test",
            source_thread_id="test",
            thread_url="https://example.com/thread",
        ),
        title="Test thread",
        body=None,
        permalink="https://example.com/thread",
        author=AuthorRef(username="op", source_author_id=None),
        comments=comments,
        comment_count=len(comments),
        provenance=ProvenanceMetadata(
            raw_artifact_path="/tmp/raw.json",
            raw_sha256="sha",
            retrieved_at_utc=1.0,
            normalized_at_utc=2.0,
            schema_version=1,
            normalization_version="reddit-to-canonical-v1",
        ),
    )


def _comment(comment_id: str, body: str, score: int = 5) -> Comment:
    return Comment(
        thread_id="reddit:test",
        comment_id=comment_id,
        parent_comment_id=None,
        author=AuthorRef(username="user", source_author_id=None),
        body=body,
        score=score,
        created_utc=1.0,
        depth=0,
        permalink="https://example.com",
    )


def test_detect_domain_keeps_configured_when_fit_is_good() -> None:
    thread = _thread(
        [
            _comment("c1", "The docs are confusing and the tutorial is incomplete"),
            _comment("c2", "Search is slow and performance is laggy"),
            _comment("c3", "I hit an error crash when expanding comments"),
            _comment("c4", "Can you add export to the dashboard workflow"),
        ]
    )
    result = detect_domain(thread, DomainType.DEVELOPER_TOOLS)

    assert result.selected == DomainType.DEVELOPER_TOOLS
    assert result.switched is False
    configured_score = next(s for s in result.scores if s.domain == DomainType.DEVELOPER_TOOLS)
    assert configured_score.fit_score >= 0.3


def test_detect_domain_switches_when_configured_domain_is_poor_fit() -> None:
    thread = _thread(
        [
            _comment("c1", "The interview process had a system design round"),
            _comment("c2", "The recruiter ghosted me after the offer"),
            _comment("c3", "Compensation was low and no equity stock bonus"),
            _comment("c4", "Culture is toxic and work life balance is terrible"),
            _comment("c5", "I need advice on salary negotiation for senior role"),
        ]
    )
    result = detect_domain(thread, DomainType.DEVELOPER_TOOLS)

    assert result.selected == DomainType.HIRING_CAREERS
    assert result.switched is True


def test_detect_domain_returns_configured_when_nothing_fits() -> None:
    thread = _thread(
        [
            _comment("c1", "This is a random discussion about nothing specific"),
            _comment("c2", "I agree with the above completely"),
            _comment("c3", "Interesting perspective on things"),
        ]
    )
    result = detect_domain(thread, DomainType.DEVELOPER_TOOLS)

    assert result.selected == DomainType.DEVELOPER_TOOLS
    assert result.switched is False


def test_detect_domain_handles_empty_thread() -> None:
    thread = _thread([])
    result = detect_domain(thread, DomainType.DEVELOPER_TOOLS)

    assert result.selected == DomainType.DEVELOPER_TOOLS
    assert result.switched is False
    assert result.scores == []


def test_detect_domain_samples_top_level_comments_only() -> None:
    top_level = _comment("c1", "The error crash is a bug in the retry logic")
    reply = Comment(
        thread_id="reddit:test",
        comment_id="c2",
        parent_comment_id="c1",
        author=AuthorRef(username="user", source_author_id=None),
        body="The salary and compensation is low",
        score=10,
        created_utc=2.0,
        depth=1,
        permalink="https://example.com",
    )
    thread = _thread([top_level, reply])
    result = detect_domain(thread, DomainType.DEVELOPER_TOOLS)

    # Only top-level comment should be sampled
    configured_score = next(s for s in result.scores if s.domain == DomainType.DEVELOPER_TOOLS)
    assert configured_score.total_sampled == 1


def test_detect_domain_scores_all_non_custom_domains() -> None:
    thread = _thread([_comment("c1", "performance is slow and laggy")])
    result = detect_domain(thread, DomainType.DEVELOPER_TOOLS)

    scored_domains = {s.domain for s in result.scores}
    assert DomainType.CUSTOM not in scored_domains
    assert DomainType.DEVELOPER_TOOLS in scored_domains
    assert DomainType.GAMING in scored_domains


def test_detect_domain_respects_min_fit_score() -> None:
    thread = _thread(
        [
            _comment("c1", "The interview process had a system design round"),
            _comment("c2", "Random unrelated comment"),
            _comment("c3", "Another generic comment here"),
            _comment("c4", "Nothing specific to any domain really"),
        ]
    )
    # With high threshold, even the best domain might not qualify
    result = detect_domain(thread, DomainType.DEVELOPER_TOOLS, min_fit_score=0.9)

    assert result.selected == DomainType.DEVELOPER_TOOLS
    assert result.switched is False


def test_detect_domain_architecture_theme_matches_developer_tools() -> None:
    thread = _thread(
        [
            _comment("c1", "I built a self-hosted server with a database and vector embedding"),
            _comment("c2", "The architecture uses mcp plugin for the api schema"),
            _comment("c3", "My stack includes obsidian vault with github integration"),
        ]
    )
    result = detect_domain(thread, DomainType.DEVELOPER_TOOLS)

    assert result.selected == DomainType.DEVELOPER_TOOLS
    assert result.switched is False
    configured_score = next(s for s in result.scores if s.domain == DomainType.DEVELOPER_TOOLS)
    assert configured_score.fit_score >= 0.3
