from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from time import time
from typing import Any

import pytest

from threadsense.config import RedditConfig
from threadsense.connectors import SourceConnector
from threadsense.connectors.reddit import (
    RedditConnector,
    RedditSearchRequest,
    RedditThreadRequest,
    collect_more_ids,
    extract_search_matches,
    flatten,
    map_time_window_to_reddit_bucket,
    normalize_url,
    parse_comment,
    should_retry_error,
    validate_subreddit_name,
    validate_thread_payload,
)
from threadsense.errors import RedditInputError, RedditRequestError, RedditResponseError


def load_fixture(name: str) -> object:
    return json.loads(Path(f"tests/fixtures/reddit/raw/{name}").read_text(encoding="utf-8"))


def test_normalize_url_strips_query_fragment_and_adds_json_suffix() -> None:
    normalized = normalize_url(
        "https://www.reddit.com/r/python/comments/abc123/example_post/?utm_source=test#frag"
    )

    assert normalized == "https://www.reddit.com/r/python/comments/abc123/example_post.json"


def test_normalize_url_rejects_non_reddit_host() -> None:
    with pytest.raises(RedditInputError):
        normalize_url("https://example.com/thread/abc123")


def test_parse_comment_preserves_nested_depth() -> None:
    payload = load_fixture("normal_thread.json")
    assert isinstance(payload, list)

    comment = parse_comment(payload[1]["data"]["children"][0])

    assert comment is not None
    assert comment.id == "c1"
    assert len(comment.replies) == 1
    assert comment.replies[0].depth == 1


def test_parse_comment_skips_deleted_and_removed_bodies() -> None:
    deleted_payload = load_fixture("deleted_thread.json")
    removed_payload = load_fixture("removed_thread.json")
    assert isinstance(deleted_payload, list)
    assert isinstance(removed_payload, list)

    assert parse_comment(deleted_payload[1]["data"]["children"][0]) is None
    assert parse_comment(removed_payload[1]["data"]["children"][0]) is None


def test_flatten_preserves_depth_first_comment_order() -> None:
    payload = load_fixture("normal_thread.json")
    assert isinstance(payload, list)

    first_comment = parse_comment(payload[1]["data"]["children"][0])
    second_comment = parse_comment(payload[1]["data"]["children"][1])

    assert first_comment is not None
    assert second_comment is not None
    assert [comment.id for comment in flatten([first_comment, second_comment])] == [
        "c1",
        "c1a",
        "c2",
    ]


def test_collect_more_ids_extracts_deferred_comment_ids() -> None:
    payload = load_fixture("large_thread.json")
    assert isinstance(payload, list)

    comments, more_ids = collect_more_ids(payload[1]["data"]["children"])

    assert len(comments) == 1
    assert more_ids == ["extra1", "extra2"]


def test_retry_policy_marks_retryable_status_codes() -> None:
    retryable_error = RedditRequestError("retry", details={"status_code": 429})
    terminal_error = RedditRequestError("terminal", details={"status_code": 404})

    assert should_retry_error(retryable_error) is True
    assert should_retry_error(terminal_error) is False


def test_validate_thread_payload_rejects_invalid_shape() -> None:
    with pytest.raises(RedditResponseError):
        validate_thread_payload({"not": "a list"})


def test_connector_expands_morechildren_from_fixture() -> None:
    payload_map: dict[
        tuple[str, tuple[tuple[str, str | int | float | bool], ...]],
        Any,
    ] = {
        (
            "https://www.reddit.com/r/ThreadSense/comments/jkl012/large_thread.json",
            (("limit", 500),),
        ): load_fixture("large_thread.json"),
        (
            "https://www.reddit.com/api/morechildren.json",
            (
                ("api_type", "json"),
                ("children", "extra1,extra2"),
                ("limit_children", False),
                ("link_id", "t3_jkl012"),
            ),
        ): load_fixture("morechildren_response.json"),
    }

    def fake_transport(
        url: str,
        headers: Mapping[str, str],
        params: Mapping[str, str | int | float | bool],
        timeout: float,
    ) -> object:
        assert headers["User-Agent"].startswith("threadsense/")
        assert timeout == 15
        key = (url, tuple(sorted(params.items())))
        return payload_map[key]

    connector = RedditConnector(
        config=RedditConfig(
            user_agent="threadsense/test",
            timeout_seconds=15,
            max_retries=0,
            backoff_seconds=0.1,
            request_delay_seconds=0,
            listing_limit=500,
        ),
        transport=fake_transport,
        sleeper=lambda value: None,
    )
    assert isinstance(connector, SourceConnector)

    result = connector.fetch_thread(
        RedditThreadRequest(
            post_url="https://www.reddit.com/r/ThreadSense/comments/jkl012/large_thread",
            expand_more=True,
        )
    )

    assert result.post.id == "jkl012"
    assert result.total_comment_count == 3
    assert result.expanded_more_count == 2


def test_extract_post_preserves_selftext() -> None:
    payload = load_fixture("normal_thread.json")
    assert isinstance(payload, list)

    connector = RedditConnector(
        config=RedditConfig(
            user_agent="threadsense/test",
            timeout_seconds=15,
            max_retries=0,
            backoff_seconds=0.1,
            request_delay_seconds=0,
            listing_limit=500,
        ),
        transport=lambda url, headers, params, timeout: payload,
        sleeper=lambda value: None,
    )
    result = connector.fetch_thread(
        RedditThreadRequest(
            post_url="https://www.reddit.com/r/ThreadSense/comments/abc123/normal_thread",
        )
    )

    assert result.post.selftext == "Exploring a second brain workflow with agents."


def test_map_time_window_to_reddit_bucket_supports_30_days() -> None:
    assert map_time_window_to_reddit_bucket("30d") == "month"
    assert map_time_window_to_reddit_bucket("7d") == "week"
    assert map_time_window_to_reddit_bucket("all") == "all"


def test_validate_subreddit_name_strips_prefix() -> None:
    assert validate_subreddit_name("r/ClaudeCode") == "ClaudeCode"


def test_extract_search_matches_reads_title_and_selftext() -> None:
    payload = {
        "data": {
            "children": [
                {
                    "kind": "t3",
                    "data": {
                        "id": "abc123",
                        "title": "Agentic PKM for Claude Code",
                        "selftext": "I am building a second brain with local agents.",
                        "subreddit": "ClaudeCode",
                        "author": "builder",
                        "permalink": "/r/ClaudeCode/comments/abc123/example/",
                        "score": 42,
                        "num_comments": 12,
                        "created_utc": 1710000000.0,
                    },
                }
            ]
        }
    }

    matches = extract_search_matches(payload, "ClaudeCode")

    assert matches[0].title == "Agentic PKM for Claude Code"
    assert matches[0].selftext == "I am building a second brain with local agents."
    assert matches[0].thread_url.startswith("https://www.reddit.com/")
    assert matches[0].normalized_url.endswith(".json")


def test_connector_searches_selected_subreddits() -> None:
    recent = time() - 86400
    payload_map: dict[tuple[str, str], object] = {
        (
            "https://www.reddit.com/r/claudecode/search.json",
            "second brain",
        ): {
            "data": {
                "children": [
                    {
                        "kind": "t3",
                        "data": {
                            "id": "abc123",
                            "title": "Second brain workflow",
                            "selftext": "Agentic PKM patterns",
                            "subreddit": "ClaudeCode",
                            "author": "alpha",
                            "permalink": "/r/ClaudeCode/comments/abc123/example/",
                            "score": 10,
                            "num_comments": 3,
                            "created_utc": recent,
                        },
                    }
                ]
            }
        },
        (
            "https://www.reddit.com/r/claudecode/search.json",
            "agentic PKM",
        ): {"data": {"children": []}},
        (
            "https://www.reddit.com/r/AI_Agents/search.json",
            "second brain",
        ): {"data": {"children": []}},
        (
            "https://www.reddit.com/r/AI_Agents/search.json",
            "agentic PKM",
        ): {
            "data": {
                "children": [
                    {
                        "kind": "t3",
                        "data": {
                            "id": "def456",
                            "title": "Agentic PKM stack",
                            "selftext": "Second brain notes",
                            "subreddit": "AI_Agents",
                            "author": "beta",
                            "permalink": "/r/AI_Agents/comments/def456/example/",
                            "score": 15,
                            "num_comments": 5,
                            "created_utc": recent,
                        },
                    }
                ]
            }
        },
    }

    def fake_transport(
        url: str,
        headers: Mapping[str, str],
        params: Mapping[str, str | int | float | bool],
        timeout: float,
    ) -> object:
        del headers, timeout
        assert params["q"] in {"second brain", "agentic PKM"}
        assert params["restrict_sr"] == "on"
        assert params["t"] == "month"
        return payload_map[(url, str(params["q"]))]

    connector = RedditConnector(
        config=RedditConfig(
            user_agent="threadsense/test",
            timeout_seconds=15,
            max_retries=0,
            backoff_seconds=0.1,
            request_delay_seconds=0,
            listing_limit=500,
        ),
        transport=fake_transport,
        sleeper=lambda value: None,
    )

    result = connector.search_threads(
        RedditSearchRequest(
            query="second brain OR agentic PKM",
            subreddits=["claudecode", "AI_Agents"],
            limit=5,
            sort="relevance",
            time_window="30d",
        )
    )

    assert result.reddit_time_bucket == "month"
    assert len(result.matches) == 2


def test_connector_searches_or_clauses_as_union() -> None:
    recent = time() - 86400
    calls: list[tuple[str, str]] = []

    def fake_transport(
        url: str,
        headers: Mapping[str, str],
        params: Mapping[str, str | int | float | bool],
        timeout: float,
    ) -> object:
        del headers, timeout
        calls.append((url, str(params["q"])))
        if str(params["q"]) == "second brain":
            return {
                "data": {
                    "children": [
                        {
                            "kind": "t3",
                            "data": {
                                "id": "abc123",
                                "title": "Second brain workflow",
                                "selftext": "",
                                "subreddit": "ClaudeCode",
                                "author": "alpha",
                                "permalink": "/r/ClaudeCode/comments/abc123/example/",
                                "score": 10,
                                "num_comments": 3,
                                "created_utc": recent,
                            },
                        }
                    ]
                }
            }
        return {
            "data": {
                "children": [
                    {
                        "kind": "t3",
                        "data": {
                            "id": "def456",
                            "title": "Agentic PKM stack",
                            "selftext": "",
                            "subreddit": "ClaudeCode",
                            "author": "beta",
                            "permalink": "/r/ClaudeCode/comments/def456/example/",
                            "score": 8,
                            "num_comments": 2,
                            "created_utc": recent,
                        },
                    }
                ]
            }
        }

    connector = RedditConnector(
        config=RedditConfig(
            user_agent="threadsense/test",
            timeout_seconds=15,
            max_retries=0,
            backoff_seconds=0.1,
            request_delay_seconds=0,
            listing_limit=500,
        ),
        transport=fake_transport,
        sleeper=lambda value: None,
    )

    result = connector.search_threads(
        RedditSearchRequest(
            query="second brain OR agentic PKM",
            subreddits=["ClaudeCode"],
            limit=5,
            sort="relevance",
            time_window="30d",
        )
    )

    assert calls == [
        ("https://www.reddit.com/r/ClaudeCode/search.json", "second brain"),
        ("https://www.reddit.com/r/ClaudeCode/search.json", "agentic PKM"),
    ]
    assert {match.post_id for match in result.matches} == {"abc123", "def456"}


def test_connector_filters_search_results_to_requested_window() -> None:
    recent = time() - 3600
    stale = time() - (40 * 86400)
    payload = {
        "data": {
            "children": [
                {
                    "kind": "t3",
                    "data": {
                        "id": "recent1",
                        "title": "Recent topic thread",
                        "selftext": "Recent body",
                        "subreddit": "ClaudeCode",
                        "author": "alpha",
                        "permalink": "/r/ClaudeCode/comments/recent1/example/",
                        "score": 10,
                        "num_comments": 3,
                        "created_utc": recent,
                    },
                },
                {
                    "kind": "t3",
                    "data": {
                        "id": "stale1",
                        "title": "Old topic thread",
                        "selftext": "Old body",
                        "subreddit": "ClaudeCode",
                        "author": "beta",
                        "permalink": "/r/ClaudeCode/comments/stale1/example/",
                        "score": 12,
                        "num_comments": 4,
                        "created_utc": stale,
                    },
                },
            ]
        }
    }

    connector = RedditConnector(
        config=RedditConfig(
            user_agent="threadsense/test",
            timeout_seconds=15,
            max_retries=0,
            backoff_seconds=0.1,
            request_delay_seconds=0,
            listing_limit=500,
        ),
        transport=lambda url, headers, params, timeout: payload,
        sleeper=lambda value: None,
    )

    result = connector.search_threads(
        RedditSearchRequest(
            query="second brain",
            subreddits=["ClaudeCode"],
            limit=5,
            sort="relevance",
            time_window="30d",
        )
    )

    assert [match.post_id for match in result.matches] == ["recent1"]
