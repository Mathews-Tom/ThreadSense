from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from threadsense.config import RedditConfig
from threadsense.connectors.reddit import (
    RedditConnector,
    RedditThreadRequest,
    collect_more_ids,
    flatten,
    normalize_url,
    parse_comment,
    should_retry_error,
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

    result = connector.fetch_thread(
        RedditThreadRequest(
            post_url="https://www.reddit.com/r/ThreadSense/comments/jkl012/large_thread",
            expand_more=True,
        )
    )

    assert result.post.id == "jkl012"
    assert result.total_comment_count == 3
    assert result.expanded_more_count == 2
