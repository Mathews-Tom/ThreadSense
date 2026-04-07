from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from threadsense.config import GitHubGistConfig
from threadsense.connectors import FetchRequest, SourceConnector
from threadsense.connectors.github_gist import (
    GitHubGistConnector,
    normalize_url,
    parse_timestamp,
)
from threadsense.errors import GitHubInputError, GitHubRequestError


def _gist_fixture() -> dict[str, Any]:
    return {
        "id": "442a6bf555914893e9891c11519de94f",
        "node_id": "G_kwDOABcdef",
        "description": "Deep Learning Curriculum",
        "public": True,
        "html_url": "https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f",
        "created_at": "2026-04-04T16:49:00Z",
        "updated_at": "2026-04-04T17:42:00Z",
        "comments": 2,
        "owner": {"login": "karpathy"},
        "files": {
            "curriculum.md": {
                "filename": "curriculum.md",
                "language": "Markdown",
                "size": 5000,
                "content": "# Deep Learning Curriculum\n\nThis is a curriculum.",
                "raw_url": "https://gist.githubusercontent.com/karpathy/.../curriculum.md",
            }
        },
        "forks": [],
    }


def _comments_fixture() -> list[dict[str, Any]]:
    return [
        {
            "id": 1001,
            "node_id": "GC_kwDOAAABBB",
            "body": "Thank you Andrej! This is extremely helpful.",
            "created_at": "2026-04-04T16:50:00Z",
            "updated_at": "2026-04-04T16:50:00Z",
            "url": "https://api.github.com/gists/442a6bf5/comments/1001",
            "user": {"login": "alice"},
            "author_association": "NONE",
        },
        {
            "id": 1002,
            "node_id": "GC_kwDOAAABCC",
            "body": "How much time did this take? I want to set expectations.",
            "created_at": "2026-04-04T16:52:00Z",
            "updated_at": "2026-04-04T16:52:00Z",
            "url": "https://api.github.com/gists/442a6bf5/comments/1002",
            "user": {"login": "bob"},
            "author_association": "CONTRIBUTOR",
        },
    ]


def _make_transport(
    gist_data: dict[str, Any],
    comments_data: list[dict[str, Any]],
) -> Any:
    def transport(
        url: str,
        headers: Any,
        timeout: float,
    ) -> tuple[Any, httpx.Headers]:
        raw_headers = httpx.Headers({"X-RateLimit-Remaining": "4999"})
        if "/comments" in url:
            return comments_data, raw_headers
        return gist_data, raw_headers

    return transport


class TestNormalizeUrl:
    def test_with_owner(self) -> None:
        url, gist_id = normalize_url(
            "https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f"
        )
        assert url == "https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f"
        assert gist_id == "442a6bf555914893e9891c11519de94f"

    def test_bare_id(self) -> None:
        url, gist_id = normalize_url("https://gist.github.com/442a6bf555914893e9891c11519de94f")
        assert url == "https://gist.github.com/442a6bf555914893e9891c11519de94f"
        assert gist_id == "442a6bf555914893e9891c11519de94f"

    def test_strips_query_and_trailing_slash(self) -> None:
        url, gist_id = normalize_url("https://gist.github.com/user/abcdef1234567890/?tab=comments")
        assert gist_id == "abcdef1234567890"

    def test_rejects_non_gist_domain(self) -> None:
        with pytest.raises(GitHubInputError):
            normalize_url("https://github.com/user/repo")

    def test_rejects_invalid_gist_path(self) -> None:
        with pytest.raises(GitHubInputError):
            normalize_url("https://gist.github.com/user/repo/extra/path")

    def test_rejects_non_hex_id(self) -> None:
        with pytest.raises(GitHubInputError):
            normalize_url("https://gist.github.com/user/not-hex-id")


class TestParseTimestamp:
    def test_iso_timestamp(self) -> None:
        ts = parse_timestamp("2026-04-04T16:49:00Z")
        assert ts > 0

    def test_empty_string(self) -> None:
        assert parse_timestamp("") == 0.0


class TestGitHubGistConnector:
    def test_implements_source_connector_protocol(self) -> None:
        connector = GitHubGistConnector(GitHubGistConfig())
        assert isinstance(connector, SourceConnector)

    def test_source_name(self) -> None:
        connector = GitHubGistConnector(GitHubGistConfig())
        assert connector.source_name == "github_gist"

    def test_supports_gist_url(self) -> None:
        connector = GitHubGistConnector(GitHubGistConfig())
        assert connector.supports_url("https://gist.github.com/user/abc123def456")
        assert not connector.supports_url("https://github.com/user/repo")
        assert not connector.supports_url("https://reddit.com/r/test")

    def test_fetch_returns_raw_artifact(self) -> None:
        gist_data = _gist_fixture()
        comments_data = _comments_fixture()
        transport = _make_transport(gist_data, comments_data)

        connector = GitHubGistConnector(
            GitHubGistConfig(),
            transport=transport,
            sleeper=lambda _: None,
        )

        result = connector.fetch(
            FetchRequest(url="https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f")
        )

        assert result.source_name == "github_gist"
        assert result.source_thread_id == "442a6bf555914893e9891c11519de94f"
        assert result.thread_title == "Deep Learning Curriculum"
        assert result.total_comment_count == 2
        assert result.normalized_url == gist_data["html_url"]
        assert result.cache_status == "miss"
        assert result.rate_limit_remaining == 4999

    def test_fetch_serializes_to_dict(self) -> None:
        gist_data = _gist_fixture()
        comments_data = _comments_fixture()
        transport = _make_transport(gist_data, comments_data)

        connector = GitHubGistConnector(
            GitHubGistConfig(),
            transport=transport,
            sleeper=lambda _: None,
        )

        result = connector.fetch(
            FetchRequest(url="https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f")
        )
        serialized = result.to_dict()

        assert serialized["artifact_version"] == 2
        assert serialized["source"] == "github_gist"
        assert len(serialized["comments"]) == 2
        assert serialized["gist"]["gist_id"] == "442a6bf555914893e9891c11519de94f"

    def test_fetch_and_normalize_roundtrip(self, tmp_path: Path) -> None:
        gist_data = _gist_fixture()
        comments_data = _comments_fixture()
        transport = _make_transport(gist_data, comments_data)

        connector = GitHubGistConnector(
            GitHubGistConfig(),
            transport=transport,
            sleeper=lambda _: None,
        )

        result = connector.fetch(
            FetchRequest(url="https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f")
        )
        raw_path = tmp_path / "gist.json"
        raw_path.write_text(json.dumps(result.to_dict()), encoding="utf-8")

        thread = connector.normalize(result.to_dict(), raw_path)

        assert thread.thread_id == "gist:442a6bf555914893e9891c11519de94f"
        assert thread.title == "Deep Learning Curriculum"
        assert thread.source.source_name == "github_gist"
        assert thread.source.community == "gist.github.com"
        assert thread.comment_count == 2
        assert all(c.depth == 0 for c in thread.comments)
        assert all(c.parent_comment_id is None for c in thread.comments)
        assert thread.comments[0].comment_id == "gist:1001"
        assert thread.comments[0].author.username == "alice"

    def test_fetch_multi_page_comments(self) -> None:
        gist_data = _gist_fixture()
        page1 = [
            {
                "id": i,
                "node_id": f"GC_{i}",
                "body": f"Comment {i}",
                "created_at": "2026-04-04T17:00:00Z",
                "updated_at": "2026-04-04T17:00:00Z",
                "url": f"https://api.github.com/gists/442a/comments/{i}",
                "user": {"login": f"user{i}"},
                "author_association": "NONE",
            }
            for i in range(100)
        ]
        page2 = [
            {
                "id": 100,
                "node_id": "GC_100",
                "body": "Last comment",
                "created_at": "2026-04-04T17:01:00Z",
                "updated_at": "2026-04-04T17:01:00Z",
                "url": "https://api.github.com/gists/442a/comments/100",
                "user": {"login": "lastuser"},
                "author_association": "NONE",
            }
        ]

        call_count = 0

        def transport(url: str, headers: Any, timeout: float) -> tuple[Any, httpx.Headers]:
            nonlocal call_count
            raw_headers = httpx.Headers({"X-RateLimit-Remaining": "4998"})
            if "/comments" in url:
                call_count += 1
                if "&page=1" in url:
                    return page1, raw_headers
                return page2, raw_headers
            return gist_data, raw_headers

        connector = GitHubGistConnector(
            GitHubGistConfig(comments_per_page=100),
            transport=transport,
            sleeper=lambda _: None,
        )

        result = connector.fetch(
            FetchRequest(url="https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f")
        )

        assert result.total_comment_count == 101
        assert call_count == 2

    def test_fetch_unauthenticated(self) -> None:
        captured_headers: list[dict[str, str]] = []

        def transport(
            url: str,
            headers: Any,
            timeout: float,
        ) -> tuple[Any, httpx.Headers]:
            captured_headers.append(dict(headers))
            raw_headers = httpx.Headers({"X-RateLimit-Remaining": "59"})
            if "/comments" in url:
                return [], raw_headers
            return _gist_fixture(), raw_headers

        connector = GitHubGistConnector(
            GitHubGistConfig(token=""),
            transport=transport,
            sleeper=lambda _: None,
        )

        connector.fetch(
            FetchRequest(url="https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f")
        )

        assert "Authorization" not in captured_headers[0]

    def test_fetch_authenticated(self) -> None:
        captured_headers: list[dict[str, str]] = []

        def transport(
            url: str,
            headers: Any,
            timeout: float,
        ) -> tuple[Any, httpx.Headers]:
            captured_headers.append(dict(headers))
            raw_headers = httpx.Headers({"X-RateLimit-Remaining": "4999"})
            if "/comments" in url:
                return [], raw_headers
            return _gist_fixture(), raw_headers

        connector = GitHubGistConnector(
            GitHubGistConfig(token="ghp_test_token"),
            transport=transport,
            sleeper=lambda _: None,
        )

        connector.fetch(
            FetchRequest(url="https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f")
        )

        assert captured_headers[0]["Authorization"] == "Bearer ghp_test_token"

    def test_fetch_error_on_non_dict_gist_response(self) -> None:
        def transport(
            url: str,
            headers: Any,
            timeout: float,
        ) -> tuple[Any, httpx.Headers]:
            return [], httpx.Headers()

        connector = GitHubGistConnector(
            GitHubGistConfig(),
            transport=transport,
            sleeper=lambda _: None,
        )

        with pytest.raises(GitHubRequestError, match="must decode to an object"):
            connector.fetch(
                FetchRequest(
                    url="https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f"
                )
            )

    def test_thread_title_falls_back_to_filename(self) -> None:
        gist_data = _gist_fixture()
        gist_data["description"] = None

        transport = _make_transport(gist_data, [])

        connector = GitHubGistConnector(
            GitHubGistConfig(),
            transport=transport,
            sleeper=lambda _: None,
        )

        result = connector.fetch(
            FetchRequest(url="https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f")
        )

        assert result.thread_title == "curriculum.md"
