from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from threadsense.config import GitHubConfig
from threadsense.connectors import FetchRequest, SourceConnector
from threadsense.connectors.github_discussions import GitHubDiscussionsConnector, normalize_url
from threadsense.errors import ConfigurationError


def graphql_fixture() -> dict[str, Any]:
    return {
        "data": {
            "repository": {
                "discussion": {
                    "title": "Better onboarding",
                    "url": "https://github.com/acme/demo/discussions/42",
                    "createdAt": "2026-04-03T10:00:00Z",
                    "author": {"login": "maintainer"},
                    "comments": {
                        "nodes": [
                            {
                                "id": "DC_kwDOAAAB",
                                "body": "The setup guide is confusing.",
                                "createdAt": "2026-04-03T11:00:00Z",
                                "url": "https://github.com/acme/demo/discussions/42#discussioncomment-1",
                                "reactions": {"totalCount": 4},
                                "author": {"login": "alice"},
                                "replies": {
                                    "nodes": [
                                        {
                                            "id": "DC_kwDOAAAC",
                                            "body": "A quickstart would help.",
                                            "createdAt": "2026-04-03T12:00:00Z",
                                            "url": "https://github.com/acme/demo/discussions/42#discussioncomment-2",
                                            "reactions": {"totalCount": 2},
                                            "author": {"login": "bob"},
                                        }
                                    ]
                                },
                            }
                        ]
                    },
                }
            },
            "rateLimit": {"remaining": 4999},
        }
    }


def test_github_discussions_normalize_url() -> None:
    normalized_url, owner, repo, number = normalize_url(
        "https://github.com/acme/demo/discussions/42?sort=top"
    )

    assert normalized_url == "https://github.com/acme/demo/discussions/42"
    assert owner == "acme"
    assert repo == "demo"
    assert number == 42


def test_github_discussions_connector_requires_token() -> None:
    connector = GitHubDiscussionsConnector(GitHubConfig(token=""))

    with pytest.raises(ConfigurationError):
        connector.fetch(FetchRequest(url="https://github.com/acme/demo/discussions/42"))


def test_github_discussions_connector_fetches_and_normalizes(tmp_path: Path) -> None:
    connector = GitHubDiscussionsConnector(
        GitHubConfig(token="token"),
        transport=lambda url, headers, payload, timeout: graphql_fixture(),
    )
    assert isinstance(connector, SourceConnector)

    result = connector.fetch(FetchRequest(url="https://github.com/acme/demo/discussions/42"))
    raw_path = tmp_path / "github.json"
    raw_path.write_text(json.dumps(result.to_dict()), encoding="utf-8")

    thread = connector.normalize(result.to_dict(), raw_path)

    assert result.source_name == "github_discussions"
    assert result.source_thread_id == "acme/demo/discussions/42"
    assert result.total_comment_count == 2
    assert thread.thread_id == "gh:acme/demo/discussions/42"
    assert thread.comments[1].parent_comment_id == "gh:DC_kwDOAAAB"
