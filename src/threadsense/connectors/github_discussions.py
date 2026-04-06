from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from time import time
from typing import Any
from urllib import parse

import httpx

from threadsense.config import GitHubConfig
from threadsense.connectors import FetchRequest, RawArtifact
from threadsense.errors import (
    ConfigurationError,
    NetworkBoundaryError,
    RedditInputError,
    RedditRequestError,
)

JsonObject = dict[str, Any]
GraphQlTransport = Callable[[str, Mapping[str, str], JsonObject, float], JsonObject]
_DISCUSSION_PATH = re.compile(r"^/([^/]+)/([^/]+)/discussions/(\d+)$")


@dataclass(frozen=True)
class GitHubDiscussionComment:
    node_id: str
    author: str
    body: str
    score: int
    created_utc: float
    depth: int
    parent_node_id: str | None
    url: str
    replies: tuple[GitHubDiscussionComment, ...]


@dataclass(frozen=True)
class GitHubDiscussion:
    owner: str
    repo: str
    number: int
    title: str
    body: str | None
    author: str
    url: str
    created_utc: float


@dataclass(frozen=True)
class GitHubDiscussionsResult:
    requested_url: str
    fetched_at_utc: float
    discussion: GitHubDiscussion
    comments: list[GitHubDiscussionComment]
    total_comment_count: int
    cache_status: str
    rate_limit_remaining: int | None
    raw_payload: JsonObject

    @property
    def source_name(self) -> str:
        return "github_discussions"

    @property
    def source_thread_id(self) -> str:
        discussion = self.discussion
        return f"{discussion.owner}/{discussion.repo}/discussions/{discussion.number}"

    @property
    def thread_title(self) -> str:
        return self.discussion.title

    @property
    def normalized_url(self) -> str:
        return self.discussion.url

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_version": 2,
            "source": self.source_name,
            "requested_url": self.requested_url,
            "normalized_url": self.normalized_url,
            "fetched_at_utc": self.fetched_at_utc,
            "discussion": {
                "owner": self.discussion.owner,
                "repo": self.discussion.repo,
                "number": self.discussion.number,
                "title": self.discussion.title,
                "body": self.discussion.body,
                "author": self.discussion.author,
                "url": self.discussion.url,
                "created_utc": self.discussion.created_utc,
            },
            "comments": [comment_to_dict(comment) for comment in self.comments],
            "total_comment_count": self.total_comment_count,
            "cache_status": self.cache_status,
            "rate_limit_remaining": self.rate_limit_remaining,
            "raw_payload": self.raw_payload,
        }


class GitHubDiscussionsConnector:
    source_name = "github_discussions"

    def __init__(
        self,
        config: GitHubConfig,
        transport: GraphQlTransport | None = None,
    ) -> None:
        self._config = config
        self._transport = transport or send_graphql_request

    def fetch(self, request: FetchRequest) -> RawArtifact:
        if not self._config.token:
            raise ConfigurationError("github token is required for github discussions fetch")
        normalized_url, owner, repo, number = normalize_url(request.url)
        headers = {
            "Authorization": f"Bearer {self._config.token}",
            "Content-Type": "application/json",
        }
        query = build_discussion_query()
        payload = self._transport(
            self._config.base_url,
            headers,
            {"query": query, "variables": {"owner": owner, "repo": repo, "number": number}},
            self._config.timeout_seconds,
        )
        discussion = parse_discussion(payload, owner, repo, number)
        comments = parse_comments(payload)
        return GitHubDiscussionsResult(
            requested_url=request.url,
            fetched_at_utc=time(),
            discussion=discussion,
            comments=comments,
            total_comment_count=len(flatten_comments(comments)),
            cache_status="disabled",
            rate_limit_remaining=extract_rate_limit_remaining(payload),
            raw_payload=payload,
        )

    def normalize(self, raw_artifact: Mapping[str, Any], raw_artifact_path: Path) -> Any:
        from threadsense.pipeline.normalize import normalize_github_discussions_artifact

        return normalize_github_discussions_artifact(raw_artifact, raw_artifact_path)

    def supports_url(self, url: str) -> bool:
        try:
            normalize_url(url)
        except RedditInputError:
            return False
        return True


def normalize_url(url: str) -> tuple[str, str, str, int]:
    parsed = parse.urlparse(url)
    if parsed.scheme not in {"http", "https"} or parsed.netloc != "github.com":
        raise RedditInputError(
            "github discussions URL must target github.com",
            details={"url": url},
        )
    match = _DISCUSSION_PATH.match(parsed.path.rstrip("/"))
    if match is None:
        raise RedditInputError(
            "github discussions URL must target an owner/repo discussion path",
            details={"url": url},
        )
    owner, repo, number_text = match.groups()
    return (
        f"https://github.com/{owner}/{repo}/discussions/{number_text}",
        owner,
        repo,
        int(number_text),
    )


def build_discussion_query() -> str:
    return """
    query Discussion($owner: String!, $repo: String!, $number: Int!) {
      repository(owner: $owner, name: $repo) {
        discussion(number: $number) {
          title
          body
          url
          createdAt
          author { login }
          comments(first: 100) {
            nodes {
              id
              body
              createdAt
              url
              reactions { totalCount }
              author { login }
              replies(first: 100) {
                nodes {
                  id
                  body
                  createdAt
                  url
                  reactions { totalCount }
                  author { login }
                }
              }
            }
          }
        }
      }
      rateLimit {
        remaining
      }
    }
    """


def send_graphql_request(
    url: str,
    headers: Mapping[str, str],
    payload: JsonObject,
    timeout_seconds: float,
) -> JsonObject:
    try:
        with httpx.Client(timeout=timeout_seconds) as client:
            response = client.post(url, json=payload, headers=dict(headers))
            response.raise_for_status()
            parsed = response.json()
    except httpx.HTTPStatusError as http_error:
        raise RedditRequestError(
            "github discussions request failed",
            details={
                "status_code": http_error.response.status_code,
                "url": str(http_error.request.url),
            },
        ) from http_error
    except httpx.TimeoutException as timeout_error:
        raise NetworkBoundaryError(
            "github discussions request timed out",
            details={"url": url, "timeout_seconds": timeout_seconds},
        ) from timeout_error
    except httpx.ConnectError as connect_error:
        raise NetworkBoundaryError(
            "github discussions endpoint is unreachable",
            details={"url": url, "reason": str(connect_error)},
        ) from connect_error
    except json.JSONDecodeError as decode_error:
        raise RedditRequestError(
            "github discussions response body is not valid JSON",
            details={"url": url},
        ) from decode_error
    if not isinstance(parsed, dict):
        raise RedditRequestError("github discussions response must decode to an object")
    return parsed


def parse_discussion(payload: JsonObject, owner: str, repo: str, number: int) -> GitHubDiscussion:
    discussion = payload.get("data", {}).get("repository", {}).get("discussion")
    if not isinstance(discussion, dict):
        raise RedditRequestError("github discussion payload is invalid")
    author = discussion.get("author") or {}
    return GitHubDiscussion(
        owner=owner,
        repo=repo,
        number=number,
        title=str(discussion.get("title", "")),
        body=optional_discussion_body(discussion),
        author=str(author.get("login", "[deleted]")),
        url=str(discussion.get("url", "")),
        created_utc=parse_timestamp(str(discussion.get("createdAt", ""))),
    )


def parse_comments(payload: JsonObject) -> list[GitHubDiscussionComment]:
    discussion = payload.get("data", {}).get("repository", {}).get("discussion", {})
    comment_nodes = discussion.get("comments", {}).get("nodes", [])
    if not isinstance(comment_nodes, list):
        return []
    comments: list[GitHubDiscussionComment] = []
    for node in comment_nodes:
        if not isinstance(node, dict):
            continue
        replies_data = node.get("replies", {}).get("nodes", [])
        replies = tuple(
            parse_comment(reply, depth=1, parent_node_id=str(node.get("id", "")))
            for reply in replies_data
            if isinstance(reply, dict)
        )
        comments.append(parse_comment(node, depth=0, parent_node_id=None, replies=replies))
    return comments


def parse_comment(
    payload: JsonObject,
    *,
    depth: int,
    parent_node_id: str | None,
    replies: tuple[GitHubDiscussionComment, ...] = (),
) -> GitHubDiscussionComment:
    author = payload.get("author") or {}
    reactions = payload.get("reactions") or {}
    return GitHubDiscussionComment(
        node_id=str(payload.get("id", "")),
        author=str(author.get("login", "[deleted]")),
        body=str(payload.get("body", "")),
        score=int(reactions.get("totalCount", 0)),
        created_utc=parse_timestamp(str(payload.get("createdAt", ""))),
        depth=depth,
        parent_node_id=parent_node_id,
        url=str(payload.get("url", "")),
        replies=replies,
    )


def extract_rate_limit_remaining(payload: JsonObject) -> int | None:
    rate_limit = payload.get("data", {}).get("rateLimit")
    if not isinstance(rate_limit, dict):
        return None
    remaining = rate_limit.get("remaining")
    return remaining if isinstance(remaining, int) else None


def optional_discussion_body(discussion: Mapping[str, Any]) -> str | None:
    if "body" not in discussion:
        raise RedditRequestError("github discussion body is missing")
    value = discussion.get("body")
    if value is None:
        return None
    if not isinstance(value, str):
        raise RedditRequestError("github discussion body is invalid")
    return value


def parse_timestamp(value: str) -> float:
    from datetime import datetime

    if not value:
        return 0.0
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def flatten_comments(comments: list[GitHubDiscussionComment]) -> list[GitHubDiscussionComment]:
    flattened: list[GitHubDiscussionComment] = []
    for comment in comments:
        flattened.append(comment)
        flattened.extend(comment.replies)
    return flattened


def comment_to_dict(comment: GitHubDiscussionComment) -> dict[str, Any]:
    return {
        "id": comment.node_id,
        "author": comment.author,
        "body": comment.body,
        "score": comment.score,
        "created_utc": comment.created_utc,
        "depth": comment.depth,
        "parent_node_id": comment.parent_node_id,
        "url": comment.url,
        "replies": [comment_to_dict(reply) for reply in comment.replies],
    }
