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
from threadsense.connectors.cache import FetchCache
from threadsense.errors import (
    ConfigurationError,
    GitHubInputError,
    GitHubRequestError,
    NetworkBoundaryError,
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
    is_answer: bool
    is_minimized: bool
    minimized_reason: str | None
    author_association: str


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
    category: str | None
    is_answered: bool
    labels: tuple[str, ...]
    locked: bool


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
                "category": self.discussion.category,
                "is_answered": self.discussion.is_answered,
                "labels": list(self.discussion.labels),
                "locked": self.discussion.locked,
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
        cache: FetchCache | None = None,
    ) -> None:
        self._config = config
        self._transport = transport or send_graphql_request
        self._cache = cache

    def fetch(self, request: FetchRequest) -> RawArtifact:
        if not self._config.token:
            raise ConfigurationError("github token is required for github discussions fetch")
        normalized_url, owner, repo, number = normalize_url(request.url)
        cache_key = f"graphql:{owner}/{repo}/discussions/{number}"

        if self._cache is not None:
            cached = self._cache.get(cache_key)
            if cached is not None:
                discussion = parse_discussion(cached, owner, repo, number)
                comments = parse_comments(cached)
                return GitHubDiscussionsResult(
                    requested_url=request.url,
                    fetched_at_utc=time(),
                    discussion=discussion,
                    comments=comments,
                    total_comment_count=len(flatten_comments(comments)),
                    cache_status="hit",
                    rate_limit_remaining=extract_rate_limit_remaining(cached),
                    raw_payload=cached,
                )

        headers = {
            "Authorization": f"Bearer {self._config.token}",
            "Content-Type": "application/json",
        }
        payload = self._fetch_all_pages(headers, owner, repo, number)
        discussion = parse_discussion(payload, owner, repo, number)
        comments = parse_comments(payload)

        if self._cache is not None:
            self._cache.put(cache_key, payload)

        cache_status = "disabled" if self._cache is None else "miss"
        return GitHubDiscussionsResult(
            requested_url=request.url,
            fetched_at_utc=time(),
            discussion=discussion,
            comments=comments,
            total_comment_count=len(flatten_comments(comments)),
            cache_status=cache_status,
            rate_limit_remaining=extract_rate_limit_remaining(payload),
            raw_payload=payload,
        )

    def _fetch_all_pages(
        self,
        headers: dict[str, str],
        owner: str,
        repo: str,
        number: int,
    ) -> JsonObject:
        comment_cursor: str | None = None
        all_comment_nodes: list[JsonObject] = []
        base_payload: JsonObject = {}

        while True:
            query = build_discussion_query(comment_cursor=comment_cursor)
            variables: JsonObject = {"owner": owner, "repo": repo, "number": number}
            if comment_cursor is not None:
                variables["commentCursor"] = comment_cursor

            payload = self._transport(
                self._config.base_url,
                headers,
                {"query": query, "variables": variables},
                self._config.timeout_seconds,
            )

            discussion_data = payload.get("data", {}).get("repository", {}).get("discussion", {})
            comments_data = discussion_data.get("comments", {})
            comment_nodes = comments_data.get("nodes", [])
            if isinstance(comment_nodes, list):
                all_comment_nodes.extend(comment_nodes)

            if not base_payload:
                base_payload = payload
            else:
                # Merge comment nodes into base payload
                base_discussion = (
                    base_payload.get("data", {}).get("repository", {}).get("discussion", {})
                )
                base_discussion["comments"]["nodes"] = all_comment_nodes
                base_discussion["comments"]["totalCount"] = comments_data.get("totalCount", 0)

            page_info = comments_data.get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                break

            comment_cursor = page_info.get("endCursor")
            if not comment_cursor:
                break

            remaining = extract_rate_limit_remaining(payload)
            if remaining is not None and remaining < 10:
                break

        # Paginate replies for each comment that has more pages
        self._fetch_remaining_replies(headers, owner, repo, number, all_comment_nodes)

        # Ensure base payload reflects all accumulated nodes
        base_discussion = base_payload.get("data", {}).get("repository", {}).get("discussion", {})
        if base_discussion:
            base_discussion.setdefault("comments", {})["nodes"] = all_comment_nodes

        return base_payload

    def _fetch_remaining_replies(
        self,
        headers: dict[str, str],
        owner: str,
        repo: str,
        number: int,
        comment_nodes: list[JsonObject],
    ) -> None:
        for node in comment_nodes:
            if not isinstance(node, dict):
                continue
            replies_data = node.get("replies", {})
            page_info = replies_data.get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                continue

            reply_cursor = page_info.get("endCursor")
            if not reply_cursor:
                continue

            comment_id = str(node.get("id", ""))
            all_reply_nodes = list(replies_data.get("nodes", []))

            while reply_cursor:
                query = build_reply_pagination_query()
                variables: JsonObject = {
                    "owner": owner,
                    "repo": repo,
                    "number": number,
                    "commentId": comment_id,
                    "replyCursor": reply_cursor,
                }
                payload = self._transport(
                    self._config.base_url,
                    headers,
                    {"query": query, "variables": variables},
                    self._config.timeout_seconds,
                )

                discussion_data = (
                    payload.get("data", {}).get("repository", {}).get("discussion", {})
                )
                comments_data = discussion_data.get("comments", {})
                target_nodes = comments_data.get("nodes", [])
                if isinstance(target_nodes, list) and target_nodes:
                    target_comment = target_nodes[0]
                    if isinstance(target_comment, dict):
                        new_replies = target_comment.get("replies", {}).get("nodes", [])
                        if isinstance(new_replies, list):
                            all_reply_nodes.extend(new_replies)

                        reply_page_info = target_comment.get("replies", {}).get("pageInfo", {})
                        if not reply_page_info.get("hasNextPage", False):
                            break
                        reply_cursor = reply_page_info.get("endCursor")
                        if not reply_cursor:
                            break
                    else:
                        break
                else:
                    break

                remaining = extract_rate_limit_remaining(payload)
                if remaining is not None and remaining < 10:
                    break

            node["replies"]["nodes"] = all_reply_nodes

    def normalize(self, raw_artifact: Mapping[str, Any], raw_artifact_path: Path) -> Any:
        from threadsense.pipeline.normalize import normalize_github_discussions_artifact

        return normalize_github_discussions_artifact(raw_artifact, raw_artifact_path)

    def supports_url(self, url: str) -> bool:
        try:
            normalize_url(url)
        except GitHubInputError:
            return False
        return True


def normalize_url(url: str) -> tuple[str, str, str, int]:
    parsed = parse.urlparse(url)
    if parsed.scheme not in {"http", "https"} or parsed.netloc != "github.com":
        raise GitHubInputError(
            "github discussions URL must target github.com",
            details={"url": url},
        )
    match = _DISCUSSION_PATH.match(parsed.path.rstrip("/"))
    if match is None:
        raise GitHubInputError(
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


def build_discussion_query(
    *,
    comment_cursor: str | None = None,
) -> str:
    comment_after = f', after: "{comment_cursor}"' if comment_cursor else ""
    return f"""
    query Discussion($owner: String!, $repo: String!, $number: Int!) {{
      repository(owner: $owner, name: $repo) {{
        discussion(number: $number) {{
          title
          body
          url
          createdAt
          author {{ login }}
          category {{ name }}
          isAnswered
          locked
          labels(first: 10) {{ nodes {{ name }} }}
          comments(first: 100{comment_after}) {{
            totalCount
            pageInfo {{ hasNextPage endCursor }}
            nodes {{
              id
              body
              createdAt
              url
              reactions {{ totalCount }}
              author {{ login }}
              authorAssociation
              isAnswer
              isMinimized
              minimizedReason
              replies(first: 100) {{
                totalCount
                pageInfo {{ hasNextPage endCursor }}
                nodes {{
                  id
                  body
                  createdAt
                  url
                  reactions {{ totalCount }}
                  author {{ login }}
                  authorAssociation
                  isMinimized
                  minimizedReason
                }}
              }}
            }}
          }}
        }}
      }}
      rateLimit {{
        remaining
      }}
    }}
    """


def build_reply_pagination_query() -> str:
    return """
    query ReplyPagination(
      $owner: String!
      $repo: String!
      $number: Int!
      $commentId: ID!
      $replyCursor: String!
    ) {
      repository(owner: $owner, name: $repo) {
        discussion(number: $number) {
          comments(first: 1) {
            nodes {
              id
              replies(first: 100, after: $replyCursor) {
                totalCount
                pageInfo { hasNextPage endCursor }
                nodes {
                  id
                  body
                  createdAt
                  url
                  reactions { totalCount }
                  author { login }
                  authorAssociation
                  isMinimized
                  minimizedReason
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
        raise GitHubRequestError(
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
        raise GitHubRequestError(
            "github discussions response body is not valid JSON",
            details={"url": url},
        ) from decode_error
    if not isinstance(parsed, dict):
        raise GitHubRequestError("github discussions response must decode to an object")
    return parsed


def parse_discussion(payload: JsonObject, owner: str, repo: str, number: int) -> GitHubDiscussion:
    discussion = payload.get("data", {}).get("repository", {}).get("discussion")
    if not isinstance(discussion, dict):
        raise GitHubRequestError("github discussion payload is invalid")
    author = discussion.get("author") or {}
    category_data = discussion.get("category") or {}
    category_name = category_data.get("name") if isinstance(category_data, dict) else None
    labels_data = discussion.get("labels", {}).get("nodes", [])
    label_names = tuple(
        str(label.get("name", "")) for label in labels_data if isinstance(label, dict)
    )
    return GitHubDiscussion(
        owner=owner,
        repo=repo,
        number=number,
        title=str(discussion.get("title", "")),
        body=optional_discussion_body(discussion),
        author=str(author.get("login", "[deleted]")),
        url=str(discussion.get("url", "")),
        created_utc=parse_timestamp(str(discussion.get("createdAt", ""))),
        category=category_name,
        is_answered=bool(discussion.get("isAnswered", False)),
        labels=label_names,
        locked=bool(discussion.get("locked", False)),
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
        is_answer=bool(payload.get("isAnswer", False)),
        is_minimized=bool(payload.get("isMinimized", False)),
        minimized_reason=payload.get("minimizedReason"),
        author_association=str(payload.get("authorAssociation", "NONE")),
    )


def extract_rate_limit_remaining(payload: JsonObject) -> int | None:
    rate_limit = payload.get("data", {}).get("rateLimit")
    if not isinstance(rate_limit, dict):
        return None
    remaining = rate_limit.get("remaining")
    return remaining if isinstance(remaining, int) else None


def optional_discussion_body(discussion: Mapping[str, Any]) -> str | None:
    if "body" not in discussion:
        raise GitHubRequestError("github discussion body is missing")
    value = discussion.get("body")
    if value is None:
        return None
    if not isinstance(value, str):
        raise GitHubRequestError("github discussion body is invalid")
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
        "is_answer": comment.is_answer,
        "is_minimized": comment.is_minimized,
        "minimized_reason": comment.minimized_reason,
        "author_association": comment.author_association,
    }
