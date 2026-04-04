from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from time import sleep, time
from typing import Any
from urllib import parse

import httpx

from threadsense.config import HackerNewsConfig
from threadsense.connectors import FetchRequest, RawArtifact
from threadsense.connectors.cache import FetchCache
from threadsense.errors import NetworkBoundaryError, RedditInputError, RedditRequestError

JsonObject = dict[str, Any]
HackerNewsTransport = Callable[[str, float], Any]
HN_HOST = "news.ycombinator.com"


@dataclass(frozen=True)
class HackerNewsComment:
    id: int
    author: str
    body: str
    created_utc: float
    depth: int
    parent: int
    permalink: str
    replies: tuple[HackerNewsComment, ...]


@dataclass(frozen=True)
class HackerNewsStory:
    id: int
    title: str
    author: str
    score: int
    created_utc: float
    permalink: str


@dataclass(frozen=True)
class HackerNewsThreadResult:
    requested_url: str
    normalized_url: str
    fetched_at_utc: float
    story: HackerNewsStory
    comments: list[HackerNewsComment]
    total_comment_count: int
    cache_status: str
    raw_item_payloads: dict[str, JsonObject]

    @property
    def source_name(self) -> str:
        return "hackernews"

    @property
    def source_thread_id(self) -> str:
        return str(self.story.id)

    @property
    def thread_title(self) -> str:
        return self.story.title

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_version": 1,
            "source": self.source_name,
            "requested_url": self.requested_url,
            "normalized_url": self.normalized_url,
            "fetched_at_utc": self.fetched_at_utc,
            "story": {
                "id": self.story.id,
                "title": self.story.title,
                "author": self.story.author,
                "score": self.story.score,
                "created_utc": self.story.created_utc,
                "permalink": self.story.permalink,
            },
            "comments": [comment_to_dict(comment) for comment in self.comments],
            "total_comment_count": self.total_comment_count,
            "cache_status": self.cache_status,
            "raw_item_payloads": self.raw_item_payloads,
        }


class HackerNewsConnector:
    source_name = "hackernews"

    def __init__(
        self,
        config: HackerNewsConfig,
        cache: FetchCache | None = None,
        transport: HackerNewsTransport | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        self._config = config
        self._cache = cache
        self._transport = transport or fetch_json
        self._sleep = sleeper or sleep

    def fetch(self, request: FetchRequest) -> RawArtifact:
        normalized_url, story_id = normalize_url(request.url)
        item_cache: dict[int, JsonObject] = {}
        story_payload, cache_status = self._get_item_with_status(story_id, item_cache)
        story = HackerNewsStory(
            id=story_id,
            title=str(story_payload.get("title", "")),
            author=str(story_payload.get("by", "[deleted]")),
            score=int(story_payload.get("score", 0)),
            created_utc=float(story_payload.get("time", 0.0)),
            permalink=normalized_url,
        )
        comments = self._collect_comments(
            kids=story_payload.get("kids", []),
            parent_id=story_id,
            depth=0,
            item_cache=item_cache,
        )
        return HackerNewsThreadResult(
            requested_url=request.url,
            normalized_url=normalized_url,
            fetched_at_utc=time(),
            story=story,
            comments=comments,
            total_comment_count=len(flatten_comments(comments)),
            cache_status=cache_status,
            raw_item_payloads={str(key): value for key, value in item_cache.items()},
        )

    def normalize(self, raw_artifact: Mapping[str, Any], raw_artifact_path: Path) -> Any:
        from threadsense.pipeline.normalize import normalize_hackernews_artifact

        return normalize_hackernews_artifact(raw_artifact, raw_artifact_path)

    def supports_url(self, url: str) -> bool:
        try:
            normalize_url(url)
        except RedditInputError:
            return False
        return True

    def _collect_comments(
        self,
        *,
        kids: Any,
        parent_id: int,
        depth: int,
        item_cache: dict[int, JsonObject],
    ) -> list[HackerNewsComment]:
        if not isinstance(kids, list):
            return []
        comments: list[HackerNewsComment] = []
        for kid in kids:
            if not isinstance(kid, int):
                continue
            payload = self._get_item(kid, item_cache)
            if payload.get("deleted") or payload.get("dead"):
                continue
            body = clean_html(str(payload.get("text", "")))
            if not body.strip():
                continue
            replies = self._collect_comments(
                kids=payload.get("kids", []),
                parent_id=kid,
                depth=depth + 1,
                item_cache=item_cache,
            )
            comments.append(
                HackerNewsComment(
                    id=kid,
                    author=str(payload.get("by", "[deleted]")),
                    body=body,
                    created_utc=float(payload.get("time", 0.0)),
                    depth=depth,
                    parent=parent_id,
                    permalink=f"https://news.ycombinator.com/item?id={kid}",
                    replies=tuple(replies),
                )
            )
        return comments

    def _get_item(self, item_id: int, item_cache: dict[int, JsonObject]) -> JsonObject:
        payload, _ = self._get_item_with_status(item_id, item_cache)
        return payload

    def _get_item_with_status(
        self,
        item_id: int,
        item_cache: dict[int, JsonObject],
    ) -> tuple[JsonObject, str]:
        cached = item_cache.get(item_id)
        if cached is not None:
            return cached, "memory"
        url = f"{self._config.base_url.rstrip('/')}/item/{item_id}.json"
        payload, cache_status = self._get_cached_item(url)
        if not isinstance(payload, dict):
            raise RedditRequestError(
                "hackernews item payload is invalid",
                details={"item_id": item_id},
            )
        item_cache[item_id] = payload
        self._sleep(self._config.request_delay_seconds)
        return payload, cache_status

    def _get_cached_item(self, url: str) -> tuple[Any, str]:
        if self._cache is not None:
            cached = self._cache.get(url)
            if cached is not None:
                return cached, "hit"
        payload = self._transport(url, self._config.timeout_seconds)
        if self._cache is not None and isinstance(payload, dict):
            self._cache.put(url, payload)
            return payload, "miss"
        return payload, "disabled"


def normalize_url(url: str) -> tuple[str, int]:
    parsed = parse.urlparse(url)
    if parsed.scheme not in {"http", "https"} or parsed.netloc != HN_HOST:
        raise RedditInputError(
            "hackernews URL must target news.ycombinator.com",
            details={"url": url},
        )
    if parsed.path != "/item":
        raise RedditInputError(
            "hackernews URL must target an item path",
            details={"url": url},
        )
    query = parse.parse_qs(parsed.query)
    ids = query.get("id")
    if not ids:
        raise RedditInputError(
            "hackernews URL must include an item id",
            details={"url": url},
        )
    try:
        item_id = int(ids[0])
    except ValueError as error:
        raise RedditInputError(
            "hackernews item id is invalid",
            details={"url": url},
        ) from error
    return f"https://{HN_HOST}/item?id={item_id}", item_id


def fetch_json(url: str, timeout_seconds: float) -> Any:
    try:
        with httpx.Client(timeout=timeout_seconds) as client:
            response = client.get(url)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPStatusError as http_error:
        raise RedditRequestError(
            "hackernews request failed",
            details={
                "url": str(http_error.request.url),
                "status_code": http_error.response.status_code,
            },
        ) from http_error
    except httpx.TimeoutException as timeout_error:
        raise NetworkBoundaryError(
            "hackernews request timed out",
            details={"url": url, "timeout_seconds": timeout_seconds},
        ) from timeout_error
    except httpx.ConnectError as connect_error:
        raise NetworkBoundaryError(
            "hackernews endpoint is unreachable",
            details={"url": url, "reason": str(connect_error)},
        ) from connect_error
    except json.JSONDecodeError as decode_error:
        raise RedditRequestError(
            "hackernews response body is not valid JSON",
            details={"url": url},
        ) from decode_error


def flatten_comments(comments: list[HackerNewsComment]) -> list[HackerNewsComment]:
    flattened: list[HackerNewsComment] = []
    queue = list(comments)
    while queue:
        comment = queue.pop(0)
        flattened.append(comment)
        queue = list(comment.replies) + queue
    return flattened


def clean_html(text: str) -> str:
    stripped = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", unescape(stripped)).strip()


def comment_to_dict(comment: HackerNewsComment) -> dict[str, Any]:
    return {
        "id": comment.id,
        "author": comment.author,
        "body": comment.body,
        "created_utc": comment.created_utc,
        "depth": comment.depth,
        "parent": comment.parent,
        "permalink": comment.permalink,
        "replies": [comment_to_dict(reply) for reply in comment.replies],
    }
