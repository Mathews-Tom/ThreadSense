from __future__ import annotations

import json
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from time import sleep, time
from typing import Any
from urllib import error, parse, request

from threadsense.config import RedditConfig
from threadsense.errors import (
    NetworkBoundaryError,
    RedditInputError,
    RedditRequestError,
    RedditResponseError,
)

JsonObject = dict[str, Any]
QueryParams = Mapping[str, str | int | float | bool]
RedditTransport = Callable[[str, Mapping[str, str], QueryParams, float], Any]

RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
REDDIT_HOST_SUFFIXES = ("reddit.com",)
MORECHILDREN_URL = "https://www.reddit.com/api/morechildren.json"


@dataclass(frozen=True)
class RedditComment:
    id: str
    author: str
    body: str
    score: int
    created_utc: float
    depth: int
    parent_id: str
    permalink: str
    replies: list[RedditComment] = field(default_factory=list)


@dataclass(frozen=True)
class RedditPost:
    id: str
    title: str
    subreddit: str
    author: str
    permalink: str
    num_comments: int


@dataclass(frozen=True)
class RedditThreadRequest:
    post_url: str
    output_path: Path | None = None
    expand_more: bool = False
    flat: bool = False


@dataclass(frozen=True)
class RedditThreadResult:
    requested_url: str
    normalized_url: str
    fetched_at_utc: float
    post: RedditPost
    comments: list[RedditComment]
    total_comment_count: int
    expanded_more_count: int
    raw_thread_payload: list[JsonObject]
    raw_morechildren_payloads: list[JsonObject]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_version": 1,
            "source": "reddit",
            "requested_url": self.requested_url,
            "normalized_url": self.normalized_url,
            "fetched_at_utc": self.fetched_at_utc,
            "post": asdict(self.post),
            "comments": [asdict(comment) for comment in self.comments],
            "total_comment_count": self.total_comment_count,
            "expanded_more_count": self.expanded_more_count,
            "raw_thread_payload": self.raw_thread_payload,
            "raw_morechildren_payloads": self.raw_morechildren_payloads,
        }


class RedditConnector:
    def __init__(
        self,
        config: RedditConfig,
        transport: RedditTransport | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        self._config = config
        self._transport = transport or fetch_json
        self._sleep = sleeper or sleep

    def fetch_thread(self, scrape_request: RedditThreadRequest) -> RedditThreadResult:
        normalized_url = normalize_url(scrape_request.post_url)
        raw_thread_payload = self._get_json(
            normalized_url,
            params={"limit": self._config.listing_limit},
        )
        payload_list = validate_thread_payload(raw_thread_payload)
        post = extract_post(payload_list[0])
        comment_listing = extract_comment_listing(payload_list[1])
        comments, more_ids = collect_more_ids(comment_listing)

        raw_morechildren_payloads: list[JsonObject] = []
        expanded_comments: list[RedditComment] = []
        if scrape_request.expand_more and more_ids:
            self._sleep(self._config.request_delay_seconds)
            expanded_comments, more_payload = self.expand_more_comments(
                link_id=f"t3_{post.id}",
                more_ids=more_ids,
                depth=0,
            )
            comments.extend(expanded_comments)
            raw_morechildren_payloads.append(more_payload)

        flattened_comments = flatten(comments)
        output_comments = flattened_comments if scrape_request.flat else comments
        total_comment_count = len(flattened_comments)
        return RedditThreadResult(
            requested_url=scrape_request.post_url,
            normalized_url=normalized_url,
            fetched_at_utc=time(),
            post=post,
            comments=output_comments,
            total_comment_count=total_comment_count,
            expanded_more_count=len(expanded_comments),
            raw_thread_payload=payload_list,
            raw_morechildren_payloads=raw_morechildren_payloads,
        )

    def expand_more_comments(
        self,
        link_id: str,
        more_ids: list[str],
        depth: int,
    ) -> tuple[list[RedditComment], JsonObject]:
        if not more_ids:
            return [], {}
        payload = self._get_json(
            MORECHILDREN_URL,
            params={
                "link_id": link_id,
                "children": ",".join(more_ids),
                "api_type": "json",
                "limit_children": False,
            },
        )
        things = extract_morechildren_things(payload)
        comments: list[RedditComment] = []
        for thing in things:
            if thing.get("kind") != "t1":
                continue
            parsed_comment = parse_comment(thing, depth=depth)
            if parsed_comment is not None:
                comments.append(parsed_comment)
        return comments, payload

    def _get_json(self, url: str, params: QueryParams) -> Any:
        attempts = self._config.max_retries + 1
        headers = {"User-Agent": self._config.user_agent}
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                return self._transport(url, headers, params, self._config.timeout_seconds)
            except RedditRequestError as fetch_error:
                if attempt == attempts or not should_retry_error(fetch_error):
                    raise
                last_error = fetch_error
            except NetworkBoundaryError as fetch_error:
                if attempt == attempts:
                    raise
                last_error = fetch_error
            self._sleep(self._config.backoff_seconds * attempt)

        if last_error is not None:
            raise last_error
        raise RedditRequestError("reddit transport failed without an error")


def normalize_url(url: str) -> str:
    parsed = parse.urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise RedditInputError(
            "reddit post URL must use http or https",
            details={"url": url},
        )
    if not parsed.netloc or not parsed.netloc.endswith(REDDIT_HOST_SUFFIXES):
        raise RedditInputError(
            "reddit post URL must target a reddit host",
            details={"url": url},
        )
    stripped_path = parsed.path.rstrip("/")
    if "/comments/" not in stripped_path:
        raise RedditInputError(
            "reddit post URL must reference a thread comments path",
            details={"url": url},
        )
    normalized_path = stripped_path if stripped_path.endswith(".json") else f"{stripped_path}.json"
    return parse.urlunparse(parsed._replace(path=normalized_path, query="", fragment=""))


def fetch_json(
    url: str,
    headers: Mapping[str, str],
    params: QueryParams,
    timeout_seconds: float,
) -> Any:
    query_string = parse.urlencode({key: str(value) for key, value in params.items()})
    resolved_url = f"{url}?{query_string}" if query_string else url
    http_request = request.Request(resolved_url, headers=dict(headers), method="GET")
    try:
        with request.urlopen(http_request, timeout=timeout_seconds) as response:
            raw_body = response.read().decode("utf-8")
            return json.loads(raw_body)
    except error.HTTPError as http_error:
        response_body = http_error.read().decode("utf-8", errors="replace")
        raise RedditRequestError(
            "reddit request failed",
            details={
                "url": resolved_url,
                "status_code": http_error.code,
                "response_body": response_body,
            },
        ) from http_error
    except error.URLError as url_error:
        raise NetworkBoundaryError(
            "reddit endpoint is unreachable",
            details={"url": resolved_url, "reason": str(url_error.reason)},
        ) from url_error
    except json.JSONDecodeError as decode_error:
        raise RedditResponseError(
            "reddit response body is not valid JSON",
            details={"url": resolved_url},
        ) from decode_error


def should_retry_error(fetch_error: RedditRequestError) -> bool:
    status_code = fetch_error.details.get("status_code")
    return isinstance(status_code, int) and status_code in RETRYABLE_STATUS_CODES


def validate_thread_payload(payload: Any) -> list[JsonObject]:
    if not isinstance(payload, list) or len(payload) < 2:
        raise RedditResponseError("reddit thread payload must be a list with post and comments")
    validated: list[JsonObject] = []
    for item in payload[:2]:
        if not isinstance(item, dict):
            raise RedditResponseError("reddit thread payload entries must be objects")
        validated.append(item)
    return validated


def extract_post(post_listing: JsonObject) -> RedditPost:
    children = nested_list(post_listing, "data", "children")
    if not children:
        raise RedditResponseError("reddit post listing is empty")
    post_data = nested_object(children[0], "data")
    return RedditPost(
        id=required_str(post_data, "id"),
        title=required_str(post_data, "title"),
        subreddit=optional_str(post_data, "subreddit", default=""),
        author=optional_str(post_data, "author", default="[deleted]"),
        permalink=f"https://reddit.com{optional_str(post_data, 'permalink', default='')}",
        num_comments=optional_int(post_data, "num_comments", default=0),
    )


def extract_comment_listing(comment_listing: JsonObject) -> list[JsonObject]:
    return nested_list(comment_listing, "data", "children")


def extract_morechildren_things(payload: Any) -> list[JsonObject]:
    if not isinstance(payload, dict):
        raise RedditResponseError("morechildren payload must be an object")
    return nested_list(payload, "json", "data", "things")


def collect_more_ids(children: list[JsonObject]) -> tuple[list[RedditComment], list[str]]:
    comments: list[RedditComment] = []
    more_ids: list[str] = []
    for child in children:
        kind = required_str(child, "kind")
        if kind == "t1":
            parsed_comment = parse_comment(child, depth=0)
            if parsed_comment is not None:
                comments.append(parsed_comment)
            continue
        if kind == "more":
            for child_id in nested_list(child, "data", "children"):
                if not isinstance(child_id, str):
                    raise RedditResponseError("morechildren child IDs must be strings")
                more_ids.append(child_id)
    return comments, more_ids


def parse_comment(raw_comment: JsonObject, depth: int = 0) -> RedditComment | None:
    data = nested_object(raw_comment, "data")
    body = optional_str(data, "body", default="")
    if body in {"", "[deleted]", "[removed]"}:
        return None

    replies = data.get("replies", "")
    reply_children: list[JsonObject] = []
    if isinstance(replies, dict):
        reply_children = nested_list(replies, "data", "children")
    elif replies not in {"", None}:
        raise RedditResponseError("reddit replies payload must be a listing object or empty string")

    parsed_replies: list[RedditComment] = []
    for child in reply_children:
        kind = required_str(child, "kind")
        if kind != "t1":
            continue
        reply = parse_comment(child, depth=depth + 1)
        if reply is not None:
            parsed_replies.append(reply)
    return RedditComment(
        id=required_str(data, "id"),
        author=optional_str(data, "author", default="[deleted]"),
        body=body,
        score=optional_int(data, "score", default=0),
        created_utc=optional_float(data, "created_utc", default=0.0),
        depth=depth,
        parent_id=optional_str(data, "parent_id", default=""),
        permalink=f"https://reddit.com{optional_str(data, 'permalink', default='')}",
        replies=parsed_replies,
    )


def flatten(comments: list[RedditComment]) -> list[RedditComment]:
    flattened: list[RedditComment] = []
    queue: deque[RedditComment] = deque(comments)
    while queue:
        comment = queue.popleft()
        flattened.append(comment)
        queue.extendleft(reversed(comment.replies))
    return flattened


def write_thread_artifact(path: Path, result: RedditThreadResult) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")


def nested_object(payload: Mapping[str, Any], *keys: str) -> JsonObject:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            raise RedditResponseError(
                "reddit payload object boundary is invalid",
                details={"key": key},
            )
        current = current.get(key)
    if not isinstance(current, dict):
        raise RedditResponseError(
            "reddit payload object boundary is invalid",
            details={"keys": list(keys)},
        )
    return current


def nested_list(payload: Mapping[str, Any], *keys: str) -> list[Any]:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            raise RedditResponseError(
                "reddit payload list boundary is invalid",
                details={"key": key},
            )
        current = current.get(key)
    if not isinstance(current, list):
        raise RedditResponseError(
            "reddit payload list boundary is invalid",
            details={"keys": list(keys)},
        )
    return current


def required_str(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise RedditResponseError(
            "reddit payload string field is missing",
            details={"key": key},
        )
    return value


def optional_str(payload: Mapping[str, Any], key: str, default: str) -> str:
    value = payload.get(key, default)
    if value is None:
        return default
    if not isinstance(value, str):
        raise RedditResponseError(
            "reddit payload string field has invalid type",
            details={"key": key},
        )
    return value


def optional_int(payload: Mapping[str, Any], key: str, default: int) -> int:
    value = payload.get(key, default)
    if not isinstance(value, int):
        raise RedditResponseError(
            "reddit payload integer field has invalid type",
            details={"key": key},
        )
    return value


def optional_float(payload: Mapping[str, Any], key: str, default: float) -> float:
    value = payload.get(key, default)
    if isinstance(value, int):
        return float(value)
    if not isinstance(value, float):
        raise RedditResponseError(
            "reddit payload float field has invalid type",
            details={"key": key},
        )
    return value
