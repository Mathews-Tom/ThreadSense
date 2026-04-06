from __future__ import annotations

import json
import re
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from time import sleep, time
from typing import Any
from urllib import parse

import httpx

from threadsense.config import RedditConfig
from threadsense.connectors import FetchRequest, RawArtifact
from threadsense.connectors.cache import FetchCache
from threadsense.errors import (
    NetworkBoundaryError,
    RedditInputError,
    RedditRequestError,
    RedditResponseError,
)
from threadsense.schema_utils import SchemaReader

JsonObject = dict[str, Any]
QueryParams = Mapping[str, str | int | float | bool]
RedditTransport = Callable[[str, Mapping[str, str], QueryParams, float], Any]

RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

_schema = SchemaReader(RedditResponseError, "reddit payload")
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
    replies: tuple[RedditComment, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class RedditPost:
    id: str
    title: str
    selftext: str
    subreddit: str
    author: str
    permalink: str
    num_comments: int


@dataclass(frozen=True)
class RedditSearchRequest:
    query: str
    subreddits: list[str]
    limit: int
    sort: str
    time_window: str


@dataclass(frozen=True)
class RedditSearchMatch:
    post_id: str
    title: str
    selftext: str
    subreddit: str
    author: str
    permalink: str
    thread_url: str
    normalized_url: str
    score: int
    num_comments: int
    created_utc: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RedditSearchResult:
    query: str
    subreddits: list[str]
    sort: str
    time_window: str
    reddit_time_bucket: str
    matches: list[RedditSearchMatch]

    def to_dict(self) -> dict[str, Any]:
        return {
            "query": self.query,
            "subreddits": self.subreddits,
            "sort": self.sort,
            "time_window": self.time_window,
            "reddit_time_bucket": self.reddit_time_bucket,
            "matches": [match.to_dict() for match in self.matches],
        }


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
    cache_status: str
    raw_thread_payload: list[JsonObject]
    raw_morechildren_payloads: list[JsonObject]

    @property
    def source_name(self) -> str:
        return "reddit"

    @property
    def source_thread_id(self) -> str:
        return self.post.id

    @property
    def thread_title(self) -> str:
        return self.post.title

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_version": 2,
            "source": "reddit",
            "requested_url": self.requested_url,
            "normalized_url": self.normalized_url,
            "fetched_at_utc": self.fetched_at_utc,
            "post": asdict(self.post),
            "comments": [asdict(comment) for comment in self.comments],
            "total_comment_count": self.total_comment_count,
            "expanded_more_count": self.expanded_more_count,
            "cache_status": self.cache_status,
            "raw_thread_payload": self.raw_thread_payload,
            "raw_morechildren_payloads": self.raw_morechildren_payloads,
        }


class RedditConnector:
    source_name = "reddit"

    def __init__(
        self,
        config: RedditConfig,
        cache: FetchCache | None = None,
        transport: RedditTransport | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        self._config = config
        self._cache = cache
        self._transport = transport or fetch_json
        self._sleep = sleeper or sleep

    def fetch_thread(self, scrape_request: RedditThreadRequest) -> RedditThreadResult:
        normalized_url = normalize_url(scrape_request.post_url)
        raw_thread_payload, cache_status = self._get_cached_json_with_status(
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
            cache_status=cache_status,
            raw_thread_payload=payload_list,
            raw_morechildren_payloads=raw_morechildren_payloads,
        )

    def search_threads(self, search_request: RedditSearchRequest) -> RedditSearchResult:
        if not search_request.query.strip():
            raise RedditInputError("reddit search query must not be empty")
        if not search_request.subreddits:
            raise RedditInputError("reddit search requires at least one subreddit")
        if search_request.limit <= 0:
            raise RedditInputError("reddit search limit must be greater than zero")
        reddit_time_bucket = map_time_window_to_reddit_bucket(search_request.time_window)
        requested_at_utc = time()
        matches: list[RedditSearchMatch] = []
        query_clauses = split_search_query_clauses(search_request.query)
        for subreddit in search_request.subreddits:
            subreddit_name = validate_subreddit_name(subreddit)
            for query_clause in query_clauses:
                payload = self._get_cached_json(
                    f"https://www.reddit.com/r/{subreddit_name}/search.json",
                    params={
                        "q": query_clause,
                        "restrict_sr": "on",
                        "sort": search_request.sort,
                        "t": reddit_time_bucket,
                        "limit": search_request.limit,
                    },
                )
                matches.extend(
                    filter_matches_by_time_window(
                        extract_search_matches(payload, subreddit_name),
                        time_window=search_request.time_window,
                        requested_at_utc=requested_at_utc,
                    )
                )
        return RedditSearchResult(
            query=search_request.query,
            subreddits=[validate_subreddit_name(name) for name in search_request.subreddits],
            sort=search_request.sort,
            time_window=search_request.time_window,
            reddit_time_bucket=reddit_time_bucket,
            matches=matches,
        )

    def fetch(self, request: FetchRequest) -> RawArtifact:
        return self.fetch_thread(
            RedditThreadRequest(
                post_url=request.url,
                expand_more=request.expand,
            )
        )

    def normalize(self, raw_artifact: Mapping[str, Any], raw_artifact_path: Path) -> Any:
        from threadsense.pipeline.normalize import normalize_reddit_artifact

        return normalize_reddit_artifact(raw_artifact, raw_artifact_path)

    def supports_url(self, url: str) -> bool:
        try:
            normalize_url(url)
        except RedditInputError:
            return False
        return True

    def expand_more_comments(
        self,
        link_id: str,
        more_ids: list[str],
        depth: int,
    ) -> tuple[list[RedditComment], JsonObject]:
        if not more_ids:
            return [], {}
        payload = self._get_cached_json(
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

    def _get_cached_json(self, url: str, params: QueryParams) -> Any:
        payload, _ = self._get_cached_json_with_status(url, params)
        return payload

    def _get_cached_json_with_status(self, url: str, params: QueryParams) -> tuple[Any, str]:
        cache_key = build_cache_key(url, params)
        if self._cache is not None:
            cached = self._cache.get(cache_key)
            if cached is not None:
                payload = cached["payload"] if "payload" in cached else cached
                return payload, "hit"
        payload = self._get_json(url, params)
        if self._cache is not None:
            stored_payload = payload if isinstance(payload, dict) else {"payload": payload}
            self._cache.put(cache_key, stored_payload)
            return payload, "miss"
        return payload, "disabled"


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
    resolved_params = {key: str(value) for key, value in params.items()}
    try:
        with httpx.Client(timeout=timeout_seconds) as client:
            response = client.get(url, headers=dict(headers), params=resolved_params)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPStatusError as http_error:
        raise RedditRequestError(
            "reddit request failed",
            details={
                "url": str(http_error.request.url),
                "status_code": http_error.response.status_code,
                "response_body": http_error.response.text,
            },
        ) from http_error
    except httpx.ConnectError as connect_error:
        raise NetworkBoundaryError(
            "reddit endpoint is unreachable",
            details={"url": url, "reason": str(connect_error)},
        ) from connect_error
    except httpx.TimeoutException as timeout_error:
        raise NetworkBoundaryError(
            "reddit request timed out",
            details={"url": url, "timeout_seconds": timeout_seconds},
        ) from timeout_error
    except json.JSONDecodeError as decode_error:
        raise RedditResponseError(
            "reddit response body is not valid JSON",
            details={"url": url},
        ) from decode_error


def should_retry_error(fetch_error: RedditRequestError) -> bool:
    status_code = fetch_error.details.get("status_code")
    return isinstance(status_code, int) and status_code in RETRYABLE_STATUS_CODES


def build_cache_key(url: str, params: QueryParams) -> str:
    serialized_params = "&".join(f"{key}={value}" for key, value in sorted(params.items()))
    return f"{url}?{serialized_params}"


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
    children = _schema.nested_list(post_listing, "data", "children")
    if not children:
        raise RedditResponseError("reddit post listing is empty")
    post_data = _schema.nested_object(children[0], "data")
    return RedditPost(
        id=_schema.required_str(post_data, "id"),
        title=_schema.required_str(post_data, "title"),
        selftext=required_present_str(post_data, "selftext"),
        subreddit=_schema.optional_str(post_data, "subreddit", ""),
        author=_schema.optional_str(post_data, "author", "[deleted]"),
        permalink=f"https://reddit.com{_schema.optional_str(post_data, 'permalink', '')}",
        num_comments=_schema.optional_int(post_data, "num_comments", 0),
    )


def extract_search_matches(payload: Any, expected_subreddit: str) -> list[RedditSearchMatch]:
    if not isinstance(payload, dict):
        raise RedditResponseError("reddit search payload must be an object")
    children = _schema.nested_list(payload, "data", "children")
    matches: list[RedditSearchMatch] = []
    for child in children:
        if _schema.required_str(child, "kind") != "t3":
            continue
        post_data = _schema.nested_object(child, "data")
        permalink = _schema.required_str(post_data, "permalink")
        thread_url = build_thread_url(permalink)
        match = RedditSearchMatch(
            post_id=_schema.required_str(post_data, "id"),
            title=_schema.required_str(post_data, "title"),
            selftext=required_present_str(post_data, "selftext"),
            subreddit=_schema.required_str(post_data, "subreddit"),
            author=_schema.required_str(post_data, "author"),
            permalink=permalink,
            thread_url=thread_url,
            normalized_url=normalize_url(thread_url),
            score=_schema.required_int(post_data, "score"),
            num_comments=_schema.required_int(post_data, "num_comments"),
            created_utc=_schema.required_float(post_data, "created_utc"),
        )
        if match.subreddit.lower() != expected_subreddit.lower():
            raise RedditResponseError(
                "reddit search result subreddit does not match requested subreddit",
                details={
                    "expected_subreddit": expected_subreddit,
                    "actual_subreddit": match.subreddit,
                    "post_id": match.post_id,
                },
            )
        matches.append(match)
    return matches


def extract_comment_listing(comment_listing: JsonObject) -> list[JsonObject]:
    return _schema.nested_list(comment_listing, "data", "children")


def map_time_window_to_reddit_bucket(time_window: str) -> str:
    normalized = time_window.strip().lower()
    if normalized == "all":
        return "all"
    match = re.fullmatch(r"(\d+)([dwmy])", normalized)
    if match is None:
        raise RedditInputError(
            "reddit time window must use forms like 1d, 7d, 30d, 12m, or all",
            details={"time_window": time_window},
        )
    amount = int(match.group(1))
    unit = match.group(2)
    if amount <= 0:
        raise RedditInputError(
            "reddit time window must be greater than zero",
            details={"time_window": time_window},
        )
    if unit == "d":
        if amount <= 1:
            return "day"
        if amount <= 7:
            return "week"
        if amount <= 30:
            return "month"
        if amount <= 365:
            return "year"
        return "all"
    if unit == "w":
        if amount <= 1:
            return "week"
        if amount <= 4:
            return "month"
        if amount <= 52:
            return "year"
        return "all"
    if unit == "m":
        if amount <= 1:
            return "month"
        if amount <= 12:
            return "year"
        return "all"
    if unit == "y":
        return "year" if amount <= 1 else "all"
    raise RedditInputError(
        "reddit time window unit is unsupported",
        details={"time_window": time_window},
    )


def validate_subreddit_name(subreddit: str) -> str:
    normalized = subreddit.strip().removeprefix("r/")
    if not normalized or not re.fullmatch(r"[A-Za-z0-9_]+", normalized):
        raise RedditInputError(
            "reddit subreddit name is invalid",
            details={"subreddit": subreddit},
        )
    return normalized


def build_thread_url(permalink: str) -> str:
    return f"https://www.reddit.com{permalink}" if permalink.startswith("/") else permalink


def required_present_str(payload: Mapping[str, Any], key: str) -> str:
    if key not in payload:
        raise RedditResponseError("reddit string field is missing", details={"key": key})
    value = payload[key]
    if not isinstance(value, str):
        raise RedditResponseError("reddit string field is invalid", details={"key": key})
    return value


def filter_matches_by_time_window(
    matches: list[RedditSearchMatch],
    *,
    time_window: str,
    requested_at_utc: float,
) -> list[RedditSearchMatch]:
    cutoff_utc = time_window_cutoff_utc(time_window, requested_at_utc)
    if cutoff_utc is None:
        return matches
    return [match for match in matches if match.created_utc >= cutoff_utc]


def time_window_cutoff_utc(time_window: str, requested_at_utc: float) -> float | None:
    normalized = time_window.strip().lower()
    if normalized == "all":
        return None
    match = re.fullmatch(r"(\d+)([dwmy])", normalized)
    if match is None:
        raise RedditInputError(
            "reddit time window must use forms like 1d, 7d, 30d, 12m, or all",
            details={"time_window": time_window},
        )
    amount = int(match.group(1))
    unit = match.group(2)
    seconds_per_unit = {
        "d": 86400,
        "w": 7 * 86400,
        "m": 30 * 86400,
        "y": 365 * 86400,
    }
    return requested_at_utc - (amount * seconds_per_unit[unit])


def split_search_query_clauses(query: str) -> list[str]:
    clauses = [clause.strip() for clause in re.split(r"\s+OR\s+|\|", query, flags=re.IGNORECASE)]
    return [clause for clause in clauses if clause]


def extract_morechildren_things(payload: Any) -> list[JsonObject]:
    if not isinstance(payload, dict):
        raise RedditResponseError("morechildren payload must be an object")
    return _schema.nested_list(payload, "json", "data", "things")


def collect_more_ids(children: list[JsonObject]) -> tuple[list[RedditComment], list[str]]:
    comments: list[RedditComment] = []
    more_ids: list[str] = []
    for child in children:
        kind = _schema.required_str(child, "kind")
        if kind == "t1":
            parsed_comment = parse_comment(child, depth=0)
            if parsed_comment is not None:
                comments.append(parsed_comment)
            continue
        if kind == "more":
            for child_id in _schema.nested_list(child, "data", "children"):
                if not isinstance(child_id, str):
                    raise RedditResponseError("morechildren child IDs must be strings")
                more_ids.append(child_id)
    return comments, more_ids


def parse_comment(raw_comment: JsonObject, depth: int = 0) -> RedditComment | None:
    data = _schema.nested_object(raw_comment, "data")
    body = _schema.optional_str(data, "body", "")
    if body in {"", "[deleted]", "[removed]"}:
        return None

    replies = data.get("replies", "")
    reply_children: list[JsonObject] = []
    if isinstance(replies, dict):
        reply_children = _schema.nested_list(replies, "data", "children")
    elif replies not in {"", None}:
        raise RedditResponseError("reddit replies payload must be a listing object or empty string")

    parsed_replies: list[RedditComment] = []
    for child in reply_children:
        kind = _schema.required_str(child, "kind")
        if kind != "t1":
            continue
        reply = parse_comment(child, depth=depth + 1)
        if reply is not None:
            parsed_replies.append(reply)
    return RedditComment(
        id=_schema.required_str(data, "id"),
        author=_schema.optional_str(data, "author", "[deleted]"),
        body=body,
        score=_schema.optional_int(data, "score", 0),
        created_utc=_schema.optional_float(data, "created_utc", 0.0),
        depth=depth,
        parent_id=_schema.optional_str(data, "parent_id", ""),
        permalink=f"https://reddit.com{_schema.optional_str(data, 'permalink', '')}",
        replies=tuple(parsed_replies),
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
