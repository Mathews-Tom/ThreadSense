from __future__ import annotations

import json
import re
import time as time_mod
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from time import time
from typing import Any
from urllib import parse

import httpx

from threadsense.config import GitHubGistConfig
from threadsense.connectors import FetchRequest, RawArtifact
from threadsense.connectors.cache import FetchCache
from threadsense.errors import GitHubInputError, GitHubRequestError, NetworkBoundaryError

RestTransport = Callable[[str, Mapping[str, str], float], tuple[Any, httpx.Headers]]

_GIST_PATH_WITH_OWNER = re.compile(r"^/([^/]+)/([0-9a-fA-F]+)$")
_GIST_PATH_BARE = re.compile(r"^/([0-9a-fA-F]+)$")


@dataclass(frozen=True)
class GitHubGistFile:
    filename: str
    language: str | None
    size: int
    content: str | None
    raw_url: str


@dataclass(frozen=True)
class GitHubGistComment:
    comment_id: int
    node_id: str
    author: str
    author_association: str
    body: str
    created_utc: float
    updated_utc: float
    url: str


@dataclass(frozen=True)
class GitHubGistInfo:
    gist_id: str
    description: str | None
    owner: str
    files: tuple[GitHubGistFile, ...]
    public: bool
    comments_count: int
    created_utc: float
    updated_utc: float
    html_url: str
    forks_count: int


@dataclass(frozen=True)
class GitHubGistResult:
    requested_url: str
    fetched_at_utc: float
    gist: GitHubGistInfo
    comments: list[GitHubGistComment]
    total_comment_count: int
    cache_status: str
    rate_limit_remaining: int | None
    raw_payload: dict[str, Any]

    @property
    def source_name(self) -> str:
        return "github_gist"

    @property
    def source_thread_id(self) -> str:
        return self.gist.gist_id

    @property
    def thread_title(self) -> str:
        description = self.gist.description
        if description:
            return description
        if self.gist.files:
            return self.gist.files[0].filename
        return self.gist.gist_id

    @property
    def normalized_url(self) -> str:
        return self.gist.html_url

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_version": 2,
            "source": self.source_name,
            "requested_url": self.requested_url,
            "normalized_url": self.normalized_url,
            "fetched_at_utc": self.fetched_at_utc,
            "gist": {
                "gist_id": self.gist.gist_id,
                "description": self.gist.description,
                "owner": self.gist.owner,
                "files": [_file_to_dict(f) for f in self.gist.files],
                "public": self.gist.public,
                "comments_count": self.gist.comments_count,
                "created_utc": self.gist.created_utc,
                "updated_utc": self.gist.updated_utc,
                "html_url": self.gist.html_url,
                "forks_count": self.gist.forks_count,
            },
            "comments": [_comment_to_dict(c) for c in self.comments],
            "total_comment_count": self.total_comment_count,
            "cache_status": self.cache_status,
            "rate_limit_remaining": self.rate_limit_remaining,
            "raw_payload": self.raw_payload,
        }


class GitHubGistConnector:
    source_name = "github_gist"

    def __init__(
        self,
        config: GitHubGistConfig,
        cache: FetchCache | None = None,
        transport: RestTransport | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        self._config = config
        self._cache = cache
        self._transport = transport or _default_transport
        self._sleeper = sleeper or time_mod.sleep

    def fetch(self, request: FetchRequest) -> RawArtifact:
        normalized_url, gist_id = normalize_url(request.url)
        cache_key = f"gist:{gist_id}"

        if self._cache is not None:
            cached = self._cache.get(cache_key)
            if cached is not None:
                return _result_from_dict(cached, request.url)

        headers = _build_headers(self._config.token)
        timeout = request.timeout_seconds or self._config.timeout_seconds

        gist_url = f"{self._config.base_url.rstrip('/')}/gists/{gist_id}"
        raw_gist, gist_headers = self._transport(gist_url, headers, timeout)
        if not isinstance(raw_gist, dict):
            raise GitHubRequestError("github gist response must decode to an object")
        rate_limit = _extract_rate_limit(gist_headers)

        gist_info = _parse_gist(raw_gist, gist_id)
        comments = _fetch_all_comments(
            base_url=self._config.base_url,
            gist_id=gist_id,
            headers=headers,
            timeout=timeout,
            per_page=self._config.comments_per_page,
            delay=self._config.request_delay_seconds,
            transport=self._transport,
            sleeper=self._sleeper,
        )

        result = GitHubGistResult(
            requested_url=request.url,
            fetched_at_utc=time(),
            gist=gist_info,
            comments=comments,
            total_comment_count=len(comments),
            cache_status="miss",
            rate_limit_remaining=rate_limit,
            raw_payload=raw_gist,
        )

        if self._cache is not None:
            self._cache.put(cache_key, result.to_dict())

        return result

    def normalize(self, raw_artifact: Mapping[str, Any], raw_artifact_path: Path) -> Any:
        from threadsense.pipeline import normalize as _norm_mod

        return _norm_mod.normalize_github_gist_artifact(raw_artifact, raw_artifact_path)

    def supports_url(self, url: str) -> bool:
        try:
            normalize_url(url)
        except GitHubInputError:
            return False
        return True


def normalize_url(url: str) -> tuple[str, str]:
    parsed = parse.urlparse(url)
    if parsed.scheme not in {"http", "https"} or parsed.netloc != "gist.github.com":
        raise GitHubInputError(
            "github gist URL must target gist.github.com",
            details={"url": url},
        )
    path = parsed.path.rstrip("/")

    match_with_owner = _GIST_PATH_WITH_OWNER.match(path)
    if match_with_owner:
        owner, gist_id = match_with_owner.groups()
        return f"https://gist.github.com/{owner}/{gist_id}", gist_id

    match_bare = _GIST_PATH_BARE.match(path)
    if match_bare:
        gist_id = match_bare.group(1)
        return f"https://gist.github.com/{gist_id}", gist_id

    raise GitHubInputError(
        "github gist URL must contain a valid gist ID",
        details={"url": url},
    )


def _build_headers(token: str) -> dict[str, str]:
    headers: dict[str, str] = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _default_transport(
    url: str,
    headers: Mapping[str, str],
    timeout: float,
) -> tuple[Any, httpx.Headers]:
    try:
        with httpx.Client(timeout=timeout) as client:
            response = client.get(url, headers=dict(headers))
            response.raise_for_status()
            parsed = response.json()
    except httpx.HTTPStatusError as http_error:
        raise GitHubRequestError(
            "github gist request failed",
            details={
                "status_code": http_error.response.status_code,
                "url": str(http_error.request.url),
            },
        ) from http_error
    except httpx.TimeoutException as timeout_error:
        raise NetworkBoundaryError(
            "github gist request timed out",
            details={"url": url, "timeout_seconds": timeout},
        ) from timeout_error
    except httpx.ConnectError as connect_error:
        raise NetworkBoundaryError(
            "github gist endpoint is unreachable",
            details={"url": url, "reason": str(connect_error)},
        ) from connect_error
    except json.JSONDecodeError as decode_error:
        raise GitHubRequestError(
            "github gist response body is not valid JSON",
            details={"url": url},
        ) from decode_error
    return parsed, response.headers


def _extract_rate_limit(headers: httpx.Headers) -> int | None:
    value = headers.get("X-RateLimit-Remaining")
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _parse_gist(payload: dict[str, Any], gist_id: str) -> GitHubGistInfo:
    owner_data = payload.get("owner") or {}
    files_data = payload.get("files") or {}

    files: list[GitHubGistFile] = []
    for file_info in files_data.values():
        if not isinstance(file_info, dict):
            continue
        files.append(
            GitHubGistFile(
                filename=str(file_info.get("filename", "")),
                language=file_info.get("language"),
                size=int(file_info.get("size", 0)),
                content=file_info.get("content"),
                raw_url=str(file_info.get("raw_url", "")),
            )
        )

    forks = payload.get("forks") or []
    forks_count = len(forks) if isinstance(forks, list) else 0

    return GitHubGistInfo(
        gist_id=gist_id,
        description=payload.get("description"),
        owner=str(owner_data.get("login", "[anonymous]")),
        files=tuple(files),
        public=bool(payload.get("public", False)),
        comments_count=int(payload.get("comments", 0)),
        created_utc=parse_timestamp(str(payload.get("created_at", ""))),
        updated_utc=parse_timestamp(str(payload.get("updated_at", ""))),
        html_url=str(payload.get("html_url", "")),
        forks_count=forks_count,
    )


def _fetch_all_comments(
    *,
    base_url: str,
    gist_id: str,
    headers: dict[str, str],
    timeout: float,
    per_page: int,
    delay: float,
    transport: RestTransport,
    sleeper: Callable[[float], None],
) -> list[GitHubGistComment]:
    comments: list[GitHubGistComment] = []
    page = 1

    while True:
        if page > 1:
            sleeper(delay)

        url = f"{base_url.rstrip('/')}/gists/{gist_id}/comments?per_page={per_page}&page={page}"
        payload, _ = transport(url, headers, timeout)

        if not isinstance(payload, list):
            raise GitHubRequestError(
                "github gist comments response must be an array",
                details={"gist_id": gist_id, "page": page},
            )

        for item in payload:
            if not isinstance(item, dict):
                continue
            comments.append(_parse_comment(item))

        if len(payload) < per_page:
            break
        page += 1

    return comments


def _parse_comment(payload: dict[str, Any]) -> GitHubGistComment:
    user = payload.get("user") or {}
    return GitHubGistComment(
        comment_id=int(payload.get("id", 0)),
        node_id=str(payload.get("node_id", "")),
        author=str(user.get("login", "[deleted]")),
        author_association=str(payload.get("author_association", "NONE")),
        body=str(payload.get("body", "")),
        created_utc=parse_timestamp(str(payload.get("created_at", ""))),
        updated_utc=parse_timestamp(str(payload.get("updated_at", ""))),
        url=str(payload.get("url", "")),
    )


def parse_timestamp(value: str) -> float:
    from datetime import datetime

    if not value:
        return 0.0
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def _file_to_dict(f: GitHubGistFile) -> dict[str, Any]:
    return {
        "filename": f.filename,
        "language": f.language,
        "size": f.size,
        "content": f.content,
        "raw_url": f.raw_url,
    }


def _comment_to_dict(c: GitHubGistComment) -> dict[str, Any]:
    return {
        "comment_id": c.comment_id,
        "node_id": c.node_id,
        "author": c.author,
        "author_association": c.author_association,
        "body": c.body,
        "created_utc": c.created_utc,
        "updated_utc": c.updated_utc,
        "url": c.url,
    }


def _result_from_dict(data: dict[str, Any], requested_url: str) -> GitHubGistResult:
    gist_data = data.get("gist", {})
    files_data = gist_data.get("files", [])
    comments_data = data.get("comments", [])

    files = tuple(
        GitHubGistFile(
            filename=str(f.get("filename", "")),
            language=f.get("language"),
            size=int(f.get("size", 0)),
            content=f.get("content"),
            raw_url=str(f.get("raw_url", "")),
        )
        for f in files_data
        if isinstance(f, dict)
    )

    gist_info = GitHubGistInfo(
        gist_id=str(gist_data.get("gist_id", "")),
        description=gist_data.get("description"),
        owner=str(gist_data.get("owner", "[anonymous]")),
        files=files,
        public=bool(gist_data.get("public", False)),
        comments_count=int(gist_data.get("comments_count", 0)),
        created_utc=float(gist_data.get("created_utc", 0.0)),
        updated_utc=float(gist_data.get("updated_utc", 0.0)),
        html_url=str(gist_data.get("html_url", "")),
        forks_count=int(gist_data.get("forks_count", 0)),
    )

    comments = [
        GitHubGistComment(
            comment_id=int(c.get("comment_id", 0)),
            node_id=str(c.get("node_id", "")),
            author=str(c.get("author", "[deleted]")),
            author_association=str(c.get("author_association", "NONE")),
            body=str(c.get("body", "")),
            created_utc=float(c.get("created_utc", 0.0)),
            updated_utc=float(c.get("updated_utc", 0.0)),
            url=str(c.get("url", "")),
        )
        for c in comments_data
        if isinstance(c, dict)
    ]

    return GitHubGistResult(
        requested_url=requested_url,
        fetched_at_utc=float(data.get("fetched_at_utc", 0.0)),
        gist=gist_info,
        comments=comments,
        total_comment_count=int(data.get("total_comment_count", 0)),
        cache_status="hit",
        rate_limit_remaining=data.get("rate_limit_remaining"),
        raw_payload=data.get("raw_payload", {}),
    )
