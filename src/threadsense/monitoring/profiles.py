from __future__ import annotations

import datetime
import hashlib
import json
import math
import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, TypeVar

from threadsense.connectors.reddit import validate_subreddit_name
from threadsense.contracts import AbstractionLevel, DomainType, ObjectiveType
from threadsense.errors import AnalysisBoundaryError, MonitoringInputError, RedditInputError
from threadsense.workflows import validate_research_query

WATCH_PROFILE_VERSION = 1

WATCH_ID_PATTERN = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?")
MAX_NAME_LENGTH = 200
MAX_QUERIES = 10
MAX_QUERY_LENGTH = 256
MAX_SUBREDDITS = 25
MAX_TERMS = 50
MAX_TERM_LENGTH = 100
REFRESH_INTERVAL_MINUTES_RANGE = (15, 10080)
LIMIT_RANGES: dict[str, tuple[int, int]] = {
    "discovered_candidates": (1, 500),
    "refreshed_threads": (1, 200),
    "analyzed_revisions": (1, 200),
    "presented_findings": (1, 100),
}

_TOP_LEVEL_FIELDS = frozenset(
    {
        "profile_version",
        "id",
        "name",
        "domain",
        "objective",
        "abstraction_level",
        "queries",
        "subreddits",
        "refresh_interval_minutes",
        "include_terms",
        "exclude_terms",
        "limits",
    }
)

_EnumT = TypeVar("_EnumT", bound=StrEnum)


@dataclass(frozen=True)
class WatchLimits:
    discovered_candidates: int
    refreshed_threads: int
    analyzed_revisions: int
    presented_findings: int

    def to_dict(self) -> dict[str, int]:
        return {
            "discovered_candidates": self.discovered_candidates,
            "refreshed_threads": self.refreshed_threads,
            "analyzed_revisions": self.analyzed_revisions,
            "presented_findings": self.presented_findings,
        }


@dataclass(frozen=True)
class WatchProfile:
    """Validated research profile.

    Queries, subreddits and terms are sets: they are stored sorted, so reordering
    them in the source document does not change the profile content hash.
    """

    watch_id: str
    name: str
    domain: DomainType
    objective: ObjectiveType
    abstraction_level: AbstractionLevel
    queries: tuple[str, ...]
    subreddits: tuple[str, ...]
    refresh_interval_minutes: int
    include_terms: tuple[str, ...]
    exclude_terms: tuple[str, ...]
    limits: WatchLimits

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile_version": WATCH_PROFILE_VERSION,
            "id": self.watch_id,
            "name": self.name,
            "domain": self.domain.value,
            "objective": self.objective.value,
            "abstraction_level": self.abstraction_level.value,
            "queries": list(self.queries),
            "subreddits": list(self.subreddits),
            "refresh_interval_minutes": self.refresh_interval_minutes,
            "include_terms": list(self.include_terms),
            "exclude_terms": list(self.exclude_terms),
            "limits": self.limits.to_dict(),
        }

    def canonical_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    def content_sha256(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


def load_watch_profile_file(path: Path) -> WatchProfile:
    try:
        with path.open("rb") as handle:
            payload = tomllib.load(handle)
    except FileNotFoundError as error:
        raise MonitoringInputError(
            "watch profile file does not exist", details={"path": str(path)}
        ) from error
    except OSError as error:
        raise MonitoringInputError(
            "watch profile file could not be read",
            details={"path": str(path), "error": str(error)},
        ) from error
    except UnicodeDecodeError as error:
        raise MonitoringInputError(
            "watch profile file is not valid UTF-8",
            details={"path": str(path), "error": str(error)},
        ) from error
    except tomllib.TOMLDecodeError as error:
        raise MonitoringInputError(
            "watch profile file is not valid TOML",
            details={"path": str(path), "error": str(error)},
        ) from error
    return parse_watch_profile(payload)


def parse_watch_profile(payload: Mapping[str, Any]) -> WatchProfile:
    _validate_profile_version(payload.get("profile_version"))
    unknown = sorted(set(payload) - _TOP_LEVEL_FIELDS)
    if unknown:
        raise MonitoringInputError(
            "watch profile contains unknown fields", details={"fields": unknown}
        )
    include_terms = _parse_terms(payload, "include_terms")
    exclude_terms = _parse_terms(payload, "exclude_terms")
    overlap = sorted(
        {term.casefold() for term in include_terms} & {term.casefold() for term in exclude_terms}
    )
    if overlap:
        raise MonitoringInputError(
            "watch include_terms and exclude_terms must not overlap",
            details={"terms": overlap},
        )
    return WatchProfile(
        watch_id=validate_watch_id(_required_str(payload, "id")),
        name=_parse_name(payload),
        domain=_parse_enum(payload, "domain", DomainType),
        objective=_parse_enum(payload, "objective", ObjectiveType),
        abstraction_level=_parse_enum(payload, "abstraction_level", AbstractionLevel),
        queries=_parse_queries(payload),
        subreddits=_parse_subreddits(payload),
        refresh_interval_minutes=_bounded_int(
            payload.get("refresh_interval_minutes"),
            "refresh_interval_minutes",
            REFRESH_INTERVAL_MINUTES_RANGE,
        ),
        include_terms=include_terms,
        exclude_terms=exclude_terms,
        limits=_parse_limits(payload.get("limits")),
    )


def validate_watch_id(watch_id: str) -> str:
    if WATCH_ID_PATTERN.fullmatch(watch_id) is None:
        raise MonitoringInputError(
            "watch id must be 1-64 lowercase letters, digits or inner hyphens",
            details={"field": "id", "value": watch_id},
        )
    return watch_id


def _validate_profile_version(version: object) -> None:
    if type(version) is not int:
        raise MonitoringInputError(
            "watch profile_version must be an integer",
            details={"field": "profile_version", "value": _detail_value(version)},
        )
    if version != WATCH_PROFILE_VERSION:
        raise MonitoringInputError(
            "watch profile_version is not supported",
            details={
                "field": "profile_version",
                "value": _detail_value(version),
                "supported": WATCH_PROFILE_VERSION,
            },
        )


def _detail_value(value: object) -> object:
    """Render a raw TOML value as JSON-safe error detail."""
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, datetime.datetime | datetime.date | datetime.time):
        return value.isoformat()
    if isinstance(value, list):
        return [_detail_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _detail_value(item) for key, item in value.items()}
    return value


def _required_str(payload: Mapping[str, Any], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str):
        raise MonitoringInputError(
            f"watch {field} must be a string",
            details={"field": field, "value": _detail_value(value)},
        )
    return value


def _parse_name(payload: Mapping[str, Any]) -> str:
    name = _required_str(payload, "name").strip()
    if not name or len(name) > MAX_NAME_LENGTH:
        raise MonitoringInputError(
            f"watch name must be 1-{MAX_NAME_LENGTH} characters", details={"field": "name"}
        )
    return name


def _parse_enum(payload: Mapping[str, Any], field: str, enum_type: type[_EnumT]) -> _EnumT:
    value = _required_str(payload, field)
    try:
        return enum_type(value)
    except ValueError as error:
        raise MonitoringInputError(
            f"watch {field} is not supported",
            details={"field": field, "value": value, "supported": [m.value for m in enum_type]},
        ) from error


def _string_set(
    payload: Mapping[str, Any], field: str, *, min_items: int, max_items: int
) -> list[str]:
    values = payload.get(field, [])
    if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
        raise MonitoringInputError(
            f"watch {field} must be a list of strings", details={"field": field}
        )
    if not min_items <= len(values) <= max_items:
        raise MonitoringInputError(
            f"watch {field} must contain {min_items}-{max_items} entries",
            details={"field": field, "count": len(values)},
        )
    stripped = [value.strip() for value in values]
    if len({value.casefold() for value in stripped}) != len(stripped):
        raise MonitoringInputError(
            f"watch {field} must not contain duplicates", details={"field": field}
        )
    return stripped


def _parse_queries(payload: Mapping[str, Any]) -> tuple[str, ...]:
    queries = _string_set(payload, "queries", min_items=1, max_items=MAX_QUERIES)
    for query in queries:
        if len(query) > MAX_QUERY_LENGTH:
            raise MonitoringInputError(
                f"watch query must be at most {MAX_QUERY_LENGTH} characters",
                details={"field": "queries", "value": query},
            )
        try:
            validate_research_query(query)
        except AnalysisBoundaryError as error:
            raise MonitoringInputError(
                f"watch query is invalid: {error.message}",
                details={"field": "queries", "value": query},
            ) from error
    return tuple(sorted(queries))


def _parse_subreddits(payload: Mapping[str, Any]) -> tuple[str, ...]:
    raw_names = _string_set(payload, "subreddits", min_items=1, max_items=MAX_SUBREDDITS)
    names: list[str] = []
    for raw_name in raw_names:
        try:
            names.append(validate_subreddit_name(raw_name))
        except RedditInputError as error:
            raise MonitoringInputError(
                "watch subreddit name is invalid",
                details={"field": "subreddits", "value": raw_name},
            ) from error
    if len({name.casefold() for name in names}) != len(names):
        raise MonitoringInputError(
            "watch subreddits must not contain duplicates", details={"field": "subreddits"}
        )
    return tuple(sorted(names, key=str.casefold))


def _parse_terms(payload: Mapping[str, Any], field: str) -> tuple[str, ...]:
    terms = _string_set(payload, field, min_items=0, max_items=MAX_TERMS)
    for term in terms:
        if not term or len(term) > MAX_TERM_LENGTH:
            raise MonitoringInputError(
                f"watch {field} entries must be 1-{MAX_TERM_LENGTH} characters",
                details={"field": field, "value": term},
            )
    return tuple(sorted(terms, key=str.casefold))


def _bounded_int(value: object, field: str, bounds: tuple[int, int]) -> int:
    low, high = bounds
    if type(value) is not int or not low <= value <= high:
        raise MonitoringInputError(
            f"watch {field} must be an integer between {low} and {high}",
            details={"field": field, "value": _detail_value(value)},
        )
    return value


def _parse_limits(value: object) -> WatchLimits:
    if not isinstance(value, dict):
        raise MonitoringInputError("watch limits must be a table", details={"field": "limits"})
    unknown = sorted(set(value) - set(LIMIT_RANGES))
    if unknown:
        raise MonitoringInputError(
            "watch limits contain unknown fields", details={"fields": unknown}
        )
    parsed = {
        name: _bounded_int(value.get(name), f"limits.{name}", bounds)
        for name, bounds in LIMIT_RANGES.items()
    }
    return WatchLimits(**parsed)
