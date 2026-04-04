from __future__ import annotations

import tomllib
from functools import cache
from pathlib import Path
from typing import Any

from threadsense.domains.vocabulary import DomainVocabulary
from threadsense.errors import AnalysisBoundaryError

_DEFINITIONS_DIR = Path(__file__).resolve().parent / "definitions"


@cache
def load_domain_vocabulary(domain: str) -> DomainVocabulary:
    domain_key = domain.strip()
    if not domain_key:
        raise AnalysisBoundaryError("domain must not be empty")

    path = _DEFINITIONS_DIR / f"{domain_key}.toml"
    try:
        with path.open("rb") as handle:
            raw = tomllib.load(handle)
    except FileNotFoundError as error:
        raise AnalysisBoundaryError(
            "domain vocabulary definition does not exist",
            details={"domain": domain_key, "path": str(path)},
        ) from error

    return _parse_domain_vocabulary(domain_key, raw)


def _parse_domain_vocabulary(domain: str, payload: dict[str, Any]) -> DomainVocabulary:
    meta = _required_table(payload, "meta", domain)
    declared_domain = _required_non_empty_str(meta, "domain", domain)
    if declared_domain != domain:
        raise AnalysisBoundaryError(
            "domain vocabulary metadata does not match requested domain",
            details={"domain": domain, "declared_domain": declared_domain},
        )

    themes_table = _required_table(payload, "themes", domain)
    theme_rules = {
        key: _required_str_tuple(values, f"themes.{key}", domain)
        for key, values in themes_table.items()
    }
    if not theme_rules:
        raise AnalysisBoundaryError(
            "domain vocabulary must define at least one theme",
            details={"domain": domain},
        )

    markers = _required_table(payload, "markers", domain)
    severity = _required_table(payload, "severity", domain)
    fallbacks = _required_table(payload, "fallbacks", domain)
    severity_levels = _required_str_tuple(severity.get("levels"), "severity.levels", domain)
    if len(severity_levels) < 3:
        raise AnalysisBoundaryError(
            "domain vocabulary severity levels must define at least three entries",
            details={"domain": domain, "levels": list(severity_levels)},
        )

    return DomainVocabulary(
        domain=declared_domain,
        version=_required_non_empty_str(meta, "version", domain),
        theme_rules=theme_rules,
        issue_markers=_required_str_tuple(markers.get("issue"), "markers.issue", domain),
        request_markers=_required_str_tuple(markers.get("request"), "markers.request", domain),
        severity_levels=severity_levels,
        issue_fallback_theme=_required_theme_name(
            fallbacks, "issue", domain, allowed=list(theme_rules) + ["general_feedback"]
        ),
        request_fallback_theme=_required_theme_name(
            fallbacks, "request", domain, allowed=list(theme_rules) + ["general_feedback"]
        ),
        default_theme=_required_theme_name(
            fallbacks, "default", domain, allowed=list(theme_rules) + ["general_feedback"]
        ),
    )


def _required_table(
    payload: dict[str, Any],
    key: str,
    domain: str,
) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise AnalysisBoundaryError(
            "domain vocabulary table is invalid",
            details={"domain": domain, "key": key},
        )
    return value


def _required_non_empty_str(payload: dict[str, Any], key: str, domain: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise AnalysisBoundaryError(
            "domain vocabulary string field is invalid",
            details={"domain": domain, "key": key},
        )
    return value.strip()


def _required_str_tuple(value: Any, key: str, domain: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise AnalysisBoundaryError(
            "domain vocabulary list field is invalid",
            details={"domain": domain, "key": key},
        )
    normalized = tuple(item.strip() for item in value if isinstance(item, str) and item.strip())
    if len(normalized) != len(value):
        raise AnalysisBoundaryError(
            "domain vocabulary entries must be non-empty strings",
            details={"domain": domain, "key": key},
        )
    return normalized


def _required_theme_name(
    payload: dict[str, Any],
    key: str,
    domain: str,
    allowed: list[str],
) -> str:
    value = _required_non_empty_str(payload, key, domain)
    if value not in allowed:
        raise AnalysisBoundaryError(
            "domain vocabulary fallback theme is invalid",
            details={"domain": domain, "key": key, "value": value, "allowed": allowed},
        )
    return value
