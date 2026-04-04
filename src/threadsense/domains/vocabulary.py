from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DomainVocabulary:
    domain: str
    version: str
    theme_rules: dict[str, tuple[str, ...]]
    issue_markers: tuple[str, ...]
    request_markers: tuple[str, ...]
    severity_levels: tuple[str, ...]
    issue_fallback_theme: str
    request_fallback_theme: str
    default_theme: str
