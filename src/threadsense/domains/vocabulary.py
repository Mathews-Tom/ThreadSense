from __future__ import annotations

from dataclasses import dataclass
from typing import Any


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


def merge_vocabulary_expansion(
    base: DomainVocabulary,
    expansion: dict[str, Any],
) -> DomainVocabulary:
    """Merge LLM-proposed keywords into a runtime copy of the vocabulary.

    *expansion* follows the validated output shape:
    ``{"existing_themes": {"theme": ["kw"]}, "new_themes": {"theme": ["kw"]}}``
    """
    existing_additions: dict[str, list[str]] = expansion.get("existing_themes", {})
    new_themes: dict[str, list[str]] = expansion.get("new_themes", {})

    merged_rules = dict(base.theme_rules)
    for theme_key, additional in existing_additions.items():
        if theme_key not in merged_rules:
            continue
        current = set(merged_rules[theme_key])
        added = [kw for kw in additional if kw not in current]
        merged_rules[theme_key] = merged_rules[theme_key] + tuple(added)

    for theme_key, keywords in new_themes.items():
        if theme_key in merged_rules or theme_key == base.default_theme:
            continue
        merged_rules[theme_key] = tuple(keywords)

    return DomainVocabulary(
        domain=base.domain,
        version=f"{base.version}+expanded",
        theme_rules=merged_rules,
        issue_markers=base.issue_markers,
        request_markers=base.request_markers,
        severity_levels=base.severity_levels,
        issue_fallback_theme=base.issue_fallback_theme,
        request_fallback_theme=base.request_fallback_theme,
        default_theme=base.default_theme,
    )
