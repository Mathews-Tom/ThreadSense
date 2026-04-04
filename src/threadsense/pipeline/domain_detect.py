from __future__ import annotations

from dataclasses import dataclass

from threadsense.contracts import DomainType
from threadsense.domains import load_domain_vocabulary
from threadsense.models.canonical import Thread
from threadsense.pipeline.strategies.keyword_heuristic import build_comment_signal, classify_theme


@dataclass(frozen=True)
class DomainScore:
    domain: DomainType
    classified_count: int
    total_sampled: int
    fit_score: float


@dataclass(frozen=True)
class DetectionResult:
    selected: DomainType
    scores: list[DomainScore]
    switched: bool


_DEFAULT_SAMPLE_LIMIT = 15
_DEFAULT_MIN_FIT_SCORE = 0.3


def detect_domain(
    thread: Thread,
    configured_domain: DomainType,
    *,
    sample_limit: int = _DEFAULT_SAMPLE_LIMIT,
    min_fit_score: float = _DEFAULT_MIN_FIT_SCORE,
) -> DetectionResult:
    """Score thread against all domain vocabularies and return the best fit.

    If the configured domain scores at or above *min_fit_score*, keep it.
    Otherwise, switch to the highest-scoring alternative.
    """
    top_level = [c for c in thread.comments if c.parent_comment_id is None]
    sampled = sorted(top_level, key=lambda c: c.score, reverse=True)[:sample_limit]
    if not sampled:
        return DetectionResult(
            selected=configured_domain,
            scores=[],
            switched=False,
        )

    scores: list[DomainScore] = []
    for domain in DomainType:
        if domain is DomainType.CUSTOM:
            continue
        score = _score_domain(sampled, domain)
        scores.append(score)

    scores.sort(key=lambda s: (-s.fit_score, s.domain.value))
    configured_score = next((s for s in scores if s.domain == configured_domain), None)

    if configured_score is not None and configured_score.fit_score >= min_fit_score:
        return DetectionResult(
            selected=configured_domain,
            scores=scores,
            switched=False,
        )

    best = scores[0] if scores else None
    if best is None or best.fit_score < min_fit_score:
        return DetectionResult(
            selected=configured_domain,
            scores=scores,
            switched=False,
        )

    return DetectionResult(
        selected=best.domain,
        scores=scores,
        switched=best.domain != configured_domain,
    )


def _score_domain(
    comments: list,
    domain: DomainType,
) -> DomainScore:
    vocabulary = load_domain_vocabulary(domain.value)
    classified = 0
    for comment in comments:
        signal = build_comment_signal(
            comment,
            theme_rules=vocabulary.theme_rules,
            issue_markers=vocabulary.issue_markers,
            request_markers=vocabulary.request_markers,
        )
        if signal is None:
            continue
        theme = classify_theme(
            signal,
            issue_fallback_theme=vocabulary.issue_fallback_theme,
            request_fallback_theme=vocabulary.request_fallback_theme,
            default_theme=vocabulary.default_theme,
        )
        if theme != vocabulary.default_theme:
            classified += 1
    total = len(comments)
    return DomainScore(
        domain=domain,
        classified_count=classified,
        total_sampled=total,
        fit_score=classified / max(total, 1),
    )
