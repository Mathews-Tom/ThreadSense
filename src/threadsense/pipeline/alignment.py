from __future__ import annotations

from typing import TYPE_CHECKING

from threadsense.contracts import AnalysisContract, DomainType
from threadsense.domains import load_domain_vocabulary
from threadsense.models.analysis import AlignmentCheck
from threadsense.models.canonical import Thread
from threadsense.pipeline.strategies.keyword_heuristic import build_comment_signal, classify_theme

if TYPE_CHECKING:
    from threadsense.models.analysis import ThreadAnalysis


def check_domain_alignment(
    thread: Thread,
    analysis: ThreadAnalysis,
    contract: AnalysisContract,
) -> AlignmentCheck:
    classified_comments = sum(
        1 for finding in analysis.findings if finding.theme_key != "general_feedback"
    )
    total_comments = max(analysis.distinct_comment_count, 1)
    general_feedback_count = next(
        (
            finding.comment_count
            for finding in analysis.findings
            if finding.theme_key == "general_feedback"
        ),
        0,
    )
    domain_fit_score = classified_comments / total_comments
    general_feedback_ratio = general_feedback_count / total_comments
    suggested_domain = None
    if domain_fit_score < 0.3:
        suggested_domain = suggest_domain(thread, exclude=contract.domain)
    warning = None
    if domain_fit_score < 0.3:
        warning = (
            f"Domain fit is low for `{contract.domain.value}` "
            f"({domain_fit_score:.2f}); consider `{suggested_domain}`."
            if suggested_domain is not None
            else f"Domain fit is low for `{contract.domain.value}` ({domain_fit_score:.2f})."
        )
    return AlignmentCheck(
        domain=contract.domain.value,
        domain_fit_score=domain_fit_score,
        general_feedback_ratio=general_feedback_ratio,
        suggested_domain=suggested_domain,
        warning=warning,
    )


def suggest_domain(thread: Thread, exclude: DomainType) -> str | None:
    best_domain: str | None = None
    best_score = 0
    for domain in DomainType:
        if domain in {exclude, DomainType.CUSTOM}:
            continue
        vocabulary = load_domain_vocabulary(domain.value)
        score = 0
        for comment in thread.comments:
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
                score += 1
        if score > best_score:
            best_score = score
            best_domain = domain.value
    return best_domain
