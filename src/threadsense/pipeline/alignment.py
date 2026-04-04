from __future__ import annotations

from typing import TYPE_CHECKING

from threadsense.contracts import AnalysisContract, DomainType
from threadsense.models.analysis import AlignmentCheck
from threadsense.models.canonical import Thread
from threadsense.pipeline.domain_detect import detect_domain

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
    result = detect_domain(thread, exclude)
    if not result.switched:
        return None
    return result.selected.value
