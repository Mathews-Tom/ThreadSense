from __future__ import annotations

from time import time

from threadsense.action_signals import classify_finding_signal
from threadsense.inference.contracts import InferenceResponse
from threadsense.models.analysis import AnalysisFinding, ThreadAnalysis
from threadsense.models.report import (
    REPORT_ENGINE_VERSION,
    REPORT_SCHEMA_VERSION,
    ReportExecutiveSummary,
    ReportFinding,
    ReportProvenance,
    ThreadReport,
    validate_report_finding_semantics,
)
from threadsense.pipeline.storage import calculate_sha256
from threadsense.reporting.quality import resolve_coverage_gaps, run_quality_checks


def build_thread_report(
    analysis: ThreadAnalysis,
    analysis_artifact_path: str,
    summary_response: InferenceResponse | None,
) -> ThreadReport:
    summary = build_executive_summary(analysis, summary_response)
    caveats = build_caveats(analysis, summary_response)
    report = ThreadReport(
        thread_id=analysis.thread_id,
        source_name=analysis.source_name,
        title=analysis.title,
        top_phrases=analysis.top_phrases[:8],
        executive_summary=summary,
        conversation_structure=analysis.conversation_structure,
        findings=[build_report_finding(finding) for finding in analysis.findings],
        caveats=caveats,
        quality_checks=[],
        provenance=ReportProvenance(
            analysis_artifact_path=analysis_artifact_path,
            analysis_sha256=calculate_sha256_path(analysis_artifact_path),
            generated_at_utc=time(),
            schema_version=REPORT_SCHEMA_VERSION,
            report_version=REPORT_ENGINE_VERSION,
            summary_provider=summary.provider,
        ),
    )
    quality_checks = run_quality_checks(report)
    report = ThreadReport(
        thread_id=report.thread_id,
        source_name=report.source_name,
        title=report.title,
        top_phrases=report.top_phrases,
        executive_summary=report.executive_summary,
        conversation_structure=report.conversation_structure,
        findings=report.findings,
        caveats=report.caveats,
        quality_checks=quality_checks,
        provenance=report.provenance,
    )
    return resolve_coverage_gaps(report)


def build_executive_summary(
    analysis: ThreadAnalysis,
    summary_response: InferenceResponse | None,
) -> ReportExecutiveSummary:
    if summary_response is None:
        first_finding = analysis.findings[0] if analysis.findings else None
        if first_finding is None:
            raise ValueError("deterministic report summary requires at least one finding")
        first_signal = classify_finding_signal(
            theme_key=first_finding.theme_key,
            severity=first_finding.severity,
            comment_count=first_finding.comment_count,
            issue_marker_count=first_finding.issue_marker_count,
            request_marker_count=first_finding.request_marker_count,
        )
        return ReportExecutiveSummary(
            headline=f"{first_finding.theme_label.title()} leads the thread",
            summary=f"Top themes: {', '.join(f.theme_key for f in analysis.findings[:3])}.",
            priority=_default_priority(first_finding.severity if first_finding else None),
            confidence=_default_confidence(first_finding),
            why_now=_default_why_now(first_finding),
            cited_theme_keys=[finding.theme_key for finding in analysis.findings[:3]],
            cited_comment_ids=(
                analysis.findings[0].evidence_comment_ids[:5] if analysis.findings else []
            ),
            next_steps=[
                f"Review {finding.theme_key} evidence group" for finding in analysis.findings[:3]
            ],
            recommended_owner=first_signal.recommended_owner,
            action_type=first_signal.action_type,
            expected_outcome=_default_expected_outcome(first_finding),
            provider="deterministic_report",
            degraded=False,
        )

    output = summary_response.output
    return ReportExecutiveSummary(
        headline=str(output["headline"]),
        summary=str(output["summary"]),
        priority=str(output["priority"]),
        confidence=float(output["confidence"]),
        why_now=str(output["why_now"]),
        cited_theme_keys=list(output["cited_theme_keys"]),
        cited_comment_ids=list(output["cited_comment_ids"]),
        next_steps=list(output["next_steps"]),
        recommended_owner=str(output["recommended_owner"]),
        action_type=str(output["action_type"]),
        expected_outcome=str(output["expected_outcome"]),
        provider=summary_response.provider,
        degraded=summary_response.degraded,
    )


def build_caveats(
    analysis: ThreadAnalysis,
    summary_response: InferenceResponse | None,
) -> list[str]:
    caveats: list[str] = []
    if summary_response is not None:
        if summary_response.degraded:
            caveats.append("Local inference was unavailable; deterministic summary was used.")
        if summary_response.failure_reason:
            caveats.append(summary_response.failure_reason)
    if analysis.alignment_check is not None and analysis.alignment_check.warning is not None:
        caveats.append(analysis.alignment_check.warning)
    return caveats


def build_report_finding(finding: AnalysisFinding) -> ReportFinding:
    signal = classify_finding_signal(
        theme_key=finding.theme_key,
        severity=finding.severity,
        comment_count=finding.comment_count,
        issue_marker_count=finding.issue_marker_count,
        request_marker_count=finding.request_marker_count,
    )
    report_finding = ReportFinding(
        theme_key=finding.theme_key,
        theme_label=finding.theme_label,
        severity=finding.severity,
        comment_count=finding.comment_count,
        issue_marker_count=finding.issue_marker_count,
        request_marker_count=finding.request_marker_count,
        signal_type=signal.signal_type,
        recommended_owner=signal.recommended_owner,
        action_type=signal.action_type,
        key_phrases=finding.key_phrases,
        evidence_comment_ids=finding.evidence_comment_ids,
        quotes=finding.quotes,
    )
    validate_report_finding_semantics(report_finding)
    return report_finding


def calculate_sha256_path(path: str) -> str:
    from pathlib import Path

    return calculate_sha256(Path(path))


def _default_priority(severity: str | None) -> str:
    if severity in {"high", "medium", "low"}:
        return severity
    return "low"


def _default_confidence(finding: object | None) -> float:
    if finding is None:
        return 0.4
    severity = getattr(finding, "severity", "low")
    comment_count = getattr(finding, "comment_count", 0)
    base = {"high": 0.85, "medium": 0.7, "low": 0.55}.get(severity, 0.5)
    if comment_count >= 3:
        base += 0.1
    elif comment_count == 2:
        base += 0.05
    return min(base, 0.95)


def _default_why_now(finding: object | None) -> str:
    if finding is None:
        return (
            "The thread did not produce enough structured evidence for a stronger recommendation."
        )
    return (
        f"{getattr(finding, 'theme_label').title()} is the strongest evidence cluster by severity "
        "and supporting comments in this thread."
    )


def _default_expected_outcome(finding: object | None) -> str:
    if finding is None:
        return "Clarify whether the thread contains a stable action signal."
    return (
        f"Reduce the most visible {getattr(finding, 'theme_label')} friction raised in the thread."
    )
