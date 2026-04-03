from __future__ import annotations

from time import time

from threadsense.inference.contracts import InferenceResponse
from threadsense.models.analysis import ThreadAnalysis
from threadsense.models.report import (
    REPORT_ENGINE_VERSION,
    REPORT_SCHEMA_VERSION,
    ReportExecutiveSummary,
    ReportFinding,
    ReportProvenance,
    ThreadReport,
)
from threadsense.pipeline.storage import calculate_sha256
from threadsense.reporting.quality import run_quality_checks


def build_thread_report(
    analysis: ThreadAnalysis,
    analysis_artifact_path: str,
    summary_response: InferenceResponse | None,
) -> ThreadReport:
    summary = build_executive_summary(analysis, summary_response)
    caveats = build_caveats(summary_response)
    report = ThreadReport(
        thread_id=analysis.thread_id,
        source_name=analysis.source_name,
        title=analysis.title,
        top_phrases=analysis.top_phrases[:8],
        executive_summary=summary,
        findings=[
            ReportFinding(
                theme_key=finding.theme_key,
                theme_label=finding.theme_label,
                severity=finding.severity,
                comment_count=finding.comment_count,
                key_phrases=finding.key_phrases,
                evidence_comment_ids=finding.evidence_comment_ids,
                quotes=finding.quotes,
            )
            for finding in analysis.findings
        ],
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
    return ThreadReport(
        thread_id=report.thread_id,
        source_name=report.source_name,
        title=report.title,
        top_phrases=report.top_phrases,
        executive_summary=report.executive_summary,
        findings=report.findings,
        caveats=report.caveats,
        quality_checks=quality_checks,
        provenance=report.provenance,
    )


def build_executive_summary(
    analysis: ThreadAnalysis,
    summary_response: InferenceResponse | None,
) -> ReportExecutiveSummary:
    if summary_response is None:
        first_finding = analysis.findings[0] if analysis.findings else None
        return ReportExecutiveSummary(
            headline=(
                f"{first_finding.theme_label.title()} leads the thread"
                if first_finding is not None
                else f"Deterministic report for {analysis.title}"
            ),
            summary=(
                f"Top themes: {', '.join(f.theme_key for f in analysis.findings[:3])}."
                if analysis.findings
                else "No findings were available from deterministic analysis."
            ),
            cited_theme_keys=[finding.theme_key for finding in analysis.findings[:3]],
            cited_comment_ids=(
                analysis.findings[0].evidence_comment_ids[:5] if analysis.findings else []
            ),
            next_steps=[
                f"Review {finding.theme_key} evidence group" for finding in analysis.findings[:3]
            ],
            provider="deterministic_report",
            degraded=False,
        )

    output = summary_response.output
    return ReportExecutiveSummary(
        headline=str(output["headline"]),
        summary=str(output["summary"]),
        cited_theme_keys=list(output["cited_theme_keys"]),
        cited_comment_ids=list(output["cited_comment_ids"]),
        next_steps=list(output["next_steps"]),
        provider=summary_response.provider,
        degraded=summary_response.degraded,
    )


def build_caveats(summary_response: InferenceResponse | None) -> list[str]:
    if summary_response is None:
        return []
    caveats: list[str] = []
    if summary_response.degraded:
        caveats.append("Local inference was unavailable; deterministic summary was used.")
    if summary_response.failure_reason:
        caveats.append(summary_response.failure_reason)
    return caveats


def calculate_sha256_path(path: str) -> str:
    from pathlib import Path

    return calculate_sha256(Path(path))
