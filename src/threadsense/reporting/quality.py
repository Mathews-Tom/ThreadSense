from __future__ import annotations

from threadsense.models.report import ReportQualityCheck, ThreadReport


def run_quality_checks(report: ThreadReport) -> list[ReportQualityCheck]:
    checks: list[ReportQualityCheck] = []
    checks.extend(check_empty_sections(report))
    checks.extend(check_summary_citations(report))
    checks.extend(check_coverage_gaps(report))
    return checks


def check_empty_sections(report: ThreadReport) -> list[ReportQualityCheck]:
    checks: list[ReportQualityCheck] = []
    if not report.executive_summary.summary.strip():
        checks.append(
            ReportQualityCheck(
                code="empty_summary",
                level="error",
                message="Executive summary is empty.",
            )
        )
    if not report.findings:
        checks.append(
            ReportQualityCheck(
                code="empty_findings",
                level="error",
                message="Report findings are empty.",
            )
        )
    return checks


def check_summary_citations(report: ThreadReport) -> list[ReportQualityCheck]:
    checks: list[ReportQualityCheck] = []
    known_theme_keys = {finding.theme_key for finding in report.findings}
    known_comment_ids = {
        comment_id for finding in report.findings for comment_id in finding.evidence_comment_ids
    }
    unknown_themes = [
        key for key in report.executive_summary.cited_theme_keys if key not in known_theme_keys
    ]
    unknown_comment_ids = [
        comment_id
        for comment_id in report.executive_summary.cited_comment_ids
        if comment_id not in known_comment_ids
    ]
    if unknown_themes:
        checks.append(
            ReportQualityCheck(
                code="unknown_theme_citation",
                level="error",
                message=f"Executive summary cites unknown theme keys: {', '.join(unknown_themes)}.",
            )
        )
    if unknown_comment_ids:
        checks.append(
            ReportQualityCheck(
                code="unknown_comment_citation",
                level="error",
                message="Executive summary cites comment ids not present in report findings.",
            )
        )
    if not report.executive_summary.cited_theme_keys:
        checks.append(
            ReportQualityCheck(
                code="missing_theme_citations",
                level="warning",
                message="Executive summary does not cite any theme keys.",
            )
        )
    return checks


def check_coverage_gaps(report: ThreadReport) -> list[ReportQualityCheck]:
    cited_theme_keys = set(report.executive_summary.cited_theme_keys)
    uncovered = [
        finding.theme_key
        for finding in report.findings
        if finding.theme_key not in cited_theme_keys
    ]
    if not uncovered:
        return []
    return [
        ReportQualityCheck(
            code="coverage_gap",
            level="warning",
            message=f"Executive summary does not reference: {', '.join(uncovered)}.",
        )
    ]
