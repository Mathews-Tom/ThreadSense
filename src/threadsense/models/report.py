from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from threadsense.action_signals import classify_finding_signal
from threadsense.errors import SchemaBoundaryError
from threadsense.models.analysis import (
    AnalysisFinding,
    ConversationStructure,
    RepresentativeQuote,
    conversation_structure_from_dict,
    load_analysis_artifact_file,
    quote_from_dict,
)

REPORT_SCHEMA_VERSION = 3
REPORT_ENGINE_VERSION = "report-v1.3"
REPORT_ARTIFACT_KIND = "thread_report"


@dataclass(frozen=True)
class ReportExecutiveSummary:
    headline: str
    summary: str
    priority: str
    confidence: float
    why_now: str
    cited_theme_keys: list[str]
    cited_comment_ids: list[str]
    next_steps: list[str]
    recommended_owner: str
    action_type: str
    expected_outcome: str
    provider: str
    degraded: bool


@dataclass(frozen=True)
class ReportFinding:
    theme_key: str
    theme_label: str
    severity: str
    comment_count: int
    issue_marker_count: int
    request_marker_count: int
    signal_type: str
    recommended_owner: str
    action_type: str
    key_phrases: list[str]
    evidence_comment_ids: list[str]
    quotes: list[RepresentativeQuote]


@dataclass(frozen=True)
class ReportQualityCheck:
    code: str
    level: str
    message: str


@dataclass(frozen=True)
class ReportProvenance:
    analysis_artifact_path: str
    analysis_sha256: str
    generated_at_utc: float
    schema_version: int
    report_version: str
    summary_provider: str


@dataclass(frozen=True)
class ThreadReport:
    thread_id: str
    source_name: str
    title: str
    top_phrases: list[str]
    executive_summary: ReportExecutiveSummary
    conversation_structure: ConversationStructure
    findings: list[ReportFinding]
    caveats: list[str]
    quality_checks: list[ReportQualityCheck]
    provenance: ReportProvenance

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_kind": REPORT_ARTIFACT_KIND,
            "schema_version": REPORT_SCHEMA_VERSION,
            "report_version": REPORT_ENGINE_VERSION,
            "report": asdict(self),
        }


def load_report_artifact_file(path: Path) -> ThreadReport:
    payload = migrate_report_payload(read_json_file(path))
    report_data = nested_object(payload, "report")
    summary_data = nested_object(report_data, "executive_summary")
    findings_data = nested_list(report_data, "findings")
    quality_data = nested_list(report_data, "quality_checks")
    provenance_data = nested_object(report_data, "provenance")
    conversation_data = optional_nested_object(report_data, "conversation_structure")
    return ThreadReport(
        thread_id=required_str(report_data, "thread_id"),
        source_name=required_str(report_data, "source_name"),
        title=required_str(report_data, "title"),
        top_phrases=required_str_list(report_data, "top_phrases"),
        executive_summary=ReportExecutiveSummary(
            headline=required_str(summary_data, "headline"),
            summary=required_str(summary_data, "summary"),
            priority=required_str(summary_data, "priority"),
            confidence=required_float(summary_data, "confidence"),
            why_now=required_str(summary_data, "why_now"),
            cited_theme_keys=required_str_list(summary_data, "cited_theme_keys"),
            cited_comment_ids=required_str_list(summary_data, "cited_comment_ids"),
            next_steps=required_str_list(summary_data, "next_steps"),
            recommended_owner=required_str(summary_data, "recommended_owner"),
            action_type=required_str(summary_data, "action_type"),
            expected_outcome=required_str(summary_data, "expected_outcome"),
            provider=required_str(summary_data, "provider"),
            degraded=required_bool(summary_data, "degraded"),
        ),
        conversation_structure=conversation_structure_from_dict(conversation_data),
        findings=[finding_from_dict(item) for item in findings_data],
        caveats=required_str_list(report_data, "caveats"),
        quality_checks=[quality_check_from_dict(item) for item in quality_data],
        provenance=ReportProvenance(
            analysis_artifact_path=required_str(provenance_data, "analysis_artifact_path"),
            analysis_sha256=required_str(provenance_data, "analysis_sha256"),
            generated_at_utc=required_float(provenance_data, "generated_at_utc"),
            schema_version=required_int(provenance_data, "schema_version"),
            report_version=required_str(provenance_data, "report_version"),
            summary_provider=required_str(provenance_data, "summary_provider"),
        ),
    )


def migrate_report_payload(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    artifact_kind = payload.get("artifact_kind")
    schema_version = payload.get("schema_version")
    if artifact_kind != REPORT_ARTIFACT_KIND:
        raise SchemaBoundaryError(
            "report artifact kind is invalid",
            details={"artifact_kind": artifact_kind},
        )
    if schema_version == REPORT_SCHEMA_VERSION:
        return payload
    if schema_version == 2:
        return migrate_report_v2_to_v3(payload)
    if schema_version == 1:
        return migrate_report_v2_to_v3(migrate_report_v1_to_v2(payload))
    raise SchemaBoundaryError(
        "report schema version is unsupported",
        details={"schema_version": schema_version, "supported": [REPORT_SCHEMA_VERSION]},
    )


def migrate_report_v2_to_v3(payload: Mapping[str, Any]) -> dict[str, Any]:
    report_data = nested_object(payload, "report")
    findings = nested_list(report_data, "findings")
    provenance = nested_object(report_data, "provenance")
    analysis = load_migration_analysis(provenance)
    analysis_findings = {finding.theme_key: finding for finding in analysis.findings}
    first_signal = _migrated_signal_for_report_lead(findings, analysis_findings)
    return {
        "artifact_kind": REPORT_ARTIFACT_KIND,
        "schema_version": REPORT_SCHEMA_VERSION,
        "report_version": REPORT_ENGINE_VERSION,
        "report": {
            **dict(report_data),
            "executive_summary": {
                **dict(nested_object(report_data, "executive_summary")),
                "recommended_owner": first_signal.recommended_owner,
                "action_type": first_signal.action_type,
            },
            "findings": [
                migrate_report_finding_to_v3(finding, analysis_findings) for finding in findings
            ],
            "provenance": {
                **dict(provenance),
                "schema_version": REPORT_SCHEMA_VERSION,
                "report_version": REPORT_ENGINE_VERSION,
            },
        },
    }


def migrate_report_v1_to_v2(payload: Mapping[str, Any]) -> dict[str, Any]:
    report_data = nested_object(payload, "report")
    summary_data = nested_object(report_data, "executive_summary")
    findings_data = nested_list(report_data, "findings")
    provenance = nested_object(report_data, "provenance")
    analysis = load_migration_analysis(provenance)
    analysis_findings = {finding.theme_key: finding for finding in analysis.findings}
    first_finding = _report_lead_analysis_finding(findings_data, analysis_findings)
    priority = _migrated_priority(first_finding)
    first_signal = _migrated_signal(first_finding)
    return {
        "artifact_kind": REPORT_ARTIFACT_KIND,
        "schema_version": 2,
        "report_version": "report-v1.2",
        "report": {
            **dict(report_data),
            "executive_summary": {
                **dict(summary_data),
                "priority": priority,
                "confidence": _migrated_confidence(first_finding),
                "why_now": _migrated_why_now(first_finding),
                "recommended_owner": first_signal.recommended_owner,
                "action_type": first_signal.action_type,
                "expected_outcome": _migrated_expected_outcome(first_finding),
            },
            "provenance": {
                **dict(provenance),
                "schema_version": 2,
                "report_version": "report-v1.2",
            },
        },
    }


def _migrated_priority(first_finding: Any) -> str:
    severity = _finding_value(first_finding, "severity")
    if severity in {"high", "medium", "low"}:
        return str(severity)
    return "low"


def _migrated_confidence(first_finding: Any) -> float:
    if first_finding is None:
        return 0.4
    severity = _finding_value(first_finding, "severity") or "low"
    comment_count = _finding_value(first_finding, "comment_count") or 0
    if not isinstance(comment_count, int):
        comment_count = 0
    base = {"high": 0.85, "medium": 0.7, "low": 0.55}.get(severity, 0.5)
    if comment_count >= 3:
        base += 0.1
    elif comment_count == 2:
        base += 0.05
    return min(base, 0.95)


def _migrated_why_now(first_finding: Any) -> str:
    if first_finding is None:
        return "The report was migrated from an older summary contract without a stronger urgency field."
    theme_label = str(_finding_value(first_finding, "theme_label") or "the lead finding").title()
    return f"{theme_label} was the strongest evidence cluster in the original report artifact."


def _migrated_expected_outcome(first_finding: Any) -> str:
    if first_finding is None:
        return "Preserve report compatibility while the richer contract rolls out."
    theme_label = str(_finding_value(first_finding, "theme_label") or "reported").lower()
    return f"Reduce the most visible {theme_label} friction captured in the original report."


def finding_from_dict(payload: Mapping[str, Any]) -> ReportFinding:
    quotes_data = nested_list(payload, "quotes")
    finding = ReportFinding(
        theme_key=required_str(payload, "theme_key"),
        theme_label=required_str(payload, "theme_label"),
        severity=required_str(payload, "severity"),
        comment_count=required_nonnegative_int(payload, "comment_count"),
        issue_marker_count=required_nonnegative_int(payload, "issue_marker_count"),
        request_marker_count=required_nonnegative_int(payload, "request_marker_count"),
        signal_type=required_choice(
            payload,
            "signal_type",
            {"discussion", "issue", "mixed", "request"},
        ),
        recommended_owner=required_choice(
            payload,
            "recommended_owner",
            {"docs", "engineering", "product", "research"},
        ),
        action_type=required_choice(
            payload,
            "action_type",
            {"design", "document", "fix", "investigate", "monitor"},
        ),
        key_phrases=required_str_list(payload, "key_phrases"),
        evidence_comment_ids=required_str_list(payload, "evidence_comment_ids"),
        quotes=[quote_from_dict(item) for item in quotes_data],
    )
    validate_report_finding_semantics(finding)
    return finding


def migrate_report_finding_to_v3(
    payload: Mapping[str, Any],
    analysis_findings: dict[str, AnalysisFinding],
) -> dict[str, Any]:
    theme_key = required_str(payload, "theme_key")
    analysis_finding = analysis_findings.get(theme_key)
    if analysis_finding is None:
        raise SchemaBoundaryError(
            "report finding is missing source analysis evidence during migration",
            details={"theme_key": theme_key},
        )
    if (
        required_str(payload, "theme_label") != analysis_finding.theme_label
        or required_str(payload, "severity") != analysis_finding.severity
        or required_nonnegative_int(payload, "comment_count") != analysis_finding.comment_count
        or required_str_list(payload, "key_phrases") != analysis_finding.key_phrases
        or required_str_list(payload, "evidence_comment_ids")
        != analysis_finding.evidence_comment_ids
    ):
        raise SchemaBoundaryError(
            "report finding does not match source analysis during migration",
            details={"theme_key": theme_key},
        )
    signal = classify_finding_signal(
        theme_key=analysis_finding.theme_key,
        severity=analysis_finding.severity,
        comment_count=analysis_finding.comment_count,
        issue_marker_count=analysis_finding.issue_marker_count,
        request_marker_count=analysis_finding.request_marker_count,
    )
    return {
        "theme_key": analysis_finding.theme_key,
        "theme_label": analysis_finding.theme_label,
        "severity": analysis_finding.severity,
        "comment_count": analysis_finding.comment_count,
        "issue_marker_count": analysis_finding.issue_marker_count,
        "request_marker_count": analysis_finding.request_marker_count,
        "signal_type": signal.signal_type,
        "recommended_owner": signal.recommended_owner,
        "action_type": signal.action_type,
        "key_phrases": analysis_finding.key_phrases,
        "evidence_comment_ids": analysis_finding.evidence_comment_ids,
        "quotes": [
            {
                "comment_id": quote.comment_id,
                "permalink": quote.permalink,
                "author": quote.author,
                "body_excerpt": quote.body_excerpt,
                "score": quote.score,
            }
            for quote in analysis_finding.quotes
        ],
    }


def quality_check_from_dict(payload: Mapping[str, Any]) -> ReportQualityCheck:
    return ReportQualityCheck(
        code=required_str(payload, "code"),
        level=required_str(payload, "level"),
        message=required_str(payload, "message"),
    )


def validate_report_finding_semantics(finding: ReportFinding) -> None:
    if (
        finding.comment_count < 0
        or finding.issue_marker_count < 0
        or finding.request_marker_count < 0
    ):
        raise SchemaBoundaryError(
            "report finding counts must be nonnegative",
            details={"theme_key": finding.theme_key},
        )
    if finding.signal_type == "discussion" and (
        finding.issue_marker_count != 0 or finding.request_marker_count != 0
    ):
        raise SchemaBoundaryError(
            "report finding signal_type is inconsistent with marker counts",
            details={"theme_key": finding.theme_key, "signal_type": finding.signal_type},
        )
    if finding.signal_type == "issue" and not (
        finding.issue_marker_count > 0 and finding.request_marker_count == 0
    ):
        raise SchemaBoundaryError(
            "report finding signal_type is inconsistent with marker counts",
            details={"theme_key": finding.theme_key, "signal_type": finding.signal_type},
        )
    if finding.signal_type == "request" and not (
        finding.request_marker_count > 0 and finding.issue_marker_count == 0
    ):
        raise SchemaBoundaryError(
            "report finding signal_type is inconsistent with marker counts",
            details={"theme_key": finding.theme_key, "signal_type": finding.signal_type},
        )
    if finding.signal_type == "mixed" and not (
        finding.request_marker_count > 0 and finding.issue_marker_count > 0
    ):
        raise SchemaBoundaryError(
            "report finding signal_type is inconsistent with marker counts",
            details={"theme_key": finding.theme_key, "signal_type": finding.signal_type},
        )
    valid_owner_actions = {
        "docs": {"document"},
        "engineering": {"fix", "investigate"},
        "product": {"design"},
        "research": {"investigate", "monitor"},
    }
    if finding.action_type not in valid_owner_actions.get(finding.recommended_owner, set()):
        raise SchemaBoundaryError(
            "report finding owner/action pairing is invalid",
            details={
                "theme_key": finding.theme_key,
                "recommended_owner": finding.recommended_owner,
                "action_type": finding.action_type,
            },
        )
    try:
        expected_signal = classify_finding_signal(
            theme_key=finding.theme_key,
            severity=finding.severity,
            comment_count=finding.comment_count,
            issue_marker_count=finding.issue_marker_count,
            request_marker_count=finding.request_marker_count,
        )
    except ValueError as error:
        raise SchemaBoundaryError(
            "report finding deterministic action signal is invalid",
            details={"theme_key": finding.theme_key},
        ) from error
    if (
        finding.signal_type != expected_signal.signal_type
        or finding.recommended_owner != expected_signal.recommended_owner
        or finding.action_type != expected_signal.action_type
    ):
        raise SchemaBoundaryError(
            "report finding deterministic action signal is inconsistent",
            details={
                "theme_key": finding.theme_key,
                "signal_type": finding.signal_type,
                "recommended_owner": finding.recommended_owner,
                "action_type": finding.action_type,
            },
        )


def read_json_file(path: Path) -> dict[str, Any]:
    import json

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise SchemaBoundaryError(
            "report artifact path does not exist",
            details={"path": str(path)},
        ) from error
    if not isinstance(payload, dict):
        raise SchemaBoundaryError("report artifact must decode to an object")
    return payload


def nested_object(payload: Mapping[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise SchemaBoundaryError("report object field is invalid", details={"key": key})
    return value


def nested_list(payload: Mapping[str, Any], key: str) -> list[Any]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise SchemaBoundaryError("report list field is invalid", details={"key": key})
    return value


def optional_nested_object(payload: Mapping[str, Any], key: str) -> dict[str, Any] | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, dict):
        raise SchemaBoundaryError("report object field is invalid", details={"key": key})
    return value


def required_str(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise SchemaBoundaryError("report string field is invalid", details={"key": key})
    return value


def required_str_list(payload: Mapping[str, Any], key: str) -> list[str]:
    value = payload.get(key)
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        raise SchemaBoundaryError("report string list field is invalid", details={"key": key})
    return value


def required_int(payload: Mapping[str, Any], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int):
        raise SchemaBoundaryError("report integer field is invalid", details={"key": key})
    return value


def required_float(payload: Mapping[str, Any], key: str) -> float:
    value = payload.get(key)
    if isinstance(value, int):
        return float(value)
    if not isinstance(value, float):
        raise SchemaBoundaryError("report float field is invalid", details={"key": key})
    return value


def required_bool(payload: Mapping[str, Any], key: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise SchemaBoundaryError("report boolean field is invalid", details={"key": key})
    return value


def required_nonnegative_int(payload: Mapping[str, Any], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int):
        raise SchemaBoundaryError("report integer field is invalid", details={"key": key})
    if value < 0:
        raise SchemaBoundaryError("report integer field is invalid", details={"key": key})
    return value


def required_choice(payload: Mapping[str, Any], key: str, choices: set[str]) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise SchemaBoundaryError("report string field is invalid", details={"key": key})
    if value not in choices:
        raise SchemaBoundaryError(
            "report string field is invalid",
            details={"key": key, "choices": sorted(choices)},
        )
    return value


def calculate_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def load_migration_analysis(provenance: Mapping[str, Any]):
    analysis_path = Path(required_str(provenance, "analysis_artifact_path"))
    expected_sha256 = required_str(provenance, "analysis_sha256")
    actual_sha256 = calculate_sha256(analysis_path)
    if actual_sha256 != expected_sha256:
        raise SchemaBoundaryError(
            "report migration source analysis hash mismatch",
            details={
                "analysis_artifact_path": str(analysis_path),
                "expected_sha256": expected_sha256,
                "actual_sha256": actual_sha256,
            },
        )
    return load_analysis_artifact_file(analysis_path)


def _migrated_signal(first_finding: AnalysisFinding | None):
    if first_finding is None:
        raise SchemaBoundaryError(
            "report migration requires at least one source analysis finding",
        )
    return classify_finding_signal(
        theme_key=first_finding.theme_key,
        severity=first_finding.severity,
        comment_count=first_finding.comment_count,
        issue_marker_count=first_finding.issue_marker_count,
        request_marker_count=first_finding.request_marker_count,
    )


def _migrated_signal_for_report_lead(
    findings_data: list[Any],
    analysis_findings: dict[str, AnalysisFinding],
):
    return _migrated_signal(_report_lead_analysis_finding(findings_data, analysis_findings))


def _report_lead_analysis_finding(
    findings_data: list[Any],
    analysis_findings: dict[str, AnalysisFinding],
) -> AnalysisFinding | None:
    if not findings_data:
        return None
    lead_finding = findings_data[0]
    if not isinstance(lead_finding, dict):
        raise SchemaBoundaryError("report finding is invalid during migration")
    theme_key = required_str(lead_finding, "theme_key")
    analysis_finding = analysis_findings.get(theme_key)
    if analysis_finding is None:
        raise SchemaBoundaryError(
            "report lead finding is missing source analysis evidence during migration",
            details={"theme_key": theme_key},
        )
    return analysis_finding


def _finding_value(first_finding: Any, key: str) -> Any:
    if isinstance(first_finding, dict):
        return first_finding.get(key)
    return getattr(first_finding, key, None)
