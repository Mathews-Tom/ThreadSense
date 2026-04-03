from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from threadsense.errors import SchemaBoundaryError
from threadsense.models.analysis import RepresentativeQuote, quote_from_dict

REPORT_SCHEMA_VERSION = 1
REPORT_ENGINE_VERSION = "report-v1"
REPORT_ARTIFACT_KIND = "thread_report"


@dataclass(frozen=True)
class ReportExecutiveSummary:
    headline: str
    summary: str
    cited_theme_keys: list[str]
    cited_comment_ids: list[str]
    next_steps: list[str]
    provider: str
    degraded: bool


@dataclass(frozen=True)
class ReportFinding:
    theme_key: str
    theme_label: str
    severity: str
    comment_count: int
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
    return ThreadReport(
        thread_id=required_str(report_data, "thread_id"),
        source_name=required_str(report_data, "source_name"),
        title=required_str(report_data, "title"),
        top_phrases=required_str_list(report_data, "top_phrases"),
        executive_summary=ReportExecutiveSummary(
            headline=required_str(summary_data, "headline"),
            summary=required_str(summary_data, "summary"),
            cited_theme_keys=required_str_list(summary_data, "cited_theme_keys"),
            cited_comment_ids=required_str_list(summary_data, "cited_comment_ids"),
            next_steps=required_str_list(summary_data, "next_steps"),
            provider=required_str(summary_data, "provider"),
            degraded=required_bool(summary_data, "degraded"),
        ),
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
    raise SchemaBoundaryError(
        "report schema version is unsupported",
        details={"schema_version": schema_version, "supported": [REPORT_SCHEMA_VERSION]},
    )


def finding_from_dict(payload: Mapping[str, Any]) -> ReportFinding:
    quotes_data = nested_list(payload, "quotes")
    return ReportFinding(
        theme_key=required_str(payload, "theme_key"),
        theme_label=required_str(payload, "theme_label"),
        severity=required_str(payload, "severity"),
        comment_count=required_int(payload, "comment_count"),
        key_phrases=required_str_list(payload, "key_phrases"),
        evidence_comment_ids=required_str_list(payload, "evidence_comment_ids"),
        quotes=[quote_from_dict(item) for item in quotes_data],
    )


def quality_check_from_dict(payload: Mapping[str, Any]) -> ReportQualityCheck:
    return ReportQualityCheck(
        code=required_str(payload, "code"),
        level=required_str(payload, "level"),
        message=required_str(payload, "message"),
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
