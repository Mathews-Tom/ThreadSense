from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from threadsense.errors import AnalysisBoundaryError

ANALYSIS_SCHEMA_VERSION = 1
ANALYSIS_ENGINE_VERSION = "deterministic-v1"
ANALYSIS_ARTIFACT_KIND = "thread_analysis"


@dataclass(frozen=True)
class RepresentativeQuote:
    comment_id: str
    permalink: str
    author: str
    body_excerpt: str
    score: int


@dataclass(frozen=True)
class DuplicateGroup:
    canonical_text: str
    comment_ids: list[str]
    count: int


@dataclass(frozen=True)
class AnalysisFinding:
    theme_key: str
    theme_label: str
    severity: str
    comment_count: int
    issue_marker_count: int
    request_marker_count: int
    key_phrases: list[str]
    evidence_comment_ids: list[str]
    quotes: list[RepresentativeQuote]


@dataclass(frozen=True)
class AnalysisProvenance:
    normalized_artifact_path: str
    normalized_sha256: str
    source_thread_id: str
    analyzed_at_utc: float
    schema_version: int
    analysis_version: str


@dataclass(frozen=True)
class ThreadAnalysis:
    thread_id: str
    source_name: str
    title: str
    total_comments: int
    distinct_comment_count: int
    duplicate_group_count: int
    top_phrases: list[str]
    findings: list[AnalysisFinding]
    duplicate_groups: list[DuplicateGroup]
    top_quotes: list[RepresentativeQuote]
    provenance: AnalysisProvenance

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_kind": ANALYSIS_ARTIFACT_KIND,
            "schema_version": ANALYSIS_SCHEMA_VERSION,
            "analysis_version": ANALYSIS_ENGINE_VERSION,
            "analysis": asdict(self),
        }


def load_analysis_artifact_file(path: Path) -> ThreadAnalysis:
    payload = migrate_analysis_payload(read_json_file(path))
    analysis_data = nested_object(payload, "analysis")
    findings_data = nested_list(analysis_data, "findings")
    duplicates_data = nested_list(analysis_data, "duplicate_groups")
    top_quotes_data = nested_list(analysis_data, "top_quotes")
    provenance_data = nested_object(analysis_data, "provenance")
    return ThreadAnalysis(
        thread_id=required_str(analysis_data, "thread_id"),
        source_name=required_str(analysis_data, "source_name"),
        title=required_str(analysis_data, "title"),
        total_comments=required_int(analysis_data, "total_comments"),
        distinct_comment_count=required_int(analysis_data, "distinct_comment_count"),
        duplicate_group_count=required_int(analysis_data, "duplicate_group_count"),
        top_phrases=required_str_list(analysis_data, "top_phrases"),
        findings=[finding_from_dict(item) for item in findings_data],
        duplicate_groups=[duplicate_group_from_dict(item) for item in duplicates_data],
        top_quotes=[quote_from_dict(item) for item in top_quotes_data],
        provenance=AnalysisProvenance(
            normalized_artifact_path=required_str(provenance_data, "normalized_artifact_path"),
            normalized_sha256=required_str(provenance_data, "normalized_sha256"),
            source_thread_id=required_str(provenance_data, "source_thread_id"),
            analyzed_at_utc=required_float(provenance_data, "analyzed_at_utc"),
            schema_version=required_int(provenance_data, "schema_version"),
            analysis_version=required_str(provenance_data, "analysis_version"),
        ),
    )


def migrate_analysis_payload(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    artifact_kind = payload.get("artifact_kind")
    schema_version = payload.get("schema_version")
    if artifact_kind != ANALYSIS_ARTIFACT_KIND:
        raise AnalysisBoundaryError(
            "analysis artifact kind is invalid",
            details={"artifact_kind": artifact_kind},
        )
    if schema_version == ANALYSIS_SCHEMA_VERSION:
        return payload
    raise AnalysisBoundaryError(
        "analysis schema version is unsupported",
        details={"schema_version": schema_version, "supported": [ANALYSIS_SCHEMA_VERSION]},
    )


def finding_from_dict(payload: Mapping[str, Any]) -> AnalysisFinding:
    quotes_data = nested_list(payload, "quotes")
    return AnalysisFinding(
        theme_key=required_str(payload, "theme_key"),
        theme_label=required_str(payload, "theme_label"),
        severity=required_str(payload, "severity"),
        comment_count=required_int(payload, "comment_count"),
        issue_marker_count=required_int(payload, "issue_marker_count"),
        request_marker_count=required_int(payload, "request_marker_count"),
        key_phrases=required_str_list(payload, "key_phrases"),
        evidence_comment_ids=required_str_list(payload, "evidence_comment_ids"),
        quotes=[quote_from_dict(item) for item in quotes_data],
    )


def duplicate_group_from_dict(payload: Mapping[str, Any]) -> DuplicateGroup:
    return DuplicateGroup(
        canonical_text=required_str(payload, "canonical_text"),
        comment_ids=required_str_list(payload, "comment_ids"),
        count=required_int(payload, "count"),
    )


def quote_from_dict(payload: Mapping[str, Any]) -> RepresentativeQuote:
    return RepresentativeQuote(
        comment_id=required_str(payload, "comment_id"),
        permalink=required_str(payload, "permalink"),
        author=required_str(payload, "author"),
        body_excerpt=required_str(payload, "body_excerpt"),
        score=required_int(payload, "score"),
    )


def read_json_file(path: Path) -> dict[str, Any]:
    import json

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise AnalysisBoundaryError(
            "analysis artifact path does not exist",
            details={"path": str(path)},
        ) from error
    if not isinstance(payload, dict):
        raise AnalysisBoundaryError("analysis artifact must decode to an object")
    return payload


def nested_object(payload: Mapping[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise AnalysisBoundaryError(
            "analysis object field is invalid",
            details={"key": key},
        )
    return value


def nested_list(payload: Mapping[str, Any], key: str) -> list[Any]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise AnalysisBoundaryError(
            "analysis list field is invalid",
            details={"key": key},
        )
    return value


def required_str(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise AnalysisBoundaryError(
            "analysis string field is invalid",
            details={"key": key},
        )
    return value


def required_str_list(payload: Mapping[str, Any], key: str) -> list[str]:
    value = payload.get(key)
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        raise AnalysisBoundaryError(
            "analysis string list field is invalid",
            details={"key": key},
        )
    return value


def required_int(payload: Mapping[str, Any], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int):
        raise AnalysisBoundaryError(
            "analysis integer field is invalid",
            details={"key": key},
        )
    return value


def required_float(payload: Mapping[str, Any], key: str) -> float:
    value = payload.get(key)
    if isinstance(value, int):
        return float(value)
    if not isinstance(value, float):
        raise AnalysisBoundaryError(
            "analysis float field is invalid",
            details={"key": key},
        )
    return value
