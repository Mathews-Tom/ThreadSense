from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from threadsense.contracts import DomainType
from threadsense.errors import SchemaBoundaryError
from threadsense.models.analysis import RepresentativeQuote, quote_from_dict

CORPUS_SCHEMA_VERSION = 1
CORPUS_ENGINE_VERSION = "corpus-v1"
CORPUS_MANIFEST_ARTIFACT_KIND = "corpus_manifest"
CORPUS_ANALYSIS_ARTIFACT_KIND = "corpus_analysis"


class TrendPeriod(StrEnum):
    MONTH = "month"
    WEEK = "week"


@dataclass(frozen=True)
class CorpusManifest:
    corpus_id: str
    name: str
    description: str
    source_filter: str | None
    domain: DomainType
    created_at_utc: float
    thread_ids: list[str]
    analysis_artifact_paths: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_kind": CORPUS_MANIFEST_ARTIFACT_KIND,
            "schema_version": CORPUS_SCHEMA_VERSION,
            "corpus_version": CORPUS_ENGINE_VERSION,
            "manifest": {
                **asdict(self),
                "domain": self.domain.value,
            },
        }


@dataclass(frozen=True)
class CrossThreadEvidence:
    thread_id: str
    thread_title: str
    finding_severity: str
    comment_count: int
    top_quote: RepresentativeQuote


@dataclass(frozen=True)
class CrossThreadFinding:
    theme_key: str
    theme_label: str
    severity: str
    thread_count: int
    total_comment_count: int
    top_evidence: list[CrossThreadEvidence]


@dataclass(frozen=True)
class TemporalTrend:
    theme_key: str
    period: str
    thread_count: int
    severity_distribution: dict[str, int]


@dataclass(frozen=True)
class CorpusProvenance:
    manifest_path: str
    input_analysis_paths: list[str]
    generated_at_utc: float
    schema_version: int
    corpus_version: str


@dataclass(frozen=True)
class CorpusAnalysis:
    corpus_id: str
    name: str
    domain: DomainType
    thread_count: int
    total_comments: int
    cross_thread_findings: list[CrossThreadFinding]
    theme_frequency: dict[str, int]
    temporal_trends: list[TemporalTrend]
    provenance: CorpusProvenance

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["domain"] = self.domain.value
        return {
            "artifact_kind": CORPUS_ANALYSIS_ARTIFACT_KIND,
            "schema_version": CORPUS_SCHEMA_VERSION,
            "corpus_version": CORPUS_ENGINE_VERSION,
            "corpus": payload,
        }


def load_corpus_manifest_file(path: Path) -> CorpusManifest:
    payload = _migrate_corpus_payload(read_json_file(path), CORPUS_MANIFEST_ARTIFACT_KIND)
    manifest_data = nested_object(payload, "manifest")
    return CorpusManifest(
        corpus_id=required_str(manifest_data, "corpus_id"),
        name=required_str(manifest_data, "name"),
        description=required_str(manifest_data, "description"),
        source_filter=optional_nullable_str(manifest_data, "source_filter"),
        domain=DomainType(required_str(manifest_data, "domain")),
        created_at_utc=required_float(manifest_data, "created_at_utc"),
        thread_ids=required_str_list(manifest_data, "thread_ids"),
        analysis_artifact_paths=required_str_list(manifest_data, "analysis_artifact_paths"),
    )


def load_corpus_analysis_file(path: Path) -> CorpusAnalysis:
    payload = _migrate_corpus_payload(read_json_file(path), CORPUS_ANALYSIS_ARTIFACT_KIND)
    corpus_data = nested_object(payload, "corpus")
    findings_data = nested_list(corpus_data, "cross_thread_findings")
    trends_data = nested_list(corpus_data, "temporal_trends")
    provenance_data = nested_object(corpus_data, "provenance")
    return CorpusAnalysis(
        corpus_id=required_str(corpus_data, "corpus_id"),
        name=required_str(corpus_data, "name"),
        domain=DomainType(required_str(corpus_data, "domain")),
        thread_count=required_int(corpus_data, "thread_count"),
        total_comments=required_int(corpus_data, "total_comments"),
        cross_thread_findings=[cross_thread_finding_from_dict(item) for item in findings_data],
        theme_frequency=required_int_mapping(corpus_data, "theme_frequency"),
        temporal_trends=[temporal_trend_from_dict(item) for item in trends_data],
        provenance=CorpusProvenance(
            manifest_path=required_str(provenance_data, "manifest_path"),
            input_analysis_paths=required_str_list(provenance_data, "input_analysis_paths"),
            generated_at_utc=required_float(provenance_data, "generated_at_utc"),
            schema_version=required_int(provenance_data, "schema_version"),
            corpus_version=required_str(provenance_data, "corpus_version"),
        ),
    )


def cross_thread_finding_from_dict(payload: Mapping[str, Any]) -> CrossThreadFinding:
    evidence_data = nested_list(payload, "top_evidence")
    return CrossThreadFinding(
        theme_key=required_str(payload, "theme_key"),
        theme_label=required_str(payload, "theme_label"),
        severity=required_str(payload, "severity"),
        thread_count=required_int(payload, "thread_count"),
        total_comment_count=required_int(payload, "total_comment_count"),
        top_evidence=[cross_thread_evidence_from_dict(item) for item in evidence_data],
    )


def cross_thread_evidence_from_dict(payload: Mapping[str, Any]) -> CrossThreadEvidence:
    quote_data = nested_object(payload, "top_quote")
    return CrossThreadEvidence(
        thread_id=required_str(payload, "thread_id"),
        thread_title=required_str(payload, "thread_title"),
        finding_severity=required_str(payload, "finding_severity"),
        comment_count=required_int(payload, "comment_count"),
        top_quote=quote_from_dict(quote_data),
    )


def temporal_trend_from_dict(payload: Mapping[str, Any]) -> TemporalTrend:
    return TemporalTrend(
        theme_key=required_str(payload, "theme_key"),
        period=required_str(payload, "period"),
        thread_count=required_int(payload, "thread_count"),
        severity_distribution=required_int_mapping(payload, "severity_distribution"),
    )


def _migrate_corpus_payload(
    payload: Mapping[str, Any],
    expected_kind: str,
) -> Mapping[str, Any]:
    artifact_kind = payload.get("artifact_kind")
    schema_version = payload.get("schema_version")
    if artifact_kind != expected_kind:
        raise SchemaBoundaryError(
            "corpus artifact kind is invalid",
            details={"artifact_kind": artifact_kind, "expected": expected_kind},
        )
    if schema_version == CORPUS_SCHEMA_VERSION:
        return payload
    raise SchemaBoundaryError(
        "corpus schema version is unsupported",
        details={"schema_version": schema_version, "supported": [CORPUS_SCHEMA_VERSION]},
    )


def read_json_file(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise SchemaBoundaryError(
            "corpus artifact path does not exist",
            details={"path": str(path)},
        ) from error
    if not isinstance(payload, dict):
        raise SchemaBoundaryError("corpus artifact must decode to an object")
    return payload


def nested_object(payload: Mapping[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise SchemaBoundaryError("corpus object field is invalid", details={"key": key})
    return value


def nested_list(payload: Mapping[str, Any], key: str) -> list[Any]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise SchemaBoundaryError("corpus list field is invalid", details={"key": key})
    return value


def required_str(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise SchemaBoundaryError("corpus string field is invalid", details={"key": key})
    return value


def optional_nullable_str(payload: Mapping[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise SchemaBoundaryError("corpus string field is invalid", details={"key": key})
    return value


def required_str_list(payload: Mapping[str, Any], key: str) -> list[str]:
    value = payload.get(key)
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        raise SchemaBoundaryError("corpus string list field is invalid", details={"key": key})
    return value


def required_int(payload: Mapping[str, Any], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int):
        raise SchemaBoundaryError("corpus integer field is invalid", details={"key": key})
    return value


def required_float(payload: Mapping[str, Any], key: str) -> float:
    value = payload.get(key)
    if isinstance(value, int):
        return float(value)
    if not isinstance(value, float):
        raise SchemaBoundaryError("corpus float field is invalid", details={"key": key})
    return value


def required_int_mapping(payload: Mapping[str, Any], key: str) -> dict[str, int]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise SchemaBoundaryError("corpus mapping field is invalid", details={"key": key})
    normalized: dict[str, int] = {}
    for map_key, item in value.items():
        if not isinstance(map_key, str) or not map_key or not isinstance(item, int):
            raise SchemaBoundaryError("corpus mapping field is invalid", details={"key": key})
        normalized[map_key] = item
    return normalized
