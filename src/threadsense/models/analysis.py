from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from threadsense.contracts import (
    ANALYSIS_CONTRACT_SCHEMA_VERSION,
    AnalysisContract,
    default_contract,
)
from threadsense.errors import AnalysisBoundaryError
from threadsense.schema_utils import SchemaReader

_schema = SchemaReader(AnalysisBoundaryError, "analysis")

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
class EngagementSubtree:
    root_comment_id: str
    root_author: str
    subtree_size: int
    max_depth_below: int
    engagement_score: float


@dataclass(frozen=True)
class ConversationStructure:
    max_depth: int
    top_level_count: int
    reply_chain_count: int
    longest_chain_length: int
    controversy_count: int
    consensus_count: int
    monologue_count: int
    top_engagement_subtrees: list[EngagementSubtree]


@dataclass(frozen=True)
class AlignmentCheck:
    domain: str
    domain_fit_score: float
    general_feedback_ratio: float
    suggested_domain: str | None
    warning: str | None


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
    contract: dict[str, str | float]
    contract_schema_version: str


@dataclass(frozen=True)
class ThreadAnalysis:
    thread_id: str
    source_name: str
    title: str
    total_comments: int
    filtered_comment_count: int
    distinct_comment_count: int
    duplicate_group_count: int
    top_phrases: list[str]
    conversation_structure: ConversationStructure
    findings: list[AnalysisFinding]
    duplicate_groups: list[DuplicateGroup]
    top_quotes: list[RepresentativeQuote]
    alignment_check: AlignmentCheck | None
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
    analysis_data = _schema.nested_object(payload, "analysis")
    findings_data = _schema.nested_list(analysis_data, "findings")
    duplicates_data = _schema.nested_list(analysis_data, "duplicate_groups")
    top_quotes_data = _schema.nested_list(analysis_data, "top_quotes")
    provenance_data = _schema.nested_object(analysis_data, "provenance")
    conversation_data = optional_nested_object(analysis_data, "conversation_structure")
    return ThreadAnalysis(
        thread_id=_schema.required_str(analysis_data, "thread_id"),
        source_name=_schema.required_str(analysis_data, "source_name"),
        title=_schema.required_str(analysis_data, "title"),
        total_comments=_schema.required_int(analysis_data, "total_comments"),
        filtered_comment_count=required_int_with_default(analysis_data, "filtered_comment_count"),
        distinct_comment_count=_schema.required_int(analysis_data, "distinct_comment_count"),
        duplicate_group_count=_schema.required_int(analysis_data, "duplicate_group_count"),
        top_phrases=required_str_list(analysis_data, "top_phrases"),
        conversation_structure=conversation_structure_from_dict(conversation_data),
        findings=[finding_from_dict(item) for item in findings_data],
        duplicate_groups=[duplicate_group_from_dict(item) for item in duplicates_data],
        top_quotes=[quote_from_dict(item) for item in top_quotes_data],
        alignment_check=alignment_check_from_dict(analysis_data.get("alignment_check")),
        provenance=AnalysisProvenance(
            normalized_artifact_path=_schema.required_str(
                provenance_data, "normalized_artifact_path"
            ),
            normalized_sha256=_schema.required_str(provenance_data, "normalized_sha256"),
            source_thread_id=_schema.required_str(provenance_data, "source_thread_id"),
            analyzed_at_utc=_schema.required_float(provenance_data, "analyzed_at_utc"),
            schema_version=_schema.required_int(provenance_data, "schema_version"),
            analysis_version=_schema.required_str(provenance_data, "analysis_version"),
            contract=contract_payload_from_dict(provenance_data),
            contract_schema_version=required_str_with_default(
                provenance_data,
                "contract_schema_version",
                ANALYSIS_CONTRACT_SCHEMA_VERSION,
            ),
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
    quotes_data = _schema.nested_list(payload, "quotes")
    return AnalysisFinding(
        theme_key=_schema.required_str(payload, "theme_key"),
        theme_label=_schema.required_str(payload, "theme_label"),
        severity=_schema.required_str(payload, "severity"),
        comment_count=_schema.required_int(payload, "comment_count"),
        issue_marker_count=_schema.required_int(payload, "issue_marker_count"),
        request_marker_count=_schema.required_int(payload, "request_marker_count"),
        key_phrases=required_str_list(payload, "key_phrases"),
        evidence_comment_ids=required_str_list(payload, "evidence_comment_ids"),
        quotes=[quote_from_dict(item) for item in quotes_data],
    )


def duplicate_group_from_dict(payload: Mapping[str, Any]) -> DuplicateGroup:
    return DuplicateGroup(
        canonical_text=_schema.required_str(payload, "canonical_text"),
        comment_ids=required_str_list(payload, "comment_ids"),
        count=_schema.required_int(payload, "count"),
    )


def quote_from_dict(payload: Mapping[str, Any]) -> RepresentativeQuote:
    return RepresentativeQuote(
        comment_id=_schema.required_str(payload, "comment_id"),
        permalink=_schema.required_str(payload, "permalink"),
        author=_schema.required_str(payload, "author"),
        body_excerpt=_schema.required_str(payload, "body_excerpt"),
        score=_schema.required_int(payload, "score"),
    )


def conversation_structure_from_dict(payload: Mapping[str, Any] | None) -> ConversationStructure:
    if payload is None:
        return ConversationStructure(
            max_depth=0,
            top_level_count=0,
            reply_chain_count=0,
            longest_chain_length=0,
            controversy_count=0,
            consensus_count=0,
            monologue_count=0,
            top_engagement_subtrees=[],
        )

    top_subtrees_data = payload.get("top_engagement_subtrees", [])
    if not isinstance(top_subtrees_data, list):
        raise AnalysisBoundaryError(
            "analysis conversation structure list field is invalid",
            details={"key": "top_engagement_subtrees"},
        )

    return ConversationStructure(
        max_depth=required_int_with_default(payload, "max_depth"),
        top_level_count=required_int_with_default(payload, "top_level_count"),
        reply_chain_count=required_int_with_default(payload, "reply_chain_count"),
        longest_chain_length=required_int_with_default(payload, "longest_chain_length"),
        controversy_count=required_int_with_default(payload, "controversy_count"),
        consensus_count=required_int_with_default(payload, "consensus_count"),
        monologue_count=required_int_with_default(payload, "monologue_count"),
        top_engagement_subtrees=[engagement_subtree_from_dict(item) for item in top_subtrees_data],
    )


def engagement_subtree_from_dict(payload: Mapping[str, Any]) -> EngagementSubtree:
    return EngagementSubtree(
        root_comment_id=_schema.required_str(payload, "root_comment_id"),
        root_author=_schema.required_str(payload, "root_author"),
        subtree_size=_schema.required_int(payload, "subtree_size"),
        max_depth_below=_schema.required_int(payload, "max_depth_below"),
        engagement_score=_schema.required_float(payload, "engagement_score"),
    )


def read_json_file(path: Path) -> dict[str, Any]:
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


def contract_payload_from_dict(payload: Mapping[str, Any]) -> dict[str, str | float]:
    contract_payload = payload.get("contract")
    if contract_payload is None:
        return default_contract().to_dict()
    if not isinstance(contract_payload, dict):
        raise AnalysisBoundaryError(
            "analysis contract payload is invalid",
            details={"key": "contract"},
        )
    return AnalysisContract.from_dict(contract_payload).to_dict()


def alignment_check_from_dict(payload: Any) -> AlignmentCheck | None:
    if payload is None:
        return None
    if not isinstance(payload, dict):
        raise AnalysisBoundaryError("analysis alignment field is invalid")
    return AlignmentCheck(
        domain=_schema.required_str(payload, "domain"),
        domain_fit_score=_schema.required_float(payload, "domain_fit_score"),
        general_feedback_ratio=_schema.required_float(payload, "general_feedback_ratio"),
        suggested_domain=_schema.optional_nullable_str(payload, "suggested_domain"),
        warning=_schema.optional_nullable_str(payload, "warning"),
    )


def required_str_list(payload: Mapping[str, Any], key: str) -> list[str]:
    value = payload.get(key)
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        raise AnalysisBoundaryError(
            "analysis string list field is invalid",
            details={"key": key},
        )
    return value


def required_str_with_default(
    payload: Mapping[str, Any],
    key: str,
    default: str,
) -> str:
    value = payload.get(key, default)
    if not isinstance(value, str) or not value:
        raise AnalysisBoundaryError(
            "analysis string field is invalid",
            details={"key": key},
        )
    return value


def required_int_with_default(payload: Mapping[str, Any], key: str, default: int = 0) -> int:
    value = payload.get(key, default)
    if not isinstance(value, int):
        raise AnalysisBoundaryError(
            "analysis integer field is invalid",
            details={"key": key},
        )
    return value


def optional_nested_object(payload: Mapping[str, Any], key: str) -> dict[str, Any] | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, dict):
        raise AnalysisBoundaryError(
            "analysis object field is invalid",
            details={"key": key},
        )
    return value
