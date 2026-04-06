from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from threadsense.errors import SchemaBoundaryError

if TYPE_CHECKING:
    from threadsense.models.analysis import ThreadAnalysis
    from threadsense.models.corpus import CorpusAnalysis


class InferenceTask(StrEnum):
    ANALYSIS_SUMMARY = "analysis_summary"
    FINDING_CLASSIFICATION = "finding_classification"
    REPORT_SUMMARY = "report_summary"
    CORPUS_SYNTHESIS = "corpus_synthesis"
    VOCABULARY_EXPANSION = "vocabulary_expansion"
    CATCH_ALL_RECLASSIFICATION = "catch_all_reclassification"


@dataclass(frozen=True)
class InferenceMessage:
    role: str
    content: str


@dataclass(frozen=True)
class InferenceRequest:
    task: InferenceTask
    messages: list[InferenceMessage]
    required: bool
    repair_retries: int
    repair_instruction: str


@dataclass(frozen=True)
class InferenceResponse:
    task: InferenceTask
    provider: str
    model: str | None
    finish_reason: str | None
    output: dict[str, Any]
    used_fallback: bool
    degraded: bool
    failure_reason: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "task": self.task.value,
            "provider": self.provider,
            "model": self.model,
            "finish_reason": self.finish_reason,
            "output": asdict_output(self.output),
            "used_fallback": self.used_fallback,
            "degraded": self.degraded,
            "failure_reason": self.failure_reason,
        }


def validate_task_output(
    task: InferenceTask,
    payload: Mapping[str, Any],
    analysis: ThreadAnalysis | None = None,
    corpus: CorpusAnalysis | None = None,
) -> dict[str, Any]:
    if task is InferenceTask.ANALYSIS_SUMMARY:
        return validate_analysis_summary_output(payload, analysis)
    if task is InferenceTask.FINDING_CLASSIFICATION:
        return validate_finding_classification_output(payload)
    if task is InferenceTask.REPORT_SUMMARY:
        return validate_report_summary_output(payload, analysis)
    if task is InferenceTask.CORPUS_SYNTHESIS:
        return validate_corpus_synthesis_output(payload, corpus)
    if task is InferenceTask.VOCABULARY_EXPANSION:
        return validate_vocabulary_expansion_output(payload)
    if task is InferenceTask.CATCH_ALL_RECLASSIFICATION:
        return validate_reclassification_output(payload)
    raise SchemaBoundaryError("inference task validator is missing", details={"task": task.value})


def validate_analysis_summary_output(
    payload: Mapping[str, Any],
    analysis: ThreadAnalysis | None = None,
) -> dict[str, Any]:
    headline = required_str(payload, "headline")
    summary = required_str(payload, "summary")
    priority = required_choice(payload, "priority", {"high", "medium", "low"})
    confidence = bounded_float(payload, "confidence")
    why_now = required_str(payload, "why_now")
    cited_theme_keys = required_str_list(payload, "cited_theme_keys")
    cited_comment_ids = required_str_list(payload, "cited_comment_ids")
    next_steps = required_str_list(payload, "next_steps")
    recommended_owner = normalized_identifier(payload, "recommended_owner")
    action_type = required_choice(
        payload,
        "action_type",
        {"fix", "investigate", "document", "design", "monitor"},
    )
    expected_outcome = required_str(payload, "expected_outcome")

    if analysis is not None:
        valid_theme_keys = {finding.theme_key for finding in analysis.findings}
        valid_comment_ids: set[str] = set()
        for finding in analysis.findings:
            valid_comment_ids.update(finding.evidence_comment_ids)
        cited_theme_keys = [k for k in cited_theme_keys if k in valid_theme_keys]
        cited_comment_ids = [cid for cid in cited_comment_ids if cid in valid_comment_ids]

    return {
        "headline": headline,
        "summary": summary,
        "priority": priority,
        "confidence": confidence,
        "why_now": why_now,
        "cited_theme_keys": cited_theme_keys,
        "cited_comment_ids": cited_comment_ids,
        "next_steps": next_steps,
        "recommended_owner": recommended_owner,
        "action_type": action_type,
        "expected_outcome": expected_outcome,
    }


def validate_finding_classification_output(payload: Mapping[str, Any]) -> dict[str, Any]:
    classifications = payload.get("classifications")
    if not isinstance(classifications, list):
        raise SchemaBoundaryError("classification output must include classifications")
    normalized: list[dict[str, Any]] = []
    for item in classifications:
        if not isinstance(item, dict):
            raise SchemaBoundaryError("classification item must be an object")
        normalized.append(
            {
                "theme_key": required_str(item, "theme_key"),
                "category": required_str(item, "category"),
                "confidence": required_float(item, "confidence"),
            }
        )
    return {"classifications": normalized}


def validate_report_summary_output(
    payload: Mapping[str, Any],
    analysis: ThreadAnalysis | None = None,
) -> dict[str, Any]:
    executive_summary = required_str(payload, "executive_summary")
    caveats = required_str_list(payload, "caveats")
    cited_theme_keys = required_str_list(payload, "cited_theme_keys")

    if analysis is not None:
        valid_theme_keys = {finding.theme_key for finding in analysis.findings}
        cited_theme_keys = [k for k in cited_theme_keys if k in valid_theme_keys]

    return {
        "executive_summary": executive_summary,
        "caveats": caveats,
        "cited_theme_keys": cited_theme_keys,
    }


def validate_corpus_synthesis_output(
    payload: Mapping[str, Any],
    corpus: CorpusAnalysis | None = None,
) -> dict[str, Any]:
    headline = required_str(payload, "headline")
    key_patterns = required_str_list(payload, "key_patterns")
    cited_thread_ids = required_str_list(payload, "cited_thread_ids")
    recommended_actions = required_str_list(payload, "recommended_actions")
    confidence_note = required_str(payload, "confidence_note")

    if corpus is not None:
        valid_thread_ids = {
            evidence.thread_id
            for finding in corpus.cross_thread_findings
            for evidence in finding.top_evidence
        }
        cited_thread_ids = [
            thread_id for thread_id in cited_thread_ids if thread_id in valid_thread_ids
        ]

    return {
        "headline": headline,
        "key_patterns": key_patterns,
        "cited_thread_ids": cited_thread_ids,
        "recommended_actions": recommended_actions,
        "confidence_note": confidence_note,
    }


def asdict_output(output: dict[str, Any]) -> dict[str, Any]:
    return dict(output)


def required_str(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise SchemaBoundaryError(
            "inference output string field is invalid",
            details={"key": key},
        )
    return value.strip()


def required_str_list(payload: Mapping[str, Any], key: str) -> list[str]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise SchemaBoundaryError(
            "inference output string list field is invalid",
            details={"key": key},
        )
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise SchemaBoundaryError(
                "inference output string list item is invalid",
                details={"key": key},
            )
        normalized.append(item.strip())
    return normalized


def validate_vocabulary_expansion_output(payload: Mapping[str, Any]) -> dict[str, Any]:
    existing_themes = payload.get("existing_themes")
    new_themes = payload.get("new_themes")
    if not isinstance(existing_themes, dict):
        raise SchemaBoundaryError(
            "vocabulary expansion must include existing_themes dict",
        )
    if not isinstance(new_themes, dict):
        raise SchemaBoundaryError(
            "vocabulary expansion must include new_themes dict",
        )
    return {
        "existing_themes": _normalize_keyword_map(existing_themes),
        "new_themes": _normalize_keyword_map(dict(list(new_themes.items())[:3])),
    }


def _normalize_keyword_map(
    raw: dict[str, object],
    max_per_theme: int = 10,
) -> dict[str, list[str]]:
    normalized: dict[str, list[str]] = {}
    for theme_key, keywords in raw.items():
        if not isinstance(keywords, list):
            continue
        normalized[str(theme_key)] = [
            str(k).strip().lower() for k in keywords if isinstance(k, str) and k.strip()
        ][:max_per_theme]
    return normalized


_RECLASSIFICATION_THEME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
_RECLASSIFICATION_MAX_NEW_THEMES = 5
_RECLASSIFICATION_MIN_CONFIDENCE = 0.6


def validate_reclassification_output(payload: Mapping[str, Any]) -> dict[str, Any]:
    classifications = payload.get("classifications")
    if not isinstance(classifications, list):
        raise SchemaBoundaryError("reclassification output must include classifications list")
    normalized: list[dict[str, Any]] = []
    for item in classifications:
        if not isinstance(item, dict):
            continue
        comment_id = str(item.get("comment_id", "")).strip()
        if not comment_id:
            continue
        raw_theme = str(item.get("theme", "general_feedback")).strip().lower()
        theme = re.sub(r"[^a-z0-9_]", "_", raw_theme).strip("_")
        if not theme or not _RECLASSIFICATION_THEME_PATTERN.match(theme):
            theme = "general_feedback"
        confidence = item.get("confidence", 0.0)
        if isinstance(confidence, int):
            confidence = float(confidence)
        if not isinstance(confidence, float):
            confidence = 0.0
        confidence = max(0.0, min(1.0, confidence))
        normalized.append(
            {
                "comment_id": comment_id,
                "theme": theme,
                "confidence": confidence,
            }
        )
    return {"classifications": normalized}


def required_float(payload: Mapping[str, Any], key: str) -> float:
    value = payload.get(key)
    if isinstance(value, int):
        return float(value)
    if not isinstance(value, float):
        raise SchemaBoundaryError(
            "inference output float field is invalid",
            details={"key": key},
        )
    return value


def bounded_float(payload: Mapping[str, Any], key: str) -> float:
    value = required_float(payload, key)
    if not 0.0 <= value <= 1.0:
        raise SchemaBoundaryError(
            "inference output float field is out of range",
            details={"key": key, "min": 0.0, "max": 1.0},
        )
    return value


def required_choice(payload: Mapping[str, Any], key: str, choices: set[str]) -> str:
    value = required_str(payload, key).lower()
    if value not in choices:
        raise SchemaBoundaryError(
            "inference output choice field is invalid",
            details={"key": key, "choices": sorted(choices)},
        )
    return value


def normalized_identifier(payload: Mapping[str, Any], key: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", required_str(payload, key).lower()).strip("_")
    if not normalized:
        raise SchemaBoundaryError(
            "inference output identifier field is invalid",
            details={"key": key},
        )
    return normalized
