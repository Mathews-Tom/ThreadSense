from __future__ import annotations

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
    raise SchemaBoundaryError("inference task validator is missing", details={"task": task.value})


def validate_analysis_summary_output(
    payload: Mapping[str, Any],
    analysis: ThreadAnalysis | None = None,
) -> dict[str, Any]:
    headline = required_str(payload, "headline")
    summary = required_str(payload, "summary")
    cited_theme_keys = required_str_list(payload, "cited_theme_keys")
    cited_comment_ids = required_str_list(payload, "cited_comment_ids")
    next_steps = required_str_list(payload, "next_steps")

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
        "cited_theme_keys": cited_theme_keys,
        "cited_comment_ids": cited_comment_ids,
        "next_steps": next_steps,
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
    normalized_existing: dict[str, list[str]] = {}
    for theme_key, keywords in existing_themes.items():
        if not isinstance(keywords, list):
            continue
        normalized_existing[str(theme_key)] = [
            str(k).strip().lower() for k in keywords if isinstance(k, str) and k.strip()
        ][:10]
    normalized_new: dict[str, list[str]] = {}
    for theme_key, keywords in list(new_themes.items())[:3]:
        if not isinstance(keywords, list):
            continue
        normalized_new[str(theme_key)] = [
            str(k).strip().lower() for k in keywords if isinstance(k, str) and k.strip()
        ][:10]
    return {
        "existing_themes": normalized_existing,
        "new_themes": normalized_new,
    }


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
