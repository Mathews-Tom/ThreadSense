from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from threadsense.errors import SchemaBoundaryError


class InferenceTask(StrEnum):
    ANALYSIS_SUMMARY = "analysis_summary"
    FINDING_CLASSIFICATION = "finding_classification"
    REPORT_SUMMARY = "report_summary"


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
) -> dict[str, Any]:
    if task is InferenceTask.ANALYSIS_SUMMARY:
        return validate_analysis_summary_output(payload)
    if task is InferenceTask.FINDING_CLASSIFICATION:
        return validate_finding_classification_output(payload)
    if task is InferenceTask.REPORT_SUMMARY:
        return validate_report_summary_output(payload)
    raise SchemaBoundaryError("inference task validator is missing", details={"task": task.value})


def validate_analysis_summary_output(payload: Mapping[str, Any]) -> dict[str, Any]:
    headline = required_str(payload, "headline")
    summary = required_str(payload, "summary")
    cited_theme_keys = required_str_list(payload, "cited_theme_keys")
    cited_comment_ids = required_str_list(payload, "cited_comment_ids")
    next_steps = required_str_list(payload, "next_steps")
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


def validate_report_summary_output(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "executive_summary": required_str(payload, "executive_summary"),
        "caveats": required_str_list(payload, "caveats"),
        "cited_theme_keys": required_str_list(payload, "cited_theme_keys"),
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
