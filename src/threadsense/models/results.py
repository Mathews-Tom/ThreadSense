from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class FetchResult:
    status: str
    source: str
    output_path: Path
    default_store_path: Path
    normalized_url: str
    post_id: str
    post_title: str
    total_comment_count: int
    expanded_more_count: int
    flat: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "source": self.source,
            "output_path": str(self.output_path),
            "default_store_path": str(self.default_store_path),
            "normalized_url": self.normalized_url,
            "post_id": self.post_id,
            "post_title": self.post_title,
            "total_comment_count": self.total_comment_count,
            "expanded_more_count": self.expanded_more_count,
            "flat": self.flat,
        }


@dataclass(frozen=True)
class NormalizeResult:
    status: str
    artifact_type: str
    input_path: Path
    output_path: Path
    default_store_path: Path
    thread_id: str
    comment_count: int
    schema_version: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "artifact_type": self.artifact_type,
            "input_path": str(self.input_path),
            "output_path": str(self.output_path),
            "default_store_path": str(self.default_store_path),
            "thread_id": self.thread_id,
            "comment_count": self.comment_count,
            "schema_version": self.schema_version,
        }


@dataclass(frozen=True)
class AnalyzeResult:
    status: str
    artifact_type: str
    input_path: Path
    output_path: Path
    default_store_path: Path
    thread_id: str
    finding_count: int
    duplicate_group_count: int
    top_phrases: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "artifact_type": self.artifact_type,
            "input_path": str(self.input_path),
            "output_path": str(self.output_path),
            "default_store_path": str(self.default_store_path),
            "thread_id": self.thread_id,
            "finding_count": self.finding_count,
            "duplicate_group_count": self.duplicate_group_count,
            "top_phrases": self.top_phrases,
        }


@dataclass(frozen=True)
class InferResult:
    status: str
    artifact_type: str
    input_path: Path
    thread_id: str
    task: str
    provider: str
    model: str | None
    used_fallback: bool
    failure_reason: str | None
    output: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "artifact_type": self.artifact_type,
            "input_path": str(self.input_path),
            "thread_id": self.thread_id,
            "task": self.task,
            "provider": self.provider,
            "model": self.model,
            "used_fallback": self.used_fallback,
            "failure_reason": self.failure_reason,
            "output": self.output,
        }


@dataclass(frozen=True)
class ReportResult:
    status: str
    artifact_type: str
    input_path: Path
    output_path: Path
    default_store_path: Path
    report_format: str
    thread_id: str
    summary_provider: str | None
    degraded_summary: bool
    quality_check_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "artifact_type": self.artifact_type,
            "input_path": str(self.input_path),
            "output_path": str(self.output_path),
            "default_store_path": str(self.default_store_path),
            "format": self.report_format,
            "thread_id": self.thread_id,
            "summary_provider": self.summary_provider,
            "degraded_summary": self.degraded_summary,
            "quality_check_count": self.quality_check_count,
        }


@dataclass(frozen=True)
class PipelineResult:
    status: str
    source: str
    thread_url: str
    fetch: FetchResult
    normalize: NormalizeResult
    analyze: AnalyzeResult
    report: ReportResult

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "source": self.source,
            "thread_url": self.thread_url,
            "fetch": self.fetch.to_dict(),
            "normalize": self.normalize.to_dict(),
            "analyze": self.analyze.to_dict(),
            "report": self.report.to_dict(),
        }
