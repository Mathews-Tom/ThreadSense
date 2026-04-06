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
class ReportFindingSummary:
    theme_label: str
    severity: str
    action_type: str
    recommended_owner: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "theme_label": self.theme_label,
            "severity": self.severity,
            "action_type": self.action_type,
            "recommended_owner": self.recommended_owner,
        }


@dataclass(frozen=True)
class RunTerminalSummary:
    headline: str
    summary: str
    priority: str
    recommended_owner: str
    action_type: str
    next_steps: list[str]
    top_findings: list[ReportFindingSummary]

    def to_dict(self) -> dict[str, Any]:
        return {
            "headline": self.headline,
            "summary": self.summary,
            "priority": self.priority,
            "recommended_owner": self.recommended_owner,
            "action_type": self.action_type,
            "next_steps": self.next_steps,
            "top_findings": [finding.to_dict() for finding in self.top_findings],
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
    terminal_summary: RunTerminalSummary | None

    def to_dict(self) -> dict[str, Any]:
        payload = {
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
        if self.terminal_summary is not None:
            payload["terminal_summary"] = self.terminal_summary.to_dict()
        return payload


@dataclass(frozen=True)
class CorpusCreateResult:
    status: str
    manifest_path: Path
    default_store_path: Path
    corpus_id: str
    thread_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "manifest_path": str(self.manifest_path),
            "default_store_path": str(self.default_store_path),
            "corpus_id": self.corpus_id,
            "thread_count": self.thread_count,
        }


@dataclass(frozen=True)
class CorpusAnalyzeResult:
    status: str
    input_path: Path
    output_path: Path
    default_store_path: Path
    corpus_id: str
    thread_count: int
    finding_count: int
    trend_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "input_path": str(self.input_path),
            "output_path": str(self.output_path),
            "default_store_path": str(self.default_store_path),
            "corpus_id": self.corpus_id,
            "thread_count": self.thread_count,
            "finding_count": self.finding_count,
            "trend_count": self.trend_count,
        }


@dataclass(frozen=True)
class CorpusReportResult:
    status: str
    input_path: Path
    output_path: Path
    default_store_path: Path
    corpus_id: str
    summary_provider: str | None
    degraded_summary: bool
    terminal_summary: ResearchTerminalSummary | None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "status": self.status,
            "input_path": str(self.input_path),
            "output_path": str(self.output_path),
            "default_store_path": str(self.default_store_path),
            "corpus_id": self.corpus_id,
            "summary_provider": self.summary_provider,
            "degraded_summary": self.degraded_summary,
        }
        if self.terminal_summary is not None:
            payload["terminal_summary"] = self.terminal_summary.to_dict()
        return payload


@dataclass(frozen=True)
class ResearchSelectedThreadSummary:
    subreddit: str
    title: str
    match_source: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "subreddit": self.subreddit,
            "title": self.title,
            "match_source": self.match_source,
        }


@dataclass(frozen=True)
class ResearchTerminalSummary:
    headline: str
    key_patterns: list[str]
    recommended_actions: list[str]
    confidence_note: str
    top_threads: list[ResearchSelectedThreadSummary]

    def to_dict(self) -> dict[str, Any]:
        return {
            "headline": self.headline,
            "key_patterns": self.key_patterns,
            "recommended_actions": self.recommended_actions,
            "confidence_note": self.confidence_note,
            "top_threads": [thread.to_dict() for thread in self.top_threads],
        }


@dataclass(frozen=True)
class ResearchThreadMatchResult:
    post_id: str
    subreddit: str
    title: str
    thread_url: str
    score: int
    num_comments: int
    match_source: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "post_id": self.post_id,
            "subreddit": self.subreddit,
            "title": self.title,
            "thread_url": self.thread_url,
            "score": self.score,
            "num_comments": self.num_comments,
            "match_source": self.match_source,
        }


@dataclass(frozen=True)
class RedditResearchResult:
    status: str
    artifact_type: str
    query: str
    subreddits: list[str]
    time_window: str
    reddit_time_bucket: str
    sort: str
    discovered_thread_count: int
    selected_thread_count: int
    fetched_thread_count: int
    failed_thread_count: int
    selected_threads: list[ResearchThreadMatchResult]
    manifest_path: Path
    corpus_analysis_path: Path
    corpus_report_path: Path
    corpus_id: str
    summary_provider: str | None
    degraded_summary: bool
    terminal_summary: ResearchTerminalSummary | None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "status": self.status,
            "artifact_type": self.artifact_type,
            "query": self.query,
            "subreddits": self.subreddits,
            "time_window": self.time_window,
            "reddit_time_bucket": self.reddit_time_bucket,
            "sort": self.sort,
            "discovered_thread_count": self.discovered_thread_count,
            "selected_thread_count": self.selected_thread_count,
            "fetched_thread_count": self.fetched_thread_count,
            "failed_thread_count": self.failed_thread_count,
            "selected_threads": [thread.to_dict() for thread in self.selected_threads],
            "manifest_path": str(self.manifest_path),
            "corpus_analysis_path": str(self.corpus_analysis_path),
            "corpus_report_path": str(self.corpus_report_path),
            "corpus_id": self.corpus_id,
            "summary_provider": self.summary_provider,
            "degraded_summary": self.degraded_summary,
        }
        if self.terminal_summary is not None:
            payload["terminal_summary"] = self.terminal_summary.to_dict()
        return payload


@dataclass(frozen=True)
class EvaluateResult:
    status: str
    dataset_path: Path
    strategy_a: str
    strategy_b: str
    winner: str | None
    metrics_a: dict[str, float]
    metrics_b: dict[str, float]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "dataset_path": str(self.dataset_path),
            "strategy_a": self.strategy_a,
            "strategy_b": self.strategy_b,
            "winner": self.winner,
            "metrics_a": self.metrics_a,
            "metrics_b": self.metrics_b,
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
