from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from threadsense.config import AppConfig
from threadsense.connectors.reddit import (
    RedditConnector,
    RedditThreadRequest,
)
from threadsense.inference import InferenceResponse, InferenceRouter, InferenceTask
from threadsense.models.results import (
    AnalyzeResult,
    FetchResult,
    InferResult,
    NormalizeResult,
    PipelineResult,
    ReportResult,
)
from threadsense.observability import (
    DEFAULT_METRICS,
    MetricsRegistry,
    TraceContext,
    observe_stage,
)
from threadsense.pipeline.analyze import analyze_thread_file
from threadsense.pipeline.normalize import normalize_reddit_artifact_file
from threadsense.pipeline.storage import (
    build_storage_paths,
    load_analysis_artifact,
    persist_analysis_artifact,
    persist_normalized_artifact,
    persist_raw_artifact,
    persist_report_artifact,
    write_text,
)
from threadsense.reporting import build_thread_report, render_report_markdown

RedditConnectorFactory = Callable[[AppConfig], RedditConnector]


def fetch_reddit_thread(
    *,
    config: AppConfig,
    logger: logging.Logger,
    trace: TraceContext,
    url: str,
    output_path: Path | None,
    expand_more: bool,
    flat: bool,
    connector_factory: RedditConnectorFactory,
    registry: MetricsRegistry = DEFAULT_METRICS,
) -> FetchResult:
    with observe_stage(
        registry=registry,
        logger=logger,
        trace=trace,
        stage="fetch",
    ):
        result = connector_factory(config).fetch_thread(
            RedditThreadRequest(
                post_url=url,
                output_path=output_path,
                expand_more=expand_more,
                flat=flat,
            )
        )
        storage_paths = build_storage_paths(config.storage, "reddit", result.post.id)
        resolved_output_path = output_path or storage_paths.raw_path
        persist_raw_artifact(resolved_output_path, result)
        return FetchResult(
            status="ready",
            source="reddit",
            output_path=resolved_output_path,
            default_store_path=storage_paths.raw_path,
            normalized_url=result.normalized_url,
            post_id=result.post.id,
            post_title=result.post.title,
            total_comment_count=result.total_comment_count,
            expanded_more_count=result.expanded_more_count,
            flat=flat,
        )


def normalize_reddit_thread(
    *,
    config: AppConfig,
    logger: logging.Logger,
    trace: TraceContext,
    input_path: Path,
    output_path: Path | None,
    registry: MetricsRegistry = DEFAULT_METRICS,
) -> NormalizeResult:
    with observe_stage(
        registry=registry,
        logger=logger,
        trace=trace,
        stage="normalize",
    ):
        thread = normalize_reddit_artifact_file(input_path)
        storage_paths = build_storage_paths(
            config.storage,
            "reddit",
            thread.source.source_thread_id,
        )
        resolved_output_path = output_path or storage_paths.normalized_path
        persist_normalized_artifact(resolved_output_path, thread)
        return NormalizeResult(
            status="ready",
            artifact_type="normalized",
            input_path=input_path,
            output_path=resolved_output_path,
            default_store_path=storage_paths.normalized_path,
            thread_id=thread.thread_id,
            comment_count=thread.comment_count,
            schema_version=thread.provenance.schema_version,
        )


def analyze_normalized_thread(
    *,
    config: AppConfig,
    logger: logging.Logger,
    trace: TraceContext,
    input_path: Path,
    output_path: Path | None,
    registry: MetricsRegistry = DEFAULT_METRICS,
) -> AnalyzeResult:
    with observe_stage(
        registry=registry,
        logger=logger,
        trace=trace,
        stage="analyze",
    ):
        analysis = analyze_thread_file(input_path)
        storage_paths = build_storage_paths(
            config.storage,
            analysis.source_name,
            analysis.provenance.source_thread_id,
        )
        resolved_output_path = output_path or storage_paths.analysis_path
        persist_analysis_artifact(resolved_output_path, analysis)
        return AnalyzeResult(
            status="ready",
            artifact_type="analysis",
            input_path=input_path,
            output_path=resolved_output_path,
            default_store_path=storage_paths.analysis_path,
            thread_id=analysis.thread_id,
            finding_count=len(analysis.findings),
            duplicate_group_count=analysis.duplicate_group_count,
            top_phrases=analysis.top_phrases[:5],
        )


def infer_analysis(
    *,
    config: AppConfig,
    logger: logging.Logger,
    trace: TraceContext,
    input_path: Path,
    task: InferenceTask,
    required: bool,
    registry: MetricsRegistry = DEFAULT_METRICS,
) -> InferResult:
    with observe_stage(
        registry=registry,
        logger=logger,
        trace=trace,
        stage="infer",
        labels={"task": task.value},
    ):
        analysis = load_analysis_artifact(input_path)
        with runtime_slot_limit(config.limits.runtime_concurrency):
            response = InferenceRouter(config).run_analysis_task(
                analysis=analysis,
                task=task,
                required=required,
            )
        record_runtime_completion(registry, response)
        return InferResult(
            status="ready" if not response.degraded else "degraded",
            artifact_type="analysis",
            input_path=input_path,
            thread_id=analysis.thread_id,
            task=response.task.value,
            provider=response.provider,
            model=response.model,
            used_fallback=response.used_fallback,
            failure_reason=response.failure_reason,
            output=response.output,
        )


def report_analysis(
    *,
    config: AppConfig,
    logger: logging.Logger,
    trace: TraceContext,
    input_path: Path,
    output_path: Path | None,
    report_format: str,
    with_summary: bool,
    summary_required: bool,
    registry: MetricsRegistry = DEFAULT_METRICS,
) -> ReportResult:
    with observe_stage(
        registry=registry,
        logger=logger,
        trace=trace,
        stage="report",
        labels={"format": report_format},
    ):
        analysis = load_analysis_artifact(input_path)
        summary_response = None
        if with_summary:
            with runtime_slot_limit(config.limits.runtime_concurrency):
                summary_response = InferenceRouter(config).run_analysis_task(
                    analysis=analysis,
                    task=InferenceTask.ANALYSIS_SUMMARY,
                    required=summary_required,
                )
            record_runtime_completion(registry, summary_response)

        report = build_thread_report(
            analysis=analysis,
            analysis_artifact_path=str(input_path),
            summary_response=summary_response,
        )
        storage_paths = build_storage_paths(
            config.storage,
            analysis.source_name,
            analysis.provenance.source_thread_id,
        )
        default_output_path = (
            storage_paths.report_markdown_path
            if report_format == "markdown"
            else storage_paths.report_json_path
        )
        resolved_output_path = output_path or default_output_path
        if report_format == "json":
            persist_report_artifact(resolved_output_path, report)
        else:
            write_text(resolved_output_path, render_report_markdown(report))
        return ReportResult(
            status="ready",
            artifact_type="report",
            input_path=input_path,
            output_path=resolved_output_path,
            default_store_path=default_output_path,
            report_format=report_format,
            thread_id=report.thread_id,
            summary_provider=report.provenance.summary_provider,
            degraded_summary=report.executive_summary.degraded,
            quality_check_count=len(report.quality_checks),
        )


def run_reddit_pipeline(
    *,
    config: AppConfig,
    logger: logging.Logger,
    trace: TraceContext,
    url: str,
    expand_more: bool,
    flat: bool,
    report_format: str,
    with_summary: bool,
    summary_required: bool,
    connector_factory: RedditConnectorFactory,
    registry: MetricsRegistry = DEFAULT_METRICS,
) -> PipelineResult:
    fetch_result = fetch_reddit_thread(
        config=config,
        logger=logger,
        trace=trace,
        url=url,
        output_path=None,
        expand_more=expand_more,
        flat=flat,
        connector_factory=connector_factory,
        registry=registry,
    )
    normalize_result = normalize_reddit_thread(
        config=config,
        logger=logger,
        trace=trace,
        input_path=fetch_result.output_path,
        output_path=None,
        registry=registry,
    )
    analyze_result = analyze_normalized_thread(
        config=config,
        logger=logger,
        trace=trace,
        input_path=normalize_result.output_path,
        output_path=None,
        registry=registry,
    )
    report_result = report_analysis(
        config=config,
        logger=logger,
        trace=trace,
        input_path=analyze_result.output_path,
        output_path=None,
        report_format=report_format,
        with_summary=with_summary,
        summary_required=summary_required,
        registry=registry,
    )
    return PipelineResult(
        status="ready",
        source="reddit",
        thread_url=url,
        fetch=fetch_result,
        normalize=normalize_result,
        analyze=analyze_result,
        report=report_result,
    )


def record_runtime_completion(
    registry: MetricsRegistry,
    response: InferenceResponse,
) -> None:
    registry.increment(
        "threadsense_stage_total",
        {
            "stage": "runtime_completion",
            "provider": response.provider,
            "task": response.task.value,
            "outcome": "degraded" if response.degraded else "ready",
        },
    )


@contextmanager
def runtime_slot_limit(concurrency_limit: int) -> Any:
    limiter = threading.BoundedSemaphore(concurrency_limit)
    limiter.acquire()
    try:
        yield
    finally:
        limiter.release()
