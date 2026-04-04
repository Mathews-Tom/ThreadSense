from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path
from time import perf_counter
from typing import Any

from threadsense.config import AppConfig
from threadsense.connectors import FetchRequest
from threadsense.connectors.cache import FetchCache
from threadsense.connectors.github_discussions import GitHubDiscussionsConnector
from threadsense.connectors.hackernews import HackerNewsConnector
from threadsense.connectors.reddit import (
    RedditConnector,
    RedditThreadRequest,
)
from threadsense.connectors.registry import SourceRegistry
from threadsense.contracts import AnalysisContract, DomainType
from threadsense.evaluation import compare_strategies, load_golden_dataset
from threadsense.inference import InferenceResponse, InferenceRouter, InferenceTask
from threadsense.models.canonical import load_canonical_thread
from threadsense.models.corpus import CorpusAnalysis
from threadsense.models.results import (
    AnalyzeResult,
    CorpusAnalyzeResult,
    CorpusCreateResult,
    CorpusReportResult,
    EvaluateResult,
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
    emit_log,
    observe_stage,
)
from threadsense.pipeline.alignment import suggest_domain
from threadsense.pipeline.analyze import analyze_thread_file
from threadsense.pipeline.corpus import build_corpus_analysis, build_corpus_manifest
from threadsense.pipeline.corpus_index import index_corpus, search_index
from threadsense.pipeline.storage import (
    build_corpus_paths,
    build_storage_paths,
    load_analysis_artifact,
    load_analysis_artifact_version,
    load_corpus_manifest,
    load_normalized_artifact,
    load_raw_artifact,
    persist_analysis_artifact_with_config,
    persist_corpus_analysis,
    persist_corpus_manifest,
    persist_normalized_artifact,
    persist_raw_artifact,
    persist_report_artifact,
    write_text,
)
from threadsense.pipeline.versioning import diff_analyses
from threadsense.reporting import build_thread_report, render_report_html, render_report_markdown
from threadsense.reporting.corpus_render import render_corpus_markdown

RedditConnectorFactory = Callable[[AppConfig], RedditConnector]
HackerNewsConnectorFactory = Callable[[AppConfig], HackerNewsConnector]
GitHubDiscussionsConnectorFactory = Callable[[AppConfig], GitHubDiscussionsConnector]
SourceRegistryFactory = Callable[[AppConfig], SourceRegistry]


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
        registry.increment(
            "threadsense_comments_processed_total",
            {
                "stage": "fetch",
                "source_name": "reddit",
                "count": str(result.total_comment_count),
            },
        )
        registry.increment(
            "threadsense_cache_hit_total",
            {"source_name": "reddit", "outcome": result.cache_status},
        )
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


def fetch_source_thread(
    *,
    config: AppConfig,
    logger: logging.Logger,
    trace: TraceContext,
    url: str,
    output_path: Path | None,
    source_name: str | None,
    registry_factory: SourceRegistryFactory,
    registry: MetricsRegistry = DEFAULT_METRICS,
) -> FetchResult:
    with observe_stage(
        registry=registry,
        logger=logger,
        trace=trace,
        stage="fetch",
    ):
        source_registry = registry_factory(config)
        resolved_source = source_name or source_registry.detect_source(url)
        connector = source_registry.get(resolved_source)
        result = connector.fetch(FetchRequest(url=url))
        storage_paths = build_storage_paths(
            config.storage,
            result.source_name,
            result.source_thread_id,
        )
        resolved_output_path = output_path or storage_paths.raw_path
        persist_raw_artifact(resolved_output_path, result)
        registry.increment(
            "threadsense_comments_processed_total",
            {
                "stage": "fetch",
                "source_name": result.source_name,
                "count": str(result.total_comment_count),
            },
        )
        registry.increment(
            "threadsense_cache_hit_total",
            {
                "source_name": result.source_name,
                "outcome": str(getattr(result, "cache_status", "disabled")),
            },
        )
        return FetchResult(
            status="ready",
            source=result.source_name,
            output_path=resolved_output_path,
            default_store_path=storage_paths.raw_path,
            normalized_url=result.normalized_url,
            post_id=result.source_thread_id,
            post_title=result.thread_title,
            total_comment_count=result.total_comment_count,
            expanded_more_count=0,
            flat=False,
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
    return normalize_source_thread(
        config=config,
        logger=logger,
        trace=trace,
        input_path=input_path,
        output_path=output_path,
        registry_factory=build_source_registry,
        registry=registry,
    )


def normalize_source_thread(
    *,
    config: AppConfig,
    logger: logging.Logger,
    trace: TraceContext,
    input_path: Path,
    output_path: Path | None,
    registry_factory: SourceRegistryFactory,
    registry: MetricsRegistry = DEFAULT_METRICS,
) -> NormalizeResult:
    with observe_stage(
        registry=registry,
        logger=logger,
        trace=trace,
        stage="normalize",
    ):
        raw_artifact = load_raw_artifact(input_path)
        source_name = str(raw_artifact["source"])
        thread = registry_factory(config).get(source_name).normalize(raw_artifact, input_path)
        storage_paths = build_storage_paths(
            config.storage,
            thread.source.source_name,
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
    contract: AnalysisContract | None = None,
    auto_domain: bool = False,
    registry: MetricsRegistry = DEFAULT_METRICS,
) -> AnalyzeResult:
    with observe_stage(
        registry=registry,
        logger=logger,
        trace=trace,
        stage="analyze",
    ):
        resolved_contract = resolve_analysis_contract_for_thread(
            config=config,
            input_path=input_path,
            contract=contract,
            auto_domain=auto_domain,
        )
        analysis = analyze_thread_file(
            input_path,
            config=config.analysis,
            contract=resolved_contract,
        )
        storage_paths = build_storage_paths(
            config.storage,
            analysis.source_name,
            analysis.provenance.source_thread_id,
        )
        resolved_output_path = output_path or storage_paths.analysis_path
        resolved_output_path = persist_analysis_artifact_with_config(
            config.storage,
            resolved_output_path,
            analysis,
        )
        duplicate_ratio = (
            analysis.duplicate_group_count / analysis.total_comments
            if analysis.total_comments
            else 0.0
        )
        registry.set_gauge(
            "threadsense_duplicate_ratio",
            {"source_name": analysis.source_name, "thread_id": analysis.thread_id},
            duplicate_ratio,
        )
        for finding in analysis.findings:
            registry.increment(
                "threadsense_findings_total",
                {
                    "source_name": analysis.source_name,
                    "theme_key": finding.theme_key,
                    "severity": finding.severity,
                },
            )
        emit_log(
            logger,
            "analysis_completed",
            trace,
            thread_id=analysis.thread_id,
            finding_count=len(analysis.findings),
            duplicate_ratio=round(duplicate_ratio, 6),
            top_severity=analysis.findings[0].severity if analysis.findings else "none",
            alignment_warning=(
                analysis.alignment_check.warning if analysis.alignment_check is not None else None
            ),
        )
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
            started_at = perf_counter()
            response = InferenceRouter(config).run_analysis_task(
                analysis=analysis,
                task=task,
                required=required,
            )
        record_runtime_completion(registry, response, perf_counter() - started_at)
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


def infer_corpus(
    *,
    config: AppConfig,
    logger: logging.Logger,
    trace: TraceContext,
    corpus: CorpusAnalysis,
    required: bool,
    registry: MetricsRegistry = DEFAULT_METRICS,
) -> InferenceResponse:
    with observe_stage(
        registry=registry,
        logger=logger,
        trace=trace,
        stage="infer",
        labels={"task": InferenceTask.CORPUS_SYNTHESIS.value},
    ):
        with runtime_slot_limit(config.limits.runtime_concurrency):
            started_at = perf_counter()
            response = InferenceRouter(config).run_corpus_task(
                corpus=corpus,
                task=InferenceTask.CORPUS_SYNTHESIS,
                required=required,
            )
        record_runtime_completion(registry, response, perf_counter() - started_at)
        return response


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
                started_at = perf_counter()
                summary_response = InferenceRouter(config).run_analysis_task(
                    analysis=analysis,
                    task=InferenceTask.ANALYSIS_SUMMARY,
                    required=summary_required,
                )
            record_runtime_completion(registry, summary_response, perf_counter() - started_at)

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
            else storage_paths.report_html_path
            if report_format == "html"
            else storage_paths.report_json_path
        )
        resolved_output_path = output_path or default_output_path
        if report_format == "json":
            persist_report_artifact(resolved_output_path, report)
        elif report_format == "html":
            write_text(resolved_output_path, render_report_html(report))
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
    contract: AnalysisContract | None = None,
    auto_domain: bool = False,
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
        contract=contract,
        auto_domain=auto_domain,
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


def run_source_pipeline(
    *,
    config: AppConfig,
    logger: logging.Logger,
    trace: TraceContext,
    url: str,
    source_name: str | None,
    report_format: str,
    with_summary: bool,
    summary_required: bool,
    contract: AnalysisContract | None,
    auto_domain: bool,
    registry_factory: SourceRegistryFactory,
    registry: MetricsRegistry = DEFAULT_METRICS,
) -> PipelineResult:
    fetch_result = fetch_source_thread(
        config=config,
        logger=logger,
        trace=trace,
        url=url,
        output_path=None,
        source_name=source_name,
        registry_factory=registry_factory,
        registry=registry,
    )
    normalize_result = normalize_source_thread(
        config=config,
        logger=logger,
        trace=trace,
        input_path=fetch_result.output_path,
        output_path=None,
        registry_factory=registry_factory,
        registry=registry,
    )
    analyze_result = analyze_normalized_thread(
        config=config,
        logger=logger,
        trace=trace,
        input_path=normalize_result.output_path,
        output_path=None,
        contract=contract,
        auto_domain=auto_domain,
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
        source=fetch_result.source,
        thread_url=url,
        fetch=fetch_result,
        normalize=normalize_result,
        analyze=analyze_result,
        report=report_result,
    )


def create_corpus(
    *,
    config: AppConfig,
    logger: logging.Logger,
    trace: TraceContext,
    name: str,
    description: str,
    domain: str,
    analysis_dir: Path,
    source_filter: str | None,
    output_path: Path | None,
    registry: MetricsRegistry = DEFAULT_METRICS,
) -> CorpusCreateResult:
    with observe_stage(
        registry=registry,
        logger=logger,
        trace=trace,
        stage="corpus_create",
    ):
        analysis_paths = sorted(analysis_dir.rglob("*.json"))
        manifest = build_corpus_manifest(
            name=name,
            description=description,
            domain=DomainType(domain),
            analysis_paths=analysis_paths,
            source_filter=source_filter,
        )
        corpus_paths = build_corpus_paths(config.storage, manifest.corpus_id)
        resolved_output_path = output_path or corpus_paths.manifest_path
        persist_corpus_manifest(resolved_output_path, manifest)
        return CorpusCreateResult(
            status="ready",
            manifest_path=resolved_output_path,
            default_store_path=corpus_paths.manifest_path,
            corpus_id=manifest.corpus_id,
            thread_count=len(manifest.thread_ids),
        )


def analyze_corpus(
    *,
    config: AppConfig,
    logger: logging.Logger,
    trace: TraceContext,
    manifest_path: Path,
    output_path: Path | None,
    registry: MetricsRegistry = DEFAULT_METRICS,
) -> CorpusAnalyzeResult:
    with observe_stage(
        registry=registry,
        logger=logger,
        trace=trace,
        stage="corpus_analyze",
    ):
        manifest = load_corpus_manifest(manifest_path)
        corpus = build_corpus_analysis(
            manifest,
            manifest_path=manifest_path,
            evidence_limit=config.corpus.evidence_limit,
            period=config.corpus.trend_period,
        )
        corpus_paths = build_corpus_paths(config.storage, manifest.corpus_id)
        resolved_output_path = output_path or corpus_paths.analysis_path
        persist_corpus_analysis(resolved_output_path, corpus)
        index_corpus(corpus_paths.index_path, corpus)
        return CorpusAnalyzeResult(
            status="ready",
            input_path=manifest_path,
            output_path=resolved_output_path,
            default_store_path=corpus_paths.analysis_path,
            corpus_id=corpus.corpus_id,
            thread_count=corpus.thread_count,
            finding_count=len(corpus.cross_thread_findings),
            trend_count=len(corpus.temporal_trends),
        )


def report_corpus(
    *,
    config: AppConfig,
    logger: logging.Logger,
    trace: TraceContext,
    manifest_path: Path,
    output_path: Path | None,
    with_summary: bool,
    summary_required: bool,
    registry: MetricsRegistry = DEFAULT_METRICS,
) -> CorpusReportResult:
    with observe_stage(
        registry=registry,
        logger=logger,
        trace=trace,
        stage="corpus_report",
    ):
        manifest = load_corpus_manifest(manifest_path)
        corpus = build_corpus_analysis(
            manifest,
            manifest_path=manifest_path,
            evidence_limit=config.corpus.evidence_limit,
            period=config.corpus.trend_period,
        )
        summary_response = None
        if with_summary:
            summary_response = infer_corpus(
                config=config,
                logger=logger,
                trace=trace,
                corpus=corpus,
                required=summary_required,
                registry=registry,
            )
        corpus_paths = build_corpus_paths(config.storage, corpus.corpus_id)
        resolved_output_path = output_path or corpus_paths.report_markdown_path
        write_text(resolved_output_path, render_corpus_markdown(corpus, summary_response))
        return CorpusReportResult(
            status="ready",
            input_path=manifest_path,
            output_path=resolved_output_path,
            default_store_path=corpus_paths.report_markdown_path,
            corpus_id=corpus.corpus_id,
            summary_provider=summary_response.provider if summary_response is not None else None,
            degraded_summary=summary_response.degraded if summary_response is not None else False,
        )


def evaluate_golden_dataset(
    *,
    config: AppConfig,
    logger: logging.Logger,
    trace: TraceContext,
    dataset_path: Path,
    strategy_names: tuple[str, str],
    registry: MetricsRegistry = DEFAULT_METRICS,
) -> EvaluateResult:
    with observe_stage(
        registry=registry,
        logger=logger,
        trace=trace,
        stage="evaluate",
    ):
        dataset = load_golden_dataset(dataset_path)
        thread = load_canonical_thread(Path(dataset.thread_fixture))
        config_a = config.analysis.model_copy(
            update={"strategy": strategy_names[0], "domain": dataset.domain}
        )
        config_b = config.analysis.model_copy(
            update={"strategy": strategy_names[1], "domain": dataset.domain}
        )
        comparison = compare_strategies(
            thread,
            Path(dataset.thread_fixture),
            config_a,
            config_b,
            dataset,
        )
        return EvaluateResult(
            status="ready",
            dataset_path=dataset_path,
            strategy_a=comparison.strategy_a,
            strategy_b=comparison.strategy_b,
            winner=comparison.winner,
            metrics_a=comparison.metrics_a.__dict__,
            metrics_b=comparison.metrics_b.__dict__,
        )


def record_runtime_completion(
    registry: MetricsRegistry,
    response: InferenceResponse,
    latency_seconds: float,
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
    registry.observe_histogram(
        "threadsense_inference_latency_seconds",
        {"provider": response.provider, "task": response.task.value},
        latency_seconds,
    )
    if response.degraded:
        registry.increment(
            "threadsense_inference_fallback_total",
            {"provider": response.provider, "task": response.task.value},
        )


@contextmanager
def runtime_slot_limit(concurrency_limit: int) -> Any:
    limiter = threading.BoundedSemaphore(concurrency_limit)
    limiter.acquire()
    try:
        yield
    finally:
        limiter.release()


def build_source_registry(config: AppConfig) -> SourceRegistry:
    return SourceRegistry(config)


def build_fetch_cache(config: AppConfig) -> FetchCache | None:
    if not config.cache.enabled:
        return None
    return FetchCache(config.cache.cache_dir, config.cache.ttl_seconds)


def resolve_analysis_contract_for_thread(
    *,
    config: AppConfig,
    input_path: Path,
    contract: AnalysisContract | None,
    auto_domain: bool,
) -> AnalysisContract | None:
    if not auto_domain:
        return contract
    base_contract = contract
    if base_contract is None:
        base_contract = AnalysisContract(
            domain=config.analysis.domain,
            objective=config.analysis.objective,
            abstraction_level=config.analysis.abstraction_level,
        )
    thread = load_normalized_artifact(input_path)
    suggested_domain = suggest_domain(thread, exclude=base_contract.domain)
    if suggested_domain is None:
        return base_contract
    return AnalysisContract(
        domain=DomainType(suggested_domain),
        objective=base_contract.objective,
        abstraction_level=base_contract.abstraction_level,
    )


def diff_analysis_versions(
    *,
    analysis_path: Path,
    left_version: int,
    right_version: int,
) -> dict[str, Any]:
    left = load_analysis_artifact_version(analysis_path, left_version)
    right = load_analysis_artifact_version(analysis_path, right_version)
    return {
        "status": "ready",
        "analysis_path": str(analysis_path),
        "left_version": left_version,
        "right_version": right_version,
        **diff_analyses(left, right),
    }


def search_corpora(
    *,
    config: AppConfig,
    query: str,
) -> dict[str, Any]:
    index_path = config.storage.root_dir / config.storage.index_dirname / "corpora.json"
    return {
        "status": "ready",
        "query": query,
        "matches": search_index(index_path, query),
    }
