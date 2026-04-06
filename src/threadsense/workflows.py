from __future__ import annotations

import logging
import re
import threading
from collections import Counter
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
    RedditSearchMatch,
    RedditSearchRequest,
    RedditThreadRequest,
)
from threadsense.connectors.registry import SourceRegistry
from threadsense.contracts import AnalysisContract, DomainType
from threadsense.domains import DomainVocabulary, load_domain_vocabulary, merge_vocabulary_expansion
from threadsense.errors import AnalysisBoundaryError
from threadsense.evaluation import compare_strategies, load_golden_dataset
from threadsense.inference import InferenceResponse, InferenceRouter, InferenceTask
from threadsense.models.analysis import AnalysisFinding, ThreadAnalysis
from threadsense.models.canonical import Thread, load_canonical_thread
from threadsense.models.corpus import CorpusAnalysis
from threadsense.models.report import ThreadReport
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
    RedditResearchResult,
    ResearchSelectedThreadSummary,
    ResearchTerminalSummary,
    ResearchThreadMatchResult,
    ReportFindingSummary,
    ReportResult,
    RunTerminalSummary,
)
from threadsense.observability import (
    DEFAULT_METRICS,
    MetricsRegistry,
    TraceContext,
    emit_log,
    observe_stage,
)
from threadsense.pipeline.analyze import analyze_thread_file
from threadsense.pipeline.corpus import build_corpus_analysis, build_corpus_manifest
from threadsense.pipeline.corpus_index import index_corpus, search_index
from threadsense.pipeline.domain_detect import detect_domain
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
        expanded_vocabulary = _try_vocabulary_expansion(config, input_path, resolved_contract)
        analysis = analyze_thread_file(
            input_path,
            config=config.analysis,
            contract=resolved_contract,
            vocabulary=expanded_vocabulary,
        )
        analysis = _try_reclassification(config, analysis, input_path)
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
        thread = (
            load_analysis_thread_context(analysis)
            if task is InferenceTask.ANALYSIS_SUMMARY
            else None
        )
        with runtime_slot_limit(config.limits.runtime_concurrency):
            started_at = perf_counter()
            response = InferenceRouter(config).run_analysis_task(
                analysis=analysis,
                task=task,
                required=required,
                thread=thread,
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
            thread = load_analysis_thread_context(analysis)
            with runtime_slot_limit(config.limits.runtime_concurrency):
                started_at = perf_counter()
                summary_response = InferenceRouter(config).run_analysis_task(
                    analysis=analysis,
                    task=InferenceTask.ANALYSIS_SUMMARY,
                    required=summary_required,
                    thread=thread,
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
            terminal_summary=build_terminal_summary(report),
        )


def build_terminal_summary(report: ThreadReport) -> RunTerminalSummary:
    return RunTerminalSummary(
        headline=report.executive_summary.headline,
        summary=report.executive_summary.summary,
        priority=report.executive_summary.priority,
        recommended_owner=report.executive_summary.recommended_owner,
        action_type=report.executive_summary.action_type,
        next_steps=report.executive_summary.next_steps[:3],
        top_findings=[
            ReportFindingSummary(
                theme_label=finding.theme_label,
                severity=finding.severity,
                action_type=finding.action_type,
                recommended_owner=finding.recommended_owner,
            )
            for finding in report.findings[:3]
        ],
    )


def build_corpus_terminal_summary(
    corpus: CorpusAnalysis,
    summary_response: InferenceResponse | None,
) -> ResearchTerminalSummary:
    if summary_response is not None:
        output = summary_response.output
        return ResearchTerminalSummary(
            headline=str(output["headline"]),
            key_patterns=list(output["key_patterns"][:3]),
            recommended_actions=list(output["recommended_actions"][:3]),
            confidence_note=str(output["confidence_note"]),
            top_threads=[],
        )

    top_findings = corpus.cross_thread_findings[:3]
    return ResearchTerminalSummary(
        headline=(
            f"{top_findings[0].theme_label.title()} is the strongest cross-thread pattern"
            if top_findings
            else f"Corpus report for {corpus.name}"
        ),
        key_patterns=[
            f"{finding.theme_label} spans {finding.thread_count} threads"
            for finding in top_findings
        ],
        recommended_actions=[
            f"Review the {finding.theme_key} pattern across {finding.thread_count} threads"
            for finding in top_findings
        ],
        confidence_note=("Generated from deterministic corpus findings without runtime synthesis."),
        top_threads=[],
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


def run_reddit_research(
    *,
    config: AppConfig,
    logger: logging.Logger,
    trace: TraceContext,
    query: str,
    subreddits: list[str],
    time_window: str,
    sort: str,
    limit: int,
    per_subreddit_limit: int,
    expand_more: bool,
    flat: bool,
    report_format: str,
    with_summary: bool,
    summary_required: bool,
    connector_factory: RedditConnectorFactory,
    contract: AnalysisContract | None = None,
    explicit_domain: DomainType | None = None,
    auto_domain: bool = False,
    registry: MetricsRegistry = DEFAULT_METRICS,
) -> RedditResearchResult:
    validate_research_query(query)
    if report_format != "markdown":
        raise AnalysisBoundaryError(
            "reddit research currently supports markdown corpus reports only",
            details={"report_format": report_format},
        )
    if limit <= 0:
        raise AnalysisBoundaryError(
            "reddit research limit must be greater than zero",
            details={"limit": limit},
        )
    if per_subreddit_limit <= 0:
        raise AnalysisBoundaryError(
            "reddit research per-subreddit limit must be greater than zero",
            details={"per_subreddit_limit": per_subreddit_limit},
        )
    if per_subreddit_limit > limit:
        raise AnalysisBoundaryError(
            "reddit research per-subreddit limit must not exceed the global limit",
            details={"limit": limit, "per_subreddit_limit": per_subreddit_limit},
        )
    with observe_stage(
        registry=registry,
        logger=logger,
        trace=trace,
        stage="research_discover",
        labels={"source": "reddit"},
    ):
        connector = connector_factory(config)
        search_result = connector.search_threads(
            RedditSearchRequest(
                query=query,
                subreddits=subreddits,
                limit=per_subreddit_limit,
                sort=sort,
                time_window=time_window,
            )
        )
    selected_matches = select_reddit_research_matches(
        matches=search_result.matches,
        query=query,
        limit=limit,
        per_subreddit_limit=per_subreddit_limit,
    )
    if not selected_matches:
        raise AnalysisBoundaryError(
            "reddit research did not find any matching threads",
            details={"query": query, "subreddits": subreddits},
        )

    analysis_paths: list[Path] = []
    for match in selected_matches:
        fetch_result = fetch_reddit_thread(
            config=config,
            logger=logger,
            trace=trace,
            url=match.thread_url,
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
        analysis_paths.append(analyze_result.output_path)

    corpus_create = create_corpus_from_analysis_paths(
        config=config,
        logger=logger,
        trace=trace,
        name=build_reddit_research_name(query, subreddits, time_window),
        description=build_reddit_research_description(query, subreddits, time_window, sort),
        domain=resolve_research_domain(analysis_paths, explicit_domain=explicit_domain),
        analysis_paths=analysis_paths,
        source_filter="reddit",
        output_path=None,
        registry=registry,
    )
    corpus_analysis = analyze_corpus(
        config=config,
        logger=logger,
        trace=trace,
        manifest_path=corpus_create.manifest_path,
        output_path=None,
        registry=registry,
    )
    corpus_report = report_corpus(
        config=config,
        logger=logger,
        trace=trace,
        manifest_path=corpus_create.manifest_path,
        output_path=None,
        with_summary=with_summary,
        summary_required=summary_required,
        registry=registry,
    )
    research_summary = None
    if corpus_report.terminal_summary is not None:
        research_summary = ResearchTerminalSummary(
            headline=corpus_report.terminal_summary.headline,
            key_patterns=corpus_report.terminal_summary.key_patterns,
            recommended_actions=corpus_report.terminal_summary.recommended_actions,
            confidence_note=corpus_report.terminal_summary.confidence_note,
            top_threads=[
                ResearchSelectedThreadSummary(
                    subreddit=match.subreddit,
                    title=match.title,
                    match_source=match_source_for_query(match, query),
                )
                for match in selected_matches[:3]
            ],
        )
    return RedditResearchResult(
        status="ready",
        artifact_type="research",
        query=query,
        subreddits=subreddits,
        time_window=time_window,
        reddit_time_bucket=search_result.reddit_time_bucket,
        sort=sort,
        discovered_thread_count=len(search_result.matches),
        selected_thread_count=len(selected_matches),
        fetched_thread_count=len(analysis_paths),
        failed_thread_count=0,
        selected_threads=[
            ResearchThreadMatchResult(
                post_id=match.post_id,
                subreddit=match.subreddit,
                title=match.title,
                thread_url=match.thread_url,
                score=match.score,
                num_comments=match.num_comments,
                match_source=match_source_for_query(match, query),
            )
            for match in selected_matches
        ],
        manifest_path=corpus_create.manifest_path,
        corpus_analysis_path=corpus_analysis.output_path,
        corpus_report_path=corpus_report.output_path,
        corpus_id=corpus_report.corpus_id,
        summary_provider=corpus_report.summary_provider,
        degraded_summary=corpus_report.degraded_summary,
        terminal_summary=research_summary,
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
    return create_corpus_from_analysis_paths(
        config=config,
        logger=logger,
        trace=trace,
        name=name,
        description=description,
        domain=DomainType(domain),
        analysis_paths=sorted(analysis_dir.rglob("*.json")),
        source_filter=source_filter,
        output_path=output_path,
        registry=registry,
    )


def create_corpus_from_analysis_paths(
    *,
    config: AppConfig,
    logger: logging.Logger,
    trace: TraceContext,
    name: str,
    description: str,
    domain: DomainType,
    analysis_paths: list[Path],
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
        manifest = build_corpus_manifest(
            name=name,
            description=description,
            domain=domain,
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
            terminal_summary=build_corpus_terminal_summary(corpus, summary_response),
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


def load_analysis_thread_context(analysis: ThreadAnalysis) -> Thread:
    return load_normalized_artifact(Path(analysis.provenance.normalized_artifact_path))


@contextmanager
def runtime_slot_limit(concurrency_limit: int) -> Any:
    limiter = threading.BoundedSemaphore(concurrency_limit)
    limiter.acquire()
    try:
        yield
    finally:
        limiter.release()


def select_reddit_research_matches(
    *,
    matches: list[RedditSearchMatch],
    query: str,
    limit: int,
    per_subreddit_limit: int,
) -> list[RedditSearchMatch]:
    matching = [match for match in matches if is_query_match(match, query)]
    ranked_by_subreddit: dict[str, list[RedditSearchMatch]] = {}
    for subreddit in sorted({match.subreddit for match in matching}):
        subreddit_matches = [match for match in matching if match.subreddit == subreddit]
        subreddit_matches.sort(
            key=lambda match: research_match_sort_key(match, query), reverse=True
        )
        ranked_by_subreddit[subreddit] = subreddit_matches[:per_subreddit_limit]

    deduped: dict[str, RedditSearchMatch] = {}
    for match_list in ranked_by_subreddit.values():
        for match in match_list:
            existing = deduped.get(match.post_id)
            if existing is None or research_match_sort_key(match, query) > research_match_sort_key(
                existing, query
            ):
                deduped[match.post_id] = match
    selected = sorted(
        deduped.values(),
        key=lambda match: research_match_sort_key(match, query),
        reverse=True,
    )
    return selected[:limit]


def is_query_match(match: RedditSearchMatch, query: str) -> bool:
    return any(query_match_components(query, match.title, match.selftext))


def research_match_sort_key(
    match: RedditSearchMatch, query: str
) -> tuple[int, int, int, int, int, int, float]:
    title_phrase_hits, title_term_hits, selftext_phrase_hits, selftext_term_hits = (
        query_match_components(
            query,
            match.title,
            match.selftext,
        )
    )
    return (
        title_phrase_hits,
        title_term_hits,
        selftext_phrase_hits,
        selftext_term_hits,
        match.score,
        match.num_comments,
        match.created_utc,
    )


def match_source_for_query(match: RedditSearchMatch, query: str) -> str:
    title_phrase_hits, title_term_hits, selftext_phrase_hits, selftext_term_hits = (
        query_match_components(
            query,
            match.title,
            match.selftext,
        )
    )
    if title_phrase_hits:
        return "title_phrase"
    if title_term_hits:
        return "title_terms"
    if selftext_phrase_hits:
        return "selftext_phrase"
    if selftext_term_hits:
        return "selftext_terms"
    raise AnalysisBoundaryError(
        "selected reddit research match does not satisfy the local query match requirement",
        details={"post_id": match.post_id, "query": query},
    )


def query_match_components(query: str, title: str, selftext: str) -> tuple[int, int, int, int]:
    title_text = title.lower()
    selftext_text = selftext.lower()
    title_tokens = set(tokenize_query_text(title_text))
    selftext_tokens = set(tokenize_query_text(selftext_text))
    title_phrase_hits = 0
    title_term_hits = 0
    selftext_phrase_hits = 0
    selftext_term_hits = 0
    for clause in parse_query_clauses(query):
        if contains_query_clause(title_text, clause):
            title_phrase_hits += 1
            continue
        if contains_query_clause(selftext_text, clause):
            selftext_phrase_hits += 1
            continue
        terms = [term for term in clause.split() if term]
        if terms and all(term in title_tokens for term in terms):
            title_term_hits += 1
            continue
        if terms and all(term in selftext_tokens for term in terms):
            selftext_term_hits += 1
    return title_phrase_hits, title_term_hits, selftext_phrase_hits, selftext_term_hits


def parse_query_clauses(query: str) -> list[str]:
    clauses = [
        clause.strip().lower() for clause in re.split(r"\s+or\s+|\|", query, flags=re.IGNORECASE)
    ]
    return [clause for clause in clauses if clause]


def validate_research_query(query: str) -> None:
    if not query.strip():
        raise AnalysisBoundaryError("reddit research query must not be empty")
    if re.search(r"[():\"']", query):
        raise AnalysisBoundaryError(
            "reddit research query uses unsupported advanced search syntax",
            details={"query": query},
        )


def contains_query_clause(text: str, clause: str) -> bool:
    pattern = r"(?<!\w)" + r"\s+".join(re.escape(part) for part in clause.split()) + r"(?!\w)"
    return re.search(pattern, text) is not None


def tokenize_query_text(text: str) -> list[str]:
    return re.findall(r"[a-z0-9_]+", text)


def resolve_research_domain(
    analysis_paths: list[Path],
    *,
    explicit_domain: DomainType | None,
) -> DomainType:
    if explicit_domain is not None:
        return explicit_domain
    domains: list[str] = []
    for path in analysis_paths:
        analysis = load_analysis_artifact(path)
        domain = analysis.provenance.contract.get("domain")
        if not isinstance(domain, str) or not domain:
            raise AnalysisBoundaryError(
                "research corpus domain is missing from analysis provenance",
                details={"analysis_path": str(path)},
            )
        domains.append(domain)
    if len(set(domains)) != 1:
        raise AnalysisBoundaryError(
            "research corpus domain is inconsistent across selected analyses",
            details={"domains": sorted(set(domains))},
        )
    return DomainType(Counter(domains).most_common(1)[0][0])


def build_reddit_research_name(query: str, subreddits: list[str], time_window: str) -> str:
    communities = "-".join(name.lower() for name in subreddits)
    return f"reddit-research-{query}-{communities}-{time_window}"


def build_reddit_research_description(
    query: str,
    subreddits: list[str],
    time_window: str,
    sort: str,
) -> str:
    communities = ", ".join(subreddits)
    return (
        f"Topic research for '{query}' across {communities} over {time_window} "
        f"using Reddit sort={sort}."
    )


def build_source_registry(config: AppConfig) -> SourceRegistry:
    return SourceRegistry(config)


def build_fetch_cache(config: AppConfig) -> FetchCache | None:
    if not config.cache.enabled:
        return None
    return FetchCache(config.cache.cache_dir, config.cache.ttl_seconds)


_RECLASSIFICATION_THRESHOLD = 0.30
_RECLASSIFICATION_CONFIDENCE = 0.6
_RECLASSIFICATION_MAX_NEW_THEMES = 5


def _try_reclassification(
    config: AppConfig,
    analysis: ThreadAnalysis,
    input_path: Path,
) -> ThreadAnalysis:
    """Reclassify general_feedback comments via LLM when the catch-all ratio is high."""
    if not config.runtime.enabled:
        return analysis
    gf_finding = next(
        (f for f in analysis.findings if f.theme_key == "general_feedback"),
        None,
    )
    if gf_finding is None:
        return analysis
    total = max(analysis.distinct_comment_count, 1)
    if gf_finding.comment_count / total < _RECLASSIFICATION_THRESHOLD:
        return analysis

    thread = load_normalized_artifact(input_path)
    existing_themes = {
        f.theme_key: tuple(f.key_phrases)
        for f in analysis.findings
        if f.theme_key != "general_feedback"
    }
    response = InferenceRouter(config).run_reclassification(
        thread=thread,
        comment_ids=gf_finding.evidence_comment_ids,
        existing_themes=existing_themes,
    )
    if response.degraded or not response.output:
        return analysis
    classifications = response.output.get("classifications", [])
    if not classifications:
        return analysis
    return _merge_reclassifications(analysis, classifications)


def _merge_reclassifications(
    analysis: ThreadAnalysis,
    classifications: list[dict[str, object]],
) -> ThreadAnalysis:
    """Move reclassified comments from general_feedback to their assigned themes."""
    reassigned: dict[str, list[str]] = {}
    for item in classifications:
        raw_confidence = item.get("confidence", 0.0)
        confidence = float(raw_confidence) if isinstance(raw_confidence, int | float) else 0.0
        if confidence < _RECLASSIFICATION_CONFIDENCE:
            continue
        theme = str(item.get("theme", "general_feedback"))
        if theme == "general_feedback":
            continue
        comment_id = str(item.get("comment_id", ""))
        if not comment_id:
            continue
        reassigned.setdefault(theme, []).append(comment_id)

    if not reassigned:
        return analysis

    # Cap new themes
    existing_keys = {f.theme_key for f in analysis.findings}
    new_theme_count = sum(1 for t in reassigned if t not in existing_keys)
    if new_theme_count > _RECLASSIFICATION_MAX_NEW_THEMES:
        allowed_new: set[str] = set()
        for theme in reassigned:
            if theme in existing_keys:
                continue
            if len(allowed_new) >= _RECLASSIFICATION_MAX_NEW_THEMES:
                break
            allowed_new.add(theme)
        reassigned = {
            t: ids for t, ids in reassigned.items() if t in existing_keys or t in allowed_new
        }

    all_reassigned_ids = {cid for ids in reassigned.values() for cid in ids}
    updated_findings: list[AnalysisFinding] = []
    for finding in analysis.findings:
        if finding.theme_key == "general_feedback":
            remaining = [
                cid for cid in finding.evidence_comment_ids if cid not in all_reassigned_ids
            ]
            if remaining:
                updated_findings.append(
                    AnalysisFinding(
                        theme_key=finding.theme_key,
                        theme_label=finding.theme_label,
                        severity=finding.severity,
                        comment_count=len(remaining),
                        issue_marker_count=finding.issue_marker_count,
                        request_marker_count=finding.request_marker_count,
                        key_phrases=finding.key_phrases,
                        evidence_comment_ids=remaining,
                        quotes=finding.quotes,
                    )
                )
        else:
            extra_ids = reassigned.get(finding.theme_key, [])
            if extra_ids:
                merged_ids = sorted(set(finding.evidence_comment_ids) | set(extra_ids))
                updated_findings.append(
                    AnalysisFinding(
                        theme_key=finding.theme_key,
                        theme_label=finding.theme_label,
                        severity=finding.severity,
                        comment_count=len(merged_ids),
                        issue_marker_count=finding.issue_marker_count,
                        request_marker_count=finding.request_marker_count,
                        key_phrases=finding.key_phrases,
                        evidence_comment_ids=merged_ids,
                        quotes=finding.quotes,
                    )
                )
            else:
                updated_findings.append(finding)

    # Add new themes from reclassification
    for theme, comment_ids in reassigned.items():
        if theme in existing_keys:
            continue
        updated_findings.append(
            AnalysisFinding(
                theme_key=theme,
                theme_label=theme.replace("_", " "),
                severity="low",
                comment_count=len(comment_ids),
                issue_marker_count=0,
                request_marker_count=0,
                key_phrases=[],
                evidence_comment_ids=sorted(comment_ids),
                quotes=[],
            )
        )

    return ThreadAnalysis(
        thread_id=analysis.thread_id,
        source_name=analysis.source_name,
        title=analysis.title,
        total_comments=analysis.total_comments,
        filtered_comment_count=analysis.filtered_comment_count,
        distinct_comment_count=analysis.distinct_comment_count,
        duplicate_group_count=analysis.duplicate_group_count,
        top_phrases=analysis.top_phrases,
        conversation_structure=analysis.conversation_structure,
        findings=updated_findings,
        duplicate_groups=analysis.duplicate_groups,
        top_quotes=analysis.top_quotes,
        alignment_check=analysis.alignment_check,
        provenance=analysis.provenance,
    )


def _try_vocabulary_expansion(
    config: AppConfig,
    input_path: Path,
    contract: AnalysisContract | None,
) -> DomainVocabulary | None:
    """Run LLM vocabulary expansion if runtime is enabled. Returns None on skip/failure."""
    if not config.runtime.enabled:
        return None
    resolved_domain = (contract.domain if contract else config.analysis.domain).value
    base_vocabulary = load_domain_vocabulary(resolved_domain)
    thread = load_normalized_artifact(input_path)
    response = InferenceRouter(config).run_vocabulary_expansion(thread, base_vocabulary)
    if response.degraded or not response.output:
        return None
    return merge_vocabulary_expansion(base_vocabulary, response.output)


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
    result = detect_domain(thread, base_contract.domain)
    if not result.switched:
        return base_contract
    return AnalysisContract(
        domain=result.selected,
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
