from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Sequence
from pathlib import Path

from threadsense.api_server import start_api_server
from threadsense.batching import run_batch_manifest
from threadsense.cli_display import cli_log_level, emit_error, emit_payload, status
from threadsense.config import AppConfig, load_config
from threadsense.connectors.hackernews import HackerNewsConnector
from threadsense.connectors.reddit import RedditConnector
from threadsense.contracts import (
    AbstractionLevel,
    AnalysisContract,
    DomainType,
    ObjectiveType,
    contract_from_config,
)
from threadsense.errors import ThreadSenseError
from threadsense.evaluation import load_golden_manifest
from threadsense.inference import InferenceTask
from threadsense.inference.local_runtime import LocalRuntimeClient, RuntimeProbeResult
from threadsense.logging_config import configure_logging
from threadsense.observability import DEFAULT_METRICS, TraceContext
from threadsense.pipeline.replay import replay_analysis
from threadsense.pipeline.storage import (
    load_analysis_artifact,
    load_corpus_analysis,
    load_normalized_artifact,
    load_report_artifact,
)
from threadsense.preflight import DiagnosticCheck, run_diagnostic_checks
from threadsense.workflows import (
    analyze_corpus,
    analyze_normalized_thread,
    build_fetch_cache,
    build_source_registry,
    create_corpus,
    diff_analysis_versions,
    evaluate_golden_dataset,
    fetch_reddit_thread,
    fetch_source_thread,
    infer_analysis,
    infer_corpus,
    normalize_reddit_thread,
    normalize_source_thread,
    report_analysis,
    report_corpus,
    run_reddit_pipeline,
    run_reddit_research,
    run_source_pipeline,
    search_corpora,
)


class _CommandDispatchError(Exception):
    pass


def _add_config_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--config",
        type=Path,
        help="Optional path to a TOML config file.",
    )


def _add_report_summary_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--with-summary",
        action="store_true",
        help="Use local inference to generate a bounded executive summary.",
    )
    parser.add_argument(
        "--summary-required",
        action="store_true",
        help="Fail instead of falling back when local summary generation is unavailable.",
    )


def _add_no_cache_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Bypass the fetch cache for this command.",
    )


def _add_auto_domain_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--auto-domain",
        action="store_true",
        help="Select the analysis domain from thread content before analysis.",
    )


def _add_contract_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--domain",
        choices=[domain.value for domain in DomainType],
        help="Analysis domain vocabulary.",
    )
    parser.add_argument(
        "--objective",
        choices=[objective.value for objective in ObjectiveType],
        help="Analysis objective.",
    )
    parser.add_argument(
        "--level",
        dest="abstraction_level",
        choices=[level.value for level in AbstractionLevel],
        help="Analysis abstraction level.",
    )


def _build_preflight_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    preflight_parser = subparsers.add_parser(
        "preflight",
        help="Validate local configuration and runtime readiness.",
    )
    _add_config_argument(preflight_parser)
    preflight_parser.add_argument(
        "--skip-runtime",
        action="store_true",
        help="Validate configuration without probing the runtime endpoint.",
    )


def _build_fetch_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    fetch_parser = subparsers.add_parser(
        "fetch",
        help="Fetch source data and persist the raw thread artifact.",
    )
    fetch_subparsers = fetch_parser.add_subparsers(dest="source", required=True)
    reddit_parser = fetch_subparsers.add_parser(
        "reddit",
        help="Fetch one Reddit thread from the public JSON API.",
    )
    reddit_parser.add_argument("url", help="Full Reddit thread URL")
    reddit_parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Raw artifact output path. Defaults to the configured raw store path.",
    )
    _add_config_argument(reddit_parser)
    _add_no_cache_argument(reddit_parser)
    reddit_parser.add_argument(
        "--expand-more",
        action="store_true",
        help="Expand deferred comment branches through morechildren.",
    )
    reddit_parser.add_argument(
        "--flat",
        action="store_true",
        help="Flatten nested comments in the persisted artifact.",
    )
    hn_parser = fetch_subparsers.add_parser(
        "hn",
        aliases=["hackernews"],
        help="Fetch one Hacker News discussion thread.",
    )
    hn_parser.add_argument("url", help="Full Hacker News item URL")
    hn_parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Raw artifact output path. Defaults to the configured raw store path.",
    )
    _add_config_argument(hn_parser)
    _add_no_cache_argument(hn_parser)
    gh_parser = fetch_subparsers.add_parser(
        "github-discussions",
        aliases=["gh-discussions"],
        help="Fetch one GitHub Discussions thread.",
    )
    gh_parser.add_argument("url", help="Full GitHub Discussions URL")
    gh_parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Raw artifact output path. Defaults to the configured raw store path.",
    )
    _add_config_argument(gh_parser)
    _add_no_cache_argument(gh_parser)
    gist_parser = fetch_subparsers.add_parser(
        "github-gist",
        aliases=["gist"],
        help="Fetch one GitHub Gist with comments.",
    )
    gist_parser.add_argument("url", help="Full GitHub Gist URL")
    gist_parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Raw artifact output path. Defaults to the configured raw store path.",
    )
    _add_config_argument(gist_parser)
    _add_no_cache_argument(gist_parser)


def _build_normalize_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    normalize_parser = subparsers.add_parser(
        "normalize",
        help="Normalize raw source artifacts into canonical thread artifacts.",
    )
    normalize_subparsers = normalize_parser.add_subparsers(dest="source", required=True)
    normalize_reddit_parser = normalize_subparsers.add_parser(
        "reddit",
        help="Normalize one Reddit raw artifact.",
    )
    normalize_reddit_parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Raw Reddit artifact path.",
    )
    normalize_reddit_parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Normalized artifact output path. Defaults to the configured normalized store path.",
    )
    _add_config_argument(normalize_reddit_parser)
    normalize_hn_parser = normalize_subparsers.add_parser(
        "hn",
        aliases=["hackernews"],
        help="Normalize one Hacker News raw artifact.",
    )
    normalize_hn_parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Raw HN artifact path.",
    )
    normalize_hn_parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Normalized artifact output path. Defaults to the configured normalized store path.",
    )
    _add_config_argument(normalize_hn_parser)
    normalize_gh_parser = normalize_subparsers.add_parser(
        "github-discussions",
        aliases=["gh-discussions"],
        help="Normalize one GitHub Discussions raw artifact.",
    )
    normalize_gh_parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Raw GitHub Discussions artifact path.",
    )
    normalize_gh_parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Normalized artifact output path. Defaults to the configured normalized store path.",
    )
    _add_config_argument(normalize_gh_parser)
    normalize_gist_parser = normalize_subparsers.add_parser(
        "github-gist",
        aliases=["gist"],
        help="Normalize one GitHub Gist raw artifact.",
    )
    normalize_gist_parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Raw GitHub Gist artifact path.",
    )
    normalize_gist_parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Normalized artifact output path. Defaults to the configured normalized store path.",
    )
    _add_config_argument(normalize_gist_parser)


def _build_analyze_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    analyze_parser = subparsers.add_parser(
        "analyze",
        help="Run deterministic analysis over canonical thread artifacts.",
    )
    analyze_subparsers = analyze_parser.add_subparsers(dest="artifact_type", required=True)
    analyze_normalized_parser = analyze_subparsers.add_parser(
        "normalized",
        help="Analyze one normalized thread artifact.",
    )
    analyze_normalized_parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Normalized artifact path.",
    )
    analyze_normalized_parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Analysis artifact output path. Defaults to the configured analysis store path.",
    )
    _add_contract_arguments(analyze_normalized_parser)
    _add_auto_domain_argument(analyze_normalized_parser)
    _add_config_argument(analyze_normalized_parser)


def _build_inspect_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    inspect_parser = subparsers.add_parser(
        "inspect",
        help="Inspect persisted thread artifacts.",
    )
    inspect_subparsers = inspect_parser.add_subparsers(dest="artifact_type", required=True)
    for artifact_type, help_text in (
        ("normalized", "Inspect one normalized thread artifact."),
        ("analysis", "Inspect one deterministic analysis artifact."),
        ("report", "Inspect one structured report artifact."),
    ):
        artifact_parser = inspect_subparsers.add_parser(artifact_type, help=help_text)
        artifact_parser.add_argument(
            "--input",
            type=Path,
            required=True,
            help=f"{artifact_type.capitalize()} artifact path.",
        )


def _build_infer_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    infer_parser = subparsers.add_parser(
        "infer",
        help="Run local inference tasks against deterministic analysis artifacts.",
    )
    infer_subparsers = infer_parser.add_subparsers(dest="artifact_type", required=True)
    infer_analysis_parser = infer_subparsers.add_parser(
        "analysis",
        help="Run one inference task for a persisted analysis artifact.",
    )
    infer_analysis_parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Analysis artifact path.",
    )
    infer_analysis_parser.add_argument(
        "--task",
        choices=[
            InferenceTask.ANALYSIS_SUMMARY.value,
            InferenceTask.FINDING_CLASSIFICATION.value,
            InferenceTask.REPORT_SUMMARY.value,
        ],
        default=InferenceTask.ANALYSIS_SUMMARY.value,
        help="Inference task to run.",
    )
    infer_analysis_parser.add_argument(
        "--required",
        action="store_true",
        help="Fail instead of falling back when local inference is unavailable.",
    )
    _add_config_argument(infer_analysis_parser)
    infer_corpus_parser = infer_subparsers.add_parser(
        "corpus",
        help="Run corpus synthesis for a persisted corpus analysis artifact.",
    )
    infer_corpus_parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Corpus analysis artifact path.",
    )
    infer_corpus_parser.add_argument(
        "--required",
        action="store_true",
        help="Fail instead of falling back when local inference is unavailable.",
    )
    _add_config_argument(infer_corpus_parser)


def _build_report_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    report_parser = subparsers.add_parser(
        "report",
        help="Generate Markdown or JSON reports from analysis artifacts.",
    )
    report_subparsers = report_parser.add_subparsers(dest="artifact_type", required=True)
    report_analysis_parser = report_subparsers.add_parser(
        "analysis",
        help="Generate a report from one analysis artifact.",
    )
    report_analysis_parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Analysis artifact path.",
    )
    report_analysis_parser.add_argument(
        "--format",
        choices=["markdown", "html", "json"],
        default="markdown",
        help="Report output format.",
    )
    report_analysis_parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help=(
            "Report output path. Defaults to the configured report store path "
            "for the selected format."
        ),
    )
    _add_report_summary_arguments(report_analysis_parser)
    _add_config_argument(report_analysis_parser)


def _build_replay_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    replay_parser = subparsers.add_parser(
        "replay",
        help="Replay deterministic analysis from a stored analysis artifact.",
    )
    replay_parser.add_argument(
        "--analysis-artifact",
        type=Path,
        required=True,
        help="Analysis artifact path.",
    )


def _build_diff_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    diff_parser = subparsers.add_parser(
        "diff",
        help="Compare two persisted analysis versions for one thread.",
    )
    diff_parser.add_argument(
        "--analysis-path",
        type=Path,
        required=True,
        help="Logical analysis artifact path used for versioned storage.",
    )
    diff_parser.add_argument(
        "--left-version",
        type=int,
        required=True,
        help="Older analysis version number.",
    )
    diff_parser.add_argument(
        "--right-version",
        type=int,
        required=True,
        help="Newer analysis version number.",
    )


def _build_corpus_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    corpus_parser = subparsers.add_parser(
        "corpus",
        help="Create and analyze cross-thread corpora from analysis artifacts.",
    )
    corpus_subparsers = corpus_parser.add_subparsers(dest="corpus_command", required=True)

    corpus_create_parser = corpus_subparsers.add_parser(
        "create",
        help="Create a corpus manifest from analysis artifacts.",
    )
    corpus_create_parser.add_argument("--name", required=True, help="Corpus name.")
    corpus_create_parser.add_argument(
        "--description",
        default="Corpus created from stored analysis artifacts.",
        help="Corpus description.",
    )
    corpus_create_parser.add_argument(
        "--domain",
        required=True,
        choices=[domain.value for domain in DomainType],
        help="Corpus domain.",
    )
    corpus_create_parser.add_argument(
        "--analysis-dir",
        type=Path,
        required=True,
        help="Directory containing analysis artifacts.",
    )
    corpus_create_parser.add_argument(
        "--source",
        choices=["reddit", "hackernews", "github_discussions"],
        help="Optional source filter.",
    )
    corpus_create_parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Manifest output path. Defaults to the configured corpus store path.",
    )
    _add_config_argument(corpus_create_parser)

    corpus_analyze_parser = corpus_subparsers.add_parser(
        "analyze",
        help="Build deterministic corpus analysis from a corpus manifest.",
    )
    corpus_analyze_parser.add_argument(
        "--corpus",
        type=Path,
        required=True,
        help="Corpus manifest path.",
    )
    corpus_analyze_parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Corpus analysis output path. Defaults to the configured corpus store path.",
    )
    _add_config_argument(corpus_analyze_parser)

    corpus_report_parser = corpus_subparsers.add_parser(
        "report",
        help="Render a corpus report from a corpus manifest.",
    )
    corpus_report_parser.add_argument(
        "--corpus",
        type=Path,
        required=True,
        help="Corpus manifest path.",
    )
    corpus_report_parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Corpus report output path. Defaults to the configured corpus store path.",
    )
    _add_report_summary_arguments(corpus_report_parser)
    _add_config_argument(corpus_report_parser)

    corpus_search_parser = corpus_subparsers.add_parser(
        "search",
        help="Search the persisted corpus index.",
    )
    corpus_search_parser.add_argument("query", help="Search query.")
    _add_config_argument(corpus_search_parser)


def _build_evaluate_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    evaluate_parser = subparsers.add_parser(
        "evaluate",
        help="Run golden dataset evaluation and compare strategy outputs.",
    )
    evaluate_parser.add_argument(
        "--golden",
        type=Path,
        required=True,
        help="Golden dataset path or manifest path.",
    )
    evaluate_parser.add_argument(
        "--strategy",
        nargs=2,
        required=True,
        help="Two strategies to compare.",
    )
    _add_config_argument(evaluate_parser)


def _build_batch_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    batch_parser = subparsers.add_parser(
        "batch",
        help="Run reproducible multi-thread workflows from a manifest.",
    )
    batch_subparsers = batch_parser.add_subparsers(dest="batch_command", required=True)
    batch_run_parser = batch_subparsers.add_parser(
        "run",
        help="Execute a batch manifest and persist the batch run artifact.",
    )
    batch_run_parser.add_argument(
        "--manifest",
        type=Path,
        required=True,
        help="Batch manifest path.",
    )
    batch_run_parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Batch run artifact output path. Defaults to the configured batch store path.",
    )
    _add_config_argument(batch_run_parser)


def _build_serve_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    serve_parser = subparsers.add_parser(
        "serve",
        help="Run the local HTTP API surface for the pipeline.",
    )
    _add_config_argument(serve_parser)
    serve_parser.add_argument(
        "--host",
        help="Override the configured API host.",
    )
    serve_parser.add_argument(
        "--port",
        type=int,
        help="Override the configured API port.",
    )


def _build_run_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    run_parser = subparsers.add_parser(
        "run",
        help="Execute the full workflow for one source input.",
    )
    run_parser.add_argument(
        "target",
        nargs="+",
        help="Either <url> or <source> <url>.",
    )
    _add_config_argument(run_parser)
    run_parser.add_argument(
        "--expand-more",
        action="store_true",
        help="Expand deferred comment branches through morechildren.",
    )
    run_parser.add_argument(
        "--flat",
        action="store_true",
        help="Flatten nested comments in the persisted raw artifact.",
    )
    run_parser.add_argument(
        "--format",
        choices=["markdown", "html", "json"],
        default="markdown",
        help="Final report output format.",
    )
    _add_no_cache_argument(run_parser)
    _add_report_summary_arguments(run_parser)
    _add_contract_arguments(run_parser)
    _add_auto_domain_argument(run_parser)


def _build_research_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    research_parser = subparsers.add_parser(
        "research",
        help="Discover and analyze topic discussions across selected communities.",
    )
    research_subparsers = research_parser.add_subparsers(dest="source", required=True)
    reddit_parser = research_subparsers.add_parser(
        "reddit",
        help="Research a topic across selected subreddits.",
    )
    reddit_parser.add_argument("--query", required=True, help="Topic query to search for.")
    reddit_parser.add_argument(
        "--subreddit",
        action="append",
        required=True,
        help="Target subreddit. Repeat for multiple subreddits.",
    )
    reddit_parser.add_argument(
        "--time-window",
        default="30d",
        help="Discovery time window such as 7d, 30d, 90d, 1y, or all.",
    )
    reddit_parser.add_argument(
        "--sort",
        choices=["relevance", "new", "top", "comments"],
        default="relevance",
        help="Reddit search sort order.",
    )
    reddit_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of selected threads to analyze.",
    )
    reddit_parser.add_argument(
        "--per-subreddit-limit",
        type=int,
        default=8,
        help="Maximum number of selected threads per subreddit.",
    )
    reddit_parser.add_argument(
        "--format",
        choices=["markdown"],
        default="markdown",
        help="Corpus report output format.",
    )
    reddit_parser.add_argument(
        "--expand-more",
        action="store_true",
        help="Expand deferred comment branches through morechildren for selected threads.",
    )
    reddit_parser.add_argument(
        "--flat",
        action="store_true",
        help="Flatten nested comments in persisted raw artifacts for selected threads.",
    )
    _add_config_argument(reddit_parser)
    _add_no_cache_argument(reddit_parser)
    _add_report_summary_arguments(reddit_parser)
    _add_contract_arguments(reddit_parser)
    _add_auto_domain_argument(reddit_parser)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="threadsense")
    parser.add_argument(
        "--output-format",
        choices=["json", "human", "quiet"],
        default=None,
        dest="output_mode",
        help="Output format: json (machine-readable), human (rich tables), quiet (status only).",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    _build_preflight_parser(subparsers)
    _build_fetch_parser(subparsers)
    _build_normalize_parser(subparsers)
    _build_analyze_parser(subparsers)
    _build_inspect_parser(subparsers)
    _build_infer_parser(subparsers)
    _build_report_parser(subparsers)
    _build_replay_parser(subparsers)
    _build_diff_parser(subparsers)
    _build_corpus_parser(subparsers)
    _build_evaluate_parser(subparsers)
    _build_batch_parser(subparsers)
    _build_serve_parser(subparsers)
    _build_run_parser(subparsers)
    _build_research_parser(subparsers)
    return parser


def render_preflight_report(
    config: AppConfig,
    probe: RuntimeProbeResult | None,
    diagnostics: list[DiagnosticCheck] | None = None,
) -> str:
    report: dict[str, object] = {
        "status": "ready" if probe is None or probe.ok else "degraded",
        "backend": config.inference_backend.value,
        "privacy_mode": config.privacy_mode.value,
        "sources": list(config.source_policy.enabled_sources),
        "runtime": {
            "enabled": config.runtime.enabled,
            "base_url": config.runtime.base_url,
            "chat_endpoint": config.runtime.chat_endpoint,
            "model": config.runtime.model,
            "timeout_seconds": config.runtime.timeout_seconds,
            "repair_retries": config.runtime.repair_retries,
            "json_mode": config.runtime.json_mode,
        },
    }
    if probe is not None:
        report["runtime_check"] = probe.to_dict()
    if diagnostics is not None:
        report["diagnostics"] = [c.to_dict() for c in diagnostics]
        if any(c.status == "fail" for c in diagnostics):
            report["status"] = "degraded"
    return json.dumps(report, indent=2)


def load_cli_context(config_path: Path | None) -> tuple[logging.Logger, AppConfig]:
    logger = configure_logging(level=cli_log_level())
    return logger, load_config(config_path)


def cli_trace(run_id: str) -> TraceContext:
    return TraceContext.create(run_id=run_id, source_name="cli")


def run_preflight(config_path: Path | None, skip_runtime: bool) -> int:
    _logger, config = load_cli_context(config_path)
    probe: RuntimeProbeResult | None = None
    if not skip_runtime:
        probe = LocalRuntimeClient(config.runtime).probe()

    diagnostics = run_diagnostic_checks(config, skip_network=skip_runtime)
    emit_payload(
        json.loads(render_preflight_report(config, probe, diagnostics)),
    )

    has_failures = any(c.status == "fail" for c in diagnostics)
    runtime_degraded = probe is not None and not probe.ok
    return 1 if has_failures or runtime_degraded else 0


def build_reddit_connector(config: AppConfig) -> RedditConnector:
    return RedditConnector(config.reddit, cache=build_fetch_cache(config))


def build_hackernews_connector(config: AppConfig) -> HackerNewsConnector:
    return HackerNewsConnector(config.hackernews, cache=build_fetch_cache(config))


def disable_cache(config: AppConfig, no_cache: bool) -> AppConfig:
    if not no_cache:
        return config
    return config.model_copy(update={"cache": config.cache.model_copy(update={"enabled": False})})


def resolve_contract_from_args(
    config: AppConfig,
    domain: str | None,
    objective: str | None,
    abstraction_level: str | None,
) -> AnalysisContract:
    analysis_config = config.analysis.model_copy(
        update={
            "domain": DomainType(domain) if domain is not None else config.analysis.domain,
            "objective": (
                ObjectiveType(objective) if objective is not None else config.analysis.objective
            ),
            "abstraction_level": (
                AbstractionLevel(abstraction_level)
                if abstraction_level is not None
                else config.analysis.abstraction_level
            ),
        }
    )
    return contract_from_config(analysis_config)


def run_reddit_fetch(
    config_path: Path | None,
    url: str,
    output_path: Path | None,
    expand_more: bool,
    flat: bool,
    no_cache: bool,
) -> int:
    logger, config = load_cli_context(config_path)
    config = disable_cache(config, no_cache)
    payload = fetch_reddit_thread(
        config=config,
        logger=logger,
        trace=cli_trace("cli-fetch"),
        url=url,
        output_path=output_path,
        expand_more=expand_more,
        flat=flat,
        connector_factory=build_reddit_connector,
    )
    emit_payload(payload.to_dict())
    return 0


def run_source_fetch(
    config_path: Path | None,
    url: str,
    output_path: Path | None,
    source_name: str,
    no_cache: bool,
) -> int:
    logger, config = load_cli_context(config_path)
    config = disable_cache(config, no_cache)
    payload = fetch_source_thread(
        config=config,
        logger=logger,
        trace=cli_trace("cli-fetch"),
        url=url,
        output_path=output_path,
        source_name=source_name,
        registry_factory=build_source_registry,
    )
    emit_payload(payload.to_dict())
    return 0


def run_reddit_normalize(
    config_path: Path | None,
    input_path: Path,
    output_path: Path | None,
) -> int:
    logger, config = load_cli_context(config_path)
    payload = normalize_reddit_thread(
        config=config,
        logger=logger,
        trace=cli_trace("cli-normalize"),
        input_path=input_path,
        output_path=output_path,
    )
    emit_payload(payload.to_dict())
    return 0


def run_source_normalize(
    config_path: Path | None,
    input_path: Path,
    output_path: Path | None,
) -> int:
    logger, config = load_cli_context(config_path)
    payload = normalize_source_thread(
        config=config,
        logger=logger,
        trace=cli_trace("cli-normalize"),
        input_path=input_path,
        output_path=output_path,
        registry_factory=build_source_registry,
    )
    emit_payload(payload.to_dict())
    return 0


def run_normalized_analyze(
    config_path: Path | None,
    input_path: Path,
    output_path: Path | None,
    domain: str | None,
    objective: str | None,
    abstraction_level: str | None,
    auto_domain: bool,
) -> int:
    logger, config = load_cli_context(config_path)
    contract = resolve_contract_from_args(config, domain, objective, abstraction_level)
    payload = analyze_normalized_thread(
        config=config,
        logger=logger,
        trace=cli_trace("cli-analyze"),
        input_path=input_path,
        output_path=output_path,
        contract=contract,
        auto_domain=auto_domain,
    )
    emit_payload(payload.to_dict())
    return 0


def run_normalized_inspect(input_path: Path) -> int:
    configure_logging(level=cli_log_level())
    thread = load_normalized_artifact(input_path)
    comment_ids = [comment.comment_id for comment in thread.comments[:10]]
    emit_payload(
        {
            "status": "ready",
            "artifact_type": "normalized",
            "input_path": str(input_path),
            "thread_id": thread.thread_id,
            "source_name": thread.source.source_name,
            "community": thread.source.community,
            "source_thread_id": thread.source.source_thread_id,
            "title": thread.title,
            "comment_count": thread.comment_count,
            "schema_version": thread.provenance.schema_version,
            "normalization_version": thread.provenance.normalization_version,
            "raw_artifact_path": thread.provenance.raw_artifact_path,
            "raw_sha256": thread.provenance.raw_sha256,
            "sample_comment_ids": comment_ids,
        }
    )
    return 0


def run_analysis_inspect(input_path: Path) -> int:
    configure_logging(level=cli_log_level())
    analysis = load_analysis_artifact(input_path)
    emit_payload(
        {
            "status": "ready",
            "artifact_type": "analysis",
            "input_path": str(input_path),
            "thread_id": analysis.thread_id,
            "title": analysis.title,
            "total_comments": analysis.total_comments,
            "distinct_comment_count": analysis.distinct_comment_count,
            "duplicate_group_count": analysis.duplicate_group_count,
            "schema_version": analysis.provenance.schema_version,
            "analysis_version": analysis.provenance.analysis_version,
            "contract": analysis.provenance.contract,
            "contract_schema_version": analysis.provenance.contract_schema_version,
            "normalized_artifact_path": analysis.provenance.normalized_artifact_path,
            "normalized_sha256": analysis.provenance.normalized_sha256,
            "top_phrases": analysis.top_phrases[:5],
            "top_findings": [
                {
                    "theme_key": finding.theme_key,
                    "severity": finding.severity,
                    "comment_count": finding.comment_count,
                    "key_phrases": finding.key_phrases[:3],
                    "evidence_comment_ids": finding.evidence_comment_ids,
                    "quotes": [quote.body_excerpt for quote in finding.quotes[:2]],
                }
                for finding in analysis.findings[:5]
            ],
        }
    )
    return 0


def run_analysis_infer(
    config_path: Path | None,
    input_path: Path,
    task_name: str,
    required: bool,
) -> int:
    logger, config = load_cli_context(config_path)
    payload = infer_analysis(
        config=config,
        logger=logger,
        trace=cli_trace("cli-infer"),
        input_path=input_path,
        task=InferenceTask(task_name),
        required=required,
    )
    emit_payload(payload.to_dict())
    return 0 if payload.status != "degraded" or not required else 1


def run_corpus_infer(
    config_path: Path | None,
    input_path: Path,
    required: bool,
) -> int:
    logger, config = load_cli_context(config_path)
    corpus = load_corpus_analysis(input_path)
    response = infer_corpus(
        config=config,
        logger=logger,
        trace=cli_trace("cli-infer"),
        corpus=corpus,
        required=required,
    )
    emit_payload(
        {
            "status": "ready" if not response.degraded else "degraded",
            "artifact_type": "corpus",
            "input_path": str(input_path),
            "task": response.task.value,
            "provider": response.provider,
            "model": response.model,
            "used_fallback": response.used_fallback,
            "failure_reason": response.failure_reason,
            "output": response.output,
        }
    )
    return 0 if not required or not response.degraded else 1


def run_analysis_report(
    config_path: Path | None,
    input_path: Path,
    output_path: Path | None,
    report_format: str,
    with_summary: bool,
    summary_required: bool,
) -> int:
    logger, config = load_cli_context(config_path)
    payload = report_analysis(
        config=config,
        logger=logger,
        trace=cli_trace("cli-report"),
        input_path=input_path,
        output_path=output_path,
        report_format=report_format,
        with_summary=with_summary,
        summary_required=summary_required,
    )
    emit_payload(payload.to_dict())
    return 0


def run_batch(
    config_path: Path | None,
    manifest_path: Path,
    output_path: Path | None,
) -> int:
    logger, config = load_cli_context(config_path)
    payload = run_batch_manifest(
        config=config,
        logger=logger,
        manifest_path=manifest_path,
        output_path=output_path,
        connector_factory=build_reddit_connector,
    )
    emit_payload(payload)
    return 0 if payload["failed_jobs"] == 0 else 1


def run_api_server(
    config_path: Path | None,
    host: str | None,
    port: int | None,
) -> int:
    logger = configure_logging()
    config = load_config(config_path)
    handle = start_api_server(
        config=config,
        logger=logger,
        connector_factory=build_reddit_connector,
        registry=DEFAULT_METRICS,
        host=host,
        port=port,
    )
    try:
        emit_payload(
            {
                "status": "ready",
                "artifact_type": "api_server",
                "host": handle.server.server_address[0],
                "port": handle.server.server_address[1],
                "metrics_path": "/v1/metrics",
            }
        )
        handle.thread.join()
    except KeyboardInterrupt:
        handle.server.shutdown()
        handle.server.server_close()
    return 0


def run_report_inspect(input_path: Path) -> int:
    configure_logging(level=cli_log_level())
    report = load_report_artifact(input_path)
    emit_payload(
        {
            "status": "ready",
            "artifact_type": "report",
            "input_path": str(input_path),
            "thread_id": report.thread_id,
            "title": report.title,
            "summary_provider": report.provenance.summary_provider,
            "finding_count": len(report.findings),
            "caveat_count": len(report.caveats),
            "quality_checks": [
                {"code": check.code, "level": check.level} for check in report.quality_checks
            ],
            "top_findings": [finding.theme_key for finding in report.findings[:5]],
        }
    )
    return 0


def run_replay(analysis_artifact_path: Path) -> int:
    configure_logging(level=cli_log_level())
    emit_payload(replay_analysis(analysis_artifact_path))
    return 0


def run_corpus_create(
    config_path: Path | None,
    name: str,
    description: str,
    domain: str,
    analysis_dir: Path,
    source_filter: str | None,
    output_path: Path | None,
) -> int:
    logger, config = load_cli_context(config_path)
    payload = create_corpus(
        config=config,
        logger=logger,
        trace=cli_trace("cli-corpus-create"),
        name=name,
        description=description,
        domain=domain,
        analysis_dir=analysis_dir,
        source_filter=source_filter,
        output_path=output_path,
    )
    emit_payload(payload.to_dict())
    return 0


def run_corpus_analyze(
    config_path: Path | None,
    manifest_path: Path,
    output_path: Path | None,
) -> int:
    logger, config = load_cli_context(config_path)
    payload = analyze_corpus(
        config=config,
        logger=logger,
        trace=cli_trace("cli-corpus-analyze"),
        manifest_path=manifest_path,
        output_path=output_path,
    )
    emit_payload(payload.to_dict())
    return 0


def run_corpus_report(
    config_path: Path | None,
    manifest_path: Path,
    output_path: Path | None,
    with_summary: bool,
    summary_required: bool,
) -> int:
    logger, config = load_cli_context(config_path)
    payload = report_corpus(
        config=config,
        logger=logger,
        trace=cli_trace("cli-corpus-report"),
        manifest_path=manifest_path,
        output_path=output_path,
        with_summary=with_summary,
        summary_required=summary_required,
    )
    emit_payload(payload.to_dict())
    return 0 if not summary_required or not payload.degraded_summary else 1


def run_corpus_search(
    config_path: Path | None,
    query: str,
) -> int:
    _logger, config = load_cli_context(config_path)
    emit_payload(search_corpora(config=config, query=query))
    return 0


def run_evaluate(
    config_path: Path | None,
    golden_path: Path,
    strategies: list[str],
) -> int:
    logger, config = load_cli_context(config_path)
    resolved_golden_path = (golden_path / "manifest.json") if golden_path.is_dir() else golden_path
    dataset_paths = (
        load_golden_manifest(resolved_golden_path)
        if resolved_golden_path.name == "manifest.json"
        else [resolved_golden_path.resolve()]
    )
    payloads = [
        evaluate_golden_dataset(
            config=config,
            logger=logger,
            trace=cli_trace("cli-evaluate"),
            dataset_path=dataset_path,
            strategy_names=(strategies[0], strategies[1]),
        ).to_dict()
        for dataset_path in dataset_paths
    ]
    emit_payload({"status": "ready", "results": payloads})
    return 0


def run_reddit_end_to_end(
    config_path: Path | None,
    url: str,
    expand_more: bool,
    flat: bool,
    report_format: str,
    with_summary: bool,
    summary_required: bool,
    no_cache: bool,
    auto_domain: bool,
    contract: AnalysisContract | None = None,
) -> int:
    logger, config = load_cli_context(config_path)
    config = disable_cache(config, no_cache)
    with status("Running Reddit workflow"):
        payload = run_reddit_pipeline(
            config=config,
            logger=logger,
            trace=cli_trace("cli-run"),
            url=url,
            expand_more=expand_more,
            flat=flat,
            report_format=report_format,
            with_summary=with_summary,
            summary_required=summary_required,
            contract=contract,
            auto_domain=auto_domain,
            connector_factory=build_reddit_connector,
        )
    emit_payload(payload.to_dict())
    return 0


def run_source_end_to_end(
    config_path: Path | None,
    target: list[str],
    expand_more: bool,
    flat: bool,
    report_format: str,
    with_summary: bool,
    summary_required: bool,
    no_cache: bool,
    domain: str | None,
    objective: str | None,
    abstraction_level: str | None,
    auto_domain: bool,
) -> int:
    logger, config = load_cli_context(config_path)
    config = disable_cache(config, no_cache)
    source_name, url = parse_run_target(target)
    contract = resolve_contract_from_args(config, domain, objective, abstraction_level)
    if source_name == "reddit":
        return run_reddit_end_to_end(
            config_path=config_path,
            url=url,
            expand_more=expand_more,
            flat=flat,
            report_format=report_format,
            with_summary=with_summary,
            summary_required=summary_required,
            no_cache=no_cache,
            auto_domain=auto_domain,
            contract=contract,
        )
    with status("Running workflow"):
        payload = run_source_pipeline(
            config=config,
            logger=logger,
            trace=cli_trace("cli-run"),
            url=url,
            source_name=source_name,
            report_format=report_format,
            with_summary=with_summary,
            summary_required=summary_required,
            contract=contract,
            auto_domain=auto_domain,
            registry_factory=build_source_registry,
        )
    emit_payload(payload.to_dict())
    return 0


def run_reddit_topic_research(
    config_path: Path | None,
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
    no_cache: bool,
    domain: str | None,
    objective: str | None,
    abstraction_level: str | None,
    auto_domain: bool,
) -> int:
    logger, config = load_cli_context(config_path)
    config = disable_cache(config, no_cache)
    contract = resolve_contract_from_args(config, domain, objective, abstraction_level)
    with status("Running Reddit research"):
        payload = run_reddit_research(
            config=config,
            logger=logger,
            trace=cli_trace("cli-research"),
            query=query,
            subreddits=subreddits,
            time_window=time_window,
            sort=sort,
            limit=limit,
            per_subreddit_limit=per_subreddit_limit,
            expand_more=expand_more,
            flat=flat,
            report_format=report_format,
            with_summary=with_summary,
            summary_required=summary_required,
            connector_factory=build_reddit_connector,
            contract=contract,
            explicit_domain=DomainType(domain) if domain is not None else None,
            auto_domain=auto_domain,
        )
    emit_payload(payload.to_dict())
    return 0 if not summary_required or not payload.degraded_summary else 1


def run_analysis_diff(
    analysis_path: Path,
    left_version: int,
    right_version: int,
) -> int:
    configure_logging(level=cli_log_level())
    emit_payload(
        diff_analysis_versions(
            analysis_path=analysis_path,
            left_version=left_version,
            right_version=right_version,
        )
    )
    return 0


def parse_run_target(target: list[str]) -> tuple[str | None, str]:
    if len(target) == 1:
        return None, target[0]
    if len(target) == 2:
        source_name = target[0]
        if source_name in {"hn", "hackernews"}:
            source_name = "hackernews"
        if source_name in {"github-discussions", "gh-discussions"}:
            source_name = "github_discussions"
        if source_name in {"github-gist", "gist"}:
            source_name = "github_gist"
        return source_name, target[1]
    raise _CommandDispatchError("run")


def _dispatch_command(args: argparse.Namespace) -> int:
    command_key = (
        args.command,
        getattr(args, "source", None),
        getattr(args, "artifact_type", None),
        getattr(args, "batch_command", None),
        getattr(args, "corpus_command", None),
    )
    match command_key:
        case ("preflight", _, _, _, _):
            return run_preflight(args.config, args.skip_runtime)
        case ("fetch", "reddit", _, _, _):
            return run_reddit_fetch(
                config_path=args.config,
                url=args.url,
                output_path=args.output,
                expand_more=args.expand_more,
                flat=args.flat,
                no_cache=args.no_cache,
            )
        case ("fetch", "hn" | "hackernews", _, _, _):
            return run_source_fetch(
                config_path=args.config,
                url=args.url,
                output_path=args.output,
                source_name="hackernews",
                no_cache=args.no_cache,
            )
        case ("fetch", "github-discussions" | "gh-discussions", _, _, _):
            return run_source_fetch(
                config_path=args.config,
                url=args.url,
                output_path=args.output,
                source_name="github_discussions",
                no_cache=args.no_cache,
            )
        case ("fetch", "github-gist" | "gist", _, _, _):
            return run_source_fetch(
                config_path=args.config,
                url=args.url,
                output_path=args.output,
                source_name="github_gist",
                no_cache=args.no_cache,
            )
        case ("normalize", "reddit", _, _, _):
            return run_reddit_normalize(
                config_path=args.config,
                input_path=args.input,
                output_path=args.output,
            )
        case (
            "normalize",
            "hn" | "hackernews" | "github-discussions" | "gh-discussions" | "github-gist" | "gist",
            _,
            _,
            _,
        ):
            return run_source_normalize(
                config_path=args.config,
                input_path=args.input,
                output_path=args.output,
            )
        case ("analyze", _, "normalized", _, _):
            return run_normalized_analyze(
                config_path=args.config,
                input_path=args.input,
                output_path=args.output,
                domain=args.domain,
                objective=args.objective,
                abstraction_level=args.abstraction_level,
                auto_domain=args.auto_domain,
            )
        case ("inspect", _, "normalized", _, _):
            return run_normalized_inspect(args.input)
        case ("inspect", _, "analysis", _, _):
            return run_analysis_inspect(args.input)
        case ("inspect", _, "report", _, _):
            return run_report_inspect(args.input)
        case ("infer", _, "analysis", _, _):
            return run_analysis_infer(
                config_path=args.config,
                input_path=args.input,
                task_name=args.task,
                required=args.required,
            )
        case ("infer", _, "corpus", _, _):
            return run_corpus_infer(
                config_path=args.config,
                input_path=args.input,
                required=args.required,
            )
        case ("report", _, "analysis", _, _):
            return run_analysis_report(
                config_path=args.config,
                input_path=args.input,
                output_path=args.output,
                report_format=args.format,
                with_summary=args.with_summary,
                summary_required=args.summary_required,
            )
        case ("replay", _, _, _, _):
            return run_replay(args.analysis_artifact)
        case ("diff", _, _, _, _):
            return run_analysis_diff(
                analysis_path=args.analysis_path,
                left_version=args.left_version,
                right_version=args.right_version,
            )
        case ("corpus", _, _, _, "create"):
            return run_corpus_create(
                config_path=args.config,
                name=args.name,
                description=args.description,
                domain=args.domain,
                analysis_dir=args.analysis_dir,
                source_filter=args.source,
                output_path=args.output,
            )
        case ("corpus", _, _, _, "analyze"):
            return run_corpus_analyze(
                config_path=args.config,
                manifest_path=args.corpus,
                output_path=args.output,
            )
        case ("corpus", _, _, _, "report"):
            return run_corpus_report(
                config_path=args.config,
                manifest_path=args.corpus,
                output_path=args.output,
                with_summary=args.with_summary,
                summary_required=args.summary_required,
            )
        case ("corpus", _, _, _, "search"):
            return run_corpus_search(
                config_path=args.config,
                query=args.query,
            )
        case ("evaluate", _, _, _, _):
            return run_evaluate(
                config_path=args.config,
                golden_path=args.golden,
                strategies=args.strategy,
            )
        case ("batch", _, _, "run", _):
            return run_batch(
                config_path=args.config,
                manifest_path=args.manifest,
                output_path=args.output,
            )
        case ("serve", _, _, _, _):
            return run_api_server(
                config_path=args.config,
                host=args.host,
                port=args.port,
            )
        case ("research", "reddit", _, _, _):
            return run_reddit_topic_research(
                config_path=args.config,
                query=args.query,
                subreddits=args.subreddit,
                time_window=args.time_window,
                sort=args.sort,
                limit=args.limit,
                per_subreddit_limit=args.per_subreddit_limit,
                expand_more=args.expand_more,
                flat=args.flat,
                report_format=args.format,
                with_summary=args.with_summary,
                summary_required=args.summary_required,
                no_cache=args.no_cache,
                domain=args.domain,
                objective=args.objective,
                abstraction_level=args.abstraction_level,
                auto_domain=args.auto_domain,
            )
        case ("run", _, _, _, _):
            return run_source_end_to_end(
                config_path=args.config,
                target=args.target,
                expand_more=args.expand_more,
                flat=args.flat,
                report_format=args.format,
                with_summary=args.with_summary,
                summary_required=args.summary_required,
                no_cache=args.no_cache,
                domain=args.domain,
                objective=args.objective,
                abstraction_level=args.abstraction_level,
                auto_domain=args.auto_domain,
            )
        case _:
            raise _CommandDispatchError(args.command)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.output_mode is not None:
        from threadsense.cli_display import OutputMode, set_output_mode

        set_output_mode(OutputMode(args.output_mode))
    try:
        return _dispatch_command(args)
    except ThreadSenseError as error:
        emit_error(error)
        return 1
    except _CommandDispatchError:
        parser.error(f"unknown command: {args.command}")
        return 2
