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
from threadsense.inference import InferenceTask
from threadsense.inference.local_runtime import LocalRuntimeClient, RuntimeProbeResult
from threadsense.logging_config import configure_logging
from threadsense.observability import DEFAULT_METRICS, TraceContext
from threadsense.pipeline.replay import replay_analysis
from threadsense.pipeline.storage import (
    load_analysis_artifact,
    load_normalized_artifact,
    load_report_artifact,
)
from threadsense.preflight import DiagnosticCheck, run_diagnostic_checks
from threadsense.workflows import (
    analyze_normalized_thread,
    build_source_registry,
    fetch_reddit_thread,
    fetch_source_thread,
    infer_analysis,
    normalize_reddit_thread,
    normalize_source_thread,
    report_analysis,
    run_reddit_pipeline,
    run_source_pipeline,
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
        choices=[task.value for task in InferenceTask],
        default=InferenceTask.ANALYSIS_SUMMARY.value,
        help="Inference task to run.",
    )
    infer_analysis_parser.add_argument(
        "--required",
        action="store_true",
        help="Fail instead of falling back when local inference is unavailable.",
    )
    _add_config_argument(infer_analysis_parser)


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
        choices=["markdown", "json"],
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
        choices=["markdown", "json"],
        default="markdown",
        help="Final report output format.",
    )
    _add_report_summary_arguments(run_parser)
    _add_contract_arguments(run_parser)


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
    _build_batch_parser(subparsers)
    _build_serve_parser(subparsers)
    _build_run_parser(subparsers)
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
    return RedditConnector(config.reddit)


def build_hackernews_connector(config: AppConfig) -> HackerNewsConnector:
    return HackerNewsConnector(config.hackernews)


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
) -> int:
    logger, config = load_cli_context(config_path)
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


def run_reddit_end_to_end(
    config_path: Path | None,
    url: str,
    expand_more: bool,
    flat: bool,
    report_format: str,
    with_summary: bool,
    summary_required: bool,
    contract: AnalysisContract | None = None,
) -> int:
    logger, config = load_cli_context(config_path)
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
    domain: str | None,
    objective: str | None,
    abstraction_level: str | None,
) -> int:
    logger, config = load_cli_context(config_path)
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
            registry_factory=build_source_registry,
        )
    emit_payload(payload.to_dict())
    return 0


def parse_run_target(target: list[str]) -> tuple[str | None, str]:
    if len(target) == 1:
        return None, target[0]
    if len(target) == 2:
        source_name = "hackernews" if target[0] in {"hn", "hackernews"} else target[0]
        return source_name, target[1]
    raise _CommandDispatchError("run")


def _dispatch_command(args: argparse.Namespace) -> int:
    command_key = (
        args.command,
        getattr(args, "source", None),
        getattr(args, "artifact_type", None),
        getattr(args, "batch_command", None),
    )
    match command_key:
        case ("preflight", _, _, _):
            return run_preflight(args.config, args.skip_runtime)
        case ("fetch", "reddit", _, _):
            return run_reddit_fetch(
                config_path=args.config,
                url=args.url,
                output_path=args.output,
                expand_more=args.expand_more,
                flat=args.flat,
            )
        case ("fetch", "hn" | "hackernews", _, _):
            logger, config = load_cli_context(args.config)
            payload = fetch_source_thread(
                config=config,
                logger=logger,
                trace=cli_trace("cli-fetch"),
                url=args.url,
                output_path=args.output,
                source_name="hackernews",
                registry_factory=build_source_registry,
            )
            emit_payload(payload.to_dict())
            return 0
        case ("normalize", "reddit", _, _):
            return run_reddit_normalize(
                config_path=args.config,
                input_path=args.input,
                output_path=args.output,
            )
        case ("normalize", "hn" | "hackernews", _, _):
            return run_source_normalize(
                config_path=args.config,
                input_path=args.input,
                output_path=args.output,
            )
        case ("analyze", _, "normalized", _):
            return run_normalized_analyze(
                config_path=args.config,
                input_path=args.input,
                output_path=args.output,
                domain=args.domain,
                objective=args.objective,
                abstraction_level=args.abstraction_level,
            )
        case ("inspect", _, "normalized", _):
            return run_normalized_inspect(args.input)
        case ("inspect", _, "analysis", _):
            return run_analysis_inspect(args.input)
        case ("inspect", _, "report", _):
            return run_report_inspect(args.input)
        case ("infer", _, "analysis", _):
            return run_analysis_infer(
                config_path=args.config,
                input_path=args.input,
                task_name=args.task,
                required=args.required,
            )
        case ("report", _, "analysis", _):
            return run_analysis_report(
                config_path=args.config,
                input_path=args.input,
                output_path=args.output,
                report_format=args.format,
                with_summary=args.with_summary,
                summary_required=args.summary_required,
            )
        case ("replay", _, _, _):
            return run_replay(args.analysis_artifact)
        case ("batch", _, _, "run"):
            return run_batch(
                config_path=args.config,
                manifest_path=args.manifest,
                output_path=args.output,
            )
        case ("serve", _, _, _):
            return run_api_server(
                config_path=args.config,
                host=args.host,
                port=args.port,
            )
        case ("run", _, _, _):
            return run_source_end_to_end(
                config_path=args.config,
                target=args.target,
                expand_more=args.expand_more,
                flat=args.flat,
                report_format=args.format,
                with_summary=args.with_summary,
                summary_required=args.summary_required,
                domain=args.domain,
                objective=args.objective,
                abstraction_level=args.abstraction_level,
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
