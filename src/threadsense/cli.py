from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path

from threadsense.api_server import start_api_server
from threadsense.batching import run_batch_manifest
from threadsense.cli_display import cli_log_level, emit_error, emit_payload, status
from threadsense.config import AppConfig, load_config
from threadsense.connectors.reddit import (
    RedditConnector,
)
from threadsense.errors import ThreadSenseError
from threadsense.inference import InferenceTask
from threadsense.inference.local_runtime import LocalRuntimeClient, RuntimeProbeResult
from threadsense.logging_config import configure_logging
from threadsense.observability import DEFAULT_METRICS, TraceContext
from threadsense.pipeline.storage import (
    load_analysis_artifact,
    load_normalized_artifact,
    load_report_artifact,
)
from threadsense.workflows import (
    analyze_normalized_thread,
    fetch_reddit_thread,
    infer_analysis,
    normalize_reddit_thread,
    report_analysis,
    run_reddit_pipeline,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="threadsense")
    subparsers = parser.add_subparsers(dest="command", required=True)

    preflight_parser = subparsers.add_parser(
        "preflight",
        help="Validate local configuration and runtime readiness.",
    )
    preflight_parser.add_argument(
        "--config",
        type=Path,
        help="Optional path to a TOML config file.",
    )
    preflight_parser.add_argument(
        "--skip-runtime",
        action="store_true",
        help="Validate configuration without probing the runtime endpoint.",
    )

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
    reddit_parser.add_argument(
        "--config",
        type=Path,
        help="Optional path to a TOML config file.",
    )
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
    normalize_reddit_parser.add_argument(
        "--config",
        type=Path,
        help="Optional path to a TOML config file.",
    )

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
    analyze_normalized_parser.add_argument(
        "--config",
        type=Path,
        help="Optional path to a TOML config file.",
    )

    inspect_parser = subparsers.add_parser(
        "inspect",
        help="Inspect persisted thread artifacts.",
    )
    inspect_subparsers = inspect_parser.add_subparsers(dest="artifact_type", required=True)
    normalized_parser = inspect_subparsers.add_parser(
        "normalized",
        help="Inspect one normalized thread artifact.",
    )
    normalized_parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Normalized artifact path.",
    )
    analysis_parser = inspect_subparsers.add_parser(
        "analysis",
        help="Inspect one deterministic analysis artifact.",
    )
    analysis_parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Analysis artifact path.",
    )

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
    infer_analysis_parser.add_argument(
        "--config",
        type=Path,
        help="Optional path to a TOML config file.",
    )

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
    report_analysis_parser.add_argument(
        "--with-summary",
        action="store_true",
        help="Use local inference to generate a bounded executive summary.",
    )
    report_analysis_parser.add_argument(
        "--summary-required",
        action="store_true",
        help="Fail instead of falling back when local summary generation is unavailable.",
    )
    report_analysis_parser.add_argument(
        "--config",
        type=Path,
        help="Optional path to a TOML config file.",
    )

    report_inspect_parser = inspect_subparsers.add_parser(
        "report",
        help="Inspect one structured report artifact.",
    )
    report_inspect_parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Report artifact path.",
    )

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
    batch_run_parser.add_argument(
        "--config",
        type=Path,
        help="Optional path to a TOML config file.",
    )

    serve_parser = subparsers.add_parser(
        "serve",
        help="Run the local HTTP API surface for the pipeline.",
    )
    serve_parser.add_argument(
        "--config",
        type=Path,
        help="Optional path to a TOML config file.",
    )
    serve_parser.add_argument(
        "--host",
        help="Override the configured API host.",
    )
    serve_parser.add_argument(
        "--port",
        type=int,
        help="Override the configured API port.",
    )

    run_parser = subparsers.add_parser(
        "run",
        help="Execute the full workflow for one source input.",
    )
    run_subparsers = run_parser.add_subparsers(dest="source", required=True)
    run_reddit_parser = run_subparsers.add_parser(
        "reddit",
        help="Fetch, normalize, analyze, and report one Reddit thread.",
    )
    run_reddit_parser.add_argument("url", help="Full Reddit thread URL")
    run_reddit_parser.add_argument(
        "--config",
        type=Path,
        help="Optional path to a TOML config file.",
    )
    run_reddit_parser.add_argument(
        "--expand-more",
        action="store_true",
        help="Expand deferred comment branches through morechildren.",
    )
    run_reddit_parser.add_argument(
        "--flat",
        action="store_true",
        help="Flatten nested comments in the persisted raw artifact.",
    )
    run_reddit_parser.add_argument(
        "--format",
        choices=["markdown", "json"],
        default="markdown",
        help="Final report output format.",
    )
    run_reddit_parser.add_argument(
        "--with-summary",
        action="store_true",
        help="Use local inference to generate a bounded executive summary.",
    )
    run_reddit_parser.add_argument(
        "--summary-required",
        action="store_true",
        help="Fail instead of falling back when local summary generation is unavailable.",
    )
    return parser


def render_preflight_report(config: AppConfig, probe: RuntimeProbeResult | None) -> str:
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
        },
    }
    if probe is not None:
        report["runtime_check"] = probe.to_dict()
    return json.dumps(report, indent=2)


def run_preflight(config_path: Path | None, skip_runtime: bool) -> int:
    configure_logging(level=cli_log_level())
    config = load_config(config_path)
    probe: RuntimeProbeResult | None = None
    if not skip_runtime:
        probe = LocalRuntimeClient(config.runtime).probe()

    emit_payload(json.loads(render_preflight_report(config, probe)))
    return 0 if probe is None or probe.ok else 1


def build_reddit_connector(config: AppConfig) -> RedditConnector:
    return RedditConnector(config.reddit)


def run_reddit_fetch(
    config_path: Path | None,
    url: str,
    output_path: Path | None,
    expand_more: bool,
    flat: bool,
) -> int:
    logger = configure_logging(level=cli_log_level())
    config = load_config(config_path)
    payload = fetch_reddit_thread(
        config=config,
        logger=logger,
        trace=TraceContext.create(run_id="cli-fetch", source_name="reddit"),
        url=url,
        output_path=output_path,
        expand_more=expand_more,
        flat=flat,
        connector_factory=build_reddit_connector,
    )
    emit_payload(payload)
    return 0


def run_reddit_normalize(
    config_path: Path | None,
    input_path: Path,
    output_path: Path | None,
) -> int:
    logger = configure_logging(level=cli_log_level())
    config = load_config(config_path)
    payload = normalize_reddit_thread(
        config=config,
        logger=logger,
        trace=TraceContext.create(run_id="cli-normalize", source_name="reddit"),
        input_path=input_path,
        output_path=output_path,
    )
    emit_payload(payload)
    return 0


def run_normalized_analyze(
    config_path: Path | None,
    input_path: Path,
    output_path: Path | None,
) -> int:
    logger = configure_logging(level=cli_log_level())
    config = load_config(config_path)
    payload = analyze_normalized_thread(
        config=config,
        logger=logger,
        trace=TraceContext.create(run_id="cli-analyze", source_name="reddit"),
        input_path=input_path,
        output_path=output_path,
    )
    emit_payload(payload)
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
    logger = configure_logging(level=cli_log_level())
    config = load_config(config_path)
    payload = infer_analysis(
        config=config,
        logger=logger,
        trace=TraceContext.create(run_id="cli-infer", source_name="reddit"),
        input_path=input_path,
        task=InferenceTask(task_name),
        required=required,
    )
    emit_payload(payload)
    return 0 if payload["status"] != "degraded" or not required else 1


def run_analysis_report(
    config_path: Path | None,
    input_path: Path,
    output_path: Path | None,
    report_format: str,
    with_summary: bool,
    summary_required: bool,
) -> int:
    logger = configure_logging(level=cli_log_level())
    config = load_config(config_path)
    payload = report_analysis(
        config=config,
        logger=logger,
        trace=TraceContext.create(run_id="cli-report", source_name="reddit"),
        input_path=input_path,
        output_path=output_path,
        report_format=report_format,
        with_summary=with_summary,
        summary_required=summary_required,
    )
    emit_payload(payload)
    return 0


def run_batch(
    config_path: Path | None,
    manifest_path: Path,
    output_path: Path | None,
) -> int:
    logger = configure_logging(level=cli_log_level())
    config = load_config(config_path)
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


def run_reddit_end_to_end(
    config_path: Path | None,
    url: str,
    expand_more: bool,
    flat: bool,
    report_format: str,
    with_summary: bool,
    summary_required: bool,
) -> int:
    logger = configure_logging(level=cli_log_level())
    config = load_config(config_path)
    with status("Running Reddit workflow"):
        payload = run_reddit_pipeline(
            config=config,
            logger=logger,
            trace=TraceContext.create(run_id="cli-run", source_name="reddit"),
            url=url,
            expand_more=expand_more,
            flat=flat,
            report_format=report_format,
            with_summary=with_summary,
            summary_required=summary_required,
            connector_factory=build_reddit_connector,
        )
    emit_payload(payload)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "preflight":
            return run_preflight(args.config, args.skip_runtime)
        if args.command == "fetch" and args.source == "reddit":
            return run_reddit_fetch(
                config_path=args.config,
                url=args.url,
                output_path=args.output,
                expand_more=args.expand_more,
                flat=args.flat,
            )
        if args.command == "normalize" and args.source == "reddit":
            return run_reddit_normalize(
                config_path=args.config,
                input_path=args.input,
                output_path=args.output,
            )
        if args.command == "analyze" and args.artifact_type == "normalized":
            return run_normalized_analyze(
                config_path=args.config,
                input_path=args.input,
                output_path=args.output,
            )
        if args.command == "inspect" and args.artifact_type == "normalized":
            return run_normalized_inspect(args.input)
        if args.command == "inspect" and args.artifact_type == "analysis":
            return run_analysis_inspect(args.input)
        if args.command == "inspect" and args.artifact_type == "report":
            return run_report_inspect(args.input)
        if args.command == "infer" and args.artifact_type == "analysis":
            return run_analysis_infer(
                config_path=args.config,
                input_path=args.input,
                task_name=args.task,
                required=args.required,
            )
        if args.command == "report" and args.artifact_type == "analysis":
            return run_analysis_report(
                config_path=args.config,
                input_path=args.input,
                output_path=args.output,
                report_format=args.format,
                with_summary=args.with_summary,
                summary_required=args.summary_required,
            )
        if args.command == "batch" and args.batch_command == "run":
            return run_batch(
                config_path=args.config,
                manifest_path=args.manifest,
                output_path=args.output,
            )
        if args.command == "serve":
            return run_api_server(
                config_path=args.config,
                host=args.host,
                port=args.port,
            )
        if args.command == "run" and args.source == "reddit":
            return run_reddit_end_to_end(
                config_path=args.config,
                url=args.url,
                expand_more=args.expand_more,
                flat=args.flat,
                report_format=args.format,
                with_summary=args.with_summary,
                summary_required=args.summary_required,
            )
    except ThreadSenseError as error:
        emit_error(error)
        return 1

    parser.error(f"unknown command: {args.command}")
    return 2
