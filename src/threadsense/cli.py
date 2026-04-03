from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path

from threadsense.config import AppConfig, load_config
from threadsense.connectors.reddit import (
    RedditConnector,
    RedditThreadRequest,
)
from threadsense.errors import ThreadSenseError
from threadsense.inference import InferenceRouter, InferenceTask
from threadsense.inference.local_runtime import LocalRuntimeClient, RuntimeProbeResult
from threadsense.logging_config import configure_logging
from threadsense.pipeline.analyze import analyze_thread_file
from threadsense.pipeline.normalize import normalize_reddit_artifact_file
from threadsense.pipeline.storage import (
    build_storage_paths,
    load_analysis_artifact,
    load_normalized_artifact,
    load_report_artifact,
    persist_analysis_artifact,
    persist_normalized_artifact,
    persist_raw_artifact,
    persist_report_artifact,
    write_text,
)
from threadsense.reporting import build_thread_report, render_report_markdown


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
    configure_logging()
    config = load_config(config_path)
    probe: RuntimeProbeResult | None = None
    if not skip_runtime:
        probe = LocalRuntimeClient(config.runtime).probe()

    print(render_preflight_report(config, probe))
    return 0 if probe is None or probe.ok else 1


def build_reddit_connector(config: AppConfig) -> RedditConnector:
    return RedditConnector(config.reddit)


def run_reddit_fetch(
    config_path: Path | None,
    url: str,
    output_path: Path,
    expand_more: bool,
    flat: bool,
) -> int:
    configure_logging()
    config = load_config(config_path)
    result = build_reddit_connector(config).fetch_thread(
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
    print(
        json.dumps(
            {
                "status": "ready",
                "source": "reddit",
                "output_path": str(resolved_output_path),
                "default_store_path": str(storage_paths.raw_path),
                "normalized_url": result.normalized_url,
                "post_id": result.post.id,
                "post_title": result.post.title,
                "total_comment_count": result.total_comment_count,
                "expanded_more_count": result.expanded_more_count,
                "flat": flat,
            },
            indent=2,
        )
    )
    return 0


def run_reddit_normalize(
    config_path: Path | None,
    input_path: Path,
    output_path: Path | None,
) -> int:
    configure_logging()
    config = load_config(config_path)
    thread = normalize_reddit_artifact_file(input_path)
    storage_paths = build_storage_paths(config.storage, "reddit", thread.source.source_thread_id)
    resolved_output_path = output_path or storage_paths.normalized_path
    persist_normalized_artifact(resolved_output_path, thread)
    print(
        json.dumps(
            {
                "status": "ready",
                "artifact_type": "normalized",
                "input_path": str(input_path),
                "output_path": str(resolved_output_path),
                "default_store_path": str(storage_paths.normalized_path),
                "thread_id": thread.thread_id,
                "comment_count": thread.comment_count,
                "schema_version": thread.provenance.schema_version,
            },
            indent=2,
        )
    )
    return 0


def run_normalized_analyze(
    config_path: Path | None,
    input_path: Path,
    output_path: Path | None,
) -> int:
    configure_logging()
    config = load_config(config_path)
    analysis = analyze_thread_file(input_path)
    storage_paths = build_storage_paths(
        config.storage,
        analysis.source_name,
        analysis.provenance.source_thread_id,
    )
    resolved_output_path = output_path or storage_paths.analysis_path
    persist_analysis_artifact(resolved_output_path, analysis)
    print(
        json.dumps(
            {
                "status": "ready",
                "artifact_type": "analysis",
                "input_path": str(input_path),
                "output_path": str(resolved_output_path),
                "default_store_path": str(storage_paths.analysis_path),
                "thread_id": analysis.thread_id,
                "finding_count": len(analysis.findings),
                "duplicate_group_count": analysis.duplicate_group_count,
                "top_phrases": analysis.top_phrases[:5],
            },
            indent=2,
        )
    )
    return 0


def run_normalized_inspect(input_path: Path) -> int:
    configure_logging()
    thread = load_normalized_artifact(input_path)
    comment_ids = [comment.comment_id for comment in thread.comments[:10]]
    print(
        json.dumps(
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
            },
            indent=2,
        )
    )
    return 0


def run_analysis_inspect(input_path: Path) -> int:
    configure_logging()
    analysis = load_analysis_artifact(input_path)
    print(
        json.dumps(
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
            },
            indent=2,
        )
    )
    return 0


def run_analysis_infer(
    config_path: Path | None,
    input_path: Path,
    task_name: str,
    required: bool,
) -> int:
    configure_logging()
    config = load_config(config_path)
    analysis = load_analysis_artifact(input_path)
    response = InferenceRouter(config).run_analysis_task(
        analysis=analysis,
        task=InferenceTask(task_name),
        required=required,
    )
    print(
        json.dumps(
            {
                "status": "ready" if not response.degraded else "degraded",
                "artifact_type": "analysis",
                "input_path": str(input_path),
                "thread_id": analysis.thread_id,
                "task": response.task.value,
                "provider": response.provider,
                "model": response.model,
                "used_fallback": response.used_fallback,
                "failure_reason": response.failure_reason,
                "output": response.output,
            },
            indent=2,
        )
    )
    return 0 if not response.degraded or not required else 1


def run_analysis_report(
    config_path: Path | None,
    input_path: Path,
    output_path: Path | None,
    report_format: str,
    with_summary: bool,
    summary_required: bool,
) -> int:
    configure_logging()
    config = load_config(config_path)
    analysis = load_analysis_artifact(input_path)
    summary_response = None
    if with_summary:
        summary_response = InferenceRouter(config).run_analysis_task(
            analysis=analysis,
            task=InferenceTask.ANALYSIS_SUMMARY,
            required=summary_required,
        )

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

    print(
        json.dumps(
            {
                "status": "ready",
                "artifact_type": "report",
                "input_path": str(input_path),
                "output_path": str(resolved_output_path),
                "default_store_path": str(default_output_path),
                "format": report_format,
                "thread_id": report.thread_id,
                "summary_provider": report.provenance.summary_provider,
                "degraded_summary": report.executive_summary.degraded,
                "quality_check_count": len(report.quality_checks),
            },
            indent=2,
        )
    )
    return 0


def run_report_inspect(input_path: Path) -> int:
    configure_logging()
    report = load_report_artifact(input_path)
    print(
        json.dumps(
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
            },
            indent=2,
        )
    )
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
    except ThreadSenseError as error:
        print(json.dumps({"status": "error", "error": error.to_dict()}, indent=2))
        return 1

    parser.error(f"unknown command: {args.command}")
    return 2
