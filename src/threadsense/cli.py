from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from threadsense.api_server import start_api_server
from threadsense.batching import run_batch_manifest
from threadsense.cli_display import cli_log_level, emit_error, emit_payload, status
from threadsense.config import AppConfig, effective_config_path, load_config
from threadsense.connectors.hackernews import HackerNewsConnector
from threadsense.connectors.reddit import RedditConnector
from threadsense.contracts import (
    AbstractionLevel,
    AnalysisContract,
    DomainType,
    ObjectiveType,
    contract_from_config,
)
from threadsense.decisions.annotate import AnnotationJob, annotate_canonical_thread
from threadsense.decisions.client import TypedDecisionClient
from threadsense.decisions.contracts import Requirement
from threadsense.decisions.evaluate import EvaluationJob, evaluate_frozen_assessment
from threadsense.decisions.policy import check_activation_policy, publish_policy_check
from threadsense.decisions.runtime import CheckpointVerifier, load_pinned_runtime_manifest
from threadsense.errors import DecisionInputError, ThreadSenseError
from threadsense.evaluation import load_golden_manifest
from threadsense.inference import InferenceTask
from threadsense.inference.local_runtime import LocalRuntimeClient, RuntimeProbeResult
from threadsense.logging_config import configure_logging
from threadsense.monitoring import (
    CORRECTION_VALUES,
    PREFERENCE_VALUES,
    TARGET_KINDS,
    FeedbackRequest,
    MonitoringStore,
    build_schedule_spec,
    feedback_request,
    load_watch_profile_file,
    monitoring_state_path,
    present_watch_digest,
    render_cron,
    render_launchd,
    run_watch_sweep,
)
from threadsense.monitoring.explain import explain_digest_entry
from threadsense.monitoring.feedback_records import FEEDBACK_KINDS
from threadsense.monitoring.issues import issue_policy
from threadsense.monitoring.ranking import load_ranking_policy_file
from threadsense.monitoring.schedule import (
    ScheduleFormat,
    render_install_instructions,
    resolve_storage_root,
    resolve_workspace,
)
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


def _build_watch_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    watch_parser = subparsers.add_parser(
        "watch",
        help="Manage saved research profiles and their persistent monitoring state.",
    )
    watch_subparsers = watch_parser.add_subparsers(dest="watch_command", required=True)

    watch_create_parser = watch_subparsers.add_parser(
        "create",
        help="Validate a profile and save it as a new watch or a new revision.",
    )
    watch_create_parser.add_argument(
        "--profile",
        type=Path,
        required=True,
        help="Path to a TOML watch profile.",
    )
    _add_config_argument(watch_create_parser)

    watch_list_parser = watch_subparsers.add_parser("list", help="List saved watches.")
    _add_config_argument(watch_list_parser)

    watch_inspect_parser = watch_subparsers.add_parser(
        "inspect",
        help="Show a watch profile, its revision history and recent run claims.",
    )
    watch_inspect_parser.add_argument("--id", dest="watch_id", required=True, help="Watch id.")
    _add_config_argument(watch_inspect_parser)
    _build_watch_execution_parsers(watch_subparsers)
    _build_watch_ranking_parsers(watch_subparsers)


def _build_issues_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    issues_parser = subparsers.add_parser(
        "issues",
        help=(
            "Inspect cross-thread issues and record operator membership decisions. "
            "Suggestions are never merged automatically."
        ),
    )
    issues_subparsers = issues_parser.add_subparsers(dest="issues_command", required=True)

    inspect_parser = issues_subparsers.add_parser(
        "inspect",
        help=(
            "List a watch's issues, or show one issue with its membership history and "
            "suggestions. Read-only: inspect never writes state."
        ),
    )
    inspect_parser.add_argument("--watch", dest="watch_id", required=True, help="Watch id.")
    inspect_parser.add_argument(
        "--issue",
        dest="issue_id",
        default=None,
        help="Issue id; omit to list all issues for the watch.",
    )
    _add_config_argument(inspect_parser)

    for command, help_text in (
        ("link", "Link a thread to an issue by appending a membership event (append-only)."),
        ("unlink", "Unlink a thread from an issue by appending a membership event (append-only)."),
    ):
        membership_parser = issues_subparsers.add_parser(
            command,
            help=f"{help_text} Suggestions are never merged automatically.",
        )
        membership_parser.add_argument("--watch", dest="watch_id", required=True, help="Watch id.")
        membership_parser.add_argument("--issue", dest="issue_id", required=True, help="Issue id.")
        membership_parser.add_argument(
            "--thread",
            dest="source_thread_id",
            required=True,
            help="Source thread id.",
        )
        membership_parser.add_argument(
            "--reason",
            required=True,
            help="Operator reason recorded in the append-only membership event.",
        )
        _add_config_argument(membership_parser)


def _build_feedback_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    feedback_parser = subparsers.add_parser(
        "feedback",
        help=(
            "Record and list operator feedback. Preferences change presentation only; "
            "corrections are assertions that never rewrite evidence."
        ),
    )
    feedback_subparsers = feedback_parser.add_subparsers(dest="feedback_command", required=True)

    record_parser = feedback_subparsers.add_parser(
        "record",
        help="Append one preference or correction event for a target the watch owns.",
    )
    record_parser.add_argument("--watch", dest="watch_id", required=True, help="Watch id.")
    record_parser.add_argument(
        "--target",
        dest="target_id",
        required=True,
        help="Target id: a digest entry (ent-), thread revision (rev-) or issue (iss-).",
    )
    record_parser.add_argument(
        "--target-kind",
        dest="target_kind",
        choices=TARGET_KINDS,
        default=None,
        help=(
            "Target kind; defaults to the narrowest kind the id names. 'finding' widens a "
            "preference to the thread's finding stream for that theme."
        ),
    )
    record_parser.add_argument(
        "--kind",
        choices=FEEDBACK_KINDS,
        required=True,
        help="Feedback kind.",
    )
    record_parser.add_argument(
        "--value",
        choices=(*PREFERENCE_VALUES, *CORRECTION_VALUES),
        required=True,
        help=(
            f"Preference values: {', '.join(PREFERENCE_VALUES)}. "
            f"Correction values: {', '.join(CORRECTION_VALUES)}."
        ),
    )
    record_parser.add_argument(
        "--key",
        dest="idempotency_key",
        required=True,
        help=(
            "Watch-scoped idempotency key. Repeating the same key with identical feedback "
            "returns the original event; a different event under the same key is refused."
        ),
    )
    record_parser.add_argument(
        "--note",
        default=None,
        help="Optional operator note, at most 500 characters.",
    )
    _add_config_argument(record_parser)

    list_parser = feedback_subparsers.add_parser(
        "list",
        help=(
            "Show a watch's preference and correction histories with their derived views. "
            "Read-only: list never writes state."
        ),
    )
    list_parser.add_argument("--watch", dest="watch_id", required=True, help="Watch id.")
    list_parser.add_argument(
        "--target",
        dest="target_id",
        default=None,
        help="Target id; omit to list all feedback for the watch.",
    )
    _add_config_argument(list_parser)


def _add_watch_lease_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--lease-seconds",
        type=int,
        default=3600,
        help="Run claim lease in seconds (1-86400).",
    )


def _build_watch_execution_parsers(
    watch_subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    watch_run_parser = watch_subparsers.add_parser(
        "run",
        help="Sweep a watch for changed discussions, then present a digest of new evidence.",
    )
    watch_run_parser.add_argument("--id", dest="watch_id", required=True, help="Watch id.")
    _add_watch_lease_argument(watch_run_parser)
    _add_config_argument(watch_run_parser)

    watch_digest_parser = watch_subparsers.add_parser(
        "digest",
        help="Present a digest of analyzed evidence not yet presented, without sweeping.",
    )
    watch_digest_parser.add_argument("--id", dest="watch_id", required=True, help="Watch id.")
    _add_watch_lease_argument(watch_digest_parser)
    _add_config_argument(watch_digest_parser)

    watch_schedule_parser = watch_subparsers.add_parser(
        "schedule",
        help="Render a launchd or cron definition for a watch; nothing is installed or enabled.",
    )
    watch_schedule_parser.add_argument("--id", dest="watch_id", required=True, help="Watch id.")
    watch_schedule_parser.add_argument(
        "--format",
        dest="schedule_format",
        choices=["launchd", "cron"],
        required=True,
        help="Scheduler definition format.",
    )
    watch_schedule_parser.add_argument(
        "--workspace",
        type=Path,
        default=None,
        help="Working directory for scheduled runs (defaults to the current directory).",
    )
    _add_config_argument(watch_schedule_parser)


def _build_watch_ranking_parsers(
    watch_subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    watch_explain_parser = watch_subparsers.add_parser(
        "explain",
        help=(
            "Explain a digest entry's rank. Read-only: replays the recorded profile, policy "
            "and preference basis and refuses unless it equals the written digest."
        ),
    )
    watch_explain_parser.add_argument("--id", dest="watch_id", required=True, help="Watch id.")
    watch_explain_parser.add_argument(
        "--entry",
        dest="entry_id",
        required=True,
        help="Digest entry id (ent-).",
    )
    watch_explain_parser.add_argument(
        "--current",
        action="store_true",
        help="Also show how the same entries would rank now; derived on demand, never recorded.",
    )
    _add_config_argument(watch_explain_parser)

    watch_policy_parser = watch_subparsers.add_parser(
        "policy",
        help=(
            "Show or select a watch's ranking policy. Policies are immutable and "
            "content-addressed (pol- ids); selecting appends a selection event, affects only "
            "digests recorded afterwards and never rewrites written digests."
        ),
    )
    watch_policy_parser.add_argument("--id", dest="watch_id", required=True, help="Watch id.")
    policy_group = watch_policy_parser.add_mutually_exclusive_group()
    policy_group.add_argument(
        "--set",
        dest="policy_path",
        type=Path,
        default=None,
        help="Path to a TOML ranking policy to record (if new) and select.",
    )
    policy_group.add_argument(
        "--select",
        dest="policy_id",
        default=None,
        help="Recorded policy id (pol-) to switch back to, including the built-in default.",
    )
    _add_config_argument(watch_policy_parser)


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


def _build_decisions_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    decisions_parser = subparsers.add_parser(
        "decisions",
        help="Shadow typed semantic decisions from a separate local Laya service.",
    )
    decisions_subparsers = decisions_parser.add_subparsers(dest="decisions_command", required=True)

    annotate_parser = decisions_subparsers.add_parser(
        "annotate",
        help="Annotate a canonical thread artifact with typed decisions from the Laya service.",
    )
    annotate_parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Canonical thread artifact (normalized artifact or monitoring revision snapshot).",
    )
    annotate_parser.add_argument(
        "--endpoint",
        required=True,
        help="Literal-loopback http URL of the owned typed-decision service.",
    )
    annotate_parser.add_argument(
        "--requirement",
        choices=[requirement.value for requirement in Requirement],
        default=Requirement.OPTIONAL.value,
        help="optional: shadow run; required: exit nonzero on any coverage gap.",
    )
    annotate_parser.add_argument(
        "--domain",
        choices=[domain.value for domain in DomainType if domain is not DomainType.CUSTOM],
        help="Analysis domain vocabulary.",
    )
    annotate_parser.add_argument(
        "--objective",
        choices=[objective.value for objective in ObjectiveType],
        help="Analysis objective.",
    )
    annotate_parser.add_argument(
        "--output-dir",
        type=Path,
        help="Annotation artifact root. Defaults to <storage root>/<annotations dirname>.",
    )
    annotate_parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=300.0,
        help="Timeout for each HTTP call to the typed-decision service.",
    )
    _add_config_argument(annotate_parser)

    evaluate_parser = decisions_subparsers.add_parser(
        "evaluate",
        help="Assess saved typed decisions; optionally compare with the configured local runtime.",
    )
    evaluate_parser.add_argument(
        "--manifest",
        required=True,
        type=Path,
        help="Frozen evaluation dataset manifest (typed_decision_evaluation_manifest JSON).",
    )
    evaluate_parser.add_argument(
        "--annotation",
        required=True,
        action="append",
        type=Path,
        dest="annotations",
        help="Saved decision document; repeat once per manifest thread.",
    )
    evaluate_parser.add_argument(
        "--output-dir",
        type=Path,
        help="Artifact root. Defaults to <storage root>/<annotations dirname>.",
    )
    evaluate_parser.add_argument(
        "--generative-capture",
        type=Path,
        help="Replay a content-addressed generative capture offline without model requests.",
    )
    _add_config_argument(evaluate_parser)

    policy_parser = decisions_subparsers.add_parser(
        "policy",
        help="Check typed decision activation eligibility offline; never activates anything.",
    )
    policy_subparsers = policy_parser.add_subparsers(dest="policy_command", required=True)
    policy_check_parser = policy_subparsers.add_parser(
        "check",
        help="Check a saved evaluation against an optional activation policy.",
    )
    policy_check_parser.add_argument(
        "--evaluation",
        required=True,
        type=Path,
        help="Saved evaluation document (<evaluation_sha256>.json from decisions evaluate).",
    )
    policy_check_parser.add_argument(
        "--policy",
        type=Path,
        help="Operator-authored activation policy JSON. Omitted: reported as policy_not_declared.",
    )
    policy_check_parser.add_argument(
        "--output-dir",
        type=Path,
        help="Artifact root. Defaults to <storage root>/<annotations dirname>.",
    )
    _add_config_argument(policy_check_parser)


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
    _build_watch_parser(subparsers)
    _build_issues_parser(subparsers)
    _build_feedback_parser(subparsers)
    _build_decisions_parser(subparsers)
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


def load_monitoring_store(config_path: Path | None) -> MonitoringStore:
    _logger, config = load_cli_context(config_path)
    return MonitoringStore(monitoring_state_path(config.storage))


def run_watch_create(config_path: Path | None, profile_path: Path) -> int:
    store = load_monitoring_store(config_path)
    result = store.save_profile(load_watch_profile_file(profile_path))
    emit_payload(
        {
            "artifact_type": "watch_profile",
            **result.to_dict(),
            "state_path": str(store.path),
        }
    )
    return 0


def run_watch_list(config_path: Path | None) -> int:
    store = load_monitoring_store(config_path)
    emit_payload(
        {
            "artifact_type": "watch_list",
            "status": "ok",
            "state_path": str(store.path),
            "watches": store.list_watches(),
        }
    )
    return 0


def run_watch_inspect(config_path: Path | None, watch_id: str) -> int:
    store = load_monitoring_store(config_path)
    emit_payload(
        {
            "artifact_type": "watch_inspection",
            "status": "ok",
            "state_path": str(store.path),
            **store.inspect_watch(watch_id),
        }
    )
    return 0


def run_issues_inspect(config_path: Path | None, watch_id: str, issue_id: str | None) -> int:
    store = load_monitoring_store(config_path)
    if issue_id is None:
        emit_payload(
            {
                "artifact_type": "issue_list",
                "status": "ok",
                "state_path": str(store.path),
                "watch_id": watch_id,
                "issues": store.list_issues(watch_id),
            }
        )
        return 0
    # The issue's own lifecycle status must not replace the command status.
    issue = store.inspect_issue(watch_id, issue_id)
    issue_status = issue.pop("status")
    emit_payload(
        {
            "artifact_type": "issue_inspection",
            "status": "ok",
            "state_path": str(store.path),
            "issue_status": issue_status,
            **issue,
        }
    )
    return 0


def run_issues_link(
    config_path: Path | None,
    watch_id: str,
    issue_id: str,
    source_thread_id: str,
    reason: str,
) -> int:
    store = load_monitoring_store(config_path)
    emit_payload(
        {
            "artifact_type": "issue_membership_change",
            "status": "linked",
            "state_path": str(store.path),
            **store.link_issue_thread(watch_id, issue_id, source_thread_id, reason),
        }
    )
    return 0


def run_issues_unlink(
    config_path: Path | None,
    watch_id: str,
    issue_id: str,
    source_thread_id: str,
    reason: str,
) -> int:
    store = load_monitoring_store(config_path)
    emit_payload(
        {
            "artifact_type": "issue_membership_change",
            "status": "unlinked",
            "state_path": str(store.path),
            **store.unlink_issue_thread(watch_id, issue_id, source_thread_id, reason),
        }
    )
    return 0


def run_feedback_record(config_path: Path | None, request: FeedbackRequest) -> int:
    store = load_monitoring_store(config_path)
    result = store.record_feedback(request)
    emit_payload(
        {
            "artifact_type": "feedback_event",
            "status": result["status"],
            "state_path": str(store.path),
            "feedback": result["feedback"],
        }
    )
    return 0


def run_feedback_list(config_path: Path | None, watch_id: str, target_id: str | None) -> int:
    store = load_monitoring_store(config_path)
    emit_payload(
        {
            "artifact_type": "feedback_history",
            "status": "ok",
            "state_path": str(store.path),
            **store.feedback_history(watch_id, target_id),
        }
    )
    return 0


def watch_trace(run_id: str) -> TraceContext:
    return TraceContext.create(run_id=run_id, source_name="watch")


def run_watch_run(config_path: Path | None, watch_id: str, lease_seconds: int) -> int:
    logger, config = load_cli_context(config_path)
    store = MonitoringStore(monitoring_state_path(config.storage))
    trace = watch_trace("cli-watch-run")
    sweep = run_watch_sweep(
        config=config,
        store=store,
        watch_id=watch_id,
        lease_seconds=lease_seconds,
        connector_factory=build_reddit_connector,
        logger=logger,
        trace=trace,
    )
    digest = present_watch_digest(
        store=store,
        watch_id=watch_id,
        lease_seconds=lease_seconds,
        issue_policy=issue_policy(config.issues),
        logger=logger,
        trace=trace,
    )
    emit_payload(
        {
            "artifact_type": "watch_run",
            "status": sweep.outcome,
            "state_path": str(store.path),
            "sweep": sweep.to_dict(),
            "digest": digest.to_dict(),
        }
    )
    return 0 if sweep.outcome in ("changes", "no_changes") else 1


def run_watch_digest(config_path: Path | None, watch_id: str, lease_seconds: int) -> int:
    logger, config = load_cli_context(config_path)
    store = MonitoringStore(monitoring_state_path(config.storage))
    digest = present_watch_digest(
        store=store,
        watch_id=watch_id,
        lease_seconds=lease_seconds,
        issue_policy=issue_policy(config.issues),
        logger=logger,
        trace=watch_trace("cli-watch-digest"),
    )
    emit_payload(
        {
            "artifact_type": "watch_digest_result",
            "status": digest.status,
            "state_path": str(store.path),
            **digest.to_dict(),
        }
    )
    return 0


def run_watch_schedule(
    config_path: Path | None,
    watch_id: str,
    schedule_format: ScheduleFormat,
    workspace: Path | None,
) -> int:
    _logger, config = load_cli_context(config_path)
    resolved_workspace = resolve_workspace(workspace if workspace is not None else Path.cwd())
    storage = config.storage.model_copy(
        update={"root_dir": resolve_storage_root(config.storage.root_dir, resolved_workspace)}
    )
    store = MonitoringStore(monitoring_state_path(storage))
    inspection = store.inspect_watch(watch_id)
    spec = build_schedule_spec(
        watch_id=watch_id,
        refresh_interval_minutes=inspection["profile"]["refresh_interval_minutes"],
        workspace=resolved_workspace,
        storage_root=storage.root_dir,
        # launchd and cron do not inherit the shell, so pin the config this render loaded.
        config_path=effective_config_path(config_path, os.environ),
        executable=Path(sys.executable).absolute(),
        monitoring_dirname=storage.monitoring_dirname,
    )
    content = render_launchd(spec) if schedule_format == "launchd" else render_cron(spec)
    emit_payload(
        {
            "artifact_type": "watch_schedule",
            "status": "rendered",
            "format": schedule_format,
            "watch_id": spec.watch_id,
            "installed": False,
            "enabled": False,
            "executable": str(spec.executable),
            "workspace": str(spec.workspace),
            "storage_root": str(spec.storage_root),
            "config_path": str(spec.config_path) if spec.config_path is not None else None,
            "interval_seconds": spec.interval_seconds,
            "lease_seconds": spec.lease_seconds,
            "argv": list(spec.argv),
            "log_path": str(spec.log_path),
            "overlap_policy": "run_claim",
            "content": content,
            "install_instructions": render_install_instructions(spec, schedule_format),
        }
    )
    return 0


def run_watch_explain(
    config_path: Path | None, watch_id: str, entry_id: str, include_current: bool
) -> int:
    store = load_monitoring_store(config_path)
    explanation = explain_digest_entry(store, watch_id, entry_id, include_current=include_current)
    emit_payload(
        {
            "artifact_type": "watch_explanation",
            "status": "ok",
            "state_path": str(store.path),
            **explanation,
        }
    )
    return 0


def run_watch_policy(
    config_path: Path | None,
    watch_id: str,
    policy_path: Path | None,
    policy_id: str | None,
) -> int:
    store = load_monitoring_store(config_path)
    if policy_path is not None:
        result = store.select_ranking_policy(watch_id, load_ranking_policy_file(policy_path))
    elif policy_id is not None:
        result = store.select_stored_ranking_policy(watch_id, policy_id)
    else:
        emit_payload(
            {
                "artifact_type": "ranking_policy",
                "status": "ok",
                "state_path": str(store.path),
                **store.ranking_policy_status(watch_id),
            }
        )
        return 0
    emit_payload(
        {
            "artifact_type": "ranking_policy_selection",
            "status": result["status"],
            "state_path": str(store.path),
            "watch_id": watch_id,
            "policy_id": result["policy_id"],
            "previous_policy_id": result["previous_policy_id"],
            "active_policy": store.ranking_policy_status(watch_id)["active_policy"],
        }
    )
    return 0


@dataclass(frozen=True)
class DecisionsAnnotateOptions:
    input_path: Path
    endpoint: str
    requirement: Requirement
    domain: str | None
    objective: str | None
    output_dir: Path | None
    timeout_seconds: float


def _annotation_roots(config: AppConfig, output_dir: Path | None) -> tuple[Path, tuple[Path, ...]]:
    storage = config.storage
    relative = Path(storage.annotations_dirname)
    if (
        relative.is_absolute()
        or not relative.parts
        or any(part in ("", ".", "..") for part in relative.parts)
    ):
        raise DecisionInputError(
            "annotations directory must be a relative path inside the storage root",
            details={
                "reason": "annotations_dirname_invalid",
                "annotations_dirname": storage.annotations_dirname,
            },
        )
    root = storage.root_dir
    protected_roots = tuple(
        root / dirname
        for dirname in (
            storage.raw_dirname,
            storage.normalized_dirname,
            storage.analysis_dirname,
            storage.report_dirname,
            storage.batch_dirname,
            storage.corpus_dirname,
            storage.index_dirname,
            storage.monitoring_dirname,
        )
    )
    return (output_dir if output_dir is not None else root / relative), protected_roots


def run_decisions_annotate(config_path: Path | None, options: DecisionsAnnotateOptions) -> int:
    _logger, config = load_cli_context(config_path)
    domain = options.domain if options.domain is not None else config.analysis.domain.value
    if domain == DomainType.CUSTOM.value:
        raise DecisionInputError(
            "custom domain is not supported for decision annotations",
            details={"reason": "domain_unsupported", "domain": domain},
        )
    output_root, protected_roots = _annotation_roots(config, options.output_dir)
    manifest = load_pinned_runtime_manifest()
    verifier = CheckpointVerifier(manifest)
    job = AnnotationJob(
        input_path=options.input_path,
        output_root=output_root,
        protected_roots=protected_roots,
        requirement=options.requirement,
        objective=(
            ObjectiveType(options.objective)
            if options.objective is not None
            else config.analysis.objective
        ),
        domain=domain,
    )
    with TypedDecisionClient(
        options.endpoint, wire=manifest.wire, timeout_seconds=options.timeout_seconds
    ) as client:
        outcome = annotate_canonical_thread(
            job, client=client, manifest=manifest, verifier=verifier
        )
    emit_payload(
        {
            "artifact_type": "decision_annotations",
            "status": outcome.coverage.status.value,
            "mode": "shadow",
            **outcome.to_dict(),
        }
    )
    return 0 if outcome.satisfied else 1


@dataclass(frozen=True)
class DecisionsEvaluateOptions:
    manifest_path: Path
    annotation_paths: tuple[Path, ...]
    output_dir: Path | None
    generative_capture_path: Path | None = None


def run_decisions_evaluate(config_path: Path | None, options: DecisionsEvaluateOptions) -> int:
    _logger, config = load_cli_context(config_path)
    output_root, protected_roots = _annotation_roots(config, options.output_dir)
    job = EvaluationJob(
        manifest_path=options.manifest_path,
        annotation_paths=options.annotation_paths,
        output_root=output_root,
        protected_roots=protected_roots,
        generative_capture_path=options.generative_capture_path,
    )
    outcome = evaluate_frozen_assessment(
        job,
        runtime_manifest=load_pinned_runtime_manifest(),
        runtime_config=config.runtime,
    )
    emit_payload(
        {
            "artifact_type": "decision_evaluation",
            "status": "ready",
            "mode": "shadow",
            **outcome.to_dict(),
        }
    )
    return 0


@dataclass(frozen=True)
class DecisionsPolicyCheckOptions:
    evaluation_path: Path
    policy_path: Path | None
    output_dir: Path | None


def run_decisions_policy_check(
    config_path: Path | None, options: DecisionsPolicyCheckOptions
) -> int:
    _logger, config = load_cli_context(config_path)
    output_root, protected_roots = _annotation_roots(config, options.output_dir)
    result = check_activation_policy(
        policy_path=options.policy_path,
        evaluation_path=options.evaluation_path,
        runtime_manifest=load_pinned_runtime_manifest(),
    )
    declared_inputs = (options.evaluation_path, options.policy_path)
    outcome = publish_policy_check(
        result,
        output_root=output_root,
        protected_roots=protected_roots,
        input_paths=tuple(path for path in declared_inputs if path is not None and path.exists()),
    )
    emit_payload(
        {
            "artifact_type": "decision_policy_check",
            "status": result.eligibility.value,
            "mode": "shadow",
            **outcome.to_dict(),
        }
    )
    return 0


def _dispatch_decisions_command(args: argparse.Namespace) -> int:
    match args.decisions_command:
        case "annotate":
            return run_decisions_annotate(
                args.config,
                DecisionsAnnotateOptions(
                    input_path=args.input,
                    endpoint=args.endpoint,
                    requirement=Requirement(args.requirement),
                    domain=args.domain,
                    objective=args.objective,
                    output_dir=args.output_dir,
                    timeout_seconds=args.timeout_seconds,
                ),
            )
        case "evaluate":
            return run_decisions_evaluate(
                args.config,
                DecisionsEvaluateOptions(
                    manifest_path=args.manifest,
                    annotation_paths=tuple(args.annotations),
                    output_dir=args.output_dir,
                    generative_capture_path=args.generative_capture,
                ),
            )
        case "policy":
            return _dispatch_decisions_policy_command(args)
        case _:
            raise _CommandDispatchError(args.decisions_command)


def _dispatch_decisions_policy_command(args: argparse.Namespace) -> int:
    match args.policy_command:
        case "check":
            return run_decisions_policy_check(
                args.config,
                DecisionsPolicyCheckOptions(
                    evaluation_path=args.evaluation,
                    policy_path=args.policy,
                    output_dir=args.output_dir,
                ),
            )
        case _:
            raise _CommandDispatchError(args.policy_command)


def _dispatch_watch_command(args: argparse.Namespace) -> int:
    match args.watch_command:
        case "create":
            return run_watch_create(args.config, args.profile)
        case "list":
            return run_watch_list(args.config)
        case "inspect":
            return run_watch_inspect(args.config, args.watch_id)
        case "run":
            return run_watch_run(args.config, args.watch_id, args.lease_seconds)
        case "digest":
            return run_watch_digest(args.config, args.watch_id, args.lease_seconds)
        case "schedule":
            return run_watch_schedule(
                args.config, args.watch_id, args.schedule_format, args.workspace
            )
        case "explain":
            return run_watch_explain(args.config, args.watch_id, args.entry_id, args.current)
        case "policy":
            return run_watch_policy(args.config, args.watch_id, args.policy_path, args.policy_id)
        case _:
            raise _CommandDispatchError(args.command)


def _dispatch_issues_command(args: argparse.Namespace) -> int:
    match args.issues_command:
        case "inspect":
            return run_issues_inspect(args.config, args.watch_id, args.issue_id)
        case "link":
            return run_issues_link(
                args.config, args.watch_id, args.issue_id, args.source_thread_id, args.reason
            )
        case "unlink":
            return run_issues_unlink(
                args.config, args.watch_id, args.issue_id, args.source_thread_id, args.reason
            )
        case _:
            raise _CommandDispatchError(args.command)


def _dispatch_feedback_command(args: argparse.Namespace) -> int:
    match args.feedback_command:
        case "record":
            request = feedback_request(
                args.watch_id,
                idempotency_key=args.idempotency_key,
                kind=args.kind,
                value=args.value,
                target_id=args.target_id,
                target_kind=args.target_kind,
                note=args.note,
            )
            return run_feedback_record(args.config, request)
        case "list":
            return run_feedback_list(args.config, args.watch_id, args.target_id)
        case _:
            raise _CommandDispatchError(args.feedback_command)


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
        case ("watch", _, _, _, _):
            return _dispatch_watch_command(args)
        case ("issues", _, _, _, _):
            return _dispatch_issues_command(args)
        case ("feedback", _, _, _, _):
            return _dispatch_feedback_command(args)
        case ("decisions", _, _, _, _):
            return _dispatch_decisions_command(args)
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
