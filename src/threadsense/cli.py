from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path

from threadsense.config import AppConfig, load_config
from threadsense.connectors.reddit import (
    RedditConnector,
    RedditThreadRequest,
    write_thread_artifact,
)
from threadsense.errors import ThreadSenseError
from threadsense.inference.local_runtime import LocalRuntimeClient, RuntimeProbeResult
from threadsense.logging_config import configure_logging


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
        required=True,
        help="Artifact output path.",
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
    return parser


def render_preflight_report(config: AppConfig, probe: RuntimeProbeResult | None) -> str:
    report: dict[str, object] = {
        "status": "ready" if probe is None or probe.ok else "degraded",
        "backend": config.inference_backend.value,
        "privacy_mode": config.privacy_mode.value,
        "sources": list(config.source_policy.enabled_sources),
        "runtime": {
            "base_url": config.runtime.base_url,
            "chat_endpoint": config.runtime.chat_endpoint,
            "model": config.runtime.model,
            "timeout_seconds": config.runtime.timeout_seconds,
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
    write_thread_artifact(output_path, result)
    print(
        json.dumps(
            {
                "status": "ready",
                "source": "reddit",
                "output_path": str(output_path),
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
    except ThreadSenseError as error:
        print(json.dumps({"status": "error", "error": error.to_dict()}, indent=2))
        return 1

    parser.error(f"unknown command: {args.command}")
    return 2
