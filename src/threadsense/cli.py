from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path

from threadsense.config import AppConfig, load_config
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


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "preflight":
            return run_preflight(args.config, args.skip_runtime)
    except ThreadSenseError as error:
        print(json.dumps({"status": "error", "error": error.to_dict()}, indent=2))
        return 1

    parser.error(f"unknown command: {args.command}")
    return 2
