from __future__ import annotations

import json
import logging
import sys
from contextlib import AbstractContextManager, nullcontext
from typing import Any

from rich.console import Console
from rich.json import JSON
from rich.panel import Panel
from rich.table import Table

from threadsense.errors import ThreadSenseError

_CONSOLE = Console()
_ERROR_CONSOLE = Console(stderr=True)


def cli_log_level() -> int:
    return logging.WARNING if use_rich_output() else logging.INFO


def use_rich_output() -> bool:
    return sys.stdout.isatty()


def emit_payload(payload: dict[str, Any]) -> None:
    if not use_rich_output():
        print(json.dumps(payload, indent=2))
        return
    if is_run_payload(payload):
        _CONSOLE.print(render_run_panel(payload))
        return
    if payload.get("artifact_type") == "api_server":
        _CONSOLE.print(render_api_panel(payload))
        return
    _CONSOLE.print(JSON.from_data(payload))


def emit_error(error: ThreadSenseError) -> None:
    payload = {"status": "error", "error": error.to_dict()}
    if not use_rich_output():
        print(json.dumps(payload, indent=2))
        return
    details = error.details
    body = f"[bold red]{error.code}[/bold red]\n{error.message}"
    if details:
        body += "\n\n" + json.dumps(details, indent=2)
    _ERROR_CONSOLE.print(Panel(body, title="ThreadSense Error", border_style="red"))


def status(message: str) -> AbstractContextManager[object]:
    if not use_rich_output():
        return nullcontext()
    return _CONSOLE.status(message, spinner="dots")


def is_run_payload(payload: dict[str, Any]) -> bool:
    required_keys = {"fetch", "normalize", "analyze", "report", "thread_url", "source"}
    return required_keys.issubset(payload.keys())


def render_run_panel(payload: dict[str, Any]) -> Panel:
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Stage")
    table.add_column("Status")
    table.add_column("Artifact")
    for stage_name in ("fetch", "normalize", "analyze", "report"):
        stage_payload = payload.get(stage_name, {})
        if not isinstance(stage_payload, dict):
            continue
        artifact_path = stage_payload.get("output_path", "n/a")
        table.add_row(
            stage_name,
            str(stage_payload.get("status", "unknown")),
            str(artifact_path),
        )
    summary_provider = payload.get("report", {}).get("summary_provider", "n/a")
    title = f"ThreadSense Run: {payload.get('source', 'unknown')}"
    subtitle = f"{payload.get('thread_url', '')}\nsummary provider: {summary_provider}"
    return Panel(table, title=title, subtitle=subtitle, border_style="green")


def render_api_panel(payload: dict[str, Any]) -> Panel:
    body = (
        f"host: {payload.get('host')}\n"
        f"port: {payload.get('port')}\n"
        f"metrics: {payload.get('metrics_path')}"
    )
    return Panel(body, title="ThreadSense API", border_style="blue")
