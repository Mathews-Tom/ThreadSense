from __future__ import annotations

import json
import logging
import sys
from contextlib import AbstractContextManager, nullcontext
from enum import StrEnum
from typing import Any

from rich.console import Console
from rich.json import JSON
from rich.panel import Panel
from rich.text import Text
from rich.table import Table

from threadsense.errors import ThreadSenseError


class OutputMode(StrEnum):
    JSON = "json"
    HUMAN = "human"
    QUIET = "quiet"


_output_mode: OutputMode | None = None

_CONSOLE = Console()
_ERROR_CONSOLE = Console(stderr=True)


def set_output_mode(mode: OutputMode | None) -> None:
    global _output_mode  # noqa: PLW0603
    _output_mode = mode


def resolve_output_mode() -> OutputMode:
    if _output_mode is not None:
        return _output_mode
    return OutputMode.HUMAN if sys.stdout.isatty() else OutputMode.JSON


def cli_log_level() -> int:
    return logging.WARNING if resolve_output_mode() is not OutputMode.JSON else logging.INFO


def use_rich_output() -> bool:
    return resolve_output_mode() is OutputMode.HUMAN


def emit_payload(payload: dict[str, Any]) -> None:
    mode = resolve_output_mode()
    if mode is OutputMode.JSON:
        print(json.dumps(payload, indent=2))
        return
    if mode is OutputMode.QUIET:
        _emit_quiet(payload)
        return
    # HUMAN mode
    if is_run_payload(payload):
        _CONSOLE.print(render_run_panel(payload))
        return
    if payload.get("artifact_type") == "api_server":
        _CONSOLE.print(render_api_panel(payload))
        return
    _CONSOLE.print(JSON.from_data(payload))


def _emit_quiet(payload: dict[str, Any]) -> None:
    status_val = payload.get("status", "unknown")
    if status_val == "error":
        error_info = payload.get("error", {})
        msg = (
            error_info.get("message", "unknown error")
            if isinstance(error_info, dict)
            else str(error_info)
        )
        print(f"error: {msg}")
    else:
        print(status_val)


def emit_error(error: ThreadSenseError) -> None:
    mode = resolve_output_mode()
    payload = {"status": "error", "error": error.to_dict()}
    if mode is OutputMode.JSON:
        print(json.dumps(payload, indent=2))
        return
    if mode is OutputMode.QUIET:
        print(f"error: {error.message}")
        return
    # HUMAN mode
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
    body = Text()
    body.append(_render_table_text(table))
    terminal_summary = payload.get("report", {}).get("terminal_summary")
    if isinstance(terminal_summary, dict):
        body.append("\n\n")
        body.append(render_run_summary_text(terminal_summary))
    return Panel(body, title=title, subtitle=subtitle, border_style="green")


def render_run_summary_text(summary: dict[str, Any]) -> Text:
    text = Text()
    text.append("Summary\n", style="bold")
    text.append(f"{summary.get('headline', 'n/a')}\n\n", style="bold cyan")
    text.append(f"{summary.get('summary', 'n/a')}\n\n")
    text.append(f"Priority: {summary.get('priority', 'n/a')}\n")
    text.append(f"Owner: {summary.get('recommended_owner', 'n/a')}\n")
    text.append(f"Action: {summary.get('action_type', 'n/a')}\n")
    next_steps = summary.get("next_steps", [])
    if isinstance(next_steps, list) and next_steps:
        text.append("\nNext Steps\n", style="bold")
        for step in next_steps:
            text.append(f"- {step}\n")
    top_findings = summary.get("top_findings", [])
    if isinstance(top_findings, list) and top_findings:
        text.append("\nTop Findings\n", style="bold")
        for finding in top_findings:
            if not isinstance(finding, dict):
                continue
            text.append(
                f"- {finding.get('theme_label', 'n/a')} "
                f"[{finding.get('severity', 'n/a')}] -> "
                f"{finding.get('recommended_owner', 'n/a')}/"
                f"{finding.get('action_type', 'n/a')}\n"
            )
    return Text(text.plain.rstrip())


def _render_table_text(table: Table) -> Text:
    console = Console(width=120, record=True)
    console.print(table)
    return Text.from_ansi(console.export_text(clear=False).rstrip())


def render_api_panel(payload: dict[str, Any]) -> Panel:
    body = (
        f"host: {payload.get('host')}\n"
        f"port: {payload.get('port')}\n"
        f"metrics: {payload.get('metrics_path')}"
    )
    return Panel(body, title="ThreadSense API", border_style="blue")
