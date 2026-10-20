"""Render launchd and cron definitions for a watch without installing or enabling them."""

from __future__ import annotations

import plistlib
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from threadsense.config import StorageConfig
from threadsense.errors import MonitoringInputError
from threadsense.monitoring.profiles import validate_watch_id
from threadsense.monitoring.state import LEASE_SECONDS_RANGE, monitoring_state_path

ScheduleFormat = Literal["launchd", "cron"]

LABEL_PREFIX = "com.threadsense.watch."
STORAGE_ROOT_ENV = "THREADSENSE_STORAGE_ROOT"
LOG_FILENAME = "watch-run.log"
WEEK_MINUTES = 7 * 24 * 60


@dataclass(frozen=True)
class ScheduleSpec:
    watch_id: str
    label: str
    executable: Path
    workspace: Path
    storage_root: Path
    config_path: Path | None
    interval_seconds: int
    lease_seconds: int
    argv: tuple[str, ...]
    log_path: Path


def resolve_workspace(workspace: Path) -> Path:
    resolved = workspace.resolve()
    if not resolved.is_dir():
        raise MonitoringInputError(
            "schedule workspace must be an existing directory",
            details={"reason": "invalid_workspace", "workspace": str(resolved)},
        )
    return resolved


def resolve_storage_root(storage_root: Path, workspace: Path) -> Path:
    """Anchor a relative storage root at the workspace the scheduled job runs in."""
    if storage_root.is_absolute():
        return storage_root.resolve()
    return (workspace / storage_root).resolve()


def build_schedule_spec(
    *,
    watch_id: str,
    refresh_interval_minutes: int,
    workspace: Path,
    storage_root: Path,
    config_path: Path | None,
    executable: Path,
    monitoring_dirname: str,
) -> ScheduleSpec:
    validate_watch_id(watch_id)
    if type(refresh_interval_minutes) is not int or refresh_interval_minutes < 1:
        raise MonitoringInputError(
            "schedule interval must be a positive whole number of minutes",
            details={"reason": "invalid_interval", "interval_minutes": refresh_interval_minutes},
        )
    if not executable.is_absolute() or not executable.is_file():
        raise MonitoringInputError(
            "schedule executable must be an existing absolute path",
            details={"reason": "invalid_executable", "executable": str(executable)},
        )
    resolved_workspace = resolve_workspace(workspace)
    resolved_root = resolve_storage_root(storage_root, resolved_workspace)
    resolved_config = _resolve_config_path(config_path)
    state_path = monitoring_state_path(
        StorageConfig(root_dir=resolved_root, monitoring_dirname=monitoring_dirname)
    )
    interval_seconds = refresh_interval_minutes * 60
    lease_seconds = min(interval_seconds, LEASE_SECONDS_RANGE[1])
    argv = (
        str(executable),
        "-m",
        "threadsense",
        "--output-format",
        "json",
        "watch",
        "run",
        "--id",
        watch_id,
        "--lease-seconds",
        str(lease_seconds),
        *(("--config", str(resolved_config)) if resolved_config is not None else ()),
    )
    return ScheduleSpec(
        watch_id=watch_id,
        label=f"{LABEL_PREFIX}{watch_id}",
        executable=executable,
        workspace=resolved_workspace,
        storage_root=resolved_root,
        config_path=resolved_config,
        interval_seconds=interval_seconds,
        lease_seconds=lease_seconds,
        argv=argv,
        log_path=state_path.parent / "watches" / watch_id / "schedule" / LOG_FILENAME,
    )


def _resolve_config_path(config_path: Path | None) -> Path | None:
    if config_path is None:
        return None
    resolved = config_path.resolve()
    if not resolved.is_file():
        raise MonitoringInputError(
            "schedule config path must be an existing file",
            details={"reason": "invalid_config_path", "config_path": str(resolved)},
        )
    return resolved


def render_launchd(spec: ScheduleSpec) -> str:
    plist = {
        "Label": spec.label,
        "ProgramArguments": list(spec.argv),
        "WorkingDirectory": str(spec.workspace),
        "StartInterval": spec.interval_seconds,
        "EnvironmentVariables": {STORAGE_ROOT_ENV: str(spec.storage_root)},
        "StandardOutPath": str(spec.log_path),
        "StandardErrorPath": str(spec.log_path),
        "RunAtLoad": False,
    }
    return plistlib.dumps(plist, fmt=plistlib.FMT_XML, sort_keys=False).decode("utf-8")


def render_cron(spec: ScheduleSpec) -> str:
    values = (str(spec.workspace), str(spec.storage_root), str(spec.log_path), *spec.argv)
    if any("\n" in value or "\r" in value for value in values):
        raise MonitoringInputError(
            "cron entries cannot carry line breaks in paths or arguments",
            details={"reason": "cron_line_break", "watch_id": spec.watch_id},
        )
    # A backslash already before % would consume the escape added below.
    if any("\\%" in value for value in values):
        raise MonitoringInputError(
            "cron entries cannot carry a backslash before % in paths or arguments",
            details={"reason": "cron_backslash_percent", "watch_id": spec.watch_id},
        )
    command = " ".join(shlex.quote(argument) for argument in spec.argv)
    line = (
        f"{cron_expression(spec.interval_seconds // 60)} "
        f"cd {shlex.quote(str(spec.workspace))} && "
        f"{STORAGE_ROOT_ENV}={shlex.quote(str(spec.storage_root))} exec {command} "
        f">> {shlex.quote(str(spec.log_path))} 2>&1"
    )
    # cron turns an unescaped % into a newline and feeds the rest to stdin.
    return line.replace("%", "\\%")


def cron_expression(interval_minutes: int) -> str:
    if type(interval_minutes) is int and interval_minutes >= 1:
        if interval_minutes < 60 and 60 % interval_minutes == 0:
            return f"*/{interval_minutes} * * * *"
        if interval_minutes == WEEK_MINUTES:
            return "0 0 * * 0"
        hours, remainder = divmod(interval_minutes, 60)
        if remainder == 0 and 24 % hours == 0:
            return "0 0 * * *" if hours == 24 else f"0 */{hours} * * *"
    raise MonitoringInputError(
        "refresh interval cannot be expressed as a cron schedule",
        details={
            "reason": "cron_interval_unrepresentable",
            "interval_minutes": interval_minutes,
            "supported": "use --format launchd for arbitrary intervals",
        },
    )


def render_install_instructions(spec: ScheduleSpec, schedule_format: ScheduleFormat) -> list[str]:
    """Describe manual installation steps; rendering never performs them."""
    log_dir = shlex.quote(str(spec.log_path.parent))
    if schedule_format == "launchd":
        plist_path = f"~/Library/LaunchAgents/{spec.label}.plist"
        return [
            f"mkdir -p {log_dir}",
            f"Save the content to {plist_path}",
            f"launchctl bootstrap gui/$(id -u) {plist_path}",
        ]
    return [
        f"mkdir -p {log_dir}",
        "Run crontab -e and append the content as a single line",
    ]
