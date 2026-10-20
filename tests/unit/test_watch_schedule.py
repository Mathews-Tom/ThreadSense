from __future__ import annotations

import plistlib
import re
import shlex
import sys
from pathlib import Path

import pytest

from threadsense.errors import MonitoringInputError
from threadsense.monitoring.schedule import (
    ScheduleSpec,
    build_schedule_spec,
    cron_expression,
    render_cron,
    render_launchd,
)

WATCH_ID = "sync-conflicts"
HOSTILE_DIRNAME = "work space 'single' \"double\" 100% $HOME"
EXECUTABLE = Path(sys.executable).absolute()


def hostile_workspace(tmp_path: Path) -> tuple[Path, Path]:
    workspace = tmp_path.resolve() / HOSTILE_DIRNAME
    workspace.mkdir()
    config_path = workspace / "conf 50% 'x'.toml"
    config_path.write_text("", encoding="utf-8")
    return workspace, config_path


def schedule_spec(
    workspace: Path,
    config_path: Path | None,
    *,
    refresh_interval_minutes: int = 60,
    storage_root: Path = Path(".threadsense"),
) -> ScheduleSpec:
    return build_schedule_spec(
        watch_id=WATCH_ID,
        refresh_interval_minutes=refresh_interval_minutes,
        workspace=workspace,
        storage_root=storage_root,
        config_path=config_path,
        executable=EXECUTABLE,
        monitoring_dirname="monitoring",
    )


@pytest.mark.parametrize(
    ("interval_minutes", "expected"),
    [
        (15, "*/15 * * * *"),
        (30, "*/30 * * * *"),
        (60, "0 */1 * * *"),
        (120, "0 */2 * * *"),
        (720, "0 */12 * * *"),
        (1440, "0 0 * * *"),
        (10080, "0 0 * * 0"),
    ],
)
def test_cron_expression_representable_interval_returns_expression(
    interval_minutes: int, expected: str
) -> None:
    assert cron_expression(interval_minutes) == expected


@pytest.mark.parametrize("interval_minutes", [0, 45, 90, 420, 2880, 5000])
def test_cron_expression_unrepresentable_interval_raises_input_error(
    interval_minutes: int,
) -> None:
    with pytest.raises(MonitoringInputError) as raised:
        cron_expression(interval_minutes)

    assert raised.value.details["reason"] == "cron_interval_unrepresentable"
    assert raised.value.details["interval_minutes"] == interval_minutes


def test_render_cron_hostile_paths_round_trip_through_shell_parsing(tmp_path: Path) -> None:
    workspace, config_path = hostile_workspace(tmp_path)
    spec = schedule_spec(workspace, config_path, refresh_interval_minutes=30)

    line = render_cron(spec)

    assert "\n" not in line
    assert re.search(r"(?<!\\)%", line) is None
    fields = line.replace("\\%", "%").split(" ", 5)
    assert fields[:5] == ["*/30", "*", "*", "*", "*"]
    assert shlex.split(fields[5]) == [
        "cd",
        str(workspace),
        "&&",
        f"THREADSENSE_STORAGE_ROOT={workspace / '.threadsense'}",
        "exec",
        *spec.argv,
        ">>",
        str(spec.log_path),
        "2>&1",
    ]
    assert spec.argv[-2:] == ("--config", str(config_path))


def test_render_cron_backslash_before_percent_raises_input_error(tmp_path: Path) -> None:
    workspace = tmp_path.resolve() / "a\\%b"
    workspace.mkdir()
    spec = schedule_spec(workspace, None)

    with pytest.raises(MonitoringInputError) as raised:
        render_cron(spec)

    assert raised.value.details["reason"] == "cron_backslash_percent"


def test_render_cron_unrepresentable_interval_raises_input_error(tmp_path: Path) -> None:
    workspace, config_path = hostile_workspace(tmp_path)
    spec = schedule_spec(workspace, config_path, refresh_interval_minutes=45)

    with pytest.raises(MonitoringInputError) as raised:
        render_cron(spec)

    assert raised.value.details["reason"] == "cron_interval_unrepresentable"


def test_render_launchd_hostile_paths_parse_back_exactly(tmp_path: Path) -> None:
    workspace, config_path = hostile_workspace(tmp_path)
    spec = schedule_spec(workspace, config_path, refresh_interval_minutes=90)

    plist = plistlib.loads(render_launchd(spec).encode("utf-8"))

    storage_root = workspace / ".threadsense"
    log_path = storage_root / "monitoring" / "watches" / WATCH_ID / "schedule" / "watch-run.log"
    assert plist == {
        "Label": f"com.threadsense.watch.{WATCH_ID}",
        "ProgramArguments": [
            str(EXECUTABLE),
            "-m",
            "threadsense",
            "--output-format",
            "json",
            "watch",
            "run",
            "--id",
            WATCH_ID,
            "--lease-seconds",
            "5400",
            "--config",
            str(config_path),
        ],
        "WorkingDirectory": str(workspace),
        "StartInterval": 5400,
        "EnvironmentVariables": {"THREADSENSE_STORAGE_ROOT": str(storage_root)},
        "StandardOutPath": str(log_path),
        "StandardErrorPath": str(log_path),
        "RunAtLoad": False,
    }


def test_build_schedule_spec_missing_workspace_raises_invalid_workspace(tmp_path: Path) -> None:
    with pytest.raises(MonitoringInputError) as raised:
        schedule_spec(tmp_path / "absent", None)

    assert raised.value.details["reason"] == "invalid_workspace"


def test_build_schedule_spec_missing_config_raises_invalid_config_path(tmp_path: Path) -> None:
    with pytest.raises(MonitoringInputError) as raised:
        schedule_spec(tmp_path, tmp_path / "absent.toml")

    assert raised.value.details["reason"] == "invalid_config_path"


def test_build_schedule_spec_traversing_watch_id_raises_input_error(tmp_path: Path) -> None:
    with pytest.raises(MonitoringInputError):
        build_schedule_spec(
            watch_id="../escape",
            refresh_interval_minutes=60,
            workspace=tmp_path,
            storage_root=Path(".threadsense"),
            config_path=None,
            executable=EXECUTABLE,
            monitoring_dirname="monitoring",
        )


def test_build_schedule_spec_relative_storage_root_anchors_at_workspace(tmp_path: Path) -> None:
    workspace = tmp_path.resolve()

    spec = schedule_spec(workspace, None, storage_root=Path("state/../store"))

    assert spec.storage_root == workspace / "store"
    assert spec.storage_root.is_absolute()
    assert spec.log_path.is_relative_to(spec.storage_root)
    assert "--config" not in spec.argv
    assert not spec.log_path.parent.exists()


@pytest.mark.parametrize(
    ("interval_minutes", "expected_lease"),
    [(15, 900), (1440, 86400), (10080, 86400)],
)
def test_build_schedule_spec_lease_capped_at_one_day(
    tmp_path: Path, interval_minutes: int, expected_lease: int
) -> None:
    spec = schedule_spec(tmp_path, None, refresh_interval_minutes=interval_minutes)

    assert spec.interval_seconds == interval_minutes * 60
    assert spec.lease_seconds == expected_lease
    assert spec.argv[spec.argv.index("--lease-seconds") + 1] == str(expected_lease)
