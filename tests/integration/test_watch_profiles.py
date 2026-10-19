from __future__ import annotations

import errno
import json
import os
import sqlite3
import subprocess
import sys
import time
import tomllib
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest

import threadsense
from threadsense.cli import main
from threadsense.cli_display import set_output_mode
from threadsense.monitoring import STATE_SCHEMA_VERSION, parse_watch_profile

FIXTURE_PROFILE = Path(__file__).resolve().parents[1] / "fixtures" / "watch" / "profile_v1.toml"

# Runs in a separate interpreter: waits for a shared start instant, then attempts
# one claim (or finalization) against the state database and prints the outcome.
CLAIM_PROCESS = """
import json, sys, time
from pathlib import Path
from threadsense.errors import RunClaimError
from threadsense.monitoring import MonitoringStore

action, state_path, watch_id, argument, start_at = sys.argv[1:6]
while time.time() < float(start_at):
    time.sleep(0.001)
store = MonitoringStore(Path(state_path))
try:
    if action == "claim":
        claim = store.claim_run(watch_id, lease_seconds=int(argument))
        print(json.dumps({"outcome": "claimed", "run_id": claim.run_id,
                          "owner_token": claim.owner_token,
                          "recovered_run_id": claim.recovered_run_id}))
    else:
        run_id, owner_token = argument.split(":", 1)
        store.finalize_run(run_id, owner_token, "succeeded")
        print(json.dumps({"outcome": "finalized"}))
except RunClaimError as error:
    print(json.dumps({"outcome": "refused", "reason": error.details["reason"]}))
"""


@pytest.fixture
def store_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    root = tmp_path / "store"
    monkeypatch.setenv("THREADSENSE_STORAGE_ROOT", str(root))
    monkeypatch.delenv("THREADSENSE_CONFIG", raising=False)
    monkeypatch.delenv("THREADSENSE_STORAGE_MONITORING_DIR", raising=False)
    monkeypatch.chdir(tmp_path)
    return root


def state_path(store_root: Path) -> Path:
    return store_root.resolve() / "monitoring" / "state.sqlite3"


def process_env() -> dict[str, str]:
    """Child interpreters import the same `threadsense` sources as this test process."""
    source_root = str(Path(threadsense.__file__).resolve().parents[1])
    inherited = os.environ.get("PYTHONPATH")
    pythonpath = source_root if not inherited else os.pathsep.join([source_root, inherited])
    return {**os.environ, "PYTHONPATH": pythonpath}


def run_cli_process(*args: str) -> tuple[int, dict[str, Any]]:
    completed = subprocess.run(
        [sys.executable, "-m", "threadsense", "--output-format", "json", *args],
        capture_output=True,
        text=True,
        check=False,
        env=process_env(),
        timeout=60,
    )
    return completed.returncode, json.loads(completed.stdout)


def run_cli(capsys: pytest.CaptureFixture[str], *args: str) -> tuple[int, dict[str, Any]]:
    exit_code = main(["--output-format", "json", *args])
    return exit_code, json.loads(capsys.readouterr().out)


def start_processes(*argvs: list[str]) -> list[dict[str, Any]]:
    start_at = time.time() + 2.0
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", CLAIM_PROCESS, *argv, str(start_at)],
            stdout=subprocess.PIPE,
            text=True,
            env=process_env(),
        )
        for argv in argvs
    ]
    outputs = [process.communicate(timeout=60)[0] for process in processes]
    assert all(process.returncode == 0 for process in processes)
    return [json.loads(output) for output in outputs]


def write_profile(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


def test_watch_profile_survives_process_restart(store_root: Path) -> None:
    create_code, created = run_cli_process("watch", "create", "--profile", str(FIXTURE_PROFILE))

    list_code, listed = run_cli_process("watch", "list")
    inspect_code, inspected = run_cli_process("watch", "inspect", "--id", "agent-tooling")

    with FIXTURE_PROFILE.open("rb") as handle:
        expected = parse_watch_profile(tomllib.load(handle))
    assert (create_code, list_code, inspect_code) == (0, 0, 0)
    assert created["status"] == "created"
    assert [watch["watch_id"] for watch in listed["watches"]] == ["agent-tooling"]
    assert inspected["profile"] == expected.to_dict()
    assert inspected["revisions"][0]["content_sha256"] == expected.content_sha256()
    assert Path(inspected["state_path"]) == state_path(store_root)


def test_watch_create_reformatted_identical_profile_is_unchanged_and_edit_revises(
    store_root: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    original = FIXTURE_PROFILE.read_text(encoding="utf-8")
    run_cli(capsys, "watch", "create", "--profile", str(FIXTURE_PROFILE))
    before = state_path(store_root).read_bytes()
    reformatted = write_profile(
        tmp_path / "reformatted.toml",
        "# same content, different layout\n"
        + original.replace('["mcp server", "coding agent"]', '[ "coding agent","mcp server" ]'),
    )
    edit = write_profile(
        tmp_path / "edit.toml",
        original.replace("refresh_interval_minutes = 360", "refresh_interval_minutes = 720"),
    )

    unchanged_code, unchanged = run_cli(capsys, "watch", "create", "--profile", str(reformatted))
    after_unchanged = state_path(store_root).read_bytes()
    revised_code, revised = run_cli(capsys, "watch", "create", "--profile", str(edit))
    _, inspected = run_cli(capsys, "watch", "inspect", "--id", "agent-tooling")

    assert (unchanged_code, unchanged["status"], unchanged["revision"]) == (0, "unchanged", 1)
    assert after_unchanged == before
    assert (revised_code, revised["status"], revised["revision"]) == (0, "revised", 2)
    assert inspected["profile"]["refresh_interval_minutes"] == 720
    assert len({item["content_sha256"] for item in inspected["revisions"]}) == 2


@pytest.mark.parametrize(
    ("original", "replacement", "field"),
    [
        ("discovered_candidates = 50", "discovered_candidates = 0", "limits.discovered_candidates"),
        ("presented_findings = 10", "presented_findings = 5000", "limits.presented_findings"),
        ('"mcp server", "coding agent"', '"mcp server", "title:agent"', "queries"),
        ('id = "agent-tooling"', 'id = "../../outside"', "id"),
    ],
)
def test_watch_create_invalid_profile_fails_before_any_write(
    store_root: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    original: str,
    replacement: str,
    field: str,
) -> None:
    text = FIXTURE_PROFILE.read_text(encoding="utf-8")
    invalid = write_profile(tmp_path / "invalid.toml", text.replace(original, replacement))

    exit_code, payload = run_cli(capsys, "watch", "create", "--profile", str(invalid))

    assert exit_code == 1
    assert payload["error"]["code"] == "monitoring_input_error"
    assert payload["error"]["details"]["field"] == field
    assert not store_root.exists()
    assert not (tmp_path / "outside").exists()


def test_watch_create_storage_escape_fails_before_any_write(
    store_root: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("THREADSENSE_STORAGE_MONITORING_DIR", "../escaped")

    exit_code, payload = run_cli(capsys, "watch", "create", "--profile", str(FIXTURE_PROFILE))

    assert exit_code == 1
    assert payload["error"]["code"] == "monitoring_input_error"
    assert not store_root.exists()
    assert not (tmp_path / "escaped").exists()


def reject_json_constant(token: str) -> object:
    raise ValueError(f"non-standard JSON constant in output: {token}")


@pytest.mark.parametrize(
    ("original", "replacement", "field", "expected_value"),
    [
        ('id = "agent-tooling"', "id = 1979-05-27", "id", "1979-05-27"),
        ('name = "Coding agent friction"', "name = 07:32:00", "name", "07:32:00"),
        (
            'domain = "developer_tools"',
            "domain = 1979-05-27T07:32:00Z",
            "domain",
            "1979-05-27T07:32:00+00:00",
        ),
        (
            'objective = "friction_analysis"',
            "objective = [[1, 2], {a = 1979-05-27}]",
            "objective",
            [[1, 2], {"a": "1979-05-27"}],
        ),
        (
            "refresh_interval_minutes = 360",
            "refresh_interval_minutes = nan",
            "refresh_interval_minutes",
            "nan",
        ),
        (
            "refreshed_threads = 25",
            "refreshed_threads = inf",
            "limits.refreshed_threads",
            "inf",
        ),
    ],
)
def test_watch_create_non_json_toml_values_returns_structured_input_error(
    store_root: Path,
    tmp_path: Path,
    original: str,
    replacement: str,
    field: str,
    expected_value: object,
) -> None:
    text = FIXTURE_PROFILE.read_text(encoding="utf-8")
    assert original in text
    invalid = write_profile(tmp_path / "invalid.toml", text.replace(original, replacement, 1))

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "threadsense",
            "--output-format",
            "json",
            "watch",
            "create",
            "--profile",
            str(invalid),
        ],
        capture_output=True,
        text=True,
        check=False,
        env=process_env(),
        timeout=60,
    )

    assert completed.returncode == 1
    payload = json.loads(completed.stdout, parse_constant=reject_json_constant)
    assert payload["status"] == "error"
    assert payload["error"]["code"] == "monitoring_input_error"
    assert payload["error"]["details"]["field"] == field
    assert payload["error"]["details"]["value"] == expected_value
    assert not store_root.exists()


@pytest.fixture
def reset_output_mode() -> Iterator[None]:
    yield
    set_output_mode(None)


def run_watch_create_process(output_format: str, profile: Path) -> tuple[int, str, str]:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "threadsense",
            "--output-format",
            output_format,
            "watch",
            "create",
            "--profile",
            str(profile),
        ],
        capture_output=True,
        text=True,
        check=False,
        env=process_env(),
        timeout=60,
    )
    return completed.returncode, completed.stdout, completed.stderr


def assert_profile_read_error(
    result: tuple[int, str, str], output_format: str, profile: Path, expected: tuple[str, str]
) -> None:
    exit_code, stdout, stderr = result
    message, error_fragment = expected
    assert exit_code == 1
    assert "Traceback" not in stdout
    assert "Traceback" not in stderr
    if output_format == "json":
        payload = json.loads(stdout, parse_constant=reject_json_constant)
        assert payload["status"] == "error"
        assert payload["error"]["type"] == "MonitoringInputError"
        assert payload["error"]["code"] == "monitoring_input_error"
        assert payload["error"]["message"] == message
        assert payload["error"]["details"]["path"] == str(profile)
        assert error_fragment in payload["error"]["details"]["error"]
    else:
        assert "monitoring_input_error" in stderr
        assert message in stderr


@pytest.mark.parametrize("output_format", ["json", "human"])
def test_watch_create_invalid_utf8_profile_returns_input_error_before_any_write(
    store_root: Path, tmp_path: Path, output_format: str
) -> None:
    profile = tmp_path / "invalid-utf8.toml"
    profile.write_bytes(b'id = "\xff\xfe"\n')

    result = run_watch_create_process(output_format, profile)

    assert_profile_read_error(
        result,
        output_format,
        profile,
        ("watch profile file is not valid UTF-8", "can't decode byte 0xff"),
    )
    assert not store_root.exists()


@pytest.mark.parametrize("output_format", ["json", "human"])
def test_watch_create_directory_profile_path_returns_input_error_before_any_write(
    store_root: Path, tmp_path: Path, output_format: str
) -> None:
    profile = tmp_path / "profile-directory.toml"
    profile.mkdir()

    result = run_watch_create_process(output_format, profile)

    assert_profile_read_error(
        result,
        output_format,
        profile,
        ("watch profile file could not be read", "Is a directory"),
    )
    assert not store_root.exists()


@pytest.mark.usefixtures("reset_output_mode")
@pytest.mark.parametrize("output_format", ["json", "human"])
def test_watch_create_unreadable_profile_returns_input_error_before_any_write(
    store_root: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    output_format: str,
) -> None:
    profile = write_profile(tmp_path / "unreadable.toml", FIXTURE_PROFILE.read_text("utf-8"))
    original_open = Path.open

    # Injected read fault: opening the profile path fails with EACCES at the real
    # read boundary, independent of the process's privileges; every other path
    # (config, state storage) delegates to the original implementation.
    def open_with_injected_read_fault(self: Path, *args: Any, **kwargs: Any) -> Any:
        if self == profile:
            raise PermissionError(errno.EACCES, "Permission denied", str(self))
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", open_with_injected_read_fault)

    exit_code = main(
        ["--output-format", output_format, "watch", "create", "--profile", str(profile)]
    )
    captured = capsys.readouterr()

    assert_profile_read_error(
        (exit_code, captured.out, captured.err),
        output_format,
        profile,
        ("watch profile file could not be read", "Permission denied"),
    )
    assert not store_root.exists()


def test_concurrent_claim_processes_admit_exactly_one_owner(
    store_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    run_cli(capsys, "watch", "create", "--profile", str(FIXTURE_PROFILE))
    path = str(state_path(store_root))

    outcomes = start_processes(*[["claim", path, "agent-tooling", "600"] for _ in range(6)])
    _, inspected = run_cli(capsys, "watch", "inspect", "--id", "agent-tooling")

    winners = [outcome for outcome in outcomes if outcome["outcome"] == "claimed"]
    assert len(winners) == 1
    assert sorted(outcome.get("reason", "") for outcome in outcomes).count("claim_held") == 5
    assert inspected["active_run"]["run_id"] == winners[0]["run_id"]
    assert inspected["run_count"] == 1


def test_recovered_claim_fences_stale_owner_process(
    store_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    run_cli(capsys, "watch", "create", "--profile", str(FIXTURE_PROFILE))
    path = str(state_path(store_root))
    (stale,) = start_processes(["claim", path, "agent-tooling", "1"])
    time.sleep(1.1)

    (current,) = start_processes(["claim", path, "agent-tooling", "600"])
    (stale_finalize,) = start_processes(
        ["finalize", path, "agent-tooling", f"{stale['run_id']}:{stale['owner_token']}"]
    )
    _, inspected = run_cli(capsys, "watch", "inspect", "--id", "agent-tooling")

    runs = {run["run_id"]: run for run in inspected["recent_runs"]}
    assert current["recovered_run_id"] == stale["run_id"]
    assert stale_finalize == {"outcome": "refused", "reason": "stale_owner"}
    assert runs[stale["run_id"]]["status"] == "abandoned"
    assert runs[stale["run_id"]]["recovered_by_run_id"] == current["run_id"]
    assert inspected["active_run"]["run_id"] == current["run_id"]


def test_newer_state_schema_is_refused_by_every_watch_command_without_modification(
    store_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    run_cli(capsys, "watch", "create", "--profile", str(FIXTURE_PROFILE))
    path = state_path(store_root)
    with closing(sqlite3.connect(path)) as conn:
        conn.execute(f"PRAGMA user_version = {STATE_SCHEMA_VERSION + 1}")
        conn.commit()
    before = {entry.name: entry.read_bytes() for entry in path.parent.iterdir()}

    results = [
        run_cli(capsys, "watch", "create", "--profile", str(FIXTURE_PROFILE)),
        run_cli(capsys, "watch", "list"),
        run_cli(capsys, "watch", "inspect", "--id", "agent-tooling"),
    ]

    assert [code for code, _ in results] == [1, 1, 1]
    assert {payload["error"]["details"]["reason"] for _, payload in results} == {
        "unsupported_schema_version"
    }
    assert {entry.name: entry.read_bytes() for entry in path.parent.iterdir()} == before
