"""Operator feedback through the real CLI, recorded sweeps and real SQLite.

Replays tests/fixtures/watch/incremental/ (synthesized Reddit payloads; see its README.md,
never live data) through in-process `threadsense.cli.main` with JSON output: `watch run`,
`watch digest`, `watch explain`, `watch policy`, `issues inspect` and the
`feedback record|list` commands.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import sqlite3
import time
from contextlib import closing, redirect_stdout
from dataclasses import dataclass, replace
from functools import partial
from pathlib import Path
from typing import Any

import pytest
from recorded_reddit import (
    PhaseReplayTransport,
    ReplayPhase,
    load_replay_phases,
    replay_connector_factory,
)

from threadsense.cli import main
from threadsense.cli_display import set_output_mode
from threadsense.config import AppConfig
from threadsense.connectors.reddit import RedditConnector
from threadsense.monitoring import MonitoringStore
from threadsense.monitoring.state import STATE_FILENAME

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "watch" / "incremental"
PROFILE_PATH = FIXTURE_DIR / "profile.toml"
PHASES_PATH = FIXTURE_DIR / "phases.json"
WATCH_ID = "sync-conflicts"
OTHER_WATCH_ID = "sync-conflicts-other"
UNKNOWN_WATCH_ID = "no-such-watch"
THREAD_A = "sc1aaa"
THREAD_B = "sc1bbb"
FEEDBACK_TABLES = ("watch_feedback_keys", "watch_preference_events", "watch_correction_events")
EVIDENCE_TABLES = {
    "watch_digest_entries",
    "watch_thread_revisions",
    "watch_comments",
    "watch_digests",
    "watch_issues",
    "watch_issue_memberships",
}


# CLI harness: in-process `threadsense.cli.main` against isolated storage.


def isolate_environment(monkeypatch: pytest.MonkeyPatch, workspace: Path) -> None:
    monkeypatch.chdir(workspace)
    monkeypatch.delenv("THREADSENSE_CONFIG", raising=False)
    monkeypatch.delenv("THREADSENSE_STORAGE_MONITORING_DIR", raising=False)
    for name in (
        "THREADSENSE_ISSUES_CANDIDATE_WINDOW_DAYS",
        "THREADSENSE_ISSUES_MAX_CANDIDATE_COMPARISONS",
        "THREADSENSE_ISSUES_MAX_SUGGESTIONS_PER_THREAD",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("THREADSENSE_STORAGE_ROOT", str(workspace / ".threadsense"))
    monkeypatch.setenv("THREADSENSE_RUNTIME_ENABLED", "false")
    monkeypatch.setenv("THREADSENSE_REDDIT_MAX_RETRIES", "0")
    monkeypatch.setenv("THREADSENSE_REDDIT_REQUEST_DELAY", "0")


def run_cli(*args: str) -> tuple[int, dict[str, Any]]:
    buffer = io.StringIO()
    try:
        with redirect_stdout(buffer):
            exit_code = main(["--output-format", "json", *args])
    finally:
        set_output_mode(None)
    payload: dict[str, Any] = json.loads(buffer.getvalue())
    return exit_code, payload


def create_watch(profile_path: Path) -> Path:
    exit_code, created = run_cli("watch", "create", "--profile", str(profile_path))
    assert exit_code == 0, created
    return Path(created["state_path"])


def write_other_profile(directory: Path) -> Path:
    """The fixture profile under a different watch id."""
    text = PROFILE_PATH.read_text(encoding="utf-8")
    original = f'\nid = "{WATCH_ID}"\n'
    assert text.count(original) == 1
    path = directory / "other_profile.toml"
    path.write_text(text.replace(original, f'\nid = "{OTHER_WATCH_ID}"\n'), encoding="utf-8")
    return path


def install_transport(monkeypatch: pytest.MonkeyPatch, phase: ReplayPhase) -> None:
    monkeypatch.setattr(
        "threadsense.cli.build_reddit_connector",
        replay_connector_factory(PhaseReplayTransport(phase)),
    )


def refuse_fetch(config: AppConfig) -> RedditConnector:
    raise AssertionError("this command must not fetch from Reddit")


def watch_run(monkeypatch: pytest.MonkeyPatch, watch_id: str, phase_name: str) -> dict[str, Any]:
    phases = {phase.name: phase for phase in load_replay_phases(PHASES_PATH)}
    install_transport(monkeypatch, phases[phase_name])
    exit_code, payload = run_cli("watch", "run", "--id", watch_id)
    assert exit_code == 0, payload
    return payload


def query_rows(state_path: Path, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    uri = f"{state_path.as_uri()}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as conn:
        return [tuple(row) for row in conn.execute(sql, params).fetchall()]


def only_value(rows: list[tuple[Any, ...]]) -> str:
    assert len(rows) == 1, rows
    return str(rows[0][0])


def feedback_rows(state_path: Path) -> dict[str, list[tuple[Any, ...]]]:
    return {
        table: query_rows(state_path, f"SELECT * FROM {table} ORDER BY feedback_id")
        for table in FEEDBACK_TABLES
    }


def no_feedback() -> dict[str, list[tuple[Any, ...]]]:
    return {table: [] for table in FEEDBACK_TABLES}


def error_reason(payload: dict[str, Any]) -> str:
    assert payload["status"] == "error", payload
    return str(payload["error"]["details"]["reason"])


# Feedback commands.


@dataclass(frozen=True)
class Feedback:
    """One `feedback record` invocation; optional flags are omitted when None."""

    watch_id: str
    target_id: str
    key: str
    kind: str = "preference"
    value: str = "useful"
    target_kind: str | None = None
    note: str | None = None

    def argv(self) -> list[str]:
        args = ["feedback", "record", "--watch", self.watch_id, "--target", self.target_id]
        args += ["--kind", self.kind, "--value", self.value, "--key", self.key]
        if self.target_kind is not None:
            args += ["--target-kind", self.target_kind]
        if self.note is not None:
            args += ["--note", self.note]
        return args


def record(feedback: Feedback) -> tuple[int, dict[str, Any]]:
    return run_cli(*feedback.argv())


def record_new(feedback: Feedback) -> dict[str, Any]:
    exit_code, payload = record(feedback)
    assert exit_code == 0, payload
    assert (payload["artifact_type"], payload["status"]) == ("feedback_event", "recorded")
    event: dict[str, Any] = payload["feedback"]
    return event


def list_feedback(watch_id: str, target_id: str | None = None) -> dict[str, Any]:
    args = ["feedback", "list", "--watch", watch_id]
    if target_id is not None:
        args += ["--target", target_id]
    exit_code, payload = run_cli(*args)
    assert exit_code == 0, payload
    assert (payload["artifact_type"], payload["status"]) == ("feedback_history", "ok")
    return payload


def feedback_ids(events: list[dict[str, Any]]) -> list[str]:
    return [str(event["feedback_id"]) for event in events]


# Two watches swept from the same recorded `initial` phase in one storage root.


@dataclass(frozen=True)
class WatchTargets:
    finding_a: str
    theme_a: str
    second_finding_a: str
    update_a: str
    finding_b: str
    revision_a: str
    revision_b: str
    issue_a: str
    issue_b: str


@dataclass(frozen=True)
class TwoWatches:
    state_path: Path
    main: WatchTargets
    other: WatchTargets


def entries_of(state_path: Path, watch_id: str, thread_id: str, kind: str) -> list[tuple[Any, ...]]:
    return query_rows(
        state_path,
        "SELECT entry_id, theme_key FROM watch_digest_entries "
        "WHERE watch_id = ? AND source_thread_id = ? AND kind = ? ORDER BY theme_key, entry_id",
        (watch_id, thread_id, kind),
    )


def revision_of(state_path: Path, watch_id: str, thread_id: str) -> str:
    return only_value(
        query_rows(
            state_path,
            "SELECT revision_id FROM watch_thread_revisions "
            "WHERE watch_id = ? AND source_thread_id = ?",
            (watch_id, thread_id),
        )
    )


def issue_of(state_path: Path, watch_id: str, thread_id: str) -> str:
    return only_value(
        query_rows(
            state_path,
            "SELECT issue_id FROM watch_issues WHERE watch_id = ? AND home_thread_id = ?",
            (watch_id, thread_id),
        )
    )


def targets_for(state_path: Path, watch_id: str) -> WatchTargets:
    findings_a = entries_of(state_path, watch_id, THREAD_A, "finding")
    findings_b = entries_of(state_path, watch_id, THREAD_B, "finding")
    assert len(findings_a) == 2 and len(findings_b) == 2
    assert findings_a[0][1] != findings_a[1][1]
    return WatchTargets(
        finding_a=str(findings_a[0][0]),
        theme_a=str(findings_a[0][1]),
        second_finding_a=str(findings_a[1][0]),
        update_a=only_value(entries_of(state_path, watch_id, THREAD_A, "thread_update")),
        finding_b=str(findings_b[0][0]),
        revision_a=revision_of(state_path, watch_id, THREAD_A),
        revision_b=revision_of(state_path, watch_id, THREAD_B),
        issue_a=issue_of(state_path, watch_id, THREAD_A),
        issue_b=issue_of(state_path, watch_id, THREAD_B),
    )


@pytest.fixture
def watches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TwoWatches:
    isolate_environment(monkeypatch, tmp_path)
    state_path = create_watch(PROFILE_PATH)
    watch_run(monkeypatch, WATCH_ID, "initial")
    profile_dir = tmp_path / "profiles"
    profile_dir.mkdir()
    assert create_watch(write_other_profile(profile_dir)) == state_path
    watch_run(monkeypatch, OTHER_WATCH_ID, "initial")
    return TwoWatches(
        state_path=state_path,
        main=targets_for(state_path, WATCH_ID),
        other=targets_for(state_path, OTHER_WATCH_ID),
    )


# Evidence snapshots.


def artifact_hashes(monitoring_root: Path) -> dict[str, str]:
    """Hash every monitoring file except the state database and its journal."""
    return {
        path.relative_to(monitoring_root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(monitoring_root.rglob("*"))
        if path.is_file() and not path.name.startswith(STATE_FILENAME)
    }


def evidence_tables(state_path: Path) -> dict[str, list[tuple[Any, ...]]]:
    """Every non-feedback table, rows sorted so the dump is order independent."""
    names = [
        str(name)
        for (name,) in query_rows(
            state_path,
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite%'",
        )
    ]
    tracked = sorted(name for name in names if name not in FEEDBACK_TABLES)
    lineage = {name for name in names if name.startswith(("watch_issue", "watch_digest"))}
    assert EVIDENCE_TABLES | lineage <= set(tracked)
    return {
        name: sorted(query_rows(state_path, f"SELECT * FROM {name}"), key=repr) for name in tracked
    }


def issues_view(watch_id: str) -> dict[str, Any]:
    exit_code, payload = run_cli("issues", "inspect", "--watch", watch_id)
    assert exit_code == 0, payload
    return payload


@pytest.mark.parametrize("target_kind", ["finding", "observation"])
def test_target_history_uses_latest_preference_across_revisions(
    watches: TwoWatches, monkeypatch: pytest.MonkeyPatch, target_kind: str
) -> None:
    old_target = watches.main.finding_b if target_kind == "finding" else watches.main.revision_b
    old_event = record_new(Feedback(WATCH_ID, old_target, "old-revision", target_kind=target_kind))
    watch_run(monkeypatch, WATCH_ID, "edit")
    if target_kind == "finding":
        [(theme_key,)] = query_rows(
            watches.state_path,
            "SELECT theme_key FROM watch_digest_entries WHERE entry_id = ? AND watch_id = ?",
            (old_target, WATCH_ID),
        )
        candidates = [
            str(entry_id)
            for entry_id, theme in entries_of(watches.state_path, WATCH_ID, THREAD_B, "finding")
            if theme == theme_key and entry_id != old_target
        ]
    else:
        candidates = [
            str(revision_id)
            for (revision_id,) in query_rows(
                watches.state_path,
                "SELECT revision_id FROM watch_thread_revisions "
                "WHERE watch_id = ? AND source_thread_id = ? AND revision_id != ?",
                (WATCH_ID, THREAD_B, old_target),
            )
        ]
    [new_target] = candidates
    assert new_target != old_target
    new_event = record_new(
        Feedback(
            WATCH_ID,
            new_target,
            "new-revision",
            value="irrelevant",
            target_kind=target_kind,
        )
    )
    assert (old_event["scope_kind"], old_event["scope_key"]) == (
        new_event["scope_kind"],
        new_event["scope_key"],
    )
    assert new_event["feedback_id"] != old_event["feedback_id"]
    assert new_event["sequence"] > old_event["sequence"]

    history = list_feedback(WATCH_ID)
    targeted = list_feedback(WATCH_ID, old_target)
    assert targeted["preferences"] == [old_event]
    assert targeted["effective_preferences"] == history["effective_preferences"]
    [winner] = targeted["effective_preferences"]
    assert winner["feedback_id"] == new_event["feedback_id"]
    assert winner["target_id"] == new_target
    assert winner["value"] == "irrelevant"
    assert winner["superseded_feedback_ids"] == [old_event["feedback_id"]]


# 1. Preferences and corrections are separate histories.


def test_feedback_record_preference_and_correction_on_finding_keeps_separate_histories(
    watches: TwoWatches,
) -> None:
    # Arrange
    finding = watches.main.finding_a
    [(entry_kind, thread_id, revision_id, theme_key, payload_json)] = query_rows(
        watches.state_path,
        "SELECT kind, source_thread_id, revision_id, theme_key, payload_json "
        "FROM watch_digest_entries WHERE entry_id = ?",
        (finding,),
    )
    severity = json.loads(payload_json)["severity"]

    # Act
    preference = record_new(Feedback(WATCH_ID, finding, "k-pref"))
    correction = record_new(
        Feedback(WATCH_ID, finding, "k-corr", "correction", "misclassified", note="wrong theme")
    )
    history = list_feedback(WATCH_ID)

    # Assert
    assert feedback_ids(history["preferences"]) == [preference["feedback_id"]]
    assert feedback_ids(history["corrections"]) == [correction["feedback_id"]]
    assert [event["kind"] for event in history["preferences"]] == ["preference"]
    assert [event["kind"] for event in history["corrections"]] == ["correction"]
    assert feedback_ids(history["effective_preferences"]) == [preference["feedback_id"]]
    assert history["correction_view"] == [
        {
            "target_kind": "entry",
            "target_id": finding,
            "original": {
                "entry_kind": entry_kind,
                "source_thread_id": thread_id,
                "revision_id": revision_id,
                "theme_key": theme_key,
                "severity": severity,
                "payload_sha256": hashlib.sha256(payload_json.encode("utf-8")).hexdigest(),
            },
            "effect": "assertion_only",
            "assertions": [
                {
                    "feedback_id": correction["feedback_id"],
                    "sequence": 1,
                    "value": "misclassified",
                    "note": "wrong theme",
                }
            ],
        }
    ]
    assert (entry_kind, thread_id, theme_key) == ("finding", THREAD_A, watches.main.theme_a)
    rows = feedback_rows(watches.state_path)
    assert [row[0] for row in rows["watch_preference_events"]] == [preference["feedback_id"]]
    assert [row[0] for row in rows["watch_correction_events"]] == [correction["feedback_id"]]
    assert sorted(
        query_rows(watches.state_path, "SELECT feedback_id, kind FROM watch_feedback_keys")
    ) == sorted(
        [(preference["feedback_id"], "preference"), (correction["feedback_id"], "correction")]
    )


# 2. Feedback never edits evidence, findings, issues or written reports.


def test_feedback_record_many_events_leaves_evidence_reports_and_issues_identical(
    watches: TwoWatches,
) -> None:
    # Arrange
    own = watches.main
    monitoring_root = watches.state_path.parent
    files_before = artifact_hashes(monitoring_root)
    tables_before = evidence_tables(watches.state_path)
    issues_before = issues_view(WATCH_ID)
    events = [
        Feedback(WATCH_ID, own.update_a, "p-entry"),
        Feedback(WATCH_ID, own.finding_a, "p-finding", value="irrelevant", target_kind="finding"),
        Feedback(WATCH_ID, own.revision_a, "p-observation", value="already_known"),
        Feedback(WATCH_ID, own.issue_a, "p-issue", value="wrong_audience"),
        Feedback(WATCH_ID, own.finding_a, "c-misclassified", "correction", "misclassified"),
        Feedback(WATCH_ID, own.revision_a, "c-unsupported", "correction", "unsupported"),
        Feedback(WATCH_ID, own.issue_b, "c-duplicate", "correction", "duplicate"),
        Feedback(WATCH_ID, own.finding_b, "c-missing", "correction", "missing_evidence"),
    ]

    # Act
    recorded = [record_new(event) for event in events]

    # Assert
    assert len({event["feedback_id"] for event in recorded}) == len(events)
    assert any(name.endswith(".md") for name in files_before)
    assert tables_before["watch_digest_entries"] and tables_before["watch_issues"]
    assert artifact_hashes(monitoring_root) == files_before
    assert evidence_tables(watches.state_path) == tables_before
    assert issues_view(WATCH_ID) == issues_before


# 3. Idempotent retry returns the original event.


def test_feedback_record_same_key_identical_content_returns_original_event(
    watches: TwoWatches,
) -> None:
    # Arrange
    feedback = Feedback(WATCH_ID, watches.main.finding_a, "k-retry", note="keep this one")

    # Act
    first_code, first = record(feedback)
    second_code, second = record(feedback)
    later = record_new(Feedback(WATCH_ID, watches.main.finding_b, "k-later"))

    # Assert
    assert (first_code, first["status"]) == (0, "recorded")
    assert (second_code, second["status"]) == (0, "unchanged")
    assert second["feedback"] == first["feedback"]
    original = first["feedback"]
    assert original["sequence"] == 1
    assert str(original["feedback_id"]).startswith("fbk-")
    assert len(str(original["feedback_id"])) == len("fbk-") + 32
    assert (second["feedback"]["recorded_at_utc"], original["idempotency_key"]) == (
        original["recorded_at_utc"],
        "k-retry",
    )
    keys = query_rows(
        watches.state_path,
        "SELECT feedback_id FROM watch_feedback_keys WHERE idempotency_key = 'k-retry'",
    )
    assert keys == [(original["feedback_id"],)]
    events = query_rows(
        watches.state_path,
        "SELECT feedback_id FROM watch_preference_events WHERE feedback_id = ?",
        (original["feedback_id"],),
    )
    assert events == [(original["feedback_id"],)]
    assert later["sequence"] == 2


# 4. A reused key with different content is refused without writes.


def conflicting(label: str, original: Feedback, targets: WatchTargets) -> Feedback:
    variants = {
        "value": replace(original, value="irrelevant"),
        "kind": replace(original, kind="correction", value="misclassified"),
        "target": replace(original, target_id=targets.second_finding_a),
        "target_kind": replace(original, target_kind="finding"),
        "note": replace(original, note="a different note"),
    }
    return variants[label]


@pytest.mark.parametrize("label", ["value", "kind", "target", "target_kind", "note"])
def test_feedback_record_reused_key_with_different_content_refuses_without_writes(
    watches: TwoWatches, label: str
) -> None:
    # Arrange
    original = Feedback(WATCH_ID, watches.main.finding_a, "k-conflict", note="first note")
    event = record_new(original)
    rows_before = feedback_rows(watches.state_path)

    # Act
    exit_code, payload = record(conflicting(label, original, watches.main))

    # Assert
    assert exit_code == 1
    assert error_reason(payload) == "idempotency_conflict"
    assert payload["error"]["details"]["feedback_id"] == event["feedback_id"]
    assert feedback_rows(watches.state_path) == rows_before


# 5. Idempotency keys are scoped to one watch.


def test_feedback_record_same_key_in_another_watch_records_independent_event(
    watches: TwoWatches,
) -> None:
    # Arrange
    own = Feedback(WATCH_ID, watches.main.finding_a, "k-shared")
    other = Feedback(OTHER_WATCH_ID, watches.other.finding_a, "k-shared")

    # Act
    own_event = record_new(own)
    other_event = record_new(other)

    # Assert
    assert own_event["feedback_id"] != other_event["feedback_id"]
    assert (own_event["watch_id"], other_event["watch_id"]) == (WATCH_ID, OTHER_WATCH_ID)
    assert (own_event["sequence"], other_event["sequence"]) == (1, 1)
    assert feedback_ids(list_feedback(WATCH_ID)["preferences"]) == [own_event["feedback_id"]]
    assert feedback_ids(list_feedback(OTHER_WATCH_ID)["preferences"]) == [
        other_event["feedback_id"]
    ]


# 6. Invalid targets and requests are refused before any write.


def invalid_request(label: str, state: TwoWatches) -> Feedback:
    own, other = state.main, state.other
    base = Feedback(WATCH_ID, own.finding_a, "k-invalid")
    cases = {
        "unknown_entry": replace(base, target_id="ent-" + "0" * 32),
        "unknown_revision": replace(base, target_id="rev-" + "0" * 32),
        "unknown_issue": replace(base, target_id="iss-" + "0" * 32),
        "other_watch_entry": replace(base, target_id=other.finding_a),
        "other_watch_revision": replace(base, target_id=other.revision_a),
        "other_watch_issue": replace(base, target_id=other.issue_a),
        "update_as_finding": replace(base, target_id=own.update_a, target_kind="finding"),
        "revision_as_entry": replace(base, target_id=own.revision_a, target_kind="entry"),
        "malformed_id": replace(base, target_id="ent-xyz"),
        "preference_with_correction_value": replace(base, value="misclassified"),
        "correction_with_preference_value": replace(base, kind="correction", value="useful"),
        "unknown_watch": replace(base, watch_id=UNKNOWN_WATCH_ID),
        "bad_key": replace(base, key="bad key!"),
        "long_key": replace(base, key="k" * 129),
        "control_note": replace(base, note="\x07bell"),
        "blank_note": replace(base, note="   "),
        "long_note": replace(base, note="n" * 501),
    }
    return cases[label]


@pytest.mark.parametrize(
    ("label", "reason"),
    [
        ("unknown_entry", "unknown_target"),
        ("unknown_revision", "unknown_target"),
        ("unknown_issue", "unknown_target"),
        ("other_watch_entry", "cross_watch_target"),
        ("other_watch_revision", "cross_watch_target"),
        ("other_watch_issue", "cross_watch_target"),
        ("update_as_finding", "target_kind_mismatch"),
        ("revision_as_entry", "target_kind_mismatch"),
        ("malformed_id", "invalid_feedback_request"),
        ("preference_with_correction_value", "value_kind_mismatch"),
        ("correction_with_preference_value", "value_kind_mismatch"),
        ("unknown_watch", "unknown_watch"),
        ("bad_key", "invalid_feedback_request"),
        ("long_key", "invalid_feedback_request"),
        ("control_note", "invalid_feedback_request"),
        ("blank_note", "invalid_feedback_request"),
        ("long_note", "invalid_feedback_request"),
    ],
)
def test_feedback_record_invalid_request_refuses_before_any_write(
    watches: TwoWatches, label: str, reason: str
) -> None:
    # Arrange
    request = invalid_request(label, watches)

    # Act
    exit_code, payload = record(request)

    # Assert
    assert exit_code == 1
    assert error_reason(payload) == reason
    assert feedback_rows(watches.state_path) == no_feedback()


# 7. Later preferences supersede earlier ones per scope without deleting them.


def test_feedback_record_later_preference_in_same_scope_supersedes_without_deleting(
    watches: TwoWatches,
) -> None:
    # Arrange
    own = watches.main
    stream = Feedback(WATCH_ID, own.finding_a, "k-a", target_kind="finding")
    first = record_new(stream)

    # Act
    second = record_new(replace(stream, key="k-b", value="irrelevant"))
    elsewhere = record_new(
        Feedback(WATCH_ID, own.second_finding_a, "k-c", "preference", "already_known", "finding")
    )
    history = list_feedback(WATCH_ID)

    # Assert
    assert [(event["feedback_id"], event["sequence"]) for event in history["preferences"]] == [
        (first["feedback_id"], 1),
        (second["feedback_id"], 2),
        (elsewhere["feedback_id"], 3),
    ]
    effective = {item["scope_key"]: item for item in history["effective_preferences"]}
    assert set(effective) == {first["scope_key"], elsewhere["scope_key"]}
    assert first["scope_key"] == f"{THREAD_A}/{own.theme_a}"
    superseding = effective[first["scope_key"]]
    assert (superseding["scope_kind"], superseding["value"]) == ("finding", "irrelevant")
    assert (superseding["feedback_id"], superseding["sequence"]) == (second["feedback_id"], 2)
    assert superseding["superseded_feedback_ids"] == [first["feedback_id"]]
    separate = effective[elsewhere["scope_key"]]
    assert (separate["value"], separate["feedback_id"]) == (
        "already_known",
        elsewhere["feedback_id"],
    )
    assert separate["superseded_feedback_ids"] == []


# 8. Each target kind records its own preference scope.


def test_feedback_record_preference_per_target_kind_records_matching_scope(
    watches: TwoWatches,
) -> None:
    # Arrange
    own = watches.main
    requests = [
        Feedback(WATCH_ID, own.update_a, "s-entry"),
        Feedback(WATCH_ID, own.finding_a, "s-finding", target_kind="finding"),
        Feedback(WATCH_ID, own.revision_b, "s-observation"),
        Feedback(WATCH_ID, own.issue_b, "s-issue"),
    ]

    # Act
    events = [record_new(request) for request in requests]

    # Assert
    shapes = [
        (
            event["target_kind"],
            event["scope_kind"],
            event["scope_key"],
            event["source_thread_id"],
            event["theme_key"],
        )
        for event in events
    ]
    assert shapes == [
        ("entry", "entry", own.update_a, THREAD_A, None),
        ("finding", "finding", f"{THREAD_A}/{own.theme_a}", THREAD_A, own.theme_a),
        ("observation", "thread", THREAD_B, THREAD_B, None),
        ("issue", "issue", own.issue_b, THREAD_B, None),
    ]
    assert [event["sequence"] for event in events] == [1, 2, 3, 4]


# 9. `feedback list --target` filters, refuses unknown targets and never writes.


def test_feedback_list_target_filters_refuses_unknown_and_writes_nothing(
    watches: TwoWatches,
) -> None:
    # Arrange
    own = watches.main
    on_finding = record_new(Feedback(WATCH_ID, own.finding_a, "l-pref"))
    on_revision = record_new(Feedback(WATCH_ID, own.revision_a, "l-rev", value="irrelevant"))
    correction = record_new(
        Feedback(WATCH_ID, own.finding_a, "l-corr", "correction", "unsupported")
    )
    state_before = watches.state_path.read_bytes()

    # Act
    finding_history = list_feedback(WATCH_ID, own.finding_a)
    revision_history = list_feedback(WATCH_ID, own.revision_a)
    unknown_code, unknown = run_cli(
        "feedback", "list", "--watch", WATCH_ID, "--target", "ent-" + "0" * 32
    )

    # Assert
    assert feedback_ids(finding_history["preferences"]) == [on_finding["feedback_id"]]
    assert feedback_ids(finding_history["corrections"]) == [correction["feedback_id"]]
    assert feedback_ids(revision_history["preferences"]) == [on_revision["feedback_id"]]
    assert revision_history["corrections"] == []
    assert revision_history["correction_view"] == []
    assert unknown_code == 1
    assert error_reason(unknown) == "unknown_target"
    assert watches.state_path.read_bytes() == state_before


# 10. Feedback survives later runs and digests; presented entries stay valid targets.


def test_feedback_history_after_later_run_and_digest_is_unchanged(
    watches: TwoWatches, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    own = watches.main
    record_new(Feedback(WATCH_ID, own.finding_a, "d-pref", target_kind="finding"))
    record_new(Feedback(WATCH_ID, own.revision_a, "d-corr", "correction", "duplicate"))
    history_before = list_feedback(WATCH_ID)
    presented = only_value(
        query_rows(
            watches.state_path,
            "SELECT entry_id FROM watch_digest_entries WHERE presented_at_utc IS NOT NULL "
            "AND digest_id = (SELECT digest_id FROM watch_digests WHERE watch_id = ? "
            "ORDER BY sequence LIMIT 1) ORDER BY entry_id LIMIT 1",
            (WATCH_ID,),
        )
    )

    # Act
    monkeypatch.setattr("threadsense.cli.build_reddit_connector", refuse_fetch)
    digest_code, digest = run_cli("watch", "digest", "--id", WATCH_ID)
    watch_run(monkeypatch, WATCH_ID, "unchanged")
    history_after = list_feedback(WATCH_ID)
    later = record_new(Feedback(WATCH_ID, presented, "d-presented", value="already_known"))

    # Assert
    assert (digest_code, digest["status"]) == (0, "presented")
    assert history_after == history_before
    assert (later["target_id"], later["sequence"]) == (presented, 2)
    assert (later["scope_kind"], later["scope_key"]) == ("entry", presented)


# Ranking: digest artifacts, explanations and policy files.

LATER_PHASES = ("unchanged", "score_only", "search_omits_known")
SEVERITY_ORDER = {"high": 3, "medium": 2, "low": 1}
KIND_ORDER = {"finding": 0, "thread_update": 1}
COMPONENTS = ["severity", "objective", "novelty", "evidence", "preference", "semantic"]
POLICY_ID = re.compile(r"pol-[0-9a-f]{32}")
EntryKey = tuple[str, str, str | None]


class ProcessDeathError(BaseException):
    """Simulates the process dying mid-digest: nothing in the CLI may handle it."""


@dataclass(frozen=True)
class DigestFiles:
    digest_id: str
    status: str
    json_path: Path
    markdown_path: Path

    def document(self) -> dict[str, Any]:
        document: dict[str, Any] = json.loads(self.json_path.read_text(encoding="utf-8"))
        return document

    def hashes(self) -> tuple[str, str]:
        return (
            hashlib.sha256(self.json_path.read_bytes()).hexdigest(),
            hashlib.sha256(self.markdown_path.read_bytes()).hexdigest(),
        )


def digests_of(state_path: Path, watch_id: str) -> list[DigestFiles]:
    rows = query_rows(
        state_path,
        "SELECT digest_id, status, json_path, markdown_path FROM watch_digests "
        "WHERE watch_id = ? ORDER BY sequence",
        (watch_id,),
    )
    root = state_path.parent
    return [
        DigestFiles(str(digest_id), str(status), root / str(json_rel), root / str(markdown_rel))
        for digest_id, status, json_rel, markdown_rel in rows
    ]


def latest_digest(state_path: Path, watch_id: str) -> DigestFiles:
    return digests_of(state_path, watch_id)[-1]


def present_digest(watch_id: str) -> dict[str, Any]:
    exit_code, payload = run_cli("watch", "digest", "--id", watch_id)
    assert exit_code == 0, payload
    assert payload["status"] == "presented", payload
    return payload


def run_later_phases(monkeypatch: pytest.MonkeyPatch, watch_id: str) -> dict[str, Any]:
    """Replay phases after `initial` up to the next new revision; return its run payload."""
    payloads = [watch_run(monkeypatch, watch_id, name) for name in LATER_PHASES]
    assert payloads[-1]["digest"]["status"] == "presented", payloads[-1]
    return payloads[-1]


def entry_payloads(state_path: Path, watch_id: str) -> dict[str, str]:
    rows = query_rows(
        state_path,
        "SELECT entry_id, payload_json FROM watch_digest_entries WHERE watch_id = ?",
        (watch_id,),
    )
    return {str(entry_id): str(payload_json) for entry_id, payload_json in rows}


def top_finding(state_path: Path, watch_id: str) -> str:
    """Thread B's unpresented finding with the higher severity."""
    severities = {
        str(entry_id): SEVERITY_ORDER[str(json.loads(payload_json)["severity"])]
        for entry_id, payload_json in query_rows(
            state_path,
            "SELECT entry_id, payload_json FROM watch_digest_entries "
            "WHERE watch_id = ? AND source_thread_id = ? AND kind = 'finding' "
            "AND digest_id IS NULL",
            (watch_id, THREAD_B),
        )
    }
    assert len(set(severities.values())) == 2, severities
    return max(severities, key=severities.__getitem__)


def entry_key(entry: dict[str, Any]) -> EntryKey:
    """An entry's evidence identity, equal across watches replaying the same fixture."""
    theme_key = entry.get("theme_key")
    return (str(entry["kind"]), str(entry["source_thread_id"]), theme_key)


def keys_by_id(document: dict[str, Any]) -> dict[str, EntryKey]:
    return {str(entry["entry_id"]): entry_key(entry) for entry in document["entries"]}


def ranked_items(document: dict[str, Any]) -> list[dict[str, Any]]:
    order: list[dict[str, Any]] = document["ranking"]["order"]
    return order


def ranked_ids(document: dict[str, Any]) -> list[str]:
    return [str(item["entry_id"]) for item in ranked_items(document)]


def item_for(document: dict[str, Any], entry_id: str) -> dict[str, Any]:
    [item] = [item for item in ranked_items(document) if item["entry_id"] == entry_id]
    return item


def component(item: dict[str, Any], name: str) -> dict[str, Any]:
    [found] = [part for part in item["components"] if part["component"] == name]
    result: dict[str, Any] = found
    return result


def evidence_by_key(document: dict[str, Any]) -> dict[EntryKey, tuple[Any, ...]]:
    """Each entry's evidence facts: counts, severity and novel comment text hashes."""
    projections: dict[EntryKey, tuple[Any, ...]] = {}
    for entry in document["entries"]:
        novel_key = "novel_evidence" if entry["kind"] == "finding" else "novel_comments"
        comments = tuple(
            (comment["comment_id"], comment["kind"], comment["text_sha256"], comment["excerpt"])
            for comment in entry[novel_key]
        )
        projections[entry_key(entry)] = (
            entry.get("severity"),
            entry.get("finding_comment_count"),
            comments,
        )
    return projections


def explain(watch_id: str, entry_id: str, *flags: str) -> tuple[int, dict[str, Any]]:
    return run_cli("watch", "explain", "--id", watch_id, "--entry", entry_id, *flags)


def explain_ok(watch_id: str, entry_id: str, *flags: str) -> dict[str, Any]:
    exit_code, payload = explain(watch_id, entry_id, *flags)
    assert exit_code == 0, payload
    assert payload["artifact_type"] == "watch_explanation", payload
    return payload


def policy_status(watch_id: str) -> dict[str, Any]:
    exit_code, payload = run_cli("watch", "policy", "--id", watch_id)
    assert exit_code == 0, payload
    assert (payload["artifact_type"], payload["status"]) == ("ranking_policy", "ok")
    return payload


def set_policy(watch_id: str, path: Path) -> tuple[int, dict[str, Any]]:
    return run_cli("watch", "policy", "--id", watch_id, "--set", str(path))


def selection(exit_code: int, payload: dict[str, Any]) -> dict[str, Any]:
    assert exit_code == 0, payload
    assert payload["artifact_type"] == "ranking_policy_selection", payload
    return payload


def select_policy(watch_id: str, policy_id: str) -> dict[str, Any]:
    return selection(*run_cli("watch", "policy", "--id", watch_id, "--select", policy_id))


def toml_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | str):
        return json.dumps(value)
    raise AssertionError(f"unsupported policy value {value!r}")


def toml_lines(table: dict[str, Any], prefix: str) -> list[str]:
    scalars = {key: value for key, value in table.items() if not isinstance(value, dict)}
    lines = [f"{key} = {toml_value(value)}" for key, value in scalars.items()]
    for key, value in table.items():
        if isinstance(value, dict):
            name = f"{prefix}.{key}" if prefix else key
            lines += ["", f"[{name}]", *toml_lines(value, name)]
    return lines


def write_policy(path: Path, document: dict[str, Any]) -> Path:
    """Write a policy document (the `watch policy` active_policy shape) as TOML."""
    path.write_text("\n".join(toml_lines(document, "")) + "\n", encoding="utf-8")
    return path


def renamed_policy(document: dict[str, Any], name: str) -> dict[str, Any]:
    copied: dict[str, Any] = json.loads(json.dumps(document))
    copied["name"] = name
    return copied


def custom_policy(document: dict[str, Any], name: str) -> dict[str, Any]:
    """The given policy with a different `useful` preference weight."""
    custom = renamed_policy(document, name)
    custom["preference"]["useful"] = int(document["preference"]["useful"]) // 4
    assert custom["preference"]["useful"] != document["preference"]["useful"]
    return custom


def zero_weights(table: dict[str, Any]) -> dict[str, Any]:
    return {
        key: zero_weights(value) if isinstance(value, dict) else 0 for key, value in table.items()
    }


def flat_policy(document: dict[str, Any], name: str) -> dict[str, Any]:
    """Every weight and the preference bound 0, so every entry scores 0."""
    flat = renamed_policy(document, name)
    for key, value in document.items():
        if isinstance(value, dict):
            flat[key] = zero_weights(value)
    return flat


def assert_components_within_policy(
    item: dict[str, Any], policy: dict[str, Any], objective: str
) -> None:
    points = {part["component"]: part["points"] for part in item["components"]}
    evidence = policy["evidence"]
    bound = int(policy["preference"]["bound"])
    assert [part["component"] for part in item["components"]] == COMPONENTS, item
    assert item["score"] == sum(value for value in points.values() if value is not None), item
    assert points["severity"] in {0, *policy["severity"].values()}, item
    assert points["objective"] in {0, *policy["objective"][objective].values()}, item
    assert points["novelty"] in {0, *policy["novelty"].values()}, item
    evidence_cap = evidence["points_per_novel_comment"] * evidence["max_counted_comments"]
    assert 0 <= points["evidence"] <= evidence_cap, item
    assert -bound <= points["preference"] <= bound, item


def baseline_key(item: dict[str, Any]) -> tuple[int, int, int, int, int, str, str]:
    """The documented baseline: kind, severity, markers, comment count, theme, entry id."""
    tie = item["tie_break"]
    return (
        KIND_ORDER[str(tie["kind"])],
        -SEVERITY_ORDER.get(str(tie["severity"]), 0),
        -int(tie["issue_markers"]),
        -int(tie["request_markers"]),
        -int(tie["comment_count"]),
        str(tie["theme_key"] or ""),
        str(tie["entry_id"]),
    )


def ranking_signature(document: dict[str, Any]) -> list[tuple[Any, ...]]:
    """Each ranked entry by evidence identity, without its watch-specific entry id."""
    keys = keys_by_id(document)
    return [
        (
            keys[str(item["entry_id"])],
            item["rank"],
            item["score"],
            item["components"],
            {key: value for key, value in item["tie_break"].items() if key != "entry_id"},
        )
        for item in ranked_items(document)
    ]


def expire_dead_claim(monkeypatch: pytest.MonkeyPatch, state_path: Path) -> None:
    """Run the CLI store on a clock past the dead digest run's lease."""
    [(expires,)] = query_rows(
        state_path, "SELECT MAX(lease_expires_at_utc) FROM watch_runs WHERE status = 'running'"
    )
    assert expires is not None
    offset = float(expires) + 1.0 - time.time()
    monkeypatch.setattr(
        "threadsense.cli.MonitoringStore",
        partial(MonitoringStore, clock=lambda: time.time() + offset),
    )


# 11. A preference reorders presentation only; evidence, counts and payloads are unchanged.


def test_watch_digest_irrelevant_preference_moves_finding_last_without_changing_evidence(
    watches: TwoWatches,
) -> None:
    # Arrange
    state_path = watches.state_path
    policy = policy_status(WATCH_ID)["active_policy"]
    payloads_before = entry_payloads(state_path, WATCH_ID)
    target = top_finding(state_path, WATCH_ID)
    preference = record_new(
        Feedback(WATCH_ID, target, "r-irrelevant", value="irrelevant", target_kind="finding")
    )
    payloads_after_feedback = entry_payloads(state_path, WATCH_ID)

    # Act
    control_result = present_digest(OTHER_WATCH_ID)
    treated_result = present_digest(WATCH_ID)
    control = latest_digest(state_path, OTHER_WATCH_ID).document()
    treated_files = latest_digest(state_path, WATCH_ID)
    treated = treated_files.document()

    # Assert
    assert payloads_after_feedback == payloads_before
    assert entry_payloads(state_path, WATCH_ID) == payloads_before
    for result, document, watch_id in (
        (control_result, control, OTHER_WATCH_ID),
        (treated_result, treated, WATCH_ID),
    ):
        stored = entry_payloads(state_path, watch_id)
        assert document["entries"] == [json.loads(stored[item]) for item in result["entry_ids"]]
        assert sorted(ranked_ids(document)) == sorted(result["entry_ids"])
        assert result["ranking"]["ranked_entry_ids"] == ranked_ids(document)
    assert evidence_by_key(treated) == evidence_by_key(control)
    assert len(evidence_by_key(treated)) == len(treated["entries"]) == 3
    assert treated["backlog"] == control["backlog"]
    assert treated_result["backlog"] == control_result["backlog"]
    control_keys, treated_keys = keys_by_id(control), keys_by_id(treated)
    target_key = treated_keys[target]
    control_order = [control_keys[entry_id] for entry_id in ranked_ids(control)]
    treated_order = [treated_keys[entry_id] for entry_id in ranked_ids(treated)]
    assert control_order[0] == target_key
    assert treated_order == [*(key for key in control_order if key != target_key), target_key]
    moved = component(item_for(treated, target), "preference")
    assert moved["points"] == policy["preference"]["irrelevant"]
    assert [match["feedback_id"] for match in moved["matched"]] == [preference["feedback_id"]]
    control_scores = {control_keys[str(i["entry_id"])]: i["score"] for i in ranked_items(control)}
    treated_scores = {treated_keys[str(i["entry_id"])]: i["score"] for i in ranked_items(treated)}
    assert treated_scores == {
        **control_scores,
        target_key: control_scores[target_key] + moved["points"],
    }
    assert control["ranking"]["basis"]["preference_sequence"] == 0
    assert treated["ranking"]["basis"]["preference_sequence"] == preference["sequence"] == 1
    markdown = treated_files.markdown_path.read_text(encoding="utf-8")
    positions = [markdown.index(f"- Entry: {entry_id}\n") for entry_id in ranked_ids(treated)]
    assert positions == sorted(positions)


# 12. Explain replays the recorded order after new feedback, restarts and policy switches.


def test_watch_explain_after_new_feedback_and_policy_switch_replays_recorded_rank(
    watches: TwoWatches, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Arrange
    state_path = watches.state_path
    initial = policy_status(WATCH_ID)
    default_id = str(initial["default_policy_id"])
    target = top_finding(state_path, WATCH_ID)
    irrelevant = record_new(
        Feedback(WATCH_ID, target, "e-irrelevant", value="irrelevant", target_kind="finding")
    )
    present_digest(WATCH_ID)
    earlier = latest_digest(state_path, WATCH_ID)
    earlier_hashes = earlier.hashes()
    recorded = explain_ok(WATCH_ID, target)
    custom_path = write_policy(
        tmp_path / "custom.toml", custom_policy(initial["active_policy"], "explain-custom")
    )

    # Act
    useful = record_new(
        Feedback(WATCH_ID, target, "e-useful", value="useful", target_kind="finding")
    )
    custom = selection(*set_policy(WATCH_ID, custom_path))
    hashes_after_set = earlier.hashes()
    run_later_phases(monkeypatch, WATCH_ID)
    later = latest_digest(state_path, WATCH_ID)
    reselected = select_policy(WATCH_ID, default_id)
    replayed = explain_ok(WATCH_ID, target)
    current = explain_ok(WATCH_ID, target, "--current")

    # Assert
    assert (recorded["status"], recorded["replay"]) == ("ranked", "matches_written_digest")
    assert (recorded["basis"]["policy_id"], recorded["basis"]["preference_sequence"]) == (
        default_id,
        irrelevant["sequence"],
    )
    assert recorded["ranking"] == item_for(earlier.document(), target)
    preference = component(recorded["ranking"], "preference")
    assert [match["feedback_id"] for match in preference["matched"]] == [irrelevant["feedback_id"]]
    assert (replayed["status"], replayed["replay"]) == ("ranked", "matches_written_digest")
    for key in ("basis", "ranking", "order", "semantic", "digest"):
        assert replayed[key] == recorded[key], key
    assert current["ranking"] == recorded["ranking"]
    assert current["current"]["recorded"] is False
    assert current["current"]["basis"]["policy_id"] == default_id
    assert current["current"]["basis"]["preference_sequence"] == useful["sequence"] == 2
    assert current["current"]["score"] != recorded["ranking"]["score"]
    current_preference = component(current["current"], "preference")
    assert [match["feedback_id"] for match in current_preference["matched"]] == [
        useful["feedback_id"]
    ]
    assert (custom["status"], reselected["status"]) == ("selected", "selected")
    assert reselected["previous_policy_id"] == custom["policy_id"]
    assert later.digest_id != earlier.digest_id
    later_basis = later.document()["ranking"]["basis"]
    assert (later_basis["policy_id"], later_basis["policy_name"]) == (
        custom["policy_id"],
        "explain-custom",
    )
    assert later_basis["preference_sequence"] == useful["sequence"]
    assert hashes_after_set == earlier_hashes
    assert earlier.hashes() == earlier_hashes


# 13. Switching policies appends selections and never rewrites earlier digests.


def test_watch_policy_switch_back_appends_selections_without_rewriting_digests(
    watches: TwoWatches, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Arrange
    state_path = watches.state_path
    initial = policy_status(WATCH_ID)
    default_id = str(initial["default_policy_id"])
    first_digest = latest_digest(state_path, WATCH_ID)
    first_hashes = first_digest.hashes()
    custom_path = write_policy(
        tmp_path / "custom.toml", custom_policy(initial["active_policy"], "switch-custom")
    )

    # Act
    selected = selection(*set_policy(WATCH_ID, custom_path))
    repeated = selection(*set_policy(WATCH_ID, custom_path))
    custom_result = present_digest(WATCH_ID)
    second_digest = latest_digest(state_path, WATCH_ID)
    second_hashes = second_digest.hashes()
    switched_back = select_policy(WATCH_ID, default_id)
    repeated_back = select_policy(WATCH_ID, default_id)
    run_later_phases(monkeypatch, WATCH_ID)
    final = policy_status(WATCH_ID)
    digests = digests_of(state_path, WATCH_ID)

    # Assert
    custom_id = str(selected["policy_id"])
    assert (initial["active_policy_id"], initial["selections"]) == (default_id, [])
    assert POLICY_ID.fullmatch(custom_id) is not None
    assert custom_id != default_id
    assert (selected["status"], selected["previous_policy_id"]) == ("selected", default_id)
    assert selected["active_policy"]["name"] == "switch-custom"
    assert (repeated["status"], repeated["policy_id"], repeated["previous_policy_id"]) == (
        "unchanged",
        custom_id,
        custom_id,
    )
    assert (
        switched_back["status"],
        switched_back["policy_id"],
        switched_back["previous_policy_id"],
    ) == ("selected", default_id, custom_id)
    assert switched_back["active_policy"] == initial["active_policy"]
    assert (repeated_back["status"], repeated_back["policy_id"]) == ("unchanged", default_id)
    assert [
        (item["sequence"], item["policy_id"], item["name"]) for item in final["selections"]
    ] == [(1, custom_id, "switch-custom"), (2, default_id, initial["active_policy"]["name"])]
    assert final["active_policy_id"] == default_id
    assert custom_result["ranking"]["policy_id"] == custom_id
    assert [digest.status for digest in digests] == ["written"] * 3
    assert [digest.document()["ranking"]["basis"]["policy_id"] for digest in digests] == [
        default_id,
        custom_id,
        default_id,
    ]
    assert query_rows(
        state_path,
        "SELECT d.sequence, r.policy_id FROM watch_digest_rankings AS r "
        "JOIN watch_digests AS d ON d.digest_id = r.digest_id "
        "WHERE r.watch_id = ? ORDER BY d.sequence",
        (WATCH_ID,),
    ) == [(1, default_id), (2, custom_id), (3, default_id)]
    assert first_digest.hashes() == first_hashes
    assert second_digest.hashes() == second_hashes


# 14. Preferences on every scope add up, then are bounded with a reason.


def test_watch_digest_useful_preferences_on_every_scope_bound_preference_points(
    watches: TwoWatches,
) -> None:
    # Arrange
    state_path = watches.state_path
    own = watches.main
    policy = policy_status(WATCH_ID)["active_policy"]
    target = top_finding(state_path, WATCH_ID)
    requests = [
        Feedback(WATCH_ID, target, "b-entry"),
        Feedback(WATCH_ID, target, "b-finding", target_kind="finding"),
        Feedback(WATCH_ID, own.revision_b, "b-observation"),
        Feedback(WATCH_ID, own.issue_b, "b-issue"),
    ]
    recorded = [record_new(request) for request in requests]

    # Act
    present_digest(WATCH_ID)
    document = latest_digest(state_path, WATCH_ID).document()

    # Assert
    useful = int(policy["preference"]["useful"])
    bound = int(policy["preference"]["bound"])
    preference = component(item_for(document, target), "preference")
    matched = preference["matched"]
    assert [match["feedback_id"] for match in matched] == feedback_ids(recorded)
    assert [(match["scope_kind"], match["value"], match["points"]) for match in matched] == [
        ("entry", "useful", useful),
        ("finding", "useful", useful),
        ("thread", "useful", useful),
        ("issue", "useful", useful),
    ]
    assert matched[3]["scope_key"] == own.issue_b
    assert preference["unbounded_points"] == 4 * useful
    assert 4 * useful > bound
    assert (preference["points"], preference["bound"]) == (bound, bound)
    assert "bounded" in preference["reason"]
    assert str(4 * useful) in preference["reason"]
    objective = str(document["ranking"]["basis"]["objective"])
    for item in ranked_items(document):
        assert_components_within_policy(item, policy, objective)
        semantic = component(item, "semantic")
        assert (semantic["points"], semantic["status"]) == (None, "not_eligible")
    assert document["ranking"]["semantic"]["status"] == "not_eligible"


# 15. Equal scores fall back to the baseline order, then the entry id, on every replay.


def test_watch_digest_equal_scores_order_by_baseline_then_entry_id(
    watches: TwoWatches, tmp_path: Path
) -> None:
    # Arrange
    state_path = watches.state_path
    base = policy_status(WATCH_ID)["active_policy"]
    flat_path = write_policy(tmp_path / "flat.toml", flat_policy(base, "flat-ties"))
    selected = selection(*set_policy(WATCH_ID, flat_path))

    # Act
    result = present_digest(WATCH_ID)
    document = latest_digest(state_path, WATCH_ID).document()
    items = ranked_items(document)
    first = [explain_ok(WATCH_ID, str(item["entry_id"])) for item in items]
    second = [explain_ok(WATCH_ID, str(item["entry_id"])) for item in items]

    # Assert
    assert selected["status"] == "selected"
    assert document["ranking"]["basis"]["policy_id"] == selected["policy_id"]
    assert len(items) == 3
    assert {item["score"] for item in items} == {0}
    assert all(part["points"] in (0, None) for item in items for part in item["components"])
    assert all(item["tie_break"]["entry_id"] == item["entry_id"] for item in items)
    assert items == sorted(items, key=baseline_key)
    assert [item["rank"] for item in items] == [1, 2, 3]
    assert [item["kind"] for item in items] == ["finding", "finding", "thread_update"]
    finding_severities = [SEVERITY_ORDER[str(item["tie_break"]["severity"])] for item in items[:2]]
    assert finding_severities[0] > finding_severities[1]
    assert result["ranking"]["ranked_entry_ids"] == ranked_ids(document)
    expected_order = [
        {"rank": item["rank"], "entry_id": item["entry_id"], "score": item["score"]}
        for item in items
    ]
    for item, explained, again in zip(items, first, second, strict=True):
        assert explained == again
        assert explained["replay"] == "matches_written_digest"
        assert explained["ranking"] == item
        assert explained["order"] == expected_order


# 16. Corrections are never a ranking input.


def test_watch_digest_with_corrections_ranks_identically_to_control_watch(
    watches: TwoWatches,
) -> None:
    # Arrange
    state_path = watches.state_path
    target = top_finding(state_path, WATCH_ID)
    corrections = [
        record_new(Feedback(WATCH_ID, target, "c-misclassified", "correction", "misclassified")),
        record_new(
            Feedback(
                WATCH_ID, watches.main.revision_b, "c-unsupported", "correction", "unsupported"
            )
        ),
    ]

    # Act
    present_digest(OTHER_WATCH_ID)
    treated_result = present_digest(WATCH_ID)
    control = latest_digest(state_path, OTHER_WATCH_ID).document()
    treated = latest_digest(state_path, WATCH_ID).document()

    # Assert
    assert feedback_ids(list_feedback(WATCH_ID)["corrections"]) == feedback_ids(corrections)
    assert len(ranking_signature(treated)) == 3
    assert ranking_signature(treated) == ranking_signature(control)
    assert treated["ranking"]["corrections"] == "not_a_ranking_input"
    assert control["ranking"]["corrections"] == "not_a_ranking_input"
    assert treated["ranking"]["basis"]["preference_sequence"] == 0
    assert treated_result["ranking"]["preference_sequence"] == 0
    assert all(component(item, "preference")["matched"] == [] for item in ranked_items(treated))


# 17. A resumed digest keeps the ranking basis recorded before the crash.


def test_watch_digest_resume_after_crash_keeps_recorded_ranking_basis(
    watches: TwoWatches, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    state_path = watches.state_path
    target = top_finding(state_path, WATCH_ID)

    def die(*args: Any, **kwargs: Any) -> None:
        raise ProcessDeathError

    with monkeypatch.context() as patch:
        patch.setattr(MonitoringStore, "complete_digest", die)
        with pytest.raises(ProcessDeathError):
            run_cli("watch", "digest", "--id", WATCH_ID)
    crashed = latest_digest(state_path, WATCH_ID)
    crashed_json = crashed.json_path.read_bytes()
    crashed_markdown = crashed.markdown_path.read_bytes()
    crashed_ranking = json.loads(crashed_json)["ranking"]
    expire_dead_claim(monkeypatch, state_path)
    preference = record_new(
        Feedback(WATCH_ID, target, "x-irrelevant", value="irrelevant", target_kind="finding")
    )
    pending = explain_ok(WATCH_ID, target)

    # Act
    exit_code, resumed = run_cli("watch", "digest", "--id", WATCH_ID)
    written = latest_digest(state_path, WATCH_ID)
    after = explain_ok(WATCH_ID, target, "--current")

    # Assert
    assert crashed.status == "pending"
    assert crashed_ranking["basis"]["preference_sequence"] == 0
    assert preference["sequence"] == 1
    assert (pending["status"], pending["replay"]) == ("ranked", "pending_digest")
    assert pending["ranking"] == item_for({"ranking": crashed_ranking}, target)
    assert exit_code == 0, resumed
    assert (resumed["status"], resumed["resumed"]) == ("presented", True)
    assert resumed["digest_id"] == crashed.digest_id == written.digest_id
    assert written.status == "written"
    assert resumed["ranking"]["preference_sequence"] == 0
    assert resumed["ranking"]["ranked_entry_ids"] == [
        item["entry_id"] for item in crashed_ranking["order"]
    ]
    assert crashed.json_path.read_bytes() == crashed_json
    assert crashed.markdown_path.read_bytes() == crashed_markdown
    assert (after["replay"], after["ranking"]) == ("matches_written_digest", pending["ranking"])
    assert component(after["ranking"], "preference")["matched"] == []
    assert after["current"]["basis"]["preference_sequence"] == preference["sequence"]
    assert after["current"]["score"] < after["ranking"]["score"]


# 18. Explain refuses unknown, foreign, malformed and tampered references.


@pytest.mark.parametrize(
    ("label", "reason"),
    [
        ("unknown", "unknown_entry"),
        ("other_watch", "cross_watch_target"),
        ("malformed", "invalid_entry_id"),
    ],
)
def test_watch_explain_invalid_entry_reference_refuses_with_reason(
    watches: TwoWatches, label: str, reason: str
) -> None:
    # Arrange
    entry_ids = {
        "unknown": "ent-" + "0" * 32,
        "other_watch": watches.other.finding_a,
        "malformed": "ent-not-an-entry",
    }
    state_before = watches.state_path.read_bytes()

    # Act
    exit_code, payload = explain(WATCH_ID, entry_ids[label])

    # Assert
    assert exit_code == 1
    assert error_reason(payload) == reason
    assert watches.state_path.read_bytes() == state_before


def test_watch_explain_tampered_digest_artifact_refuses_hash_mismatch(
    watches: TwoWatches,
) -> None:
    # Arrange
    entry_id = watches.main.finding_a
    before = explain_ok(WATCH_ID, entry_id)
    digest = digests_of(watches.state_path, WATCH_ID)[0]
    with digest.json_path.open("ab") as handle:
        handle.write(b" ")

    # Act
    exit_code, payload = explain(WATCH_ID, entry_id)

    # Assert
    assert (before["status"], before["replay"]) == ("ranked", "matches_written_digest")
    assert before["digest"]["digest_id"] == digest.digest_id
    assert exit_code == 1
    assert error_reason(payload) == "artifact_hash_mismatch"


def test_watch_explain_ranked_artifact_without_basis_refuses_basis_missing(
    watches: TwoWatches,
) -> None:
    # Arrange
    entry_id = watches.main.finding_a
    digest = digests_of(watches.state_path, WATCH_ID)[0]
    with closing(sqlite3.connect(watches.state_path)) as conn, conn:
        conn.execute("DROP TRIGGER watch_digest_rankings_no_delete")
        conn.execute("DELETE FROM watch_digest_rankings WHERE digest_id = ?", (digest.digest_id,))

    # Act
    exit_code, payload = explain(WATCH_ID, entry_id)

    # Assert
    assert exit_code == 1
    assert error_reason(payload) == "ranking_basis_missing"


def test_watch_explain_unpresented_entry_reports_no_digest_and_no_ranking(
    watches: TwoWatches,
) -> None:
    # Arrange
    entry_id = watches.main.finding_b
    [(digest_id,)] = query_rows(
        watches.state_path,
        "SELECT digest_id FROM watch_digest_entries WHERE entry_id = ?",
        (entry_id,),
    )

    # Act
    payload = explain_ok(WATCH_ID, entry_id)
    with_current = explain_ok(WATCH_ID, entry_id, "--current")

    # Assert
    assert digest_id is None
    assert (payload["status"], payload["digest"], payload["ranking"]) == ("unpresented", None, None)
    assert "current" not in payload
    current = with_current["current"]
    assert (current["recorded"], current["entry_id"], current["rank_scope"]) == (
        False,
        entry_id,
        "entry_only",
    )
    assert "rank" not in current
    assert current["score"] == sum(
        part["points"] for part in current["components"] if part["points"] is not None
    )


# 19. Invalid policy files are refused and select nothing.


def invalid_policy(label: str, base: dict[str, Any]) -> dict[str, Any]:
    document = renamed_policy(base, "invalid-policy")
    match label:
        case "unknown_field":
            document["comment"] = "not a policy field"
        case "missing_objective":
            del document["objective"][next(iter(document["objective"]))]
        case "out_of_range_weight":
            document["severity"]["high"] = 101
        case "bool_weight":
            document["severity"]["high"] = True
        case "semantic_weights":
            document["semantic"] = {"annotation_weight": 10}
        case _:
            raise AssertionError(label)
    return document


@pytest.mark.parametrize(
    ("label", "field"),
    [
        ("unknown_field", "document"),
        ("missing_objective", "objective"),
        ("out_of_range_weight", "severity.high"),
        ("bool_weight", "severity.high"),
        ("semantic_weights", "document"),
    ],
)
def test_watch_policy_set_invalid_policy_file_refuses_without_selection(
    watches: TwoWatches, tmp_path: Path, label: str, field: str
) -> None:
    # Arrange
    state_path = watches.state_path
    status_before = policy_status(WATCH_ID)
    tables = ("watch_ranking_policies", "watch_ranking_selections")
    rows_before = {
        table: sorted(query_rows(state_path, f"SELECT * FROM {table}"), key=repr)
        for table in tables
    }
    path = write_policy(
        tmp_path / f"{label}.toml", invalid_policy(label, status_before["active_policy"])
    )

    # Act
    exit_code, payload = set_policy(WATCH_ID, path)

    # Assert
    assert exit_code == 1
    assert error_reason(payload) == "invalid_ranking_policy"
    assert payload["error"]["details"]["field"] == field
    assert policy_status(WATCH_ID) == status_before
    assert {
        table: sorted(query_rows(state_path, f"SELECT * FROM {table}"), key=repr)
        for table in tables
    } == rows_before
