"""Operator feedback through the real CLI, recorded sweeps and real SQLite.

Replays tests/fixtures/watch/incremental/ (synthesized Reddit payloads; see its README.md,
never live data) through in-process `threadsense.cli.main` with JSON output: `watch run`,
`watch digest`, `issues inspect` and the `feedback record|list` commands.
"""

from __future__ import annotations

import hashlib
import io
import json
import sqlite3
from contextlib import closing, redirect_stdout
from dataclasses import dataclass, replace
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
