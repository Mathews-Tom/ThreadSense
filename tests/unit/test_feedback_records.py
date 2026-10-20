from __future__ import annotations

import dataclasses
import sqlite3
import tomllib
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest

from threadsense.errors import MonitoringInputError, MonitoringStateError
from threadsense.monitoring import MonitoringStore, parse_watch_profile
from threadsense.monitoring.feedback_records import (
    CORRECTION_VALUES,
    FEEDBACK_ID_PATTERN,
    PREFERENCE_VALUES,
    FeedbackRequest,
    effective_preferences,
    feedback_id_for,
    feedback_request,
)

FIXTURE_PROFILE = Path(__file__).resolve().parents[1] / "fixtures" / "watch" / "profile_v1.toml"
WATCH_ID = "agent-tooling"
ENTRY_ID = "ent-" + "a" * 32
REVISION_ID = "rev-" + "b" * 32
ISSUE_ID = "iss-" + "c" * 32
FEEDBACK_TABLES = ("watch_feedback_keys", "watch_preference_events", "watch_correction_events")


def request_of(**changes: Any) -> FeedbackRequest:
    arguments: dict[str, Any] = {
        "idempotency_key": "key-1",
        "kind": "preference",
        "value": "useful",
        "target_id": ENTRY_ID,
        "target_kind": None,
        "note": None,
    }
    arguments.update(changes)
    watch_id = str(arguments.pop("watch_id", WATCH_ID))
    return feedback_request(watch_id, **arguments)


def input_error_details(**changes: Any) -> dict[str, Any]:
    with pytest.raises(MonitoringInputError) as raised:
        request_of(**changes)
    return raised.value.details


@pytest.mark.parametrize(
    ("target_id", "expected_kind"),
    [(ENTRY_ID, "entry"), (REVISION_ID, "observation"), (ISSUE_ID, "issue")],
)
def test_feedback_request_without_target_kind_defaults_by_id_prefix(
    target_id: str, expected_kind: str
) -> None:
    request = request_of(target_id=target_id)

    assert request.target_kind == expected_kind
    assert request.target_id == target_id


def test_feedback_request_explicit_finding_for_entry_id_is_accepted() -> None:
    request = request_of(target_kind="finding")

    assert request.target_kind == "finding"


@pytest.mark.parametrize(
    ("target_kind", "target_id"),
    [
        ("finding", REVISION_ID),
        ("entry", ISSUE_ID),
        ("observation", ENTRY_ID),
        ("issue", REVISION_ID),
        ("entry", "bogus"),
    ],
)
def test_feedback_request_kind_not_matching_id_raises_target_kind_mismatch(
    target_kind: str, target_id: str
) -> None:
    details = input_error_details(target_kind=target_kind, target_id=target_id)

    assert details["reason"] == "target_kind_mismatch"
    assert details["target_kind"] == target_kind
    assert details["target_id"] == target_id


def test_feedback_request_unknown_target_kind_raises_invalid_target_kind() -> None:
    details = input_error_details(target_kind="thread")

    assert details["reason"] == "invalid_feedback_request"
    assert details["field"] == "target_kind"


@pytest.mark.parametrize(
    "target_id",
    ["bogus", "thr-" + "a" * 32, "ent-" + "a" * 31, "ent-" + "A" * 32, "iss-" + "c" * 33, ""],
)
def test_feedback_request_malformed_target_id_raises_invalid_target_id(target_id: str) -> None:
    details = input_error_details(target_id=target_id)

    assert details["reason"] == "invalid_feedback_request"
    assert details["field"] == "target_id"


@pytest.mark.parametrize("value", PREFERENCE_VALUES)
def test_feedback_request_preference_value_accepted_only_as_preference(value: str) -> None:
    accepted = request_of(kind="preference", value=value)

    details = input_error_details(kind="correction", value=value)

    assert (accepted.kind, accepted.value) == ("preference", value)
    assert details["reason"] == "value_kind_mismatch"
    assert details["kind"] == "correction"
    assert details["allowed_values"] == list(CORRECTION_VALUES)


@pytest.mark.parametrize("value", CORRECTION_VALUES)
def test_feedback_request_correction_value_accepted_only_as_correction(value: str) -> None:
    accepted = request_of(kind="correction", value=value)

    details = input_error_details(kind="preference", value=value)

    assert (accepted.kind, accepted.value) == ("correction", value)
    assert details["reason"] == "value_kind_mismatch"
    assert details["kind"] == "preference"
    assert details["allowed_values"] == list(PREFERENCE_VALUES)


def test_feedback_request_unknown_kind_raises_invalid_kind() -> None:
    details = input_error_details(kind="rating")

    assert details["reason"] == "invalid_feedback_request"
    assert details["field"] == "kind"


@pytest.mark.parametrize("idempotency_key", ["a" * 128, "A", "9", "key.with_all:allowed-chars"])
def test_feedback_request_valid_idempotency_key_is_accepted(idempotency_key: str) -> None:
    request = request_of(idempotency_key=idempotency_key)

    assert request.idempotency_key == idempotency_key


@pytest.mark.parametrize(
    "idempotency_key", ["a" * 129, "-key", ".key", "key with space", "", "key/slash"]
)
def test_feedback_request_invalid_idempotency_key_raises_invalid_key(
    idempotency_key: str,
) -> None:
    details = input_error_details(idempotency_key=idempotency_key)

    assert details["reason"] == "invalid_feedback_request"
    assert details["field"] == "idempotency_key"


def test_feedback_request_note_is_stripped() -> None:
    request = request_of(note="  already covered upstream \n")

    assert request.note == "already covered upstream"


def test_feedback_request_note_of_max_chars_after_strip_is_accepted() -> None:
    request = request_of(note="  " + "n" * 500 + "  ")

    assert request.note == "n" * 500


def test_feedback_request_without_note_keeps_none() -> None:
    request = request_of(note=None)

    assert request.note is None


@pytest.mark.parametrize("note", ["n" * 501, "   ", "", "bell\x07note", "two\nlines", "del\x7f"])
def test_feedback_request_invalid_note_raises_invalid_note(note: str) -> None:
    details = input_error_details(note=note)

    assert details["reason"] == "invalid_feedback_request"
    assert details["field"] == "note"


@pytest.mark.parametrize("watch_id", ["Agent-Tooling", "-agent", "agent-", "agent_tooling", ""])
def test_feedback_request_invalid_watch_id_raises_invalid_watch_id(watch_id: str) -> None:
    details = input_error_details(watch_id=watch_id)

    assert details["reason"] == "invalid_feedback_request"
    assert details["field"] == "watch_id"


def test_feedback_id_for_same_watch_and_key_is_deterministic() -> None:
    first = feedback_id_for(WATCH_ID, "key-1")
    second = feedback_id_for(WATCH_ID, "key-1")

    assert first == second
    assert FEEDBACK_ID_PATTERN.fullmatch(first) is not None


def test_feedback_id_for_other_watch_or_key_differs() -> None:
    base = feedback_id_for(WATCH_ID, "key-1")

    other_watch = feedback_id_for("other-watch", "key-1")
    other_key = feedback_id_for(WATCH_ID, "key-2")

    assert len({base, other_watch, other_key}) == 3


def test_feedback_request_feedback_id_uses_watch_and_key() -> None:
    request = request_of(idempotency_key="retry-7")

    assert request.feedback_id == feedback_id_for(WATCH_ID, "retry-7")


def test_feedback_request_content_sha256_ignores_idempotency_key() -> None:
    first = request_of(idempotency_key="key-1")
    second = request_of(idempotency_key="key-2")

    assert first.feedback_id != second.feedback_id
    assert first.content_sha256() == second.content_sha256()


@pytest.mark.parametrize(
    "changes",
    [
        {"kind": "correction"},
        {"value": "irrelevant"},
        {"target_kind": "finding"},
        {"target_id": "ent-" + "d" * 32},
        {"note": "a reason"},
    ],
)
def test_feedback_request_content_sha256_changes_with_each_content_field(
    changes: dict[str, Any],
) -> None:
    base = request_of(note=None)

    changed = dataclasses.replace(base, **changes)

    assert changed.content_sha256() != base.content_sha256()


def preference_event(
    feedback_id: str, sequence: int, value: str, scope_kind: str, scope_key: str
) -> dict[str, Any]:
    return {
        "feedback_id": feedback_id,
        "sequence": sequence,
        "value": value,
        "target_kind": "entry" if scope_kind == "entry" else "issue",
        "target_id": scope_key,
        "scope_kind": scope_kind,
        "scope_key": scope_key,
    }


def test_effective_preferences_latest_sequence_per_scope_wins_regardless_of_order() -> None:
    events = [
        preference_event("fbk-3", 3, "already_known", "entry", ENTRY_ID),
        preference_event("fbk-1", 1, "useful", "entry", ENTRY_ID),
        preference_event("fbk-2", 2, "irrelevant", "entry", ENTRY_ID),
    ]

    effective = effective_preferences(events)

    assert effective == [
        {
            "scope_kind": "entry",
            "scope_key": ENTRY_ID,
            "value": "already_known",
            "feedback_id": "fbk-3",
            "sequence": 3,
            "target_kind": "entry",
            "target_id": ENTRY_ID,
            "superseded_feedback_ids": ["fbk-1", "fbk-2"],
        }
    ]


def test_effective_preferences_scopes_are_independent_and_sorted_by_scope() -> None:
    other_entry = "ent-" + "0" * 32
    events = [
        preference_event("fbk-4", 4, "useful", "issue", ISSUE_ID),
        preference_event("fbk-3", 3, "wrong_audience", "entry", ENTRY_ID),
        preference_event("fbk-2", 2, "irrelevant", "entry", other_entry),
        preference_event("fbk-1", 1, "useful", "entry", ENTRY_ID),
    ]

    effective = effective_preferences(events)

    assert [
        (item["scope_kind"], item["scope_key"], item["value"], item["superseded_feedback_ids"])
        for item in effective
    ] == [
        ("entry", other_entry, "irrelevant", []),
        ("entry", ENTRY_ID, "wrong_audience", ["fbk-1"]),
        ("issue", ISSUE_ID, "useful", []),
    ]


def test_effective_preferences_without_events_is_empty() -> None:
    assert effective_preferences([]) == []


def saved_store(tmp_path: Path) -> MonitoringStore:
    with FIXTURE_PROFILE.open("rb") as handle:
        payload = tomllib.load(handle)
    store = MonitoringStore(tmp_path / "monitoring" / "state.sqlite3", lambda: 1_000.0)
    store.save_profile(parse_watch_profile(payload))
    return store


def feedback_row_counts(store: MonitoringStore) -> dict[str, int]:
    with closing(sqlite3.connect(store.path)) as conn:
        return {
            table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in FEEDBACK_TABLES
        }


def insert_feedback_rows(conn: sqlite3.Connection) -> None:
    preference_id = feedback_id_for(WATCH_ID, "pref-1")
    correction_id = feedback_id_for(WATCH_ID, "corr-1")
    conn.executemany(
        "INSERT INTO watch_feedback_keys (watch_id, idempotency_key, feedback_id, kind, "
        "content_sha256, recorded_at_utc) VALUES (?, ?, ?, ?, ?, 1000.0)",
        [
            (WATCH_ID, "pref-1", preference_id, "preference", "0" * 64),
            (WATCH_ID, "corr-1", correction_id, "correction", "1" * 64),
        ],
    )
    conn.execute(
        "INSERT INTO watch_preference_events (feedback_id, watch_id, sequence, value, "
        "target_kind, target_id, source_thread_id, theme_key, scope_kind, scope_key, note, "
        "recorded_at_utc) VALUES (?, ?, 1, 'useful', 'entry', ?, 't1', NULL, 'entry', ?, "
        "NULL, 1000.0)",
        (preference_id, WATCH_ID, ENTRY_ID, ENTRY_ID),
    )
    conn.execute(
        "INSERT INTO watch_correction_events (feedback_id, watch_id, sequence, value, "
        "target_kind, target_id, source_thread_id, theme_key, note, recorded_at_utc) "
        "VALUES (?, ?, 1, 'unsupported', 'entry', ?, 't1', NULL, NULL, 1000.0)",
        (correction_id, WATCH_ID, ENTRY_ID),
    )


@pytest.mark.parametrize("table", FEEDBACK_TABLES)
def test_feedback_tables_update_and_delete_are_refused_as_append_only(
    tmp_path: Path, table: str
) -> None:
    store = saved_store(tmp_path)
    with closing(sqlite3.connect(store.path)) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        insert_feedback_rows(conn)
        conn.commit()
        inserted = conn.execute(
            "SELECT idempotency_key, kind FROM watch_feedback_keys ORDER BY idempotency_key"
        ).fetchall()

        with pytest.raises(sqlite3.DatabaseError, match="append-only"):
            conn.execute(f"UPDATE {table} SET recorded_at_utc = 2000.0")
        with pytest.raises(sqlite3.DatabaseError, match="append-only"):
            conn.execute(f"DELETE FROM {table}")
        conn.rollback()

    assert inserted == [("corr-1", "correction"), ("pref-1", "preference")]
    assert feedback_row_counts(store) == {
        "watch_feedback_keys": 2,
        "watch_preference_events": 1,
        "watch_correction_events": 1,
    }


def test_monitoring_store_record_feedback_unknown_entry_raises_unknown_target(
    tmp_path: Path,
) -> None:
    store = saved_store(tmp_path)

    with pytest.raises(MonitoringStateError) as raised:
        store.record_feedback(request_of())

    assert raised.value.details["reason"] == "unknown_target"
    assert raised.value.details["target_id"] == ENTRY_ID
    assert feedback_row_counts(store) == dict.fromkeys(FEEDBACK_TABLES, 0)


def test_monitoring_store_feedback_history_without_feedback_returns_empty_lists(
    tmp_path: Path,
) -> None:
    store = saved_store(tmp_path)

    history = store.feedback_history(WATCH_ID)

    assert history == {
        "watch_id": WATCH_ID,
        "preferences": [],
        "effective_preferences": [],
        "corrections": [],
        "correction_view": [],
    }


def test_monitoring_store_feedback_history_unknown_watch_raises_unknown_watch(
    tmp_path: Path,
) -> None:
    store = saved_store(tmp_path)

    with pytest.raises(MonitoringStateError) as raised:
        store.feedback_history("other-watch")

    assert raised.value.details["reason"] == "unknown_watch"
    assert raised.value.details["watch_id"] == "other-watch"
