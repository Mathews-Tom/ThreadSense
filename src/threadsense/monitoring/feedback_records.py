"""Operator feedback events: subjective preferences kept apart from correction assertions.

Owned by `MonitoringStore`. Preferences and corrections live in separate append-only tables
that share one watch-scoped idempotency namespace. Every target is resolved against the
watch's own digest entries, thread revisions or issues before anything is written. Neither
kind edits evidence, findings, issue membership or any written report: a preference only
feeds presentation ranking, and a correction is an auditable assertion beside the original.
"""

from __future__ import annotations

import hashlib
import re
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal

from threadsense.errors import MonitoringInputError, MonitoringStateError
from threadsense.monitoring.digest_records import _ENTRY_ID_PATTERN, _stored_json
from threadsense.monitoring.issue_records import ISSUE_ID_PATTERN
from threadsense.monitoring.profiles import WATCH_ID_PATTERN
from threadsense.monitoring.sweep_records import _canonical_json

FeedbackKind = Literal["preference", "correction"]
TargetKind = Literal["entry", "finding", "observation", "issue"]
ScopeKind = Literal["entry", "finding", "thread", "issue"]

_FEEDBACK_SCHEMA_VERSION = 6
FEEDBACK_KINDS: tuple[FeedbackKind, ...] = ("preference", "correction")
PREFERENCE_VALUES = ("useful", "irrelevant", "already_known", "wrong_audience")
CORRECTION_VALUES = ("misclassified", "unsupported", "duplicate", "missing_evidence")
TARGET_KINDS: tuple[TargetKind, ...] = ("entry", "finding", "observation", "issue")
MAX_NOTE_CHARS = 500
FEEDBACK_ID_PATTERN = re.compile(r"^fbk-[0-9a-f]{32}$")
IDEMPOTENCY_KEY_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
REVISION_ID_PATTERN = re.compile(r"^rev-[0-9a-f]{32}$")

_VALUES_BY_KIND: dict[FeedbackKind, tuple[str, ...]] = {
    "preference": PREFERENCE_VALUES,
    "correction": CORRECTION_VALUES,
}
# The narrowest target kind an id prefix names when `target_kind` is not given.
_DEFAULT_TARGET_KIND: tuple[tuple[re.Pattern[str], TargetKind], ...] = (
    (_ENTRY_ID_PATTERN, "entry"),
    (REVISION_ID_PATTERN, "observation"),
    (ISSUE_ID_PATTERN, "issue"),
)
_TARGET_PATTERN: dict[TargetKind, re.Pattern[str]] = {
    "entry": _ENTRY_ID_PATTERN,
    "finding": _ENTRY_ID_PATTERN,
    "observation": REVISION_ID_PATTERN,
    "issue": ISSUE_ID_PATTERN,
}
_TABLE_BY_KIND: dict[FeedbackKind, str] = {
    "preference": "watch_preference_events",
    "correction": "watch_correction_events",
}


def _append_only(table: str) -> tuple[str, str]:
    return (
        f"CREATE TRIGGER {table}_no_update BEFORE UPDATE ON {table} "
        "BEGIN SELECT RAISE(ABORT, 'operator feedback is append-only'); END",
        f"CREATE TRIGGER {table}_no_delete BEFORE DELETE ON {table} "
        "BEGIN SELECT RAISE(ABORT, 'operator feedback is append-only'); END",
    )


def _quoted(values: Sequence[str]) -> str:
    return ", ".join(f"'{value}'" for value in values)


FEEDBACK_SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE watch_feedback_keys (
        watch_id TEXT NOT NULL REFERENCES watches (watch_id),
        idempotency_key TEXT NOT NULL,
        feedback_id TEXT NOT NULL UNIQUE,
        kind TEXT NOT NULL CHECK (kind IN ('preference', 'correction')),
        content_sha256 TEXT NOT NULL,
        recorded_at_utc REAL NOT NULL,
        PRIMARY KEY (watch_id, idempotency_key)
    )
    """,
    f"""
    CREATE TABLE watch_preference_events (
        feedback_id TEXT PRIMARY KEY REFERENCES watch_feedback_keys (feedback_id),
        watch_id TEXT NOT NULL REFERENCES watches (watch_id),
        sequence INTEGER NOT NULL CHECK (sequence >= 1),
        value TEXT NOT NULL CHECK (value IN ({_quoted(PREFERENCE_VALUES)})),
        target_kind TEXT NOT NULL CHECK (target_kind IN ({_quoted(TARGET_KINDS)})),
        target_id TEXT NOT NULL,
        source_thread_id TEXT NOT NULL,
        theme_key TEXT,
        scope_kind TEXT NOT NULL CHECK (scope_kind IN ('entry', 'finding', 'thread', 'issue')),
        scope_key TEXT NOT NULL,
        note TEXT,
        recorded_at_utc REAL NOT NULL,
        UNIQUE (watch_id, sequence)
    )
    """,
    f"""
    CREATE TABLE watch_correction_events (
        feedback_id TEXT PRIMARY KEY REFERENCES watch_feedback_keys (feedback_id),
        watch_id TEXT NOT NULL REFERENCES watches (watch_id),
        sequence INTEGER NOT NULL CHECK (sequence >= 1),
        value TEXT NOT NULL CHECK (value IN ({_quoted(CORRECTION_VALUES)})),
        target_kind TEXT NOT NULL CHECK (target_kind IN ({_quoted(TARGET_KINDS)})),
        target_id TEXT NOT NULL,
        source_thread_id TEXT NOT NULL,
        theme_key TEXT,
        note TEXT,
        recorded_at_utc REAL NOT NULL,
        UNIQUE (watch_id, sequence)
    )
    """,
    *_append_only("watch_feedback_keys"),
    *_append_only("watch_preference_events"),
    *_append_only("watch_correction_events"),
)


@dataclass(frozen=True)
class FeedbackRequest:
    """A validated operator feedback request; `note` is stripped or None."""

    watch_id: str
    idempotency_key: str
    kind: FeedbackKind
    value: str
    target_id: str
    target_kind: TargetKind
    note: str | None

    @property
    def feedback_id(self) -> str:
        return feedback_id_for(self.watch_id, self.idempotency_key)

    def content(self) -> dict[str, Any]:
        """The fields an idempotent retry must repeat exactly."""
        return {
            "kind": self.kind,
            "value": self.value,
            "target_kind": self.target_kind,
            "target_id": self.target_id,
            "note": self.note,
        }

    def content_sha256(self) -> str:
        text = _canonical_json(self.content(), "feedback")
        return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ResolvedTarget:
    """A target proven to belong to the watch, with the scope a preference applies to."""

    target_kind: TargetKind
    target_id: str
    source_thread_id: str
    theme_key: str | None
    scope_kind: ScopeKind
    scope_key: str


def feedback_id_for(watch_id: str, idempotency_key: str) -> str:
    text = f"{watch_id}\n{idempotency_key}"
    return "fbk-" + hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]


def finding_scope_key(source_thread_id: str, theme_key: str) -> str:
    """Scope of one thread's finding stream for a theme, across its revisions."""
    return f"{source_thread_id}/{theme_key}"


def _invalid(field: str, **details: Any) -> MonitoringInputError:
    return MonitoringInputError(
        f"feedback {field} is invalid",
        details={"reason": "invalid_feedback_request", "field": field, **details},
    )


def _refusal(message: str, reason: str, **details: Any) -> MonitoringStateError:
    return MonitoringStateError(message, details={"reason": reason, **details})


def _matches(pattern: re.Pattern[str], value: object) -> bool:
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def _printable(text: str) -> bool:
    return not any(ord(char) < 32 or ord(char) == 127 for char in text)


def default_target_kind(target_id: str) -> TargetKind:
    """The narrowest target kind an id names: entries, observations or issues."""
    for pattern, kind in _DEFAULT_TARGET_KIND:
        if _matches(pattern, target_id):
            return kind
    raise _invalid("target_id")


def feedback_request(
    watch_id: str,
    *,
    idempotency_key: str,
    kind: str,
    value: str,
    target_id: str,
    target_kind: str | None,
    note: str | None,
) -> FeedbackRequest:
    """Validate an operator feedback request before the database is touched."""
    if not _matches(WATCH_ID_PATTERN, watch_id):
        raise _invalid("watch_id")
    if not _matches(IDEMPOTENCY_KEY_PATTERN, idempotency_key):
        raise _invalid("idempotency_key")
    if kind not in FEEDBACK_KINDS:
        raise _invalid("kind", kind=kind)
    feedback_kind: FeedbackKind = "preference" if kind == "preference" else "correction"
    if value not in _VALUES_BY_KIND[feedback_kind]:
        raise MonitoringInputError(
            f"{value!r} is not a {feedback_kind} value",
            details={
                "reason": "value_kind_mismatch",
                "kind": feedback_kind,
                "value": value,
                "allowed_values": list(_VALUES_BY_KIND[feedback_kind]),
            },
        )
    resolved_kind = default_target_kind(target_id) if target_kind is None else target_kind
    if resolved_kind not in TARGET_KINDS:
        raise _invalid("target_kind", target_kind=target_kind)
    checked_kind: TargetKind = _target_kind(resolved_kind)
    if not _matches(_TARGET_PATTERN[checked_kind], target_id):
        raise MonitoringInputError(
            f"target id does not name a {checked_kind}",
            details={
                "reason": "target_kind_mismatch",
                "target_kind": checked_kind,
                "target_id": target_id,
            },
        )
    stripped: str | None = None
    if note is not None:
        stripped = note.strip()
        if not 1 <= len(stripped) <= MAX_NOTE_CHARS or not _printable(stripped):
            raise _invalid("note")
    return FeedbackRequest(
        watch_id=watch_id,
        idempotency_key=idempotency_key,
        kind=feedback_kind,
        value=value,
        target_id=target_id,
        target_kind=checked_kind,
        note=stripped,
    )


def _target_kind(value: str) -> TargetKind:
    for kind in TARGET_KINDS:
        if kind == value:
            return kind
    raise _invalid("target_kind", target_kind=value)


def _owned(
    row: Sequence[Any] | None, watch_id: str, target_kind: TargetKind, target_id: str
) -> Sequence[Any]:
    """Refuse a missing target, or one owned by a different watch, before any write."""
    if row is None:
        raise _refusal(
            f"{target_kind} target does not exist",
            "unknown_target",
            watch_id=watch_id,
            target_kind=target_kind,
            target_id=target_id,
        )
    if row[0] != watch_id:
        raise _refusal(
            f"{target_kind} target belongs to a different watch",
            "cross_watch_target",
            watch_id=watch_id,
            target_kind=target_kind,
            target_id=target_id,
        )
    return row


def resolve_target(
    conn: sqlite3.Connection, watch_id: str, target_kind: TargetKind, target_id: str
) -> ResolvedTarget:
    if target_kind in ("entry", "finding"):
        row = _owned(
            conn.execute(
                "SELECT watch_id, source_thread_id, kind, theme_key FROM watch_digest_entries "
                "WHERE entry_id = ?",
                (target_id,),
            ).fetchone(),
            watch_id,
            target_kind,
            target_id,
        )
        thread_id, entry_kind, theme_key = str(row[1]), str(row[2]), row[3]
        if target_kind == "entry":
            return ResolvedTarget(target_kind, target_id, thread_id, theme_key, "entry", target_id)
        if entry_kind != "finding" or not isinstance(theme_key, str):
            raise MonitoringStateError(
                "finding feedback must target a finding entry",
                details={
                    "reason": "target_kind_mismatch",
                    "target_kind": target_kind,
                    "target_id": target_id,
                    "entry_kind": entry_kind,
                },
            )
        return ResolvedTarget(
            target_kind,
            target_id,
            thread_id,
            theme_key,
            "finding",
            finding_scope_key(thread_id, theme_key),
        )
    if target_kind == "observation":
        row = _owned(
            conn.execute(
                "SELECT watch_id, source_thread_id FROM watch_thread_revisions "
                "WHERE revision_id = ?",
                (target_id,),
            ).fetchone(),
            watch_id,
            target_kind,
            target_id,
        )
        return ResolvedTarget(target_kind, target_id, str(row[1]), None, "thread", str(row[1]))
    row = _owned(
        conn.execute(
            "SELECT watch_id, home_thread_id FROM watch_issues WHERE issue_id = ?",
            (target_id,),
        ).fetchone(),
        watch_id,
        target_kind,
        target_id,
    )
    return ResolvedTarget(target_kind, target_id, str(row[1]), None, "issue", target_id)


_PREFERENCE_COLUMNS = (
    "feedback_id",
    "sequence",
    "value",
    "target_kind",
    "target_id",
    "source_thread_id",
    "theme_key",
    "scope_kind",
    "scope_key",
    "note",
    "recorded_at_utc",
)
_CORRECTION_COLUMNS = (
    "feedback_id",
    "sequence",
    "value",
    "target_kind",
    "target_id",
    "source_thread_id",
    "theme_key",
    "note",
    "recorded_at_utc",
)
_COLUMNS_BY_KIND: dict[FeedbackKind, tuple[str, ...]] = {
    "preference": _PREFERENCE_COLUMNS,
    "correction": _CORRECTION_COLUMNS,
}


def _event_rows(
    conn: sqlite3.Connection, kind: FeedbackKind, watch_id: str, target_id: str | None
) -> list[dict[str, Any]]:
    columns = _COLUMNS_BY_KIND[kind]
    query = (
        f"SELECT e.{', e.'.join(columns)}, k.idempotency_key, k.content_sha256 "
        f"FROM {_TABLE_BY_KIND[kind]} AS e "
        "JOIN watch_feedback_keys AS k ON k.feedback_id = e.feedback_id "
        "WHERE e.watch_id = ?"
    )
    params: tuple[Any, ...] = (watch_id,)
    if target_id is not None:
        query += " AND e.target_id = ?"
        params = (watch_id, target_id)
    rows = conn.execute(query + " ORDER BY e.sequence ASC", params).fetchall()
    return [
        {
            "kind": kind,
            "watch_id": watch_id,
            **dict(zip((*columns, "idempotency_key", "content_sha256"), row, strict=True)),
        }
        for row in rows
    ]


def _stored_event(
    conn: sqlite3.Connection, kind: FeedbackKind, watch_id: str, feedback_id: str
) -> dict[str, Any]:
    events = [
        event
        for event in _event_rows(conn, kind, watch_id, None)
        if event["feedback_id"] == feedback_id
    ]
    if len(events) != 1:
        raise MonitoringStateError(
            "feedback key exists without its event",
            details={"reason": "feedback_state_unreadable", "feedback_id": feedback_id},
        )
    return events[0]


def record_feedback(
    conn: sqlite3.Connection, request: FeedbackRequest, now: float
) -> dict[str, Any]:
    """Record one event, or return the identical event a reused key already recorded."""
    target = resolve_target(conn, request.watch_id, request.target_kind, request.target_id)
    content_sha256 = request.content_sha256()
    existing = conn.execute(
        "SELECT feedback_id, kind, content_sha256 FROM watch_feedback_keys "
        "WHERE watch_id = ? AND idempotency_key = ?",
        (request.watch_id, request.idempotency_key),
    ).fetchone()
    if existing is not None:
        if existing[1] != request.kind or existing[2] != content_sha256:
            raise MonitoringStateError(
                "idempotency key was already used for different feedback",
                details={
                    "reason": "idempotency_conflict",
                    "watch_id": request.watch_id,
                    "idempotency_key": request.idempotency_key,
                    "feedback_id": existing[0],
                },
            )
        return {
            "status": "unchanged",
            "feedback": _stored_event(conn, request.kind, request.watch_id, str(existing[0])),
        }
    table = _TABLE_BY_KIND[request.kind]
    sequence = (
        int(
            conn.execute(
                f"SELECT COALESCE(MAX(sequence), 0) FROM {table} WHERE watch_id = ?",
                (request.watch_id,),
            ).fetchone()[0]
        )
        + 1
    )
    feedback_id = request.feedback_id
    conn.execute(
        "INSERT INTO watch_feedback_keys (watch_id, idempotency_key, feedback_id, kind, "
        "content_sha256, recorded_at_utc) VALUES (?, ?, ?, ?, ?, ?)",
        (request.watch_id, request.idempotency_key, feedback_id, request.kind, content_sha256, now),
    )
    common = (
        feedback_id,
        request.watch_id,
        sequence,
        request.value,
        target.target_kind,
        target.target_id,
        target.source_thread_id,
        target.theme_key,
    )
    if request.kind == "preference":
        conn.execute(
            "INSERT INTO watch_preference_events (feedback_id, watch_id, sequence, value, "
            "target_kind, target_id, source_thread_id, theme_key, scope_kind, scope_key, note, "
            "recorded_at_utc) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (*common, target.scope_kind, target.scope_key, request.note, now),
        )
    else:
        conn.execute(
            "INSERT INTO watch_correction_events (feedback_id, watch_id, sequence, value, "
            "target_kind, target_id, source_thread_id, theme_key, note, recorded_at_utc) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (*common, request.note, now),
        )
    return {
        "status": "recorded",
        "feedback": _stored_event(conn, request.kind, request.watch_id, feedback_id),
    }


def effective_preferences(preferences: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """The latest preference per scope; earlier ones in that scope are superseded, not lost."""
    by_scope: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for event in sorted(preferences, key=lambda event: int(event["sequence"])):
        by_scope.setdefault((str(event["scope_kind"]), str(event["scope_key"])), []).append(event)
    return [
        {
            "scope_kind": scope_kind,
            "scope_key": scope_key,
            "value": events[-1]["value"],
            "feedback_id": events[-1]["feedback_id"],
            "sequence": events[-1]["sequence"],
            "target_kind": events[-1]["target_kind"],
            "target_id": events[-1]["target_id"],
            "superseded_feedback_ids": [event["feedback_id"] for event in events[:-1]],
        }
        for (scope_kind, scope_key), events in sorted(by_scope.items())
    ]


def _required_row(row: Sequence[Any] | None, target_id: str) -> Sequence[Any]:
    if row is None:
        raise MonitoringStateError(
            "a recorded feedback target is missing",
            details={"reason": "feedback_state_unreadable", "target_id": target_id},
        )
    return row


def _original_target(
    conn: sqlite3.Connection, watch_id: str, target_kind: str, target_id: str
) -> dict[str, Any]:
    """The unchanged stored record a correction asserts something about."""
    if target_kind in ("entry", "finding"):
        row = _required_row(
            conn.execute(
                "SELECT kind, source_thread_id, revision_id, theme_key, payload_json "
                "FROM watch_digest_entries WHERE entry_id = ? AND watch_id = ?",
                (target_id, watch_id),
            ).fetchone(),
            target_id,
        )
        payload = _stored_json(str(row[4]), entry_id=target_id)
        if not isinstance(payload, dict):
            raise MonitoringStateError(
                "digest entry payload is not an object",
                details={"reason": "feedback_state_unreadable", "target_id": target_id},
            )
        return {
            "entry_kind": row[0],
            "source_thread_id": row[1],
            "revision_id": row[2],
            "theme_key": row[3],
            "severity": payload.get("severity"),
            "payload_sha256": hashlib.sha256(str(row[4]).encode("utf-8")).hexdigest(),
        }
    if target_kind == "observation":
        row = _required_row(
            conn.execute(
                "SELECT source_thread_id, sequence, substantive_sha256, snapshot_sha256 "
                "FROM watch_thread_revisions WHERE revision_id = ? AND watch_id = ?",
                (target_id, watch_id),
            ).fetchone(),
            target_id,
        )
        return {
            "source_thread_id": row[0],
            "revision_sequence": row[1],
            "substantive_sha256": row[2],
            "snapshot_sha256": row[3],
        }
    row = _required_row(
        conn.execute(
            "SELECT home_thread_id FROM watch_issues WHERE issue_id = ? AND watch_id = ?",
            (target_id, watch_id),
        ).fetchone(),
        target_id,
    )
    return {"home_thread_id": row[0]}


def _correction_view(
    conn: sqlite3.Connection, watch_id: str, corrections: Sequence[dict[str, Any]]
) -> list[dict[str, Any]]:
    by_target: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for event in corrections:
        by_target.setdefault((str(event["target_kind"]), str(event["target_id"])), []).append(event)
    return [
        {
            "target_kind": target_kind,
            "target_id": target_id,
            "original": _original_target(conn, watch_id, target_kind, target_id),
            "effect": "assertion_only",
            "assertions": [
                {
                    "feedback_id": event["feedback_id"],
                    "sequence": event["sequence"],
                    "value": event["value"],
                    "note": event["note"],
                }
                for event in events
            ],
        }
        for (target_kind, target_id), events in sorted(by_target.items())
    ]


def empty_feedback_history(watch_id: str) -> dict[str, Any]:
    return {
        "watch_id": watch_id,
        "preferences": [],
        "effective_preferences": [],
        "corrections": [],
        "correction_view": [],
    }


def feedback_history(
    conn: sqlite3.Connection, watch_id: str, target_id: str | None
) -> dict[str, Any]:
    """Preference and correction histories with their separate derived views."""
    if target_id is not None:
        resolve_target(conn, watch_id, default_target_kind(target_id), target_id)
    preferences = _event_rows(conn, "preference", watch_id, target_id)
    corrections = _event_rows(conn, "correction", watch_id, target_id)
    return {
        "watch_id": watch_id,
        "preferences": preferences,
        "effective_preferences": effective_preferences(preferences),
        "corrections": corrections,
        "correction_view": _correction_view(conn, watch_id, corrections),
    }
