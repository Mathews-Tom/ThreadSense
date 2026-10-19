"""Durable digest records and the SQL helpers that persist them.

Owned by `MonitoringStore`: every writing helper here runs inside a transaction that the
store has already fenced against stale run owners.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from threadsense.errors import MonitoringInputError, MonitoringStateError
from threadsense.monitoring.sweep_records import (
    _TRACKED_THREADS_WHERE,
    RevisionRef,
    _canonical_json,
    _count,
    _pending_analysis_count,
)

if TYPE_CHECKING:
    from threadsense.monitoring.state import RunClaim

EntryKind = Literal["thread_update", "finding"]
_DIGEST_SCHEMA_VERSION = 3
_ENTRY_KINDS: tuple[EntryKind, ...] = ("thread_update", "finding")
_FIRST_OBSERVATION_KINDS = ("initial_observation", "new_source_event", "newly_observed_historical")
_ENTRY_ID_PATTERN = re.compile(r"^ent-[0-9a-f]{32}$")
_DIGEST_ID_PATTERN = re.compile(r"^dig-[0-9a-f]{32}$")

DIGEST_SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE watch_digests (
        digest_id TEXT PRIMARY KEY,
        watch_id TEXT NOT NULL REFERENCES watches (watch_id),
        sequence INTEGER NOT NULL CHECK (sequence >= 1),
        run_id TEXT NOT NULL REFERENCES watch_runs (run_id),
        status TEXT NOT NULL CHECK (status IN ('pending', 'written')),
        entry_ids_json TEXT NOT NULL,
        backlog_json TEXT NOT NULL,
        json_path TEXT NOT NULL,
        markdown_path TEXT NOT NULL,
        json_sha256 TEXT,
        markdown_sha256 TEXT,
        created_at_utc REAL NOT NULL,
        written_at_utc REAL,
        UNIQUE (watch_id, sequence)
    )
    """,
    """
    CREATE UNIQUE INDEX watch_digests_single_pending
        ON watch_digests (watch_id) WHERE status = 'pending'
    """,
    """
    CREATE TABLE watch_digest_entries (
        entry_id TEXT PRIMARY KEY,
        watch_id TEXT NOT NULL,
        source_thread_id TEXT NOT NULL,
        revision_id TEXT NOT NULL REFERENCES watch_thread_revisions (revision_id),
        kind TEXT NOT NULL CHECK (kind IN ('thread_update', 'finding')),
        theme_key TEXT,
        payload_json TEXT NOT NULL,
        created_at_utc REAL NOT NULL,
        created_run_id TEXT NOT NULL REFERENCES watch_runs (run_id),
        digest_id TEXT REFERENCES watch_digests (digest_id),
        presented_at_utc REAL
    )
    """,
)


@dataclass(frozen=True)
class DigestEntryRecord:
    entry_id: str
    source_thread_id: str
    revision_id: str
    kind: EntryKind
    theme_key: str | None
    payload: dict[str, Any]


@dataclass(frozen=True)
class MaterializationInput:
    """An analyzed revision without entries, with the novelty folded since the last one.

    `monitoring_root` is the store root that the revision and analysis paths live under, so
    entry payloads can cite the same relative paths the store records.
    """

    revision: RevisionRef
    analysis_path: Path
    analysis_sha256: str
    thread_url: str
    subreddit: str
    title: str
    novel_comments: tuple[dict[str, Any], ...]
    thread_text_changed: bool
    monitoring_root: Path


@dataclass(frozen=True)
class PendingDigest:
    digest_id: str
    sequence: int
    json_path: Path
    markdown_path: Path
    created_at_utc: float
    entries: tuple[dict[str, Any], ...]
    backlog: dict[str, Any]
    resumed: bool


@dataclass(frozen=True)
class _DigestRow:
    digest_id: str
    sequence: int
    status: str
    entry_ids: tuple[str, ...]
    backlog: dict[str, Any]
    json_path: str
    markdown_path: str
    created_at_utc: float


_UNMATERIALIZED_REVISIONS = (
    "FROM watch_revision_analyses AS a "
    "JOIN watch_thread_revisions AS r ON r.revision_id = a.revision_id "
    "JOIN watch_threads AS t ON t.watch_id = r.watch_id "
    "AND t.source_thread_id = r.source_thread_id "
    "LEFT JOIN watch_digest_entries AS e ON e.revision_id = r.revision_id "
    "AND e.kind = 'thread_update' "
    "WHERE r.watch_id = ? AND a.watch_id = r.watch_id AND e.entry_id IS NULL "
    "ORDER BY r.created_at_utc ASC, r.revision_id ASC"
)

_DIGEST_COLUMNS = (
    "digest_id, sequence, status, entry_ids_json, backlog_json, json_path, markdown_path, "
    "created_at_utc"
)


def thread_update_entry_id(watch_id: str, revision_id: str) -> str:
    return "ent-" + _sha256_text(f"{watch_id}\n{revision_id}\nthread_update")[:32]


def finding_entry_id(watch_id: str, revision_id: str, theme_key: str) -> str:
    return "ent-" + _sha256_text(f"{watch_id}\n{revision_id}\nfinding\n{theme_key}")[:32]


def digest_id_for(watch_id: str, sequence: int, entry_ids: tuple[str, ...]) -> str:
    return "dig-" + _sha256_text(f"{watch_id}\n{sequence}\n" + "\n".join(entry_ids))[:32]


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _unreadable(message: str, **details: Any) -> MonitoringStateError:
    return MonitoringStateError(message, details={"reason": "digest_state_unreadable", **details})


def _folded_novelty(
    conn: sqlite3.Connection, watch_id: str, source_thread_id: str, sequence: int
) -> tuple[tuple[dict[str, Any], ...], bool]:
    """Fold every revision change since the thread's previous analyzed revision."""
    # Analyzed revisions of a thread are materialized strictly in order, so the previous
    # analyzed revision is the last materialized one once this batch reaches this revision;
    # superseded, never-analyzed revisions in between are folded here so no novelty is lost.
    lower = conn.execute(
        "SELECT COALESCE(MAX(r.sequence), 0) FROM watch_thread_revisions AS r "
        "JOIN watch_revision_analyses AS a ON a.revision_id = r.revision_id "
        "WHERE r.watch_id = ? AND r.source_thread_id = ? AND r.sequence < ?",
        (watch_id, source_thread_id, sequence),
    ).fetchone()[0]
    rows = conn.execute(
        "SELECT revision_id, acquisition_id, changes_json FROM watch_thread_revisions "
        "WHERE watch_id = ? AND source_thread_id = ? AND sequence > ? AND sequence <= ? "
        "ORDER BY sequence ASC",
        (watch_id, source_thread_id, int(lower), sequence),
    ).fetchall()
    folded: dict[str, tuple[dict[str, Any], str]] = {}
    thread_text_changed = False
    for revision_id, acquisition_id, changes_json in rows:
        comment_changes, text_changed = _parse_changes(revision_id, changes_json)
        thread_text_changed = thread_text_changed or text_changed
        for change in comment_changes:
            prior = folded.get(change["comment_id"])
            if prior is not None and prior[0]["kind"] in _FIRST_OBSERVATION_KINDS:
                # A comment first seen inside the window stays a first observation of its
                # latest text, whatever edits followed within the same window.
                merged = {**prior[0], "text_sha256": change["text_sha256"]}
                folded[change["comment_id"]] = (merged, prior[1])
            elif prior is not None:
                merged = {**change, "previous_text_sha256": prior[0]["previous_text_sha256"]}
                folded[change["comment_id"]] = (merged, str(acquisition_id))
            else:
                folded[change["comment_id"]] = (dict(change), str(acquisition_id))
    novel = tuple(
        _with_observation_time(conn, watch_id, source_thread_id, change, acquisition_id)
        for change, acquisition_id in (folded[key] for key in sorted(folded))
    )
    return novel, thread_text_changed


def _stored_json(text: str, **details: Any) -> Any:
    try:
        return json.loads(text)
    except ValueError as error:
        raise _unreadable("stored digest state is not valid JSON", **details) from error


def _parse_changes(revision_id: str, changes_json: str) -> tuple[list[dict[str, Any]], bool]:
    parsed = _stored_json(changes_json, revision_id=revision_id)
    if not isinstance(parsed, dict):
        raise _unreadable("revision changes are not an object", revision_id=revision_id)
    comment_changes = parsed.get("comment_changes")
    text_changed = parsed.get("thread_text_changed")
    if not isinstance(comment_changes, list) or type(text_changed) is not bool:
        raise _unreadable("revision changes have an unexpected shape", revision_id=revision_id)
    for change in comment_changes:
        if (
            not isinstance(change, dict)
            or not isinstance(change.get("comment_id"), str)
            or not isinstance(change.get("text_sha256"), str)
            or change.get("kind") not in (*_FIRST_OBSERVATION_KINDS, "edited")
        ):
            raise _unreadable("revision change is malformed", revision_id=revision_id)
    return comment_changes, text_changed


def _with_observation_time(
    conn: sqlite3.Connection,
    watch_id: str,
    source_thread_id: str,
    change: dict[str, Any],
    acquisition_id: str,
) -> dict[str, Any]:
    comment_id = change["comment_id"]
    if change["kind"] in _FIRST_OBSERVATION_KINDS:
        row = conn.execute(
            "SELECT first_observed_at_utc FROM watch_comments "
            "WHERE watch_id = ? AND source_thread_id = ? AND comment_id = ?",
            (watch_id, source_thread_id, comment_id),
        ).fetchone()
        key = "first_observed_at_utc"
    else:
        row = conn.execute(
            "SELECT observed_at_utc FROM watch_comment_texts WHERE watch_id = ? "
            "AND source_thread_id = ? AND comment_id = ? AND acquisition_id = ? "
            "AND change = 'edited'",
            (watch_id, source_thread_id, comment_id, acquisition_id),
        ).fetchone()
        key = "observed_at_utc"
    if row is None:
        raise MonitoringStateError(
            "comment history is missing for a recorded change",
            details={
                "reason": "comment_history_missing",
                "watch_id": watch_id,
                "source_thread_id": source_thread_id,
                "comment_id": comment_id,
            },
        )
    return {**change, key: float(row[0])}


def _validate_entry(entry: DigestEntryRecord) -> None:
    details: dict[str, Any] = {"reason": "invalid_entry", "entry_id": entry.entry_id}
    if not isinstance(entry.entry_id, str) or _ENTRY_ID_PATTERN.fullmatch(entry.entry_id) is None:
        raise MonitoringInputError("digest entry id is malformed", details=details)
    if entry.kind not in _ENTRY_KINDS:
        raise MonitoringInputError(
            "digest entry kind must be thread_update or finding",
            details={**details, "kind": entry.kind},
        )
    if (entry.kind == "finding") != isinstance(entry.theme_key, str):
        raise MonitoringInputError(
            "exactly finding entries carry a theme key", details={**details, "kind": entry.kind}
        )
    if entry.payload.get("entry_id") != entry.entry_id or entry.payload.get("kind") != entry.kind:
        raise MonitoringInputError(
            "digest entry payload does not match its record", details=details
        )


def _require_analyzed_revision(
    conn: sqlite3.Connection, watch_id: str, entry: DigestEntryRecord
) -> None:
    row = conn.execute(
        "SELECT r.source_thread_id FROM watch_thread_revisions AS r "
        "JOIN watch_revision_analyses AS a ON a.revision_id = r.revision_id "
        "WHERE r.revision_id = ? AND r.watch_id = ?",
        (entry.revision_id, watch_id),
    ).fetchone()
    if row is None or row[0] != entry.source_thread_id:
        raise MonitoringStateError(
            "digest entry does not cite an analyzed revision of this watch",
            details={
                "reason": "unknown_revision",
                "watch_id": watch_id,
                "revision_id": entry.revision_id,
                "source_thread_id": entry.source_thread_id,
            },
        )


def _insert_entry(
    conn: sqlite3.Connection, claim: RunClaim, entry: DigestEntryRecord, now: float
) -> bool:
    """Insert one entry; an existing identical entry is left untouched and returns False."""
    payload_json = _canonical_json(entry.payload, "payload")
    existing = conn.execute(
        "SELECT payload_json FROM watch_digest_entries WHERE entry_id = ?", (entry.entry_id,)
    ).fetchone()
    if existing is not None:
        if existing[0] != payload_json:
            raise MonitoringStateError(
                "digest entry already exists with different content",
                details={"reason": "entry_conflict", "entry_id": entry.entry_id},
            )
        return False
    conn.execute(
        "INSERT INTO watch_digest_entries (entry_id, watch_id, source_thread_id, revision_id, "
        "kind, theme_key, payload_json, created_at_utc, created_run_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            entry.entry_id,
            claim.watch_id,
            entry.source_thread_id,
            entry.revision_id,
            entry.kind,
            entry.theme_key,
            payload_json,
            now,
            claim.run_id,
        ),
    )
    return True


def _pending_digest_row(conn: sqlite3.Connection, watch_id: str) -> _DigestRow | None:
    row = conn.execute(
        f"SELECT {_DIGEST_COLUMNS} FROM watch_digests WHERE watch_id = ? AND status = 'pending'",
        (watch_id,),
    ).fetchone()
    return None if row is None else _digest_row(row)


def _digest_row_by_id(conn: sqlite3.Connection, watch_id: str, digest_id: str) -> _DigestRow:
    row = conn.execute(
        f"SELECT {_DIGEST_COLUMNS} FROM watch_digests WHERE watch_id = ? AND digest_id = ?",
        (watch_id, digest_id),
    ).fetchone()
    if row is None:
        raise MonitoringStateError(
            "digest does not exist for this watch",
            details={"reason": "unknown_digest", "watch_id": watch_id, "digest_id": digest_id},
        )
    return _digest_row(row)


def _digest_row(row: tuple[Any, ...]) -> _DigestRow:
    digest_id, sequence, status, entry_ids_json, backlog_json, json_path, md_path, created = row
    entry_ids = _stored_json(entry_ids_json, digest_id=digest_id)
    backlog = _stored_json(backlog_json, digest_id=digest_id)
    if (
        not isinstance(entry_ids, list)
        or not all(isinstance(entry_id, str) for entry_id in entry_ids)
        or not isinstance(backlog, dict)
    ):
        raise _unreadable("stored digest record is malformed", digest_id=digest_id)
    return _DigestRow(
        digest_id=str(digest_id),
        sequence=int(sequence),
        status=str(status),
        entry_ids=tuple(entry_ids),
        backlog=backlog,
        json_path=str(json_path),
        markdown_path=str(md_path),
        created_at_utc=float(created),
    )


def _entry_payloads(
    conn: sqlite3.Connection, watch_id: str, entry_ids: tuple[str, ...]
) -> tuple[dict[str, Any], ...]:
    """Load entry payloads in the given order; every entry must still be unpresented."""
    payloads: list[dict[str, Any]] = []
    for entry_id in entry_ids:
        row = conn.execute(
            "SELECT payload_json, digest_id FROM watch_digest_entries "
            "WHERE entry_id = ? AND watch_id = ?",
            (entry_id, watch_id),
        ).fetchone()
        if row is None:
            raise _unreadable("digest entry is missing", entry_id=entry_id)
        if row[1] is not None:
            raise MonitoringStateError(
                "digest entry was already presented",
                details={
                    "reason": "entry_already_presented",
                    "entry_id": entry_id,
                    "digest_id": row[1],
                },
            )
        payload = _stored_json(row[0], entry_id=entry_id)
        if not isinstance(payload, dict):
            raise _unreadable("digest entry payload is not an object", entry_id=entry_id)
        payloads.append(payload)
    return tuple(payloads)


def _unpresented_entry_ids(conn: sqlite3.Connection, watch_id: str, limit: int) -> list[str]:
    rows = conn.execute(
        "SELECT entry_id FROM watch_digest_entries WHERE watch_id = ? AND digest_id IS NULL "
        "ORDER BY created_at_utc ASC, entry_id ASC LIMIT ?",
        (watch_id, limit),
    ).fetchall()
    return [str(row[0]) for row in rows]


def _unpresented_count(conn: sqlite3.Connection, watch_id: str) -> int:
    return _count(conn, "watch_digest_entries WHERE watch_id = ? AND digest_id IS NULL", watch_id)


def _backlog_snapshot(
    conn: sqlite3.Connection, watch_id: str, limit: int, presented: int
) -> dict[str, Any]:
    latest = conn.execute(
        "SELECT outcome, summary_json FROM watch_sweeps WHERE watch_id = ? "
        "ORDER BY finished_at_utc DESC, rowid DESC LIMIT 1",
        (watch_id,),
    ).fetchone()
    refresh_pending: int | None = None
    latest_outcome: str | None = None
    if latest is not None:
        latest_outcome = str(latest[0])
        summary = _stored_json(latest[1], watch_id=watch_id)
        refresh = summary.get("refresh") if isinstance(summary, dict) else None
        # A sweep that failed before refreshing records no refresh summary.
        if isinstance(refresh, dict) and type(refresh.get("pending")) is int:
            refresh_pending = refresh["pending"]
    return {
        "presentation_limit": limit,
        "presented_in_digest": presented,
        "unpresented_entries_after": _unpresented_count(conn, watch_id) - presented,
        "pending_analyses": _pending_analysis_count(conn, watch_id),
        "tracked_threads": _count(conn, _TRACKED_THREADS_WHERE, watch_id),
        "refresh_pending": refresh_pending,
        "latest_sweep_outcome": latest_outcome,
        "search_completeness": "unknown",
    }


def _next_sequence(conn: sqlite3.Connection, watch_id: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(sequence), 0) FROM watch_digests WHERE watch_id = ?", (watch_id,)
    ).fetchone()
    return int(row[0]) + 1


def _digest_relative_paths(watch_id: str, sequence: int, digest_id: str) -> tuple[str, str]:
    if _DIGEST_ID_PATTERN.fullmatch(digest_id) is None:
        raise MonitoringStateError(
            "digest id is not safe to use in an artifact path",
            details={"reason": "invalid_digest_id", "digest_id": digest_id},
        )
    stem = f"watches/{watch_id}/digests/{sequence:06d}-{digest_id}"
    return f"{stem}.json", f"{stem}.md"


def _empty_digest_status() -> dict[str, Any]:
    return {
        "entry_count": 0,
        "unpresented_entries": 0,
        "pending_digest_id": None,
        "digest_count": 0,
        "latest_digest": None,
    }


def _digest_status(
    conn: sqlite3.Connection | None, version: int, watch_id: str, root: Path
) -> dict[str, Any]:
    if conn is None or version < _DIGEST_SCHEMA_VERSION:
        return _empty_digest_status()
    pending = conn.execute(
        "SELECT digest_id FROM watch_digests WHERE watch_id = ? AND status = 'pending'",
        (watch_id,),
    ).fetchone()
    latest = conn.execute(
        "SELECT digest_id, sequence, json_path, markdown_path, entry_ids_json, written_at_utc "
        "FROM watch_digests WHERE watch_id = ? AND status = 'written' "
        "ORDER BY sequence DESC LIMIT 1",
        (watch_id,),
    ).fetchone()
    latest_digest: dict[str, Any] | None = None
    if latest is not None:
        latest_digest = {
            "digest_id": latest[0],
            "sequence": latest[1],
            "json_path": str(root.absolute() / latest[2]),
            "markdown_path": str(root.absolute() / latest[3]),
            "entry_count": len(_stored_json(latest[4], digest_id=latest[0])),
            "written_at_utc": latest[5],
        }
    return {
        "entry_count": _count(conn, "watch_digest_entries WHERE watch_id = ?", watch_id),
        "unpresented_entries": _unpresented_count(conn, watch_id),
        "pending_digest_id": None if pending is None else pending[0],
        "digest_count": _count(
            conn, "watch_digests WHERE watch_id = ? AND status = 'written'", watch_id
        ),
        "latest_digest": latest_digest,
    }
