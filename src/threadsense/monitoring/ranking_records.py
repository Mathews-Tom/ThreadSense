"""Ranking policy revisions, per-watch selection and the recorded basis of each digest's order.

Owned by `MonitoringStore`. Policies are content-addressed and immutable; selecting one
appends a selection event, so switching back never rewrites anything. Each digest recorded
from schema 7 on stores the exact profile revision, policy id and preference sequence its
order is materialized from, so the order and reasons can be replayed later.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from threadsense.errors import MonitoringInputError, MonitoringStateError
from threadsense.monitoring.digest_records import (
    _ENTRY_ID_PATTERN,
    _digest_row_by_id,
    _pending_digest_row,
    _stored_digest_issue_view,
    _stored_json,
)
from threadsense.monitoring.ranking import (
    DEFAULT_RANKING_POLICY,
    POLICY_ID_PATTERN,
    RankingBasis,
    RankingPolicy,
    stored_ranking_policy,
)

_RANKING_SCHEMA_VERSION = 7


def _append_only(table: str) -> tuple[str, str]:
    return (
        f"CREATE TRIGGER {table}_no_update BEFORE UPDATE ON {table} "
        "BEGIN SELECT RAISE(ABORT, 'ranking history is append-only'); END",
        f"CREATE TRIGGER {table}_no_delete BEFORE DELETE ON {table} "
        "BEGIN SELECT RAISE(ABORT, 'ranking history is append-only'); END",
    )


RANKING_SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE watch_ranking_policies (
        policy_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        policy_json TEXT NOT NULL,
        created_at_utc REAL NOT NULL
    )
    """,
    """
    CREATE TABLE watch_ranking_selections (
        watch_id TEXT NOT NULL REFERENCES watches (watch_id),
        sequence INTEGER NOT NULL CHECK (sequence >= 1),
        policy_id TEXT NOT NULL REFERENCES watch_ranking_policies (policy_id),
        selected_at_utc REAL NOT NULL,
        PRIMARY KEY (watch_id, sequence)
    )
    """,
    """
    CREATE TABLE watch_digest_rankings (
        digest_id TEXT PRIMARY KEY REFERENCES watch_digests (digest_id),
        watch_id TEXT NOT NULL REFERENCES watches (watch_id),
        profile_revision INTEGER NOT NULL CHECK (profile_revision >= 1),
        policy_id TEXT NOT NULL REFERENCES watch_ranking_policies (policy_id),
        preference_sequence INTEGER NOT NULL CHECK (preference_sequence >= 0),
        created_at_utc REAL NOT NULL
    )
    """,
    *_append_only("watch_ranking_policies"),
    *_append_only("watch_ranking_selections"),
    *_append_only("watch_digest_rankings"),
)


def _unknown_policy(policy_id: str) -> MonitoringStateError:
    return MonitoringStateError(
        "ranking policy is not recorded",
        details={"reason": "unknown_policy", "policy_id": policy_id},
    )


def _ensure_policy(conn: sqlite3.Connection, policy: RankingPolicy, now: float) -> str:
    """Record a policy once; an id is a content hash, so a stored row must match exactly."""
    policy_id = policy.policy_id
    row = conn.execute(
        "SELECT policy_json FROM watch_ranking_policies WHERE policy_id = ?", (policy_id,)
    ).fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO watch_ranking_policies (policy_id, name, policy_json, created_at_utc) "
            "VALUES (?, ?, ?, ?)",
            (policy_id, policy.name, policy.canonical_json(), now),
        )
    elif row[0] != policy.canonical_json():
        raise MonitoringStateError(
            "stored ranking policy does not match its recorded identity",
            details={"reason": "ranking_policy_unreadable", "policy_id": policy_id},
        )
    return policy_id


def _stored_policy(conn: sqlite3.Connection | None, policy_id: str) -> RankingPolicy:
    """A recorded policy; the built-in default is known before it is first recorded."""
    row = None
    if conn is not None:
        row = conn.execute(
            "SELECT policy_json FROM watch_ranking_policies WHERE policy_id = ?", (policy_id,)
        ).fetchone()
    if row is not None:
        return stored_ranking_policy(policy_id, str(row[0]))
    if policy_id == DEFAULT_RANKING_POLICY.policy_id:
        return DEFAULT_RANKING_POLICY
    raise _unknown_policy(policy_id)


def _selections(conn: sqlite3.Connection | None, watch_id: str) -> list[dict[str, Any]]:
    if conn is None:
        return []
    rows = conn.execute(
        "SELECT s.sequence, s.policy_id, p.name, s.selected_at_utc "
        "FROM watch_ranking_selections AS s "
        "JOIN watch_ranking_policies AS p ON p.policy_id = s.policy_id "
        "WHERE s.watch_id = ? ORDER BY s.sequence ASC",
        (watch_id,),
    ).fetchall()
    return [
        {"sequence": row[0], "policy_id": row[1], "name": row[2], "selected_at_utc": row[3]}
        for row in rows
    ]


def _active_policy_id(conn: sqlite3.Connection | None, watch_id: str) -> str:
    """The latest selection, or the built-in default when nothing was ever selected."""
    selections = _selections(conn, watch_id)
    if not selections:
        return DEFAULT_RANKING_POLICY.policy_id
    return str(selections[-1]["policy_id"])


def _select_policy(
    conn: sqlite3.Connection, watch_id: str, policy: RankingPolicy, now: float
) -> dict[str, Any]:
    policy_id = _ensure_policy(conn, policy, now)
    previous = _active_policy_id(conn, watch_id)
    if previous == policy_id:
        return {"status": "unchanged", "policy_id": policy_id, "previous_policy_id": previous}
    sequence = len(_selections(conn, watch_id)) + 1
    conn.execute(
        "INSERT INTO watch_ranking_selections (watch_id, sequence, policy_id, selected_at_utc) "
        "VALUES (?, ?, ?, ?)",
        (watch_id, sequence, policy_id, now),
    )
    return {"status": "selected", "policy_id": policy_id, "previous_policy_id": previous}


def _require_policy_id(policy_id: str) -> None:
    if not isinstance(policy_id, str) or POLICY_ID_PATTERN.fullmatch(policy_id) is None:
        raise MonitoringStateError(
            "ranking policy id is malformed",
            details={"reason": "unknown_policy", "policy_id": policy_id},
        )


def _ranking_policy_status(conn: sqlite3.Connection | None, watch_id: str) -> dict[str, Any]:
    active_id = _active_policy_id(conn, watch_id)
    active = _stored_policy(conn, active_id)
    return {
        "watch_id": watch_id,
        "active_policy_id": active_id,
        "active_policy": active.to_dict(),
        "default_policy_id": DEFAULT_RANKING_POLICY.policy_id,
        "selections": _selections(conn, watch_id),
    }


def _preference_sequence(conn: sqlite3.Connection | None, watch_id: str) -> int:
    if conn is None:
        return 0
    row = conn.execute(
        "SELECT COALESCE(MAX(sequence), 0) FROM watch_preference_events WHERE watch_id = ?",
        (watch_id,),
    ).fetchone()
    return int(row[0])


def _preferences_upto(
    conn: sqlite3.Connection | None, watch_id: str, sequence: int
) -> tuple[dict[str, Any], ...]:
    if conn is None or sequence == 0:
        return ()
    columns = (
        "feedback_id",
        "sequence",
        "value",
        "target_kind",
        "target_id",
        "scope_kind",
        "scope_key",
    )
    rows = conn.execute(
        f"SELECT {', '.join(columns)} FROM watch_preference_events "
        "WHERE watch_id = ? AND sequence <= ? ORDER BY sequence ASC",
        (watch_id, sequence),
    ).fetchall()
    return tuple(dict(zip(columns, row, strict=True)) for row in rows)


def _insert_basis(
    conn: sqlite3.Connection, watch_id: str, digest_id: str, basis: RankingBasis, now: float
) -> None:
    conn.execute(
        "INSERT INTO watch_digest_rankings (digest_id, watch_id, profile_revision, policy_id, "
        "preference_sequence, created_at_utc) VALUES (?, ?, ?, ?, ?, ?)",
        (
            digest_id,
            watch_id,
            basis.profile_revision,
            basis.policy_id,
            basis.preference_sequence,
            now,
        ),
    )


def _stored_basis(
    conn: sqlite3.Connection | None, version: int, watch_id: str, digest_id: str
) -> RankingBasis | None:
    """The recorded basis; None for a digest recorded before rankings existed."""
    if conn is None or version < _RANKING_SCHEMA_VERSION:
        return None
    row = conn.execute(
        "SELECT profile_revision, policy_id, preference_sequence FROM watch_digest_rankings "
        "WHERE watch_id = ? AND digest_id = ?",
        (watch_id, digest_id),
    ).fetchone()
    if row is None:
        return None
    return RankingBasis(
        profile_revision=int(row[0]), policy_id=str(row[1]), preference_sequence=int(row[2])
    )


@dataclass(frozen=True)
class EntryPresentation:
    """One entry with the digest that presented it (or is presenting it), if any."""

    entry: dict[str, Any]
    digest_id: str | None
    sequence: int | None
    status: str | None
    json_path: Path | None
    json_sha256: str | None
    entries: tuple[dict[str, Any], ...]
    issue_view: dict[str, Any] | None
    basis: RankingBasis | None


def _entry_payload(conn: sqlite3.Connection, watch_id: str, entry_id: str) -> dict[str, Any]:
    row = conn.execute(
        "SELECT watch_id, payload_json FROM watch_digest_entries WHERE entry_id = ?", (entry_id,)
    ).fetchone()
    if row is None:
        raise MonitoringStateError(
            "digest entry does not exist",
            details={"reason": "unknown_entry", "watch_id": watch_id, "entry_id": entry_id},
        )
    if row[0] != watch_id:
        raise MonitoringStateError(
            "digest entry belongs to a different watch",
            details={"reason": "cross_watch_target", "watch_id": watch_id, "entry_id": entry_id},
        )
    payload = _stored_json(str(row[1]), entry_id=entry_id)
    if not isinstance(payload, dict):
        raise MonitoringStateError(
            "digest entry payload is not an object",
            details={"reason": "digest_state_unreadable", "entry_id": entry_id},
        )
    return payload


def _require_entry_id(entry_id: str) -> None:
    if not isinstance(entry_id, str) or _ENTRY_ID_PATTERN.fullmatch(entry_id) is None:
        raise MonitoringInputError(
            "digest entry id is malformed",
            details={"reason": "invalid_entry_id", "entry_id": entry_id},
        )


def _entry_presentation(
    conn: sqlite3.Connection, version: int, watch_id: str, entry_id: str, root: Path
) -> EntryPresentation:
    entry = _entry_payload(conn, watch_id, entry_id)
    presented = conn.execute(
        "SELECT digest_id FROM watch_digest_entries WHERE entry_id = ? AND watch_id = ?",
        (entry_id, watch_id),
    ).fetchone()[0]
    if presented is not None:
        digest = _digest_row_by_id(conn, watch_id, str(presented))
    else:
        pending = _pending_digest_row(conn, watch_id)
        if pending is None or entry_id not in pending.entry_ids:
            return EntryPresentation(entry, None, None, None, None, None, (entry,), None, None)
        digest = pending
    json_sha256 = conn.execute(
        "SELECT json_sha256 FROM watch_digests WHERE digest_id = ?", (digest.digest_id,)
    ).fetchone()[0]
    written = digest.status == "written"
    return EntryPresentation(
        entry=entry,
        digest_id=digest.digest_id,
        sequence=digest.sequence,
        status=digest.status,
        json_path=root / digest.json_path if written else None,
        json_sha256=str(json_sha256) if written else None,
        entries=tuple(_entry_payload(conn, watch_id, item) for item in digest.entry_ids),
        issue_view=(
            _stored_digest_issue_view(conn, watch_id, digest.digest_id) if version >= 5 else None
        ),
        basis=_stored_basis(conn, version, watch_id, digest.digest_id),
    )
