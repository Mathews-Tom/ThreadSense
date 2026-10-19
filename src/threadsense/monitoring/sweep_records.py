"""Durable sweep records and the SQL helpers that persist them.

Owned by `MonitoringStore`: every helper here runs inside a transaction that the
store has already fenced against stale run owners.
"""

from __future__ import annotations

import json
import math
import re
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from threadsense.errors import MonitoringInputError, MonitoringStateError

if TYPE_CHECKING:
    from threadsense.monitoring.state import RunClaim

SweepOutcome = Literal["changes", "no_changes", "partial", "failed"]
Novelty = Literal["initial_observation", "new_source_event", "newly_observed_historical"]
Completeness = Literal["truncated", "unknown"]
FailureStage = Literal["search", "fetch", "analyze"]
ChangeKind = Literal[
    "initial_observation", "new_source_event", "newly_observed_historical", "edited"
]
_SWEEP_SCHEMA_VERSION = 2
_SWEEP_OUTCOMES: tuple[SweepOutcome, ...] = ("changes", "no_changes", "partial", "failed")
_SUCCEEDED_SWEEP_OUTCOMES: tuple[SweepOutcome, ...] = ("changes", "no_changes")
_NOVELTIES: tuple[Novelty, ...] = (
    "initial_observation",
    "new_source_event",
    "newly_observed_historical",
)
_COMPLETENESS_VALUES: tuple[Completeness, ...] = ("truncated", "unknown")
_FAILURE_STAGES: tuple[FailureStage, ...] = ("search", "fetch", "analyze")
_REJECTION_REASONS = ("query_mismatch", "excluded_term")
_SOURCE_THREAD_ID_PATTERN = re.compile(r"^[a-z0-9]{1,16}$")


@dataclass(frozen=True)
class DiscoveredThread:
    """One search hit of a sweep's discovery list, already evaluated for selection."""

    query: str
    source_thread_id: str
    thread_url: str
    subreddit: str
    title: str
    source_created_utc: float
    rank: int
    score: int
    num_comments: int
    selected: bool
    rejection_reason: str | None


@dataclass(frozen=True)
class TrackedThread:
    source_thread_id: str
    thread_url: str
    subreddit: str
    title: str
    last_refreshed_at_utc: float | None
    current_revision_id: str | None


@dataclass(frozen=True)
class KnownComment:
    comment_id: str
    text_sha256: str
    source_created_utc: float
    novelty: Novelty
    first_observed_at_utc: float


@dataclass(frozen=True)
class RevisionRef:
    revision_id: str
    source_thread_id: str
    sequence: int
    snapshot_path: Path
    snapshot_sha256: str
    thread_text_sha256: str
    created_at_utc: float


@dataclass(frozen=True)
class ThreadHistory:
    """Durable per-thread state that novelty is computed against."""

    source_thread_id: str
    known_comments: dict[str, KnownComment]
    current_revision: RevisionRef | None
    prior_observed_at_utc: float | None


@dataclass(frozen=True)
class CommentChange:
    comment_id: str
    kind: ChangeKind
    text_sha256: str
    previous_text_sha256: str | None
    source_created_utc: float

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "comment_id": self.comment_id,
            "kind": self.kind,
            "text_sha256": self.text_sha256,
            "previous_text_sha256": self.previous_text_sha256,
            "source_created_utc": self.source_created_utc,
        }
        if self.kind == "edited":
            # The source does not report edit times; the observation time is recorded instead.
            payload["source_edited_at_utc"] = None
        return payload


@dataclass(frozen=True)
class AcquisitionRecord:
    acquisition_id: str
    source_thread_id: str
    observed_at_utc: float
    raw_path: Path
    raw_sha256: str
    normalized_path: Path
    normalized_sha256: str
    completeness: Completeness
    cache_status: str
    parsed_comment_count: int
    reported_comment_count: int
    observed_comments: tuple[tuple[str, str, float], ...]
    absent_known_comment_count: int


@dataclass(frozen=True)
class RevisionRecord:
    revision_id: str
    previous_revision_id: str | None
    substantive_sha256: str
    thread_text_sha256: str
    snapshot_path: Path
    snapshot_sha256: str
    changes: tuple[CommentChange, ...]
    thread_text_changed: bool


_REVISION_REF_COLUMNS = (
    "r.revision_id, r.source_thread_id, r.sequence, r.snapshot_path, r.snapshot_sha256, "
    "r.thread_text_sha256, r.created_at_utc"
)


_TRACKED_THREADS_WHERE = "watch_threads WHERE watch_id = ? AND selected = 1"


_PENDING_ANALYSES_FROM = (
    "FROM watch_thread_revisions AS r "
    "JOIN watch_threads AS t ON t.watch_id = r.watch_id "
    "AND t.source_thread_id = r.source_thread_id AND t.current_revision_id = r.revision_id "
    "LEFT JOIN watch_revision_analyses AS a ON a.revision_id = r.revision_id "
    "WHERE r.watch_id = ? AND a.revision_id IS NULL"
)


@dataclass(frozen=True)
class _AcquisitionWrite:
    """The fenced claim, acquisition and store time shared by one acquisition's writes."""

    claim: RunClaim
    acquisition: AcquisitionRecord
    now: float


@dataclass(frozen=True)
class _CommentWrite:
    comment_id: str
    text_sha256: str
    source_created_utc: float
    action: Literal["first_observed", "seen", "edited"]
    novelty: Novelty | None


def _validate_limit(limit: int) -> None:
    if type(limit) is not int or limit < 0:
        raise MonitoringInputError(
            "limit must be a non-negative integer",
            details={"reason": "invalid_limit", "limit": limit},
        )


def _validate_discovery(discovery: DiscoveredThread) -> None:
    details: dict[str, Any] = {
        "reason": "invalid_discovery",
        "query": discovery.query,
        "source_thread_id": discovery.source_thread_id,
    }
    thread_id = discovery.source_thread_id
    if not isinstance(thread_id, str) or _SOURCE_THREAD_ID_PATTERN.fullmatch(thread_id) is None:
        raise MonitoringInputError(
            "discovery source thread id must be 1-16 lowercase letters or digits",
            details=details,
        )
    if type(discovery.selected) is not bool:
        raise MonitoringInputError("discovery selection must be a boolean", details=details)
    if discovery.selected and discovery.rejection_reason is not None:
        raise MonitoringInputError(
            "a selected discovery cannot carry a rejection reason", details=details
        )
    if not discovery.selected and discovery.rejection_reason not in _REJECTION_REASONS:
        raise MonitoringInputError(
            "a rejected discovery needs a known rejection reason",
            details={**details, "rejection_reason": discovery.rejection_reason},
        )
    if type(discovery.rank) is not int or discovery.rank < 1:
        raise MonitoringInputError(
            "discovery rank must be a positive integer",
            details={**details, "rank": discovery.rank},
        )


def _upsert_watch_thread(
    conn: sqlite3.Connection, claim: RunClaim, discovery: DiscoveredThread, now: float
) -> bool:
    """Insert or refresh a discovered thread; return True when it is newly tracked."""
    row = conn.execute(
        "SELECT selected FROM watch_threads WHERE watch_id = ? AND source_thread_id = ?",
        (claim.watch_id, discovery.source_thread_id),
    ).fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO watch_threads (watch_id, source_thread_id, thread_url, subreddit, "
            "title, source_created_utc, selected, rejection_reason, first_discovered_at_utc, "
            "first_discovered_run_id, last_discovered_at_utc) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                claim.watch_id,
                discovery.source_thread_id,
                discovery.thread_url,
                discovery.subreddit,
                discovery.title,
                discovery.source_created_utc,
                int(discovery.selected),
                discovery.rejection_reason,
                now,
                claim.run_id,
                now,
            ),
        )
        return discovery.selected
    was_selected = bool(row[0])
    selected = was_selected or discovery.selected
    conn.execute(
        "UPDATE watch_threads SET last_discovered_at_utc = ?, title = ?, selected = ?, "
        "rejection_reason = ? WHERE watch_id = ? AND source_thread_id = ?",
        (
            now,
            discovery.title,
            int(selected),
            None if selected else discovery.rejection_reason,
            claim.watch_id,
            discovery.source_thread_id,
        ),
    )
    return selected and not was_selected


def _validate_acquisition(acquisition: AcquisitionRecord) -> None:
    details: dict[str, Any] = {
        "reason": "invalid_acquisition",
        "acquisition_id": acquisition.acquisition_id,
    }
    observed_at = acquisition.observed_at_utc
    if type(observed_at) is not float or not math.isfinite(observed_at):
        raise MonitoringInputError(
            "acquisition observation time must be a finite float",
            details={**details, "observed_at_utc": observed_at},
        )
    if acquisition.completeness not in _COMPLETENESS_VALUES:
        raise MonitoringInputError(
            "acquisition completeness must be truncated or unknown",
            details={**details, "completeness": acquisition.completeness},
        )
    if not isinstance(acquisition.cache_status, str) or not acquisition.cache_status:
        raise MonitoringInputError("acquisition cache status must be recorded", details=details)
    counts = {
        "parsed_comment_count": acquisition.parsed_comment_count,
        "reported_comment_count": acquisition.reported_comment_count,
        "absent_known_comment_count": acquisition.absent_known_comment_count,
    }
    for name, value in counts.items():
        if type(value) is not int or value < 0:
            raise MonitoringInputError(
                f"acquisition {name} must be a non-negative integer",
                details={**details, name: value},
            )


def _validate_observed_at(
    conn: sqlite3.Connection, watch_id: str, acquisition: AcquisitionRecord, now: float
) -> None:
    """Refuse an observation time after commit time or before the thread's latest one."""
    details: dict[str, Any] = {
        "reason": "invalid_acquisition",
        "acquisition_id": acquisition.acquisition_id,
        "observed_at_utc": acquisition.observed_at_utc,
    }
    if acquisition.observed_at_utc > now:
        raise MonitoringInputError(
            "acquisition observation time is later than the store time",
            details={**details, "now": now},
        )
    latest = conn.execute(
        "SELECT MAX(observed_at_utc) FROM watch_acquisitions "
        "WHERE watch_id = ? AND source_thread_id = ?",
        (watch_id, acquisition.source_thread_id),
    ).fetchone()[0]
    if latest is not None and acquisition.observed_at_utc < latest:
        raise MonitoringInputError(
            "acquisition observation time is earlier than the thread's latest acquisition",
            details={**details, "latest_observed_at_utc": latest},
        )


def _novelty_cutoff(conn: sqlite3.Connection, watch_id: str, source_thread_id: str) -> float | None:
    """Observation time of the latest complete acquisition, else of the first acquisition."""
    # A truncated acquisition did not see every comment, so it must not move the cutoff past
    # comments it could have missed.
    row = conn.execute(
        "SELECT COALESCE("
        "(SELECT MAX(observed_at_utc) FROM watch_acquisitions "
        "WHERE watch_id = ? AND source_thread_id = ? AND completeness != 'truncated'), "
        "(SELECT MIN(observed_at_utc) FROM watch_acquisitions "
        "WHERE watch_id = ? AND source_thread_id = ?))",
        (watch_id, source_thread_id, watch_id, source_thread_id),
    ).fetchone()
    return None if row[0] is None else float(row[0])


def _tracked_thread(
    conn: sqlite3.Connection, watch_id: str, source_thread_id: str
) -> tuple[str | None, str | None]:
    """Return (current_revision_id, last_acquisition_id) of a selected thread of the watch."""
    row = conn.execute(
        "SELECT selected, current_revision_id, last_acquisition_id FROM watch_threads "
        "WHERE watch_id = ? AND source_thread_id = ?",
        (watch_id, source_thread_id),
    ).fetchone()
    if row is None or not row[0]:
        raise MonitoringStateError(
            "thread is not tracked by this watch",
            details={
                "reason": "unknown_thread",
                "watch_id": watch_id,
                "source_thread_id": source_thread_id,
            },
        )
    return row[1], row[2]


def _known_comment_hashes(
    conn: sqlite3.Connection, watch_id: str, source_thread_id: str
) -> dict[str, str]:
    rows = conn.execute(
        "SELECT comment_id, current_text_sha256 FROM watch_comments "
        "WHERE watch_id = ? AND source_thread_id = ?",
        (watch_id, source_thread_id),
    ).fetchall()
    return {str(comment_id): str(text_sha256) for comment_id, text_sha256 in rows}


def _inconsistent(message: str, **details: Any) -> MonitoringStateError:
    # The mismatch is against durable history, not against the shape of caller input.
    return MonitoringStateError(message, details={"reason": "inconsistent_changes", **details})


def _comment_writes(
    acquisition: AcquisitionRecord,
    revision: RevisionRecord | None,
    known: dict[str, str],
    *,
    first_acquisition: bool,
) -> list[_CommentWrite]:
    """Derive per-comment writes from the observation, refusing disagreeing revision changes."""
    if first_acquisition and revision is None:
        raise _inconsistent("the first acquisition of a thread must create its first revision")
    if (
        revision is not None
        and not first_acquisition
        and not revision.changes
        and not revision.thread_text_changed
    ):
        raise _inconsistent("a later revision needs a comment or thread text change")
    changes: dict[str, CommentChange] = {}
    for change in revision.changes if revision is not None else ():
        if change.comment_id in changes:
            raise _inconsistent("revision lists a comment twice", comment_id=change.comment_id)
        changes[change.comment_id] = change
    observed_ids: set[str] = set()
    writes: list[_CommentWrite] = []
    for observed in acquisition.observed_comments:
        comment_id = observed[0]
        if comment_id in observed_ids:
            raise _inconsistent("acquisition observes a comment twice", comment_id=comment_id)
        observed_ids.add(comment_id)
        matched = changes.pop(comment_id) if comment_id in changes else None
        writes.append(_comment_write(observed, known.get(comment_id), matched, first_acquisition))
    if changes:
        raise _inconsistent(
            "revision changes comments that were not observed", comment_ids=sorted(changes)
        )
    absent = len(set(known) - observed_ids)
    if absent != acquisition.absent_known_comment_count:
        raise _inconsistent(
            "absent known comment count does not match the durable history",
            expected=absent,
            recorded=acquisition.absent_known_comment_count,
        )
    return writes


def _comment_write(
    observed: tuple[str, str, float],
    known_sha256: str | None,
    change: CommentChange | None,
    first_acquisition: bool,
) -> _CommentWrite:
    comment_id, text_sha256, created_utc = observed
    if known_sha256 == text_sha256:
        if change is not None:
            raise _inconsistent("unchanged known comment has a change", comment_id=comment_id)
        return _CommentWrite(comment_id, text_sha256, created_utc, "seen", None)
    if change is None:
        raise _inconsistent("new or edited comment has no matching change", comment_id=comment_id)
    if change.text_sha256 != text_sha256 or change.source_created_utc != created_utc:
        raise _inconsistent("change does not match the observed comment", comment_id=comment_id)
    if known_sha256 is not None:
        if change.kind != "edited" or change.previous_text_sha256 != known_sha256:
            raise _inconsistent("edited comment needs an edited change", comment_id=comment_id)
        return _CommentWrite(comment_id, text_sha256, created_utc, "edited", None)
    if change.kind == "edited" or change.previous_text_sha256 is not None:
        raise _inconsistent("first observed comment cannot be an edit", comment_id=comment_id)
    if (change.kind == "initial_observation") != first_acquisition:
        raise _inconsistent(
            "initial observation applies exactly to the first acquisition",
            comment_id=comment_id,
            kind=change.kind,
        )
    return _CommentWrite(comment_id, text_sha256, created_utc, "first_observed", change.kind)


def _insert_revision(
    conn: sqlite3.Connection, write: _AcquisitionWrite, revision: RevisionRecord, snapshot: str
) -> None:
    sequence = 1
    if revision.previous_revision_id is not None:
        previous = conn.execute(
            "SELECT sequence FROM watch_thread_revisions WHERE revision_id = ?",
            (revision.previous_revision_id,),
        ).fetchone()
        if previous is None:
            raise MonitoringStateError(
                "previous revision is missing",
                details={
                    "reason": "revision_missing",
                    "revision_id": revision.previous_revision_id,
                },
            )
        sequence = int(previous[0]) + 1
    changes_json = _canonical_json(
        {
            "comment_changes": [change.to_dict() for change in revision.changes],
            "thread_text_changed": revision.thread_text_changed,
        },
        "changes",
    )
    conn.execute(
        "INSERT INTO watch_thread_revisions (revision_id, watch_id, source_thread_id, "
        "sequence, previous_revision_id, substantive_sha256, thread_text_sha256, "
        "snapshot_path, snapshot_sha256, acquisition_id, run_id, created_at_utc, "
        "changes_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            revision.revision_id,
            write.claim.watch_id,
            write.acquisition.source_thread_id,
            sequence,
            revision.previous_revision_id,
            revision.substantive_sha256,
            revision.thread_text_sha256,
            snapshot,
            revision.snapshot_sha256,
            write.acquisition.acquisition_id,
            write.claim.run_id,
            write.now,
            changes_json,
        ),
    )


def _apply_comment_writes(
    conn: sqlite3.Connection, write: _AcquisitionWrite, comments: Sequence[_CommentWrite]
) -> None:
    watch_id = write.claim.watch_id
    thread_id = write.acquisition.source_thread_id
    acquisition_id = write.acquisition.acquisition_id
    observed_at = write.acquisition.observed_at_utc
    for comment in comments:
        key = (watch_id, thread_id, comment.comment_id)
        if comment.action == "first_observed":
            conn.execute(
                "INSERT INTO watch_comments (watch_id, source_thread_id, comment_id, "
                "current_text_sha256, source_created_utc, novelty, first_observed_at_utc, "
                "first_observed_run_id, first_acquisition_id, last_seen_at_utc, "
                "last_seen_acquisition_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    *key,
                    comment.text_sha256,
                    comment.source_created_utc,
                    comment.novelty,
                    observed_at,
                    write.claim.run_id,
                    acquisition_id,
                    observed_at,
                    acquisition_id,
                ),
            )
        else:
            conn.execute(
                "UPDATE watch_comments SET current_text_sha256 = ?, last_seen_at_utc = ?, "
                "last_seen_acquisition_id = ? "
                "WHERE watch_id = ? AND source_thread_id = ? AND comment_id = ?",
                (comment.text_sha256, observed_at, acquisition_id, *key),
            )
        if comment.action != "seen":
            conn.execute(
                "INSERT INTO watch_comment_texts (watch_id, source_thread_id, comment_id, "
                "acquisition_id, text_sha256, change, observed_at_utc) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (*key, acquisition_id, comment.text_sha256, comment.action, observed_at),
            )


def _canonical_json(value: object, field: str) -> str:
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
    except (TypeError, ValueError) as error:
        raise MonitoringInputError(
            f"{field} must be JSON serializable",
            details={"reason": "invalid_json", "field": field, "error": str(error)},
        ) from error


def _stored_novelty(value: str) -> Novelty:
    if value not in _NOVELTIES:
        raise MonitoringStateError(
            "stored comment novelty is not recognized",
            details={"reason": "novelty_unreadable", "novelty": value},
        )
    return value


def _count(conn: sqlite3.Connection, from_where: str, watch_id: str) -> int:
    """Count rows; `from_where` is always a constant clause of this module."""
    return int(conn.execute(f"SELECT COUNT(*) FROM {from_where}", (watch_id,)).fetchone()[0])


def _pending_analysis_count(conn: sqlite3.Connection, watch_id: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) {_PENDING_ANALYSES_FROM}", (watch_id,)).fetchone()[0])


def _sweep_status(conn: sqlite3.Connection | None, version: int, watch_id: str) -> dict[str, Any]:
    if conn is None or version < _SWEEP_SCHEMA_VERSION:
        return {
            "tracked_threads": 0,
            "refresh_due_never_refreshed": 0,
            "pending_analyses": 0,
            "revision_count": 0,
            "comment_count": 0,
            "latest_sweep": None,
        }
    latest = conn.execute(
        "SELECT run_id, outcome, finished_at_utc, summary_json FROM watch_sweeps "
        "WHERE watch_id = ? ORDER BY finished_at_utc DESC, rowid DESC LIMIT 1",
        (watch_id,),
    ).fetchone()
    latest_sweep: dict[str, Any] | None = None
    if latest is not None:
        latest_sweep = {
            "run_id": latest[0],
            "outcome": latest[1],
            "finished_at_utc": latest[2],
            "summary": json.loads(latest[3]),
        }
    return {
        "tracked_threads": _count(conn, _TRACKED_THREADS_WHERE, watch_id),
        "refresh_due_never_refreshed": _count(
            conn,
            "watch_threads WHERE watch_id = ? AND selected = 1 AND last_refreshed_at_utc IS NULL",
            watch_id,
        ),
        "pending_analyses": _pending_analysis_count(conn, watch_id),
        "revision_count": _count(conn, "watch_thread_revisions WHERE watch_id = ?", watch_id),
        "comment_count": _count(conn, "watch_comments WHERE watch_id = ?", watch_id),
        "latest_sweep": latest_sweep,
    }
