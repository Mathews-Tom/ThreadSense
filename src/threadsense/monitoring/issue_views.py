"""Operator membership corrections and read views of cross-thread issue history.

Owned by `MonitoringStore`: membership changes run inside the store's immediate transaction
and write exactly one append-only event or refuse without writing. Read views never write.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal

from threadsense.errors import MonitoringInputError, MonitoringStateError
from threadsense.monitoring.issue_corrections import LineageProjection, _load_lineage_projection
from threadsense.monitoring.issue_records import (
    ISSUE_ID_PATTERN,
    MAX_REASON_CHARS,
    UPDATE_KINDS,
    _current_issue,
    _insert_membership,
    _issue_refusal,
    _issue_unreadable,
    _matches,
    _MembershipEvent,
    _stored_issue_json,
    _stored_str_list,
    issue_id_for,
)
from threadsense.monitoring.profiles import WATCH_ID_PATTERN
from threadsense.monitoring.sweep_records import _SOURCE_THREAD_ID_PATTERN

_RESOLUTION_BASIS = "resolution claims are unverified updates; a claim never resolves an issue"
_INDEPENDENCE_BASIS = (
    "distinct accounts and communities are observations, not proof of independent sources"
)
_MEMBERSHIP_KEYS = (
    "event_id",
    "watch_id",
    "source_thread_id",
    "issue_id",
    "previous_issue_id",
    "action",
    "basis",
    "reason",
    "report_id",
    "policy_sha256",
    "run_id",
    "created_at_utc",
)
_MEMBERSHIP_COLUMNS = ", ".join(_MEMBERSHIP_KEYS)
_REPORT_KEYS = (
    "report_id",
    "source_thread_id",
    "comment_id",
    "permalink",
    "community",
    "lineage",
    "copy_of_report_id",
    "first_text_sha256",
    "first_entry_id",
    "source_created_utc",
    "recorded_at_utc",
)
_UPDATE_KEYS = (
    "update_id",
    "kind",
    "cue",
    "report_id",
    "source_thread_id",
    "comment_id",
    "permalink",
    "entry_id",
    "text_sha256",
    "recorded_at_utc",
)
_UPDATE_SELECT = (
    "u.update_id, u.kind, u.cue, u.report_id, u.source_thread_id, r.comment_id, "
    "r.permalink, u.entry_id, u.text_sha256, u.recorded_at_utc"
)
_COUNT_KEYS = (
    "observed_threads",
    "observed_communities",
    "observed_accounts",
    "reports_without_account",
    "reports",
    "firsthand_reports",
    "exact_copies",
)
_MEMBERS_SQL = (
    "SELECT m.source_thread_id, m.basis, m.event_id, m.created_at_utc, "
    "t.thread_url, t.subreddit, t.title "
    "FROM watch_issue_memberships AS m "
    "LEFT JOIN watch_threads AS t "
    "ON t.watch_id = m.watch_id AND t.source_thread_id = m.source_thread_id "
    "WHERE m.watch_id = ? AND m.issue_id = ? AND m.event_id = ("
    "SELECT MAX(l.event_id) FROM watch_issue_memberships AS l "
    "WHERE l.watch_id = m.watch_id AND l.source_thread_id = m.source_thread_id) "
    "ORDER BY m.event_id ASC"
)


@dataclass(frozen=True)
class _IssueRequest:
    """A validated operator membership request; `reason` is already stripped."""

    watch_id: str
    issue_id: str
    source_thread_id: str
    reason: str


@dataclass(frozen=True)
class _IssueRow:
    home_thread_id: str
    policy_sha256: str


def _unknown_issue(watch_id: str, issue_id: str) -> MonitoringStateError:
    return _issue_refusal(
        "issue does not exist in this watch", "unknown_issue", watch_id=watch_id, issue_id=issue_id
    )


def _invalid_request(field: str) -> MonitoringInputError:
    return MonitoringInputError(
        f"issue request {field} is invalid",
        details={"reason": "invalid_issue_request", "field": field},
    )


def _placeholders(count: int) -> str:
    return ", ".join("?" for _ in range(count))


def _require_issue_ids(watch_id: str, issue_id: str) -> None:
    if not _matches(WATCH_ID_PATTERN, watch_id):
        raise _invalid_request("watch_id")
    if not _matches(ISSUE_ID_PATTERN, issue_id):
        raise _invalid_request("issue_id")


def _issue_request(
    watch_id: str, issue_id: str, source_thread_id: str, reason: str
) -> _IssueRequest:
    """Validate an operator membership request before the database is touched."""
    _require_issue_ids(watch_id, issue_id)
    if not _matches(_SOURCE_THREAD_ID_PATTERN, source_thread_id):
        raise _invalid_request("source_thread_id")
    stripped = reason.strip() if isinstance(reason, str) else ""
    if not 1 <= len(stripped) <= MAX_REASON_CHARS or any(
        ord(char) < 32 or ord(char) == 127 for char in stripped
    ):
        raise _invalid_request("reason")
    return _IssueRequest(
        watch_id=watch_id, issue_id=issue_id, source_thread_id=source_thread_id, reason=stripped
    )


def _membership_event_dict(row: Sequence[Any]) -> dict[str, Any]:
    return dict(zip(_MEMBERSHIP_KEYS, row, strict=True))


def _issue_row(conn: sqlite3.Connection, watch_id: str, issue_id: str) -> _IssueRow:
    row = conn.execute(
        "SELECT home_thread_id, policy_sha256 FROM watch_issues "
        "WHERE issue_id = ? AND watch_id = ?",
        (issue_id, watch_id),
    ).fetchone()
    if row is None:
        raise _unknown_issue(watch_id, issue_id)
    return _IssueRow(home_thread_id=str(row[0]), policy_sha256=str(row[1]))


def _change_issue_membership(
    conn: sqlite3.Connection,
    request: _IssueRequest,
    action: Literal["linked", "unlinked"],
    now: float,
) -> dict[str, Any]:
    """Write exactly one operator membership event, or refuse without writing."""
    watch_id, thread_id = request.watch_id, request.source_thread_id
    _issue_row(conn, watch_id, request.issue_id)
    home = issue_id_for(watch_id, thread_id)
    has_home = conn.execute(
        "SELECT 1 FROM watch_issues WHERE issue_id = ? AND watch_id = ?", (home, watch_id)
    ).fetchone()
    if has_home is None:
        raise _issue_refusal(
            "thread has no issue history in this watch",
            "unknown_issue_thread",
            watch_id=watch_id,
            source_thread_id=thread_id,
        )
    current = _current_issue(conn, watch_id, thread_id)
    if current is None:
        raise _issue_unreadable(
            "issue thread has no membership event", watch_id=watch_id, source_thread_id=thread_id
        )
    if action == "linked":
        _require_linkable(conn, request, current=current, home=home)
        new_issue = request.issue_id
    else:
        _require_unlinkable(request, current=current, home=home)
        new_issue = home
    event_id = _insert_membership(
        conn,
        _MembershipEvent(
            watch_id=watch_id,
            source_thread_id=thread_id,
            issue_id=new_issue,
            previous_issue_id=current,
            action=action,
            basis="operator",
            reason=request.reason,
            report_id=None,
            policy_sha256=None,
            run_id=None,
            created_at_utc=now,
        ),
    )
    row = conn.execute(
        f"SELECT {_MEMBERSHIP_COLUMNS} FROM watch_issue_memberships WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    return {
        "event": _membership_event_dict(row),
        "membership": {
            "source_thread_id": thread_id,
            "issue_id": new_issue,
            "home_issue_id": home,
        },
    }


def _require_linkable(
    conn: sqlite3.Connection, request: _IssueRequest, *, current: str, home: str
) -> None:
    details = {
        "watch_id": request.watch_id,
        "issue_id": request.issue_id,
        "source_thread_id": request.source_thread_id,
        "current_issue_id": current,
    }
    if current == request.issue_id:
        raise _issue_refusal("thread is already a member of the issue", "already_member", **details)
    target_home = _issue_row(conn, request.watch_id, request.issue_id).home_thread_id
    if _current_issue(conn, request.watch_id, target_home) != request.issue_id:
        raise _issue_refusal("issue is not active", "inactive_issue", **details)
    if current != home:
        return
    others = sorted(
        str(member["source_thread_id"])
        for member in _issue_members(conn, request.watch_id, home)
        if member["source_thread_id"] != request.source_thread_id
    )
    if others:
        raise _issue_refusal(
            "thread's own issue has other members",
            "issue_has_other_members",
            member_thread_ids=others,
            **details,
        )


def _require_unlinkable(request: _IssueRequest, *, current: str, home: str) -> None:
    details = {
        "watch_id": request.watch_id,
        "issue_id": request.issue_id,
        "source_thread_id": request.source_thread_id,
        "current_issue_id": current,
    }
    if current != request.issue_id:
        raise _issue_refusal("thread is not a member of the issue", "not_member", **details)
    if current == home:
        raise _issue_refusal("thread is in its own issue", "not_linked", **details)


# Read views ----------------------------------------------------------------------------


def _issue_state(
    conn: sqlite3.Connection, watch_id: str, issue_id: str, home_thread_id: str
) -> str:
    return "active" if _current_issue(conn, watch_id, home_thread_id) == issue_id else "inactive"


def _issue_members(conn: sqlite3.Connection, watch_id: str, issue_id: str) -> list[dict[str, Any]]:
    """Threads whose current issue is this one, ordered by the event that joined them."""
    members: list[dict[str, Any]] = []
    for thread_id, basis, event_id, joined_at, url, subreddit, title in conn.execute(
        _MEMBERS_SQL, (watch_id, issue_id)
    ).fetchall():
        if url is None:
            raise _issue_unreadable(
                "issue member thread is not tracked by the watch",
                watch_id=watch_id,
                source_thread_id=thread_id,
            )
        members.append(
            {
                "source_thread_id": thread_id,
                "thread_url": url,
                "subreddit": subreddit,
                "title": title,
                "basis": basis,
                "joined_event_id": event_id,
                "joined_at_utc": joined_at,
            }
        )
    return members


def _issue_counts(
    conn: sqlite3.Connection,
    watch_id: str,
    member_thread_ids: Sequence[str],
    projection: LineageProjection,
) -> dict[str, Any]:
    """Observation counts over every member report; lineage counts use effective lineage."""
    rows = conn.execute(
        "SELECT report_id, source_thread_id, community, author_sha256, copy_of_report_id "
        "FROM watch_issue_reports WHERE watch_id = ? "
        f"AND source_thread_id IN ({_placeholders(len(member_thread_ids))})",
        (watch_id, *member_thread_ids),
    ).fetchall()
    copies = sum(
        1
        for report_id, _thread, _community, _author, recorded_origin in rows
        if projection.effective_origin(str(report_id), recorded_origin) is not None
    )
    values = (
        len({row[1] for row in rows}),
        len({row[2] for row in rows if row[2] is not None}),
        len({row[3] for row in rows if row[3] is not None}),
        sum(1 for row in rows if row[3] is None),
        len(rows),
        len(rows) - copies,
        copies,
    )
    counts: dict[str, Any] = dict(zip(_COUNT_KEYS, values, strict=True))
    counts["independence"] = {
        "status": "not_verified",
        "verified_independent_reports": None,
        "basis": _INDEPENDENCE_BASIS,
    }
    return counts


def _copied_from(projection: LineageProjection, report_id: str, text_sha256: str) -> str | None:
    """The origin an update's cue was copied from: its report's correction proves that text."""
    correction = projection.correction_for(report_id)
    if correction is None or correction.text_sha256 != text_sha256:
        return None
    return correction.origin_report_id


def _update_counts(
    conn: sqlite3.Connection,
    watch_id: str,
    member_thread_ids: Sequence[str],
    projection: LineageProjection,
) -> dict[str, int]:
    """Update counts by kind, excluding cues copied from an origin that records them itself."""
    counts: dict[str, int] = dict.fromkeys(UPDATE_KINDS, 0)
    for kind, report_id, text_sha256 in conn.execute(
        "SELECT kind, report_id, text_sha256 FROM watch_issue_updates WHERE watch_id = ? "
        f"AND source_thread_id IN ({_placeholders(len(member_thread_ids))})",
        (watch_id, *member_thread_ids),
    ).fetchall():
        if kind not in counts:
            raise _issue_unreadable("stored update kind is not recognized", kind=kind)
        if _copied_from(projection, str(report_id), str(text_sha256)) is None:
            counts[kind] += 1
    return counts


def _member_suggestions(
    conn: sqlite3.Connection, watch_id: str, member_thread_ids: Sequence[str]
) -> list[dict[str, Any]]:
    """Suggestions whose source or candidate thread is a current member."""
    marks = _placeholders(len(member_thread_ids))
    rows = conn.execute(
        "SELECT suggestion_id, source_thread_id, candidate_thread_id, status, refusal_reason, "
        "signals_json, policy_sha256, created_at_utc FROM watch_issue_suggestions "
        f"WHERE watch_id = ? AND (source_thread_id IN ({marks}) "
        f"OR candidate_thread_id IN ({marks})) "
        "ORDER BY created_at_utc ASC, suggestion_id ASC",
        (watch_id, *member_thread_ids, *member_thread_ids),
    ).fetchall()
    suggestions: list[dict[str, Any]] = []
    for suggestion_id, source, candidate, status, refusal, signals_json, policy, created in rows:
        signals = _stored_issue_json(str(signals_json), suggestion_id=suggestion_id)
        if not isinstance(signals, dict):
            raise _issue_unreadable(
                "suggestion signals are not an object", suggestion_id=suggestion_id
            )
        source_issue = _current_issue(conn, watch_id, str(source))
        suggestions.append(
            {
                "suggestion_id": suggestion_id,
                "source_thread_id": source,
                "candidate_thread_id": candidate,
                "status": status,
                "refusal_reason": refusal,
                "signals": signals,
                "policy_sha256": policy,
                "created_at_utc": created,
                "currently_grouped": source_issue is not None
                and source_issue == _current_issue(conn, watch_id, str(candidate)),
            }
        )
    return suggestions


def _issue_summary(conn: sqlite3.Connection, watch_id: str, issue_id: str) -> dict[str, Any]:
    """One issue as a list item: state, members, counts and open suggestions."""
    return _summarize_issue(conn, watch_id, issue_id, _load_lineage_projection(conn, watch_id))


def _summarize_issue(
    conn: sqlite3.Connection, watch_id: str, issue_id: str, projection: LineageProjection
) -> dict[str, Any]:
    issue = _issue_row(conn, watch_id, issue_id)
    member_ids = [
        str(member["source_thread_id"]) for member in _issue_members(conn, watch_id, issue_id)
    ]
    suggestions = _member_suggestions(conn, watch_id, member_ids)
    return {
        "issue_id": issue_id,
        "home_thread_id": issue.home_thread_id,
        "state": _issue_state(conn, watch_id, issue_id, issue.home_thread_id),
        "member_thread_ids": sorted(member_ids),
        "counts": _issue_counts(conn, watch_id, member_ids, projection),
        "update_counts": _update_counts(conn, watch_id, member_ids, projection),
        "unconfirmed_suggestions": sum(
            1
            for suggestion in suggestions
            if suggestion["status"] == "unconfirmed" and not suggestion["currently_grouped"]
        ),
    }


def _list_issues(conn: sqlite3.Connection, watch_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT issue_id FROM watch_issues WHERE watch_id = ? ORDER BY created_sequence ASC",
        (watch_id,),
    ).fetchall()
    projection = _load_lineage_projection(conn, watch_id)
    return [_summarize_issue(conn, watch_id, str(row[0]), projection) for row in rows]


def _inspect_issue(conn: sqlite3.Connection, watch_id: str, issue_id: str) -> dict[str, Any]:
    issue = _issue_row(conn, watch_id, issue_id)
    members = _issue_members(conn, watch_id, issue_id)
    member_ids = [str(member["source_thread_id"]) for member in members]
    projection = _load_lineage_projection(conn, watch_id)
    update_counts = _update_counts(conn, watch_id, member_ids, projection)
    return {
        "issue_id": issue_id,
        "watch_id": watch_id,
        "home_thread_id": issue.home_thread_id,
        "state": _issue_state(conn, watch_id, issue_id, issue.home_thread_id),
        "status": "open",
        "resolution": {
            "resolved": False,
            "resolution_claims": update_counts["resolution_claim"],
            "basis": _RESOLUTION_BASIS,
        },
        "members": members,
        "counts": _issue_counts(conn, watch_id, member_ids, projection),
        "update_counts": update_counts,
        "reports": _projected_member_reports(conn, watch_id, member_ids, projection),
        "updates": _member_updates(conn, watch_id, member_ids, projection),
        "membership_history": _membership_history(conn, watch_id, issue_id),
        "suggestions": _member_suggestions(conn, watch_id, member_ids),
        "grouping_policy": _grouping_policy(conn, issue.policy_sha256),
    }


def _member_reports(
    conn: sqlite3.Connection, watch_id: str, member_thread_ids: Sequence[str]
) -> list[dict[str, Any]]:
    projection = _load_lineage_projection(conn, watch_id)
    return _projected_member_reports(conn, watch_id, member_thread_ids, projection)


def _projected_member_reports(
    conn: sqlite3.Connection,
    watch_id: str,
    member_thread_ids: Sequence[str],
    projection: LineageProjection,
) -> list[dict[str, Any]]:
    """Member reports with effective lineage, the recorded lineage and its latest correction."""
    marks = _placeholders(len(member_thread_ids))
    evidence: dict[str, list[dict[str, Any]]] = {}
    for report_id, entry_id, text_sha256, revision_id, snapshot_sha256, links_json in conn.execute(
        "SELECT ev.report_id, ev.entry_id, ev.text_sha256, ev.revision_id, ev.snapshot_sha256, "
        "ev.links_json FROM watch_issue_evidence AS ev "
        "JOIN watch_issue_reports AS r ON r.report_id = ev.report_id "
        f"WHERE r.watch_id = ? AND r.source_thread_id IN ({marks}) "
        "ORDER BY ev.recorded_at_utc ASC, ev.entry_id ASC",
        (watch_id, *member_thread_ids),
    ).fetchall():
        evidence.setdefault(str(report_id), []).append(
            {
                "entry_id": entry_id,
                "text_sha256": text_sha256,
                "revision_id": revision_id,
                "snapshot_sha256": snapshot_sha256,
                "links": _stored_str_list(str(links_json), report_id=report_id, entry_id=entry_id),
            }
        )
    reports: list[dict[str, Any]] = []
    for row in conn.execute(
        f"SELECT {', '.join(_REPORT_KEYS)} FROM watch_issue_reports "
        f"WHERE watch_id = ? AND source_thread_id IN ({marks}) ORDER BY record_sequence ASC",
        (watch_id, *member_thread_ids),
    ).fetchall():
        report: dict[str, Any] = dict(zip(_REPORT_KEYS, row, strict=True))
        report_key = str(report["report_id"])
        recorded_lineage = report["lineage"]
        recorded_origin = report["copy_of_report_id"]
        origin = projection.effective_origin(report_key, recorded_origin)
        correction = projection.correction_for(report_key)
        report["lineage"] = "firsthand" if origin is None else "exact_copy"
        report["copy_of_report_id"] = origin
        report["recorded_lineage"] = recorded_lineage
        report["recorded_copy_of_report_id"] = recorded_origin
        report["lineage_correction"] = None if correction is None else correction.to_dict()
        report_evidence = evidence.get(report_key)
        if report_evidence is None:
            raise _issue_unreadable("issue report has no evidence", report_id=report["report_id"])
        report["evidence"] = report_evidence
        reports.append(report)
    return reports


def _member_updates(
    conn: sqlite3.Connection,
    watch_id: str,
    member_thread_ids: Sequence[str],
    projection: LineageProjection,
) -> list[dict[str, Any]]:
    """Every recorded update; a cue copied from an origin names that origin, never hidden."""
    rows = conn.execute(
        f"SELECT {_UPDATE_SELECT} FROM watch_issue_updates AS u "
        "JOIN watch_issue_reports AS r ON r.report_id = u.report_id "
        f"WHERE u.watch_id = ? AND u.source_thread_id IN ({_placeholders(len(member_thread_ids))}) "
        "ORDER BY u.recorded_at_utc ASC, u.update_id ASC",
        (watch_id, *member_thread_ids),
    ).fetchall()
    updates: list[dict[str, Any]] = []
    for row in rows:
        update: dict[str, Any] = dict(zip(_UPDATE_KEYS, row, strict=True))
        update["copied_from_report_id"] = _copied_from(
            projection, str(update["report_id"]), str(update["text_sha256"])
        )
        updates.append(update)
    return updates


def _membership_history(
    conn: sqlite3.Connection, watch_id: str, issue_id: str
) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"SELECT {_MEMBERSHIP_COLUMNS} FROM watch_issue_memberships "
        "WHERE watch_id = ? AND (issue_id = ? OR previous_issue_id = ?) ORDER BY event_id ASC",
        (watch_id, issue_id, issue_id),
    ).fetchall()
    return [_membership_event_dict(row) for row in rows]


def _grouping_policy(conn: sqlite3.Connection, policy_sha256: str) -> dict[str, Any]:
    row = conn.execute(
        "SELECT policy_json FROM watch_issue_policies WHERE policy_sha256 = ?", (policy_sha256,)
    ).fetchone()
    if row is None:
        raise _issue_unreadable("issue grouping policy is missing", policy_sha256=policy_sha256)
    policy = _stored_issue_json(str(row[0]), policy_sha256=policy_sha256)
    if not isinstance(policy, dict):
        raise _issue_unreadable(
            "issue grouping policy is not an object", policy_sha256=policy_sha256
        )
    return {"sha256": policy_sha256, **policy}


# Digest issue references -----------------------------------------------------------------

_ENTRY_REPORT_KEYS = ("report_id", "comment_id", "lineage", "copy_of_report_id", "text_sha256")
_ENTRY_UPDATE_KEYS = ("update_id", "kind", "cue", "report_id", "comment_id")


def _digest_issue_view(
    conn: sqlite3.Connection, watch_id: str, entries: Sequence[dict[str, Any]]
) -> dict[str, Any]:
    """Issue references of one new digest, recorded with it so a resume renders the same bytes.

    Entries follow digest order and appear only when their thread has issue history; each
    referenced current issue is summarized once, in first-reference order.
    """
    entry_refs: list[dict[str, Any]] = []
    issue_ids: list[str] = []
    # Each thread's reports are read once per digest, however many of its entries it presents.
    thread_reports: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        entry_id = str(entry["entry_id"])
        source_thread_id, kind = _entry_identity(conn, watch_id, entry_id)
        issue_id = _current_issue(conn, watch_id, source_thread_id)
        if issue_id is None:
            continue
        if source_thread_id not in thread_reports:
            thread_reports[source_thread_id] = _member_reports(conn, watch_id, [source_thread_id])
        entry_refs.append(
            {
                "entry_id": entry_id,
                "kind": kind,
                "source_thread_id": source_thread_id,
                "issue_id": issue_id,
                "home_issue_id": issue_id_for(watch_id, source_thread_id),
                "reports": _entry_reports(entry_id, thread_reports[source_thread_id]),
                "updates": _entry_updates(
                    conn, watch_id, entry_id, thread_reports[source_thread_id]
                ),
            }
        )
        if issue_id not in issue_ids:
            issue_ids.append(issue_id)
    return {
        "semantic_grouping": "not_eligible",
        "entry_refs": entry_refs,
        "issues": [_digest_issue(conn, watch_id, issue_id) for issue_id in issue_ids],
    }


def _entry_identity(conn: sqlite3.Connection, watch_id: str, entry_id: str) -> tuple[str, str]:
    """The stored thread and kind of a digest entry; its payload is not trusted for identity."""
    row = conn.execute(
        "SELECT source_thread_id, kind FROM watch_digest_entries "
        "WHERE watch_id = ? AND entry_id = ?",
        (watch_id, entry_id),
    ).fetchone()
    if row is None:
        raise _issue_unreadable("digest entry is not recorded", entry_id=entry_id)
    return str(row[0]), str(row[1])


def _digest_issue(conn: sqlite3.Connection, watch_id: str, issue_id: str) -> dict[str, Any]:
    summary = _issue_summary(conn, watch_id, issue_id)
    suggestions = _member_suggestions(conn, watch_id, summary["member_thread_ids"])
    return {**summary, "suggestions": suggestions}


def _entry_reports(entry_id: str, thread_reports: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """This entry's evidence rows, with the lineage `_member_reports` gives each report."""
    pairs = [
        (report, evidence)
        for report in thread_reports
        for evidence in report["evidence"]
        if evidence["entry_id"] == entry_id
    ]
    pairs.sort(key=lambda pair: (pair[0]["comment_id"], pair[0]["report_id"]))
    return [
        {key: evidence[key] if key == "text_sha256" else report[key] for key in _ENTRY_REPORT_KEYS}
        for report, evidence in pairs
    ]


def _entry_updates(
    conn: sqlite3.Connection,
    watch_id: str,
    entry_id: str,
    thread_reports: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Keep entry cues from original or edited-added text, not a copy's first text."""
    rows = conn.execute(
        "SELECT u.update_id, u.kind, u.cue, u.report_id, r.comment_id, u.text_sha256 "
        "FROM watch_issue_updates AS u "
        "JOIN watch_issue_reports AS r ON r.report_id = u.report_id "
        "WHERE u.watch_id = ? AND u.entry_id = ? ORDER BY u.update_id ASC",
        (watch_id, entry_id),
    ).fetchall()
    reports = {str(report["report_id"]): report for report in thread_reports}
    updates: list[dict[str, Any]] = []
    for row in rows:
        report_id = str(row[3])
        report = reports.get(report_id)
        if report is None:
            raise _issue_unreadable("stored update has no member report", report_id=report_id)
        if report["lineage"] == "exact_copy" and row[5] == report["first_text_sha256"]:
            continue
        updates.append(dict(zip(_ENTRY_UPDATE_KEYS, row[:5], strict=True)))
    return updates
