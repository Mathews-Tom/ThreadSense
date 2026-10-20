"""Record issue evidence: report lineage, initial memberships and bounded suggestions.

An origin discovered after its copy is recorded as a ledger correction event in the ingestion
summary; recorded reports and memberships never change.

Owned by `MonitoringStore`: `_record_issue_evidence` runs inside a transaction the store has
already fenced against stale run owners. Every SQL value is a bound parameter; f-strings only
splice constant column lists and placeholder runs defined in these modules.
"""

from __future__ import annotations

import json
import math
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from threadsense.errors import MonitoringInputError
from threadsense.monitoring.digest_records import _ENTRY_ID_PATTERN
from threadsense.monitoring.issue_corrections import (
    _SHA256_PATTERN,
    LineageCorrection,
    LineageProjection,
    OriginMatch,
    _effective_origin_match,
    _late_origin_corrections,
    _lineage_events_json,
    _load_lineage_projection,
)
from threadsense.monitoring.issue_records import (
    LINEAGE_MIN_CHARS,
    SHARED_LINK_REFUSAL,
    UPDATE_CUES,
    UPDATE_KINDS,
    IssueEntryInput,
    IssueIngestion,
    IssuePolicy,
    IssueReportInput,
    MembershipBasis,
    PendingIssueEntry,
    _current_issue,
    _insert_membership,
    _issue_refusal,
    _issue_unreadable,
    _matches,
    _MembershipEvent,
    _stored_str_list,
    issue_id_for,
    report_id_for,
    suggestion_id_for,
    update_id_for,
)
from threadsense.monitoring.sweep_records import _SOURCE_THREAD_ID_PATTERN, _canonical_json

if TYPE_CHECKING:
    from threadsense.monitoring.state import RunClaim


# Store helpers. Every SQL value is a bound parameter; f-strings only splice the constant
# column lists and placeholder runs defined in this module.

_SECONDS_PER_DAY = 86400.0
_SEED_REASON = "first finding evidence recorded for this thread"
# The issue's home thread's latest membership event, correlated with `watch_issues AS i`.
_HOME_CURRENT_ISSUE = (
    "(SELECT m.issue_id FROM watch_issue_memberships AS m "
    "WHERE m.watch_id = i.watch_id AND m.source_thread_id = i.home_thread_id "
    "ORDER BY m.event_id DESC LIMIT 1)"
)
_CANDIDATES_SQL = (
    "SELECT i.home_thread_id, i.anchor_utc FROM watch_issues AS i "
    "WHERE i.watch_id = ? AND i.created_sequence < ? AND i.anchor_utc BETWEEN ? AND ? "
    f"AND {_HOME_CURRENT_ISSUE} IS NOT ? "
    "ORDER BY ABS(i.anchor_utc - ?) ASC, i.home_thread_id ASC LIMIT ?"
)
_PENDING_ENTRIES_SQL = (
    "SELECT e.entry_id, e.source_thread_id, e.revision_id, e.theme_key, e.payload_json, "
    "r.snapshot_path, r.snapshot_sha256, a.analysis_path, a.analysis_sha256 "
    "FROM watch_digest_entries AS e "
    "JOIN watch_thread_revisions AS r "
    "ON r.revision_id = e.revision_id AND r.watch_id = e.watch_id "
    "JOIN watch_revision_analyses AS a ON a.revision_id = r.revision_id "
    "WHERE e.watch_id = ? AND e.kind = 'finding' AND NOT EXISTS "
    "(SELECT 1 FROM watch_issue_entries AS x WHERE x.entry_id = e.entry_id) "
    "ORDER BY e.created_at_utc ASC, e.entry_id ASC"
)


@dataclass(frozen=True)
class _IssueBatch:
    """Fixed context of one fenced ingestion transaction."""

    watch_id: str
    run_id: str
    policy: IssuePolicy
    policy_sha256: str
    now: float
    # Effective lineage of the watch; this batch's corrections advance it as they are found.
    lineage: LineageProjection


@dataclass
class _IssueTally:
    """Running counts of one ingestion batch, with the lineage corrections it appended."""

    lineage_corrections: list[LineageCorrection]
    threads_created: int = 0
    lineage_assignments: int = 0
    reports_recorded: int = 0
    exact_copies: int = 0
    evidence_recorded: int = 0
    updates_recorded: int = 0
    comparisons: int = 0
    suggestions: int = 0
    suggestions_truncated: int = 0
    refusals: int = 0


@dataclass(frozen=True)
class _ExactCopy:
    """A report recorded in this batch as an exact copy of an earlier firsthand report."""

    report_id: str
    origin_report_id: str
    origin_thread_id: str
    origin_sequence: int


@dataclass(frozen=True)
class _ThreadSignals:
    thread_id: str
    themes: frozenset[str]
    key_phrases: frozenset[str]
    links: frozenset[str]
    # (exact-copy report id, thread of the report it copies)
    copies: frozenset[tuple[str, str]]


@dataclass(frozen=True)
class _Comparison:
    candidate_thread_id: str
    themes: tuple[str, ...]
    key_phrases: tuple[str, ...]
    exact_copies: tuple[str, ...]
    links: tuple[str, ...]
    gap_seconds: float

    def qualifies(self) -> bool:
        return bool(self.themes or self.key_phrases or self.exact_copies)

    def signals(self) -> dict[str, Any]:
        return {
            "themes": list(self.themes),
            "key_phrases": list(self.key_phrases),
            "exact_copies": list(self.exact_copies),
            "links": list(self.links),
            "source_time_gap_seconds": self.gap_seconds,
        }

    def rank(self) -> tuple[int, float, str]:
        return (
            -(len(self.themes) + len(self.key_phrases) + len(self.exact_copies)),
            self.gap_seconds,
            self.candidate_thread_id,
        )


def _invalid_input(message: str, **details: Any) -> MonitoringInputError:
    return MonitoringInputError(message, details={"reason": "invalid_issue_input", **details})


def _is_str_tuple(value: object) -> bool:
    return isinstance(value, tuple) and all(isinstance(item, str) for item in value)


def _is_finite_number(value: object) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool) and math.isfinite(value)


def _valid_cues(value: object) -> bool:
    if not isinstance(value, tuple):
        return False
    for cue in value:
        if not isinstance(cue, tuple) or len(cue) != 2:
            return False
        kind, text = cue
        if kind not in UPDATE_KINDS or text not in UPDATE_CUES[kind]:
            return False
    return True


def _require_checks(checks: tuple[tuple[str, bool], ...], **details: Any) -> None:
    for field, valid in checks:
        if not valid:
            raise _invalid_input(f"issue input {field} is invalid", field=field, **details)


# Pending entries -----------------------------------------------------------------------


def _pending_issue_entries(
    conn: sqlite3.Connection, watch_id: str, root: Path
) -> list[PendingIssueEntry]:
    """Finding entries of the watch not yet in issue history, oldest first."""
    pending: list[PendingIssueEntry] = []
    for row in conn.execute(_PENDING_ENTRIES_SQL, (watch_id,)).fetchall():
        entry_id = str(row[0])
        theme_key = row[3]
        if not isinstance(theme_key, str):
            raise _issue_refusal(
                "finding entry has no theme key", "issue_entry_unreadable", entry_id=entry_id
            )
        pending.append(
            PendingIssueEntry(
                entry_id=entry_id,
                source_thread_id=str(row[1]),
                revision_id=str(row[2]),
                theme_key=theme_key,
                snapshot_path=root / str(row[5]),
                snapshot_sha256=str(row[6]),
                analysis_path=root / str(row[7]),
                analysis_sha256=str(row[8]),
                novel_evidence=_novel_evidence(entry_id, str(row[4])),
            )
        )
    return pending


def _novel_evidence(entry_id: str, payload_json: str) -> tuple[dict[str, Any], ...]:
    try:
        payload = json.loads(payload_json)
    except ValueError as error:
        raise _issue_refusal(
            "finding entry payload is not valid JSON", "issue_entry_unreadable", entry_id=entry_id
        ) from error
    evidence = payload.get("novel_evidence") if isinstance(payload, dict) else None
    if (
        not isinstance(evidence, list)
        or not evidence
        or not all(_is_cited_comment(item) for item in evidence)
    ):
        raise _issue_refusal(
            "finding entry novel evidence is malformed",
            "issue_entry_unreadable",
            entry_id=entry_id,
        )
    return tuple(dict(item) for item in evidence)


def _is_cited_comment(item: object) -> bool:
    return (
        isinstance(item, dict)
        and isinstance(item.get("comment_id"), str)
        and isinstance(item.get("text_sha256"), str)
    )


# Input validation ----------------------------------------------------------------------


def _validate_issue_batch(
    entries: Sequence[IssueEntryInput], policy: object
) -> tuple[IssueEntryInput, ...]:
    """Validate every ingestion input before any transaction opens."""
    if type(policy) is not IssuePolicy:
        raise _invalid_input("issue policy must be an IssuePolicy", field="policy")
    batch = tuple(entries)
    for entry in batch:
        _validate_issue_entry(entry)
    return batch


def _validate_issue_entry(entry: object) -> None:
    if not isinstance(entry, IssueEntryInput):
        raise _invalid_input("issue entry must be an IssueEntryInput", field="entry")
    _require_checks(
        (
            ("entry_id", _matches(_ENTRY_ID_PATTERN, entry.entry_id)),
            ("source_thread_id", _matches(_SOURCE_THREAD_ID_PATTERN, entry.source_thread_id)),
            ("revision_id", isinstance(entry.revision_id, str)),
            ("snapshot_sha256", isinstance(entry.snapshot_sha256, str)),
            ("theme_key", isinstance(entry.theme_key, str)),
            ("key_phrases", _is_str_tuple(entry.key_phrases)),
            ("reports", isinstance(entry.reports, tuple) and len(entry.reports) > 0),
        ),
        entry_id=entry.entry_id if isinstance(entry.entry_id, str) else None,
    )
    comment_ids = [_validate_issue_report(entry.entry_id, report) for report in entry.reports]
    if len(set(comment_ids)) != len(comment_ids):
        raise _invalid_input(
            "issue entry cites a comment more than once", field="reports", entry_id=entry.entry_id
        )


def _validate_issue_report(entry_id: str, report: object) -> str:
    if not isinstance(report, IssueReportInput):
        raise _invalid_input(
            "issue report must be an IssueReportInput", field="reports", entry_id=entry_id
        )
    comment_id = report.comment_id
    _require_checks(
        (
            ("comment_id", isinstance(comment_id, str) and comment_id != ""),
            ("text_sha256", _matches(_SHA256_PATTERN, report.text_sha256)),
            (
                "content_chars",
                type(report.content_chars) is int and report.content_chars >= 0,
            ),
            (
                "author_sha256",
                report.author_sha256 is None or isinstance(report.author_sha256, str),
            ),
            ("community", isinstance(report.community, str)),
            ("permalink", isinstance(report.permalink, str)),
            ("source_created_utc", _is_finite_number(report.source_created_utc)),
            ("links", _is_str_tuple(report.links)),
            ("cues", _valid_cues(report.cues)),
        ),
        entry_id=entry_id,
        comment_id=comment_id if isinstance(comment_id, str) else None,
    )
    return comment_id


# Ingestion -----------------------------------------------------------------------------


def _empty_issue_ingestion(run_id: str) -> IssueIngestion:
    return IssueIngestion(
        run_id=run_id,
        policy_sha256=None,
        entries=0,
        threads_created=0,
        lineage_assignments=0,
        reports_recorded=0,
        exact_copies=0,
        evidence_recorded=0,
        updates_recorded=0,
        comparisons=0,
        suggestions=0,
        suggestions_truncated=0,
        refusals=0,
    )


def _record_issue_evidence(
    conn: sqlite3.Connection,
    claim: RunClaim,
    entries: tuple[IssueEntryInput, ...],
    *,
    policy: IssuePolicy,
    now: float,
) -> IssueIngestion:
    """Record a validated, non-empty batch inside the caller's fenced transaction."""
    batch = _IssueBatch(
        watch_id=claim.watch_id,
        run_id=claim.run_id,
        policy=policy,
        policy_sha256=policy.sha256(),
        now=now,
        lineage=_load_lineage_projection(conn, claim.watch_id),
    )
    known_policy = conn.execute(
        "SELECT 1 FROM watch_issue_policies WHERE policy_sha256 = ?", (batch.policy_sha256,)
    ).fetchone()
    if known_policy is None:
        conn.execute(
            "INSERT INTO watch_issue_policies (policy_sha256, policy_json, created_at_utc) "
            "VALUES (?, ?, ?)",
            (batch.policy_sha256, policy.canonical_json(), now),
        )
    tally = _IssueTally(lineage_corrections=[])
    created: list[str] = []
    recorded: dict[str, int] = {}
    copies: dict[str, list[_ExactCopy]] = {}
    # Earliest-posted first, so an original swept in the same batch as its repost is recorded
    # (and created, and assigned) before the copy looks for its origin.
    for entry in sorted(entries, key=_processing_key):
        _require_new_issue_entry(conn, batch.watch_id, entry)
        if _insert_home_issue(conn, batch, entry):
            created.append(entry.source_thread_id)
        _insert_issue_entry(conn, batch, entry)
        for report in entry.reports:
            is_new, copy = _record_report(conn, batch, entry, report, tally=tally)
            if is_new:
                recorded[entry.source_thread_id] = recorded.get(entry.source_thread_id, 0) + 1
            if copy is not None:
                copies.setdefault(entry.source_thread_id, []).append(copy)
    # Creation order: every earlier-created thread is assigned before a later one compares.
    for thread_id in created:
        _assign_new_thread(
            conn,
            batch,
            thread_id,
            copies=_pure_repost_copies(recorded.get(thread_id, 0), copies.get(thread_id, [])),
            tally=tally,
        )
        _compare_candidates(conn, batch, thread_id, tally)
    result = IssueIngestion(
        run_id=batch.run_id,
        policy_sha256=batch.policy_sha256,
        entries=len(entries),
        threads_created=len(created),
        lineage_assignments=tally.lineage_assignments,
        reports_recorded=tally.reports_recorded,
        exact_copies=tally.exact_copies,
        evidence_recorded=tally.evidence_recorded,
        updates_recorded=tally.updates_recorded,
        comparisons=tally.comparisons,
        suggestions=tally.suggestions,
        suggestions_truncated=tally.suggestions_truncated,
        refusals=tally.refusals,
        lineage_corrections=len(tally.lineage_corrections),
    )
    _insert_ingestion(conn, batch, result, tally.lineage_corrections)
    return result


def _processing_key(entry: IssueEntryInput) -> tuple[float, str]:
    return (float(min(report.source_created_utc for report in entry.reports)), entry.entry_id)


def _pure_repost_copies(recorded: int, copies: list[_ExactCopy]) -> list[_ExactCopy]:
    """The thread's copies when every report it recorded is an exact copy; else none."""
    if recorded == 0 or len(copies) != recorded:
        return []
    return copies


def _require_new_issue_entry(
    conn: sqlite3.Connection, watch_id: str, entry: IssueEntryInput
) -> None:
    # The cited snapshot must be the one the store recorded for the entry's revision.
    row = conn.execute(
        "SELECT e.source_thread_id, e.revision_id, e.theme_key, r.snapshot_sha256 "
        "FROM watch_digest_entries AS e "
        "JOIN watch_thread_revisions AS r "
        "ON r.revision_id = e.revision_id AND r.watch_id = e.watch_id "
        "WHERE e.entry_id = ? AND e.watch_id = ? AND e.kind = 'finding'",
        (entry.entry_id, watch_id),
    ).fetchone()
    expected = (entry.source_thread_id, entry.revision_id, entry.theme_key, entry.snapshot_sha256)
    if row is None or tuple(row) != expected:
        raise _issue_refusal(
            "issue entry does not match a recorded finding entry of this watch",
            "unknown_issue_entry",
            watch_id=watch_id,
            entry_id=entry.entry_id,
        )
    recorded = conn.execute(
        "SELECT 1 FROM watch_issue_entries WHERE entry_id = ?", (entry.entry_id,)
    ).fetchone()
    if recorded is not None:
        raise _issue_refusal(
            "issue entry is already recorded",
            "issue_entry_already_recorded",
            watch_id=watch_id,
            entry_id=entry.entry_id,
        )


def _insert_home_issue(
    conn: sqlite3.Connection, batch: _IssueBatch, entry: IssueEntryInput
) -> bool:
    """Create the thread's home issue on its first finding evidence; False when it exists."""
    thread_id = entry.source_thread_id
    existing = conn.execute(
        "SELECT 1 FROM watch_issues WHERE watch_id = ? AND home_thread_id = ?",
        (batch.watch_id, thread_id),
    ).fetchone()
    if existing is not None:
        return False
    sequence = conn.execute(
        "SELECT COALESCE(MAX(created_sequence), 0) + 1 FROM watch_issues WHERE watch_id = ?",
        (batch.watch_id,),
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO watch_issues (issue_id, watch_id, home_thread_id, created_sequence, "
        "anchor_utc, created_at_utc, created_run_id, policy_sha256) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            issue_id_for(batch.watch_id, thread_id),
            batch.watch_id,
            thread_id,
            int(sequence),
            float(min(report.source_created_utc for report in entry.reports)),
            batch.now,
            batch.run_id,
            batch.policy_sha256,
        ),
    )
    return True


def _insert_issue_entry(
    conn: sqlite3.Connection, batch: _IssueBatch, entry: IssueEntryInput
) -> None:
    conn.execute(
        "INSERT INTO watch_issue_entries (entry_id, watch_id, source_thread_id, theme_key, "
        "key_phrases_json, ingested_at_utc, run_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            entry.entry_id,
            batch.watch_id,
            entry.source_thread_id,
            entry.theme_key,
            _canonical_json(list(entry.key_phrases), "key_phrases"),
            batch.now,
            batch.run_id,
        ),
    )


def _record_report(
    conn: sqlite3.Connection,
    batch: _IssueBatch,
    entry: IssueEntryInput,
    report: IssueReportInput,
    *,
    tally: _IssueTally,
) -> tuple[bool, _ExactCopy | None]:
    """Record one cited comment's evidence.

    Returns whether the report is new and, when a new report is an exact copy, the copy.
    """
    report_id = report_id_for(batch.watch_id, entry.source_thread_id, report.comment_id)
    stored = conn.execute(
        "SELECT copy_of_report_id FROM watch_issue_reports WHERE report_id = ?", (report_id,)
    ).fetchone()
    copy: _ExactCopy | None = None
    origin_report_id: str | None
    if stored is None:
        copy = _insert_report(conn, batch, entry, report)
        # The new report may be the earlier-posted origin of reports already recorded.
        tally.lineage_corrections.extend(
            _late_origin_corrections(
                conn,
                batch.lineage,
                discovered_report_id=report_id,
                content_chars=report.content_chars,
                discovery_entry_id=entry.entry_id,
                run_id=batch.run_id,
                policy_sha256=batch.policy_sha256,
            )
        )
        origin_report_id = None if copy is None else copy.origin_report_id
        tally.reports_recorded += 1
        tally.exact_copies += 0 if copy is None else 1
    else:
        recorded_origin = None if stored[0] is None else str(stored[0])
        origin_report_id = batch.lineage.effective_origin(report_id, recorded_origin)
    # A copy's cues belong to its origin unless the reposter added text the origin never had.
    records_cues = origin_report_id is None or (
        conn.execute(
            "SELECT 1 FROM watch_issue_evidence WHERE report_id = ? AND text_sha256 = ?",
            (origin_report_id, report.text_sha256),
        ).fetchone()
        is None
    )
    conn.execute(
        "INSERT INTO watch_issue_evidence (report_id, entry_id, watch_id, text_sha256, "
        "revision_id, snapshot_sha256, links_json, recorded_at_utc) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            report_id,
            entry.entry_id,
            batch.watch_id,
            report.text_sha256,
            entry.revision_id,
            entry.snapshot_sha256,
            _canonical_json(sorted(set(report.links)), "links"),
            batch.now,
        ),
    )
    tally.evidence_recorded += 1
    if records_cues:
        tally.updates_recorded += _insert_updates(
            conn, batch, report_id=report_id, entry=entry, report=report
        )
    return stored is None, copy


def _insert_report(
    conn: sqlite3.Connection,
    batch: _IssueBatch,
    entry: IssueEntryInput,
    report: IssueReportInput,
) -> _ExactCopy | None:
    """Insert a new report with its lineage fixed now; earlier rows of this batch are visible."""
    report_id = report_id_for(batch.watch_id, entry.source_thread_id, report.comment_id)
    origin: OriginMatch | None = None
    if report.content_chars >= LINEAGE_MIN_CHARS:
        origin = _effective_origin_match(
            conn,
            batch.lineage,
            report_id=report_id,
            text_sha256=report.text_sha256,
            source_created_utc=float(report.source_created_utc),
        )
    sequence = conn.execute(
        "SELECT COALESCE(MAX(record_sequence), 0) + 1 FROM watch_issue_reports WHERE watch_id = ?",
        (batch.watch_id,),
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO watch_issue_reports (report_id, watch_id, source_thread_id, comment_id, "
        "record_sequence, first_text_sha256, author_sha256, community, permalink, "
        "source_created_utc, lineage, copy_of_report_id, first_entry_id, recorded_at_utc, "
        "run_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            report_id,
            batch.watch_id,
            entry.source_thread_id,
            report.comment_id,
            int(sequence),
            report.text_sha256,
            report.author_sha256,
            report.community,
            report.permalink,
            float(report.source_created_utc),
            "firsthand" if origin is None else "exact_copy",
            None if origin is None else origin.report_id,
            entry.entry_id,
            batch.now,
            batch.run_id,
        ),
    )
    if origin is None:
        return None
    return _ExactCopy(
        report_id=report_id,
        origin_report_id=origin.report_id,
        origin_thread_id=origin.source_thread_id,
        origin_sequence=origin.record_sequence,
    )


def _insert_updates(
    conn: sqlite3.Connection,
    batch: _IssueBatch,
    *,
    report_id: str,
    entry: IssueEntryInput,
    report: IssueReportInput,
) -> int:
    """Record each cue of a report once per (report, text, kind)."""
    inserted = 0
    for kind, cue in report.cues:
        existing = conn.execute(
            "SELECT 1 FROM watch_issue_updates "
            "WHERE report_id = ? AND text_sha256 = ? AND kind = ?",
            (report_id, report.text_sha256, kind),
        ).fetchone()
        if existing is not None:
            continue
        conn.execute(
            "INSERT INTO watch_issue_updates (update_id, watch_id, source_thread_id, report_id, "
            "entry_id, text_sha256, kind, cue, recorded_at_utc, run_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                update_id_for(report_id, report.text_sha256, kind),
                batch.watch_id,
                entry.source_thread_id,
                report_id,
                entry.entry_id,
                report.text_sha256,
                kind,
                cue,
                batch.now,
                batch.run_id,
            ),
        )
        inserted += 1
    return inserted


def _assign_new_thread(
    conn: sqlite3.Connection,
    batch: _IssueBatch,
    thread_id: str,
    *,
    copies: list[_ExactCopy],
    tally: _IssueTally,
) -> None:
    """Write a new thread's initial membership: exact-content lineage, else its own issue."""
    lineage = _lineage_origin(conn, batch.watch_id, thread_id, copies)
    issue_id = issue_id_for(batch.watch_id, thread_id)
    basis: MembershipBasis = "thread_seed"
    reason = _SEED_REASON
    report_id: str | None = None
    if lineage is not None:
        copy, issue_id = lineage
        basis = "exact_content_lineage"
        reason = (
            f"report {copy.report_id} is an exact copy of report {copy.origin_report_id} "
            f"in thread {copy.origin_thread_id}"
        )
        report_id = copy.report_id
        tally.lineage_assignments += 1
    _insert_membership(
        conn,
        _MembershipEvent(
            watch_id=batch.watch_id,
            source_thread_id=thread_id,
            issue_id=issue_id,
            previous_issue_id=None,
            action="assigned",
            basis=basis,
            reason=reason,
            report_id=report_id,
            policy_sha256=batch.policy_sha256,
            run_id=batch.run_id,
            created_at_utc=batch.now,
        ),
    )


def _lineage_origin(
    conn: sqlite3.Connection, watch_id: str, thread_id: str, copies: list[_ExactCopy]
) -> tuple[_ExactCopy, str] | None:
    """For a pure repost, the copy whose origin is earliest, with that origin thread's issue.

    Lineage applies only when every copy's origin is in another thread that already has a
    membership; any other copy makes the thread seed its own issue.
    """
    if not copies or any(copy.origin_thread_id == thread_id for copy in copies):
        return None
    origin_issues: dict[str, str] = {}
    for copy in copies:
        origin_issue = _current_issue(conn, watch_id, copy.origin_thread_id)
        if origin_issue is None:
            return None
        origin_issues[copy.origin_thread_id] = origin_issue
    earliest = min(copies, key=lambda candidate: candidate.origin_sequence)
    return earliest, origin_issues[earliest.origin_thread_id]


def _compare_candidates(
    conn: sqlite3.Connection, batch: _IssueBatch, thread_id: str, tally: _IssueTally
) -> None:
    """Run the single bounded candidate query for a new thread and record its suggestions."""
    issue = conn.execute(
        "SELECT created_sequence, anchor_utc FROM watch_issues "
        "WHERE watch_id = ? AND home_thread_id = ?",
        (batch.watch_id, thread_id),
    ).fetchone()
    anchor = float(issue[1])
    window = batch.policy.candidate_window_days * _SECONDS_PER_DAY
    rows = conn.execute(
        _CANDIDATES_SQL,
        (
            batch.watch_id,
            int(issue[0]),
            anchor - window,
            anchor + window,
            _current_issue(conn, batch.watch_id, thread_id),
            anchor,
            batch.policy.max_candidate_comparisons,
        ),
    ).fetchall()
    tally.comparisons += len(rows)
    own = _thread_signals(conn, batch, thread_id)
    unconfirmed: list[_Comparison] = []
    refused: list[_Comparison] = []
    for candidate_thread_id, candidate_anchor in rows:
        comparison = _compare(
            own,
            _thread_signals(conn, batch, str(candidate_thread_id)),
            candidate_thread_id=str(candidate_thread_id),
            gap_seconds=abs(float(candidate_anchor) - anchor),
        )
        if comparison.qualifies():
            unconfirmed.append(comparison)
        elif comparison.links:
            refused.append(comparison)
    kept = sorted(unconfirmed, key=_Comparison.rank)[: batch.policy.max_suggestions_per_thread]
    tally.suggestions_truncated += len(unconfirmed) - len(kept)
    for comparison in kept:
        _insert_suggestion(conn, batch, thread_id, comparison, refusal=None)
    for comparison in refused:
        _insert_suggestion(conn, batch, thread_id, comparison, refusal=SHARED_LINK_REFUSAL)
    tally.suggestions += len(kept)
    tally.refusals += len(refused)


def _compare(
    own: _ThreadSignals,
    other: _ThreadSignals,
    *,
    candidate_thread_id: str,
    gap_seconds: float,
) -> _Comparison:
    return _Comparison(
        candidate_thread_id=candidate_thread_id,
        themes=tuple(sorted(own.themes & other.themes)),
        key_phrases=tuple(sorted(own.key_phrases & other.key_phrases)),
        exact_copies=tuple(
            sorted(
                {report_id for report_id, origin in own.copies if origin == other.thread_id}
                | {report_id for report_id, origin in other.copies if origin == own.thread_id}
            )
        ),
        links=tuple(sorted(own.links & other.links)),
        gap_seconds=gap_seconds,
    )


def _thread_signals(conn: sqlite3.Connection, batch: _IssueBatch, thread_id: str) -> _ThreadSignals:
    watch_id = batch.watch_id
    themes: set[str] = set()
    phrases: set[str] = set()
    for entry_id, theme_key, phrases_json in conn.execute(
        "SELECT entry_id, theme_key, key_phrases_json FROM watch_issue_entries "
        "WHERE watch_id = ? AND source_thread_id = ?",
        (watch_id, thread_id),
    ).fetchall():
        themes.add(str(theme_key))
        phrases.update(_stored_str_list(str(phrases_json), entry_id=entry_id))
    links: set[str] = set()
    for report_id, entry_id, links_json in conn.execute(
        "SELECT ev.report_id, ev.entry_id, ev.links_json FROM watch_issue_evidence AS ev "
        "JOIN watch_issue_reports AS r ON r.report_id = ev.report_id "
        "WHERE r.watch_id = ? AND r.source_thread_id = ?",
        (watch_id, thread_id),
    ).fetchall():
        links.update(_stored_str_list(str(links_json), report_id=report_id, entry_id=entry_id))
    # Exact copies by effective lineage: a correction overrides the recorded origin.
    origins: dict[str, str] = {}
    for report_id, recorded_origin in conn.execute(
        "SELECT report_id, copy_of_report_id FROM watch_issue_reports "
        "WHERE watch_id = ? AND source_thread_id = ?",
        (watch_id, thread_id),
    ).fetchall():
        origin = batch.lineage.effective_origin(
            str(report_id), None if recorded_origin is None else str(recorded_origin)
        )
        if origin is not None:
            origins[str(report_id)] = origin
    origin_threads = _origin_threads(conn, watch_id, set(origins.values()))
    return _ThreadSignals(
        thread_id=thread_id,
        themes=frozenset(themes),
        key_phrases=frozenset(phrases),
        links=frozenset(links),
        copies=frozenset(
            (report_id, origin_threads[origin]) for report_id, origin in origins.items()
        ),
    )


def _origin_threads(
    conn: sqlite3.Connection, watch_id: str, origin_ids: set[str]
) -> dict[str, str]:
    """The source thread of each origin report, in one bounded query of the watch."""
    if not origin_ids:
        return {}
    ids = sorted(origin_ids)
    placeholders = ", ".join("?" for _ in ids)
    threads = {
        str(report_id): str(thread_id)
        for report_id, thread_id in conn.execute(
            "SELECT report_id, source_thread_id FROM watch_issue_reports "
            f"WHERE watch_id = ? AND report_id IN ({placeholders})",
            (watch_id, *ids),
        ).fetchall()
    }
    missing = [origin_id for origin_id in ids if origin_id not in threads]
    if missing:
        raise _issue_unreadable(
            "exact-copy origin report is not recorded for this watch",
            watch_id=watch_id,
            report_ids=missing,
        )
    return threads


def _insert_suggestion(
    conn: sqlite3.Connection,
    batch: _IssueBatch,
    source_thread_id: str,
    comparison: _Comparison,
    *,
    refusal: str | None,
) -> None:
    conn.execute(
        "INSERT INTO watch_issue_suggestions (suggestion_id, watch_id, source_thread_id, "
        "candidate_thread_id, status, refusal_reason, signals_json, policy_sha256, run_id, "
        "created_at_utc) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            suggestion_id_for(
                batch.watch_id,
                source_thread_id,
                comparison.candidate_thread_id,
                batch.policy_sha256,
            ),
            batch.watch_id,
            source_thread_id,
            comparison.candidate_thread_id,
            "unconfirmed" if refusal is None else "refused",
            refusal,
            _canonical_json(comparison.signals(), "signals"),
            batch.policy_sha256,
            batch.run_id,
            batch.now,
        ),
    )


def _insert_ingestion(
    conn: sqlite3.Connection,
    batch: _IssueBatch,
    result: IssueIngestion,
    corrections: Sequence[LineageCorrection],
) -> None:
    """Append the ingestion's ledger row: its summary and the correction events it found."""
    recorded = conn.execute(
        "SELECT 1 FROM watch_issue_ingestions WHERE run_id = ?", (batch.run_id,)
    ).fetchone()
    if recorded is not None:
        raise _issue_refusal(
            "issue ingestion is already recorded for this run",
            "issue_ingestion_already_recorded",
            run_id=batch.run_id,
        )
    conn.execute(
        "INSERT INTO watch_issue_ingestions (run_id, watch_id, policy_sha256, summary_json, "
        "created_at_utc) VALUES (?, ?, ?, ?, ?)",
        (
            batch.run_id,
            batch.watch_id,
            batch.policy_sha256,
            _canonical_json({**result.to_dict(), **_lineage_events_json(corrections)}, "summary"),
            batch.now,
        ),
    )
