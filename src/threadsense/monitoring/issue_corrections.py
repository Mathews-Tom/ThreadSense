"""Late-discovered exact-copy lineage: correction events and the effective-lineage projection.

A report's recorded lineage is fixed when it is ingested. When an ingestion later records an
earlier-posted report with the same exact eligible text, it appends a typed correction event to
its own `watch_issue_ingestions.summary_json` ledger row. Effective lineage is the recorded
lineage overridden by each report's latest correction. Reports, evidence, memberships and earlier
ledger rows never change: a correction never regroups a thread.

Every SQL value is a bound parameter; f-strings only splice placeholder runs.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

from threadsense.monitoring.digest_records import _ENTRY_ID_PATTERN
from threadsense.monitoring.issue_records import (
    LINEAGE_MIN_CHARS,
    REPORT_ID_PATTERN,
    _issue_unreadable,
    _matches,
    _stored_issue_json,
)

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
LINEAGE_CORRECTIONS_KEY = "lineage_corrections"
LINEAGE_EVENTS_KEY = "lineage_events"
_EVENT_KEYS = frozenset(
    {
        "watch_id",
        "run_id",
        "policy_sha256",
        "report_id",
        "origin_report_id",
        "previous_origin_report_id",
        "text_sha256",
        "discovered_report_id",
        "discovery_entry_id",
    }
)
_LEDGER_SQL = (
    "SELECT run_id, policy_sha256, summary_json FROM watch_issue_ingestions "
    "WHERE watch_id = ? ORDER BY rowid ASC"
)
# Recorded reports whose first text is exactly the given text, found through the evidence
# text index, ordered by source time then record sequence.
_SAME_TEXT_LATER_SQL = (
    "SELECT DISTINCT r.report_id, r.copy_of_report_id, r.source_created_utc, r.record_sequence "
    "FROM watch_issue_evidence AS ev "
    "JOIN watch_issue_reports AS r ON r.report_id = ev.report_id AND r.watch_id = ev.watch_id "
    "WHERE ev.watch_id = ? AND ev.text_sha256 = ? AND r.first_text_sha256 = ev.text_sha256 "
    "AND r.report_id != ? AND r.source_created_utc > ? "
    "ORDER BY r.source_created_utc ASC, r.record_sequence ASC"
)
# Recorded-firsthand reports with evidence of the given text posted no later than a report.
_FIRSTHAND_WITH_TEXT_SQL = (
    "SELECT DISTINCT r.report_id, r.source_thread_id, r.source_created_utc, r.record_sequence "
    "FROM watch_issue_evidence AS ev "
    "JOIN watch_issue_reports AS r ON r.report_id = ev.report_id AND r.watch_id = ev.watch_id "
    "WHERE ev.watch_id = ? AND ev.text_sha256 = ? AND r.lineage = 'firsthand' "
    "AND r.report_id != ? AND r.source_created_utc <= ? "
    "ORDER BY r.source_created_utc ASC, r.record_sequence ASC"
)


@dataclass(frozen=True)
class LineageCorrection:
    """Proof that `report_id` exactly copies the earlier-posted `origin_report_id`.

    `discovered_report_id` is the report newly recorded by `discovery_entry_id` in ingestion
    `run_id` whose recording revealed the origin: the origin itself, or the origin it copies.
    """

    watch_id: str
    run_id: str
    policy_sha256: str
    report_id: str
    origin_report_id: str
    previous_origin_report_id: str | None
    text_sha256: str
    discovered_report_id: str
    discovery_entry_id: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "watch_id": self.watch_id,
            "run_id": self.run_id,
            "policy_sha256": self.policy_sha256,
            "report_id": self.report_id,
            "origin_report_id": self.origin_report_id,
            "previous_origin_report_id": self.previous_origin_report_id,
            "text_sha256": self.text_sha256,
            "discovered_report_id": self.discovered_report_id,
            "discovery_entry_id": self.discovery_entry_id,
        }


@dataclass(frozen=True)
class OriginMatch:
    """The effective-firsthand report a newly recorded report exactly copies."""

    report_id: str
    source_thread_id: str
    record_sequence: int


@dataclass
class LineageProjection:
    """Effective lineage of one watch: each corrected report's latest correction."""

    watch_id: str
    corrections: dict[str, LineageCorrection] = field(default_factory=dict)

    def correction_for(self, report_id: str) -> LineageCorrection | None:
        return self.corrections.get(report_id)

    def effective_origin(self, report_id: str, recorded_origin: str | None) -> str | None:
        """The report's origin now: its latest correction's, else the one recorded with it."""
        correction = self.corrections.get(report_id)
        return recorded_origin if correction is None else correction.origin_report_id

    def apply(self, correction: LineageCorrection, *, recorded_origin: str | None) -> None:
        """Advance the projection by one event; it must change the report's current origin."""
        current = self.effective_origin(correction.report_id, recorded_origin)
        if (
            correction.watch_id != self.watch_id
            or correction.previous_origin_report_id != current
            or correction.origin_report_id == current
        ):
            raise _issue_unreadable(
                "lineage correction does not follow the report's current lineage",
                watch_id=self.watch_id,
                run_id=correction.run_id,
                report_id=correction.report_id,
            )
        self.corrections[correction.report_id] = correction


@dataclass(frozen=True)
class _ReportFacts:
    first_text_sha256: str
    recorded_origin: str | None
    order: tuple[float, int]
    first_entry_id: str


# Loading the ledger ----------------------------------------------------------------------


def _load_lineage_projection(conn: sqlite3.Connection, watch_id: str) -> LineageProjection:
    """Read and validate every correction event of the watch once, in ledger order.

    Ingestions recorded before corrections existed carry neither the count nor the events; a
    summary with only one of them, or with events that do not prove a correction of the
    report's current lineage, is unreadable.
    """
    events: list[LineageCorrection] = []
    for run_id, policy_sha256, summary_json in conn.execute(_LEDGER_SQL, (watch_id,)).fetchall():
        events.extend(
            _ledger_events(
                str(summary_json),
                watch_id=watch_id,
                run_id=str(run_id),
                policy_sha256=str(policy_sha256),
            )
        )
    projection = LineageProjection(watch_id=watch_id)
    if not events:
        return projection
    report_ids = {
        report_id
        for event in events
        for report_id in (event.report_id, event.origin_report_id, event.discovered_report_id)
    }
    facts = _report_facts(conn, watch_id, report_ids)
    entry_runs = _entry_runs(conn, watch_id, {event.discovery_entry_id for event in events})
    origin_texts = _evidence_pairs(conn, watch_id, {event.origin_report_id for event in events})
    for event in events:
        _require_provenance(event, projection, facts, entry_runs, origin_texts)
        projection.apply(event, recorded_origin=facts[event.report_id].recorded_origin)
    return projection


def _ledger_events(
    summary_json: str, *, watch_id: str, run_id: str, policy_sha256: str
) -> list[LineageCorrection]:
    summary = _stored_issue_json(summary_json, watch_id=watch_id, run_id=run_id)
    if not isinstance(summary, dict):
        raise _issue_unreadable(
            "issue ingestion summary is not an object", watch_id=watch_id, run_id=run_id
        )
    has_count = LINEAGE_CORRECTIONS_KEY in summary
    has_events = LINEAGE_EVENTS_KEY in summary
    if not has_count and not has_events:
        return []
    count = summary.get(LINEAGE_CORRECTIONS_KEY)
    raw_events = summary.get(LINEAGE_EVENTS_KEY)
    if type(count) is not int or not isinstance(raw_events, list) or count != len(raw_events):
        raise _issue_unreadable(
            "issue ingestion lineage corrections do not match their events",
            watch_id=watch_id,
            run_id=run_id,
        )
    return [
        _parse_event(raw, watch_id=watch_id, run_id=run_id, policy_sha256=policy_sha256)
        for raw in raw_events
    ]


def _parse_event(
    raw: object, *, watch_id: str, run_id: str, policy_sha256: str
) -> LineageCorrection:
    if not isinstance(raw, dict) or set(raw) != _EVENT_KEYS:
        raise _issue_unreadable(
            "lineage correction event is malformed", watch_id=watch_id, run_id=run_id
        )
    previous = raw["previous_origin_report_id"]
    valid = (
        raw["watch_id"] == watch_id
        and raw["run_id"] == run_id
        and raw["policy_sha256"] == policy_sha256
        and _matches(REPORT_ID_PATTERN, raw["report_id"])
        and _matches(REPORT_ID_PATTERN, raw["origin_report_id"])
        and (previous is None or _matches(REPORT_ID_PATTERN, previous))
        and _matches(_SHA256_PATTERN, raw["text_sha256"])
        and _matches(REPORT_ID_PATTERN, raw["discovered_report_id"])
        and _matches(_ENTRY_ID_PATTERN, raw["discovery_entry_id"])
    )
    if not valid:
        raise _issue_unreadable(
            "lineage correction event does not belong to its ingestion",
            watch_id=watch_id,
            run_id=run_id,
            report_id=raw["report_id"] if isinstance(raw["report_id"], str) else None,
        )
    return LineageCorrection(
        watch_id=watch_id,
        run_id=run_id,
        policy_sha256=policy_sha256,
        report_id=str(raw["report_id"]),
        origin_report_id=str(raw["origin_report_id"]),
        previous_origin_report_id=None if previous is None else str(previous),
        text_sha256=str(raw["text_sha256"]),
        discovered_report_id=str(raw["discovered_report_id"]),
        discovery_entry_id=str(raw["discovery_entry_id"]),
    )


def _require_provenance(
    event: LineageCorrection,
    projection: LineageProjection,
    facts: dict[str, _ReportFacts],
    entry_runs: dict[str, str],
    origin_texts: set[tuple[str, str]],
) -> None:
    """The event names reports of this watch: an exact text and an origin posted first."""
    report = facts.get(event.report_id)
    origin = facts.get(event.origin_report_id)
    discovered = facts.get(event.discovered_report_id)
    proven = (
        report is not None
        and origin is not None
        and discovered is not None
        and report.first_text_sha256 == event.text_sha256
        and discovered.first_text_sha256 == event.text_sha256
        and (event.origin_report_id, event.text_sha256) in origin_texts
        and origin.order < report.order
        and discovered.first_entry_id == event.discovery_entry_id
        and entry_runs.get(event.discovery_entry_id) == event.run_id
        and event.origin_report_id
        in (
            event.discovered_report_id,
            projection.effective_origin(event.discovered_report_id, discovered.recorded_origin),
        )
    )
    if not proven:
        raise _issue_unreadable(
            "lineage correction event is not proven by recorded reports",
            watch_id=event.watch_id,
            run_id=event.run_id,
            report_id=event.report_id,
        )


def _placeholders(count: int) -> str:
    return ", ".join("?" for _ in range(count))


def _report_facts(
    conn: sqlite3.Connection, watch_id: str, report_ids: Iterable[str]
) -> dict[str, _ReportFacts]:
    ids = sorted(report_ids)
    rows = conn.execute(
        "SELECT report_id, first_text_sha256, copy_of_report_id, source_created_utc, "
        "record_sequence, first_entry_id FROM watch_issue_reports "
        f"WHERE watch_id = ? AND report_id IN ({_placeholders(len(ids))})",
        (watch_id, *ids),
    ).fetchall()
    return {
        str(report_id): _ReportFacts(
            first_text_sha256=str(text),
            recorded_origin=None if origin is None else str(origin),
            order=(float(created), int(sequence)),
            first_entry_id=str(entry_id),
        )
        for report_id, text, origin, created, sequence, entry_id in rows
    }


def _required_facts(conn: sqlite3.Connection, watch_id: str, report_id: str) -> _ReportFacts:
    facts = _report_facts(conn, watch_id, (report_id,)).get(report_id)
    if facts is None:
        raise _issue_unreadable(
            "lineage report is not recorded for this watch", watch_id=watch_id, report_id=report_id
        )
    return facts


def _entry_runs(
    conn: sqlite3.Connection, watch_id: str, entry_ids: Iterable[str]
) -> dict[str, str]:
    ids = sorted(entry_ids)
    rows = conn.execute(
        "SELECT entry_id, run_id FROM watch_issue_entries "
        f"WHERE watch_id = ? AND entry_id IN ({_placeholders(len(ids))})",
        (watch_id, *ids),
    ).fetchall()
    return {str(entry_id): str(run_id) for entry_id, run_id in rows}


def _evidence_pairs(
    conn: sqlite3.Connection, watch_id: str, report_ids: Iterable[str]
) -> set[tuple[str, str]]:
    ids = sorted(report_ids)
    rows = conn.execute(
        "SELECT report_id, text_sha256 FROM watch_issue_evidence "
        f"WHERE watch_id = ? AND report_id IN ({_placeholders(len(ids))})",
        (watch_id, *ids),
    ).fetchall()
    return {(str(report_id), str(text)) for report_id, text in rows}


# Recording within an ingestion -----------------------------------------------------------


def _effective_origin_match(
    conn: sqlite3.Connection,
    projection: LineageProjection,
    *,
    report_id: str,
    text_sha256: str,
    source_created_utc: float,
) -> OriginMatch | None:
    """The earliest-posted effectively firsthand report with this exact text, for a new report.

    Ties on source time go to the earlier-recorded report; a recorded-firsthand report that a
    correction made a copy is never an origin.
    """
    for origin_id, thread_id, _created, sequence in conn.execute(
        _FIRSTHAND_WITH_TEXT_SQL,
        (projection.watch_id, text_sha256, report_id, float(source_created_utc)),
    ).fetchall():
        if projection.correction_for(str(origin_id)) is None:
            return OriginMatch(
                report_id=str(origin_id),
                source_thread_id=str(thread_id),
                record_sequence=int(sequence),
            )
    return None


def _late_origin_corrections(
    conn: sqlite3.Connection,
    projection: LineageProjection,
    *,
    discovered_report_id: str,
    content_chars: int,
    discovery_entry_id: str,
    run_id: str,
    policy_sha256: str,
) -> list[LineageCorrection]:
    """Correct every later-posted report whose exact first text the new report reveals earlier.

    Call right after the new report is inserted with its recorded lineage; each returned
    correction is already applied to `projection`. Content shorter than `LINEAGE_MIN_CHARS`
    never corrects anything, mirroring recorded lineage.
    """
    if content_chars < LINEAGE_MIN_CHARS:
        return []
    watch_id = projection.watch_id
    discovered = _required_facts(conn, watch_id, discovered_report_id)
    origin_id = (
        projection.effective_origin(discovered_report_id, discovered.recorded_origin)
        or discovered_report_id
    )
    origin_order = _required_facts(conn, watch_id, origin_id).order
    corrections: list[LineageCorrection] = []
    for report_id, recorded_origin, _created, _sequence in conn.execute(
        _SAME_TEXT_LATER_SQL,
        (watch_id, discovered.first_text_sha256, discovered_report_id, discovered.order[0]),
    ).fetchall():
        recorded = None if recorded_origin is None else str(recorded_origin)
        current = projection.effective_origin(str(report_id), recorded)
        # Already the origin, or already copying a report posted even earlier.
        if current == origin_id or (
            current is not None and _required_facts(conn, watch_id, current).order < origin_order
        ):
            continue
        correction = LineageCorrection(
            watch_id=watch_id,
            run_id=run_id,
            policy_sha256=policy_sha256,
            report_id=str(report_id),
            origin_report_id=origin_id,
            previous_origin_report_id=current,
            text_sha256=discovered.first_text_sha256,
            discovered_report_id=discovered_report_id,
            discovery_entry_id=discovery_entry_id,
        )
        projection.apply(correction, recorded_origin=recorded)
        corrections.append(correction)
    return corrections


def _lineage_events_json(corrections: Sequence[LineageCorrection]) -> dict[str, Any]:
    """The ledger fields an ingestion summary carries alongside `IssueIngestion.to_dict()`."""
    return {
        LINEAGE_CORRECTIONS_KEY: len(corrections),
        LINEAGE_EVENTS_KEY: [correction.to_dict() for correction in corrections],
    }
