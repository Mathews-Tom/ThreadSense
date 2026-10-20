"""Cross-thread issue history: stable issues, report lineage and reversible memberships.

Owned by `MonitoringStore`: every writing helper here runs inside a transaction that the
store has already opened, and run-scoped writes are fenced against stale run owners.

Model:

- An *issue* is identified by its home thread: every watch thread with issue evidence has
  exactly one home issue, created the first time the thread's finding entries are recorded.
- A *report* is one source comment cited as finding evidence. Its lineage is fixed when it
  is first recorded: `exact_copy` when its text is identical to an earlier recorded
  firsthand report of another comment, otherwise `firsthand`.
- A thread's *membership* is the latest of its append-only membership events. Exact-content
  lineage may assign a new thread to the issue of the report it copies; only an explicit
  operator event links or unlinks a thread afterwards. Suggestions never change membership.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from threadsense.config import (
    ISSUE_CANDIDATE_WINDOW_DAYS_RANGE,
    ISSUE_MAX_CANDIDATE_COMPARISONS_RANGE,
    ISSUE_MAX_SUGGESTIONS_RANGE,
)
from threadsense.errors import MonitoringInputError, MonitoringStateError

UpdateKind = Literal["reproduction", "contradiction", "workaround", "resolution_claim"]
Lineage = Literal["firsthand", "exact_copy"]
MembershipAction = Literal["assigned", "linked", "unlinked"]
MembershipBasis = Literal["thread_seed", "exact_content_lineage", "operator"]
SuggestionStatus = Literal["unconfirmed", "refused"]

_ISSUE_SCHEMA_VERSION = 4
_DIGEST_ISSUE_VIEW_SCHEMA_VERSION = 5
ISSUE_POLICY_NAME = "threadsense-issue-grouping"
ISSUE_POLICY_VERSION = 1
UPDATE_KINDS: tuple[UpdateKind, ...] = (
    "reproduction",
    "contradiction",
    "workaround",
    "resolution_claim",
)
# Explicit lexical cues, matched case-insensitively on word boundaries. A match records the
# report as an unverified update claim citing the cue; it never asserts the claim is true.
UPDATE_CUES: dict[UpdateKind, tuple[str, ...]] = {
    "reproduction": (
        "steps to reproduce",
        "repro steps",
        "reproduction steps",
        "i can reproduce",
        "reproduced it",
        "reliably reproduce",
    ),
    "contradiction": (
        "cannot reproduce",
        "can't reproduce",
        "could not reproduce",
        "couldn't reproduce",
        "unable to reproduce",
        "not reproducible",
        "works fine for me",
        "does not happen for me",
        "doesn't happen for me",
    ),
    "workaround": (
        "workaround",
        "work around",
        "work-around",
        "temporary fix",
        "what worked for me",
    ),
    "resolution_claim": (
        "fixed in",
        "is fixed",
        "been fixed",
        "resolved in",
        "no longer happens",
        "fix is out",
        "patched in",
    ),
}
# Identical text shorter than this (whitespace-collapsed) is not lineage evidence: short
# acknowledgements repeat verbatim without one copying the other.
LINEAGE_MIN_CHARS = 40
MAX_REASON_CHARS = 500
SHARED_LINK_REFUSAL = "shared_link_only"

ISSUE_ID_PATTERN = re.compile(r"^iss-[0-9a-f]{32}$")
REPORT_ID_PATTERN = re.compile(r"^rpt-[0-9a-f]{32}$")
UPDATE_ID_PATTERN = re.compile(r"^upd-[0-9a-f]{32}$")
SUGGESTION_ID_PATTERN = re.compile(r"^sug-[0-9a-f]{32}$")

_APPEND_ONLY_TABLES = (
    "watch_issue_policies",
    "watch_issues",
    "watch_issue_entries",
    "watch_issue_reports",
    "watch_issue_evidence",
    "watch_issue_updates",
    "watch_issue_memberships",
    "watch_issue_suggestions",
    "watch_issue_ingestions",
)

ISSUE_SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE watch_issue_policies (
        policy_sha256 TEXT PRIMARY KEY,
        policy_json TEXT NOT NULL,
        created_at_utc REAL NOT NULL
    )
    """,
    """
    CREATE TABLE watch_issues (
        issue_id TEXT PRIMARY KEY,
        watch_id TEXT NOT NULL REFERENCES watches (watch_id),
        home_thread_id TEXT NOT NULL,
        created_sequence INTEGER NOT NULL CHECK (created_sequence >= 1),
        anchor_utc REAL NOT NULL,
        created_at_utc REAL NOT NULL,
        created_run_id TEXT NOT NULL REFERENCES watch_runs (run_id),
        policy_sha256 TEXT NOT NULL REFERENCES watch_issue_policies (policy_sha256),
        UNIQUE (watch_id, home_thread_id),
        UNIQUE (watch_id, created_sequence)
    )
    """,
    """
    CREATE TABLE watch_issue_entries (
        entry_id TEXT PRIMARY KEY REFERENCES watch_digest_entries (entry_id),
        watch_id TEXT NOT NULL,
        source_thread_id TEXT NOT NULL,
        theme_key TEXT NOT NULL,
        key_phrases_json TEXT NOT NULL,
        ingested_at_utc REAL NOT NULL,
        run_id TEXT NOT NULL REFERENCES watch_runs (run_id)
    )
    """,
    """
    CREATE TABLE watch_issue_reports (
        report_id TEXT PRIMARY KEY,
        watch_id TEXT NOT NULL,
        source_thread_id TEXT NOT NULL,
        comment_id TEXT NOT NULL,
        record_sequence INTEGER NOT NULL CHECK (record_sequence >= 1),
        first_text_sha256 TEXT NOT NULL,
        author_sha256 TEXT,
        community TEXT NOT NULL,
        permalink TEXT NOT NULL,
        source_created_utc REAL NOT NULL,
        lineage TEXT NOT NULL CHECK (lineage IN ('firsthand', 'exact_copy')),
        copy_of_report_id TEXT REFERENCES watch_issue_reports (report_id),
        first_entry_id TEXT NOT NULL REFERENCES watch_digest_entries (entry_id),
        recorded_at_utc REAL NOT NULL,
        run_id TEXT NOT NULL REFERENCES watch_runs (run_id),
        CHECK ((lineage = 'exact_copy') = (copy_of_report_id IS NOT NULL)),
        UNIQUE (watch_id, source_thread_id, comment_id),
        UNIQUE (watch_id, record_sequence)
    )
    """,
    """
    CREATE TABLE watch_issue_evidence (
        report_id TEXT NOT NULL REFERENCES watch_issue_reports (report_id),
        entry_id TEXT NOT NULL REFERENCES watch_digest_entries (entry_id),
        watch_id TEXT NOT NULL,
        text_sha256 TEXT NOT NULL,
        revision_id TEXT NOT NULL REFERENCES watch_thread_revisions (revision_id),
        snapshot_sha256 TEXT NOT NULL,
        links_json TEXT NOT NULL,
        recorded_at_utc REAL NOT NULL,
        PRIMARY KEY (report_id, entry_id)
    )
    """,
    """
    CREATE INDEX watch_issue_evidence_text ON watch_issue_evidence (watch_id, text_sha256)
    """,
    """
    CREATE TABLE watch_issue_updates (
        update_id TEXT PRIMARY KEY,
        watch_id TEXT NOT NULL,
        source_thread_id TEXT NOT NULL,
        report_id TEXT NOT NULL REFERENCES watch_issue_reports (report_id),
        entry_id TEXT NOT NULL REFERENCES watch_digest_entries (entry_id),
        text_sha256 TEXT NOT NULL,
        kind TEXT NOT NULL CHECK (
            kind IN ('reproduction', 'contradiction', 'workaround', 'resolution_claim')
        ),
        cue TEXT NOT NULL,
        recorded_at_utc REAL NOT NULL,
        run_id TEXT NOT NULL REFERENCES watch_runs (run_id),
        UNIQUE (report_id, text_sha256, kind)
    )
    """,
    """
    CREATE TABLE watch_issue_memberships (
        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
        watch_id TEXT NOT NULL REFERENCES watches (watch_id),
        source_thread_id TEXT NOT NULL,
        issue_id TEXT NOT NULL REFERENCES watch_issues (issue_id),
        previous_issue_id TEXT REFERENCES watch_issues (issue_id),
        action TEXT NOT NULL CHECK (action IN ('assigned', 'linked', 'unlinked')),
        basis TEXT NOT NULL CHECK (
            basis IN ('thread_seed', 'exact_content_lineage', 'operator')
        ),
        reason TEXT NOT NULL,
        report_id TEXT REFERENCES watch_issue_reports (report_id),
        policy_sha256 TEXT REFERENCES watch_issue_policies (policy_sha256),
        run_id TEXT REFERENCES watch_runs (run_id),
        created_at_utc REAL NOT NULL,
        CHECK ((action = 'assigned') = (previous_issue_id IS NULL)),
        CHECK ((basis = 'operator') = (run_id IS NULL)),
        CHECK ((basis = 'operator') = (policy_sha256 IS NULL)),
        CHECK ((basis = 'exact_content_lineage') = (report_id IS NOT NULL))
    )
    """,
    """
    CREATE INDEX watch_issue_memberships_thread
        ON watch_issue_memberships (watch_id, source_thread_id, event_id)
    """,
    """
    CREATE TABLE watch_issue_suggestions (
        suggestion_id TEXT PRIMARY KEY,
        watch_id TEXT NOT NULL,
        source_thread_id TEXT NOT NULL,
        candidate_thread_id TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('unconfirmed', 'refused')),
        refusal_reason TEXT CHECK (
            refusal_reason IS NULL OR refusal_reason = 'shared_link_only'
        ),
        signals_json TEXT NOT NULL,
        policy_sha256 TEXT NOT NULL REFERENCES watch_issue_policies (policy_sha256),
        run_id TEXT NOT NULL REFERENCES watch_runs (run_id),
        created_at_utc REAL NOT NULL,
        CHECK ((status = 'refused') = (refusal_reason IS NOT NULL))
    )
    """,
    """
    CREATE TABLE watch_issue_ingestions (
        run_id TEXT PRIMARY KEY REFERENCES watch_runs (run_id),
        watch_id TEXT NOT NULL,
        policy_sha256 TEXT NOT NULL REFERENCES watch_issue_policies (policy_sha256),
        summary_json TEXT NOT NULL,
        created_at_utc REAL NOT NULL
    )
    """,
    *(
        statement
        for table in _APPEND_ONLY_TABLES
        for statement in (
            f"""
            CREATE TRIGGER {table}_no_update BEFORE UPDATE ON {table}
            BEGIN SELECT RAISE(ABORT, 'issue history is append-only'); END
            """,
            f"""
            CREATE TRIGGER {table}_no_delete BEFORE DELETE ON {table}
            BEGIN SELECT RAISE(ABORT, 'issue history is append-only'); END
            """,
        )
    ),
)


# Issue references recorded with each new digest, so a resumed digest renders the same bytes.
DIGEST_ISSUE_VIEW_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE watch_digest_issue_views (
        digest_id TEXT PRIMARY KEY REFERENCES watch_digests (digest_id),
        watch_id TEXT NOT NULL REFERENCES watches (watch_id),
        view_json TEXT NOT NULL,
        created_at_utc REAL NOT NULL
    )
    """,
    """
    CREATE TRIGGER watch_digest_issue_views_no_update BEFORE UPDATE ON watch_digest_issue_views
    BEGIN SELECT RAISE(ABORT, 'issue history is append-only'); END
    """,
    """
    CREATE TRIGGER watch_digest_issue_views_no_delete BEFORE DELETE ON watch_digest_issue_views
    BEGIN SELECT RAISE(ABORT, 'issue history is append-only'); END
    """,
)


@dataclass(frozen=True)
class IssuePolicy:
    """Versioned grouping policy; its canonical JSON hash binds every decision it makes.

    Semantic grouping is `not_eligible`: no activation policy is available, so candidate
    suggestions stay unconfirmed and never merge memberships or counts.
    """

    candidate_window_days: int
    max_candidate_comparisons: int
    max_suggestions_per_thread: int

    def __post_init__(self) -> None:
        for field, value, bounds in (
            (
                "candidate_window_days",
                self.candidate_window_days,
                ISSUE_CANDIDATE_WINDOW_DAYS_RANGE,
            ),
            (
                "max_candidate_comparisons",
                self.max_candidate_comparisons,
                ISSUE_MAX_CANDIDATE_COMPARISONS_RANGE,
            ),
            (
                "max_suggestions_per_thread",
                self.max_suggestions_per_thread,
                ISSUE_MAX_SUGGESTIONS_RANGE,
            ),
        ):
            low, high = bounds
            if type(value) is not int or not low <= value <= high:
                raise MonitoringInputError(
                    f"issue policy {field} must be an integer between {low} and {high}",
                    details={"reason": "invalid_issue_policy", "field": field, "value": value},
                )

    def to_dict(self) -> dict[str, Any]:
        return {
            "policy": ISSUE_POLICY_NAME,
            "version": ISSUE_POLICY_VERSION,
            "membership": {
                "exact_source_identity": "evidence from one thread stays with that thread",
                "exact_content_lineage": {
                    "basis": "comment_text_sha256",
                    "min_chars": LINEAGE_MIN_CHARS,
                    "origin": "the earliest firsthand report posted no later than the copy",
                    "effect": (
                        "a new thread joins the issue of the report it copies only when every "
                        "report recorded for it in the batch is an exact copy of a report in "
                        "another thread; otherwise it seeds its own issue"
                    ),
                    "updates": "a copy records cues only for text not present in its origin",
                },
                "shared_link": "signal only; never grouping evidence",
                "operator": "explicit link or unlink events are the only later changes",
            },
            "candidates": {
                "window_days": self.candidate_window_days,
                "max_comparisons_per_thread": self.max_candidate_comparisons,
                "max_suggestions_per_thread": self.max_suggestions_per_thread,
                "ordering": "nearest_source_time_then_thread_id",
                "qualifying_signals": ["theme", "key_phrase", "exact_copy"],
                "refusal": SHARED_LINK_REFUSAL,
            },
            "semantic_grouping": {
                "status": "not_eligible",
                "reason": "no_activation_policy_available",
                "effect": "suggestions stay unconfirmed; memberships and counts are unchanged",
                "shadow_annotations": "not_joined",
            },
            "updates": {
                "basis": "lexical_cue",
                "claims_are_verified": False,
                "cues": {kind: list(UPDATE_CUES[kind]) for kind in UPDATE_KINDS},
            },
        }

    def canonical_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    def sha256(self) -> str:
        return _sha256_text(self.canonical_json())


@dataclass(frozen=True)
class PendingIssueEntry:
    """A recorded finding entry not yet in issue history, with its store-recorded artifacts."""

    entry_id: str
    source_thread_id: str
    revision_id: str
    theme_key: str
    snapshot_path: Path
    snapshot_sha256: str
    analysis_path: Path
    analysis_sha256: str
    novel_evidence: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class IssueReportInput:
    """One cited comment, read from the verified revision snapshot."""

    comment_id: str
    text_sha256: str
    content_chars: int
    author_sha256: str | None
    community: str
    permalink: str
    source_created_utc: float
    links: tuple[str, ...]
    cues: tuple[tuple[UpdateKind, str], ...]


@dataclass(frozen=True)
class IssueEntryInput:
    entry_id: str
    source_thread_id: str
    revision_id: str
    snapshot_sha256: str
    theme_key: str
    key_phrases: tuple[str, ...]
    reports: tuple[IssueReportInput, ...]


@dataclass(frozen=True)
class IssueIngestion:
    run_id: str
    policy_sha256: str | None
    entries: int
    threads_created: int
    lineage_assignments: int
    reports_recorded: int
    exact_copies: int
    evidence_recorded: int
    updates_recorded: int
    comparisons: int
    suggestions: int
    suggestions_truncated: int
    refusals: int
    # Late-discovered exact-copy origins; each correction's proof is in the ingestion ledger.
    lineage_corrections: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "policy_sha256": self.policy_sha256,
            "entries": self.entries,
            "threads_created": self.threads_created,
            "lineage_assignments": self.lineage_assignments,
            "reports_recorded": self.reports_recorded,
            "exact_copies": self.exact_copies,
            "evidence_recorded": self.evidence_recorded,
            "updates_recorded": self.updates_recorded,
            "comparisons": self.comparisons,
            "suggestions": self.suggestions,
            "suggestions_truncated": self.suggestions_truncated,
            "refusals": self.refusals,
            "lineage_corrections": self.lineage_corrections,
        }


def issue_id_for(watch_id: str, source_thread_id: str) -> str:
    return "iss-" + _sha256_text(f"{watch_id}\n{source_thread_id}\nissue")[:32]


def report_id_for(watch_id: str, source_thread_id: str, comment_id: str) -> str:
    return "rpt-" + _sha256_text(f"{watch_id}\n{source_thread_id}\n{comment_id}")[:32]


def update_id_for(report_id: str, text_sha256: str, kind: UpdateKind) -> str:
    return "upd-" + _sha256_text(f"{report_id}\n{text_sha256}\n{kind}")[:32]


def suggestion_id_for(
    watch_id: str, source_thread_id: str, candidate_thread_id: str, policy_sha256: str
) -> str:
    chain = f"{watch_id}\n{source_thread_id}\n{candidate_thread_id}\n{policy_sha256}"
    return "sug-" + _sha256_text(chain)[:32]


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# Shared store helpers. Every SQL value is a bound parameter.
_CURRENT_ISSUE_SQL = (
    "SELECT issue_id FROM watch_issue_memberships WHERE watch_id = ? AND source_thread_id = ? "
    "ORDER BY event_id DESC LIMIT 1"
)


@dataclass(frozen=True)
class _MembershipEvent:
    watch_id: str
    source_thread_id: str
    issue_id: str
    previous_issue_id: str | None
    action: MembershipAction
    basis: MembershipBasis
    reason: str
    report_id: str | None
    policy_sha256: str | None
    run_id: str | None
    created_at_utc: float


def _issue_unreadable(message: str, **details: Any) -> MonitoringStateError:
    return MonitoringStateError(message, details={"reason": "issue_state_unreadable", **details})


def _issue_refusal(message: str, reason: str, **details: Any) -> MonitoringStateError:
    return MonitoringStateError(message, details={"reason": reason, **details})


def _stored_issue_json(text: str, **details: Any) -> Any:
    try:
        return json.loads(text)
    except ValueError as error:
        raise _issue_unreadable("stored issue state is not valid JSON", **details) from error


def _stored_str_list(text: str, **details: Any) -> list[str]:
    parsed = _stored_issue_json(text, **details)
    if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
        raise _issue_unreadable("stored issue list is not a list of strings", **details)
    return parsed


def _matches(pattern: re.Pattern[str], value: object) -> bool:
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def _insert_membership(conn: sqlite3.Connection, event: _MembershipEvent) -> int:
    cursor = conn.execute(
        "INSERT INTO watch_issue_memberships (watch_id, source_thread_id, issue_id, "
        "previous_issue_id, action, basis, reason, report_id, policy_sha256, run_id, "
        "created_at_utc) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            event.watch_id,
            event.source_thread_id,
            event.issue_id,
            event.previous_issue_id,
            event.action,
            event.basis,
            event.reason,
            event.report_id,
            event.policy_sha256,
            event.run_id,
            event.created_at_utc,
        ),
    )
    event_id = cursor.lastrowid
    if event_id is None:
        raise _issue_unreadable(
            "membership event was not assigned an id",
            watch_id=event.watch_id,
            source_thread_id=event.source_thread_id,
        )
    return event_id


# Membership ----------------------------------------------------------------------------


def _current_issue(conn: sqlite3.Connection, watch_id: str, source_thread_id: str) -> str | None:
    """The issue of the thread's latest membership event; None when it has none."""
    row = conn.execute(_CURRENT_ISSUE_SQL, (watch_id, source_thread_id)).fetchone()
    return None if row is None else str(row[0])
