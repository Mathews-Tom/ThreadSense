"""Watch-scoped working facts and immutable completed-coverage certificates.

Working facts belong to a watch, not a historical target. Historical meaning lives
in the immutable artifacts/decisions; one fenced pending target owns working edits.
"""

from __future__ import annotations

from typing import Any

from threadsense.errors import MonitoringStateError
from threadsense.monitoring.alert_budget import AlertBudget, AlertSession, encode_control
from threadsense.monitoring.alert_records import identity

PROJECTION_SCHEMA_STATEMENTS: tuple[str, ...] = (
    """CREATE TABLE alert_watch_heads (
        watch_id TEXT PRIMARY KEY, completed_highwater INTEGER NOT NULL DEFAULT 0,
        completed_target TEXT, completed_checkpoint TEXT, pending_target TEXT,
        cutoff REAL, policy_sha256 TEXT, evidence_count INTEGER NOT NULL DEFAULT 0)""",
    """CREATE TABLE alert_watch_entries (
        watch_id TEXT NOT NULL, entry_id TEXT NOT NULL, source_thread_id TEXT NOT NULL,
        revision_id TEXT NOT NULL, observed REAL NOT NULL, revision_sequence INTEGER NOT NULL,
        stage INTEGER NOT NULL DEFAULT 0, comment_cursor INTEGER NOT NULL DEFAULT 0,
        novel_cursor INTEGER NOT NULL DEFAULT 0, info_json TEXT,
        acquisition_id TEXT NOT NULL DEFAULT '', snapshot_binding TEXT,
        eligible INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY(watch_id,entry_id))""",
    "CREATE INDEX alert_watch_entries_work "
    "ON alert_watch_entries(watch_id,stage,observed,revision_sequence,entry_id)",
    "CREATE INDEX alert_watch_entries_revision "
    "ON alert_watch_entries(watch_id,revision_id,entry_id)",
    "CREATE INDEX alert_watch_entries_thread "
    "ON alert_watch_entries(watch_id,source_thread_id,entry_id)",
    "CREATE INDEX alert_watch_entries_observed ON alert_watch_entries(watch_id,observed,entry_id)",
    "CREATE INDEX alert_watch_entries_acquisition "
    "ON alert_watch_entries(watch_id,acquisition_id,entry_id)",
    "CREATE INDEX alert_watch_entries_cutoff "
    "ON alert_watch_entries(watch_id,eligible,observed,entry_id)",
    """CREATE TABLE alert_watch_comments (
        watch_id TEXT NOT NULL, entry_id TEXT NOT NULL, comment_id TEXT NOT NULL,
        payload_json TEXT NOT NULL, PRIMARY KEY(watch_id,entry_id,comment_id))""",
    """CREATE TABLE alert_watch_memberships (
        watch_id TEXT NOT NULL, source_thread_id TEXT NOT NULL, event_id INTEGER NOT NULL,
        issue_id TEXT NOT NULL, PRIMARY KEY(watch_id,source_thread_id))""",
    """CREATE TABLE alert_watch_observations (
        watch_id TEXT NOT NULL, source_thread_id TEXT NOT NULL, first_observed REAL NOT NULL,
        PRIMARY KEY(watch_id,source_thread_id))""",
    """CREATE TABLE alert_watch_thread_metadata (
        watch_id TEXT NOT NULL, source_thread_id TEXT NOT NULL, community TEXT NOT NULL,
        source_created_utc REAL, community_corrected INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(watch_id,source_thread_id))""",
    """CREATE TABLE alert_watch_themes (
        watch_id TEXT NOT NULL, report_id TEXT NOT NULL, text_sha256 TEXT NOT NULL,
        recorded REAL NOT NULL, entry_id TEXT NOT NULL, theme_key TEXT,
        PRIMARY KEY(watch_id,report_id,text_sha256))""",
    "CREATE INDEX alert_watch_themes_entry "
    "ON alert_watch_themes(watch_id,entry_id,report_id,text_sha256)",
    """CREATE TABLE alert_watch_evidence (
        watch_id TEXT NOT NULL, evidence_key TEXT NOT NULL, report_id TEXT NOT NULL,
        text_sha256 TEXT NOT NULL, observed REAL NOT NULL, revision_sequence INTEGER NOT NULL,
        entry_id TEXT NOT NULL, slot INTEGER NOT NULL, citation_json TEXT NOT NULL,
        raw_sha256 TEXT NOT NULL, copy_of_report_id TEXT, community_key TEXT NOT NULL,
        done INTEGER NOT NULL DEFAULT 0, source_thread_id TEXT NOT NULL DEFAULT '',
        comment_id TEXT, eligible INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY(watch_id,evidence_key),
        UNIQUE(watch_id,report_id,text_sha256))""",
    "CREATE INDEX alert_watch_evidence_work ON alert_watch_evidence"
    "(watch_id,done,observed,revision_sequence,entry_id,slot,evidence_key)",
    "CREATE INDEX alert_watch_text_origins ON alert_watch_evidence"
    "(watch_id,text_sha256,observed,revision_sequence,entry_id,slot,report_id)",
    "CREATE INDEX alert_watch_eligible_origins "
    "ON alert_watch_evidence(watch_id,text_sha256,eligible,observed,"
    "revision_sequence,entry_id,slot,report_id)",
    "CREATE INDEX alert_watch_text_dependencies "
    "ON alert_watch_evidence(watch_id,text_sha256,evidence_key)",
    "CREATE INDEX alert_watch_copy_dependencies "
    "ON alert_watch_evidence(watch_id,copy_of_report_id,text_sha256,evidence_key)",
    "CREATE INDEX alert_watch_report_dependencies "
    "ON alert_watch_evidence(watch_id,report_id,evidence_key)",
    "CREATE INDEX alert_watch_entry_dependencies "
    "ON alert_watch_evidence(watch_id,entry_id,evidence_key)",
    "CREATE INDEX alert_watch_thread_dependencies "
    "ON alert_watch_evidence(watch_id,source_thread_id,evidence_key)",
    "CREATE INDEX alert_watch_comment_dependencies "
    "ON alert_watch_evidence(watch_id,source_thread_id,comment_id,evidence_key)",
    """CREATE TABLE alert_watch_acquisitions (
        watch_id TEXT NOT NULL, acquisition_id TEXT NOT NULL,
        source_thread_id TEXT NOT NULL, observed REAL NOT NULL,
        PRIMARY KEY(watch_id,acquisition_id))""",
    "CREATE INDEX alert_watch_first_observation "
    "ON alert_watch_acquisitions(watch_id,source_thread_id,observed,acquisition_id)",
    """CREATE TABLE alert_watch_acquisition_contributions (
        watch_id TEXT NOT NULL, acquisition_id TEXT NOT NULL, sweep_id TEXT NOT NULL,
        source_sequence INTEGER NOT NULL, contribution_json TEXT NOT NULL,
        PRIMARY KEY(watch_id,acquisition_id))""",
    """CREATE TABLE alert_watch_sweep_totals (
        watch_id TEXT NOT NULL, sweep_id TEXT NOT NULL,
        acquired_threads INTEGER NOT NULL DEFAULT 0, parsed_comments INTEGER NOT NULL DEFAULT 0,
        source_reported_comments INTEGER NOT NULL DEFAULT 0,
        absent_known_comments INTEGER NOT NULL DEFAULT 0,
        comment_coverage_truncated INTEGER NOT NULL DEFAULT 0,
        comment_coverage_unknown INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(watch_id,sweep_id))""",
    """CREATE TABLE alert_coverage_completions (
        target_id TEXT PRIMARY KEY, watch_id TEXT NOT NULL, expected_target TEXT,
        expected_highwater INTEGER NOT NULL, checkpoint_id TEXT NOT NULL UNIQUE,
        highwater INTEGER NOT NULL, cutoff REAL NOT NULL, policy_sha256 TEXT NOT NULL,
        terminal_json TEXT NOT NULL)""",
    """CREATE TABLE alert_presence_bindings (
        target_id TEXT PRIMARY KEY, watch_id TEXT NOT NULL, highwater INTEGER NOT NULL,
        basis_sha256 TEXT NOT NULL, expected_target TEXT, expected_highwater INTEGER NOT NULL,
        inherited_sha256 TEXT NOT NULL)""",
    "CREATE TABLE alert_projection_initializations(target_id TEXT PRIMARY KEY)",
    """CREATE TABLE alert_watch_occurrences (
        watch_id TEXT NOT NULL, evidence_key TEXT NOT NULL, entry_id TEXT NOT NULL,
        slot INTEGER NOT NULL, observed REAL NOT NULL, revision_sequence INTEGER NOT NULL,
        citation_json TEXT NOT NULL, raw_sha256 TEXT NOT NULL, eligible INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY(watch_id,evidence_key,entry_id,slot))""",
    "CREATE INDEX alert_watch_occurrence_minimum "
    "ON alert_watch_occurrences"
    "(watch_id,evidence_key,eligible DESC,observed,revision_sequence,entry_id,slot)",
    "CREATE INDEX alert_watch_occurrence_entry "
    "ON alert_watch_occurrences(watch_id,entry_id,evidence_key,slot)",
    *tuple(
        f"CREATE TRIGGER {table}_no_{operation.lower()} BEFORE {operation} ON {table} "
        "BEGIN SELECT RAISE(ABORT,'alert coverage is append-only'); END"
        for table in ("alert_coverage_completions", "alert_presence_bindings")
        for operation in ("UPDATE", "DELETE")
    ),
)


def coverage_binding(session: AlertSession) -> dict[str, Any]:
    row = session.conn.execute(
        "SELECT completed_highwater,completed_target,completed_checkpoint,cutoff,policy_sha256,"
        "evidence_count,pending_target FROM alert_watch_heads WHERE watch_id=?",
        (session.watch_id,),
    ).fetchone()
    if row is None:
        return {
            "highwater": 0,
            "target_id": None,
            "checkpoint_id": None,
            "cutoff": None,
            "policy_sha256": None,
            "evidence_count": 0,
            "pending_target": None,
        }
    return dict(
        zip(
            (
                "highwater",
                "target_id",
                "checkpoint_id",
                "cutoff",
                "policy_sha256",
                "evidence_count",
                "pending_target",
            ),
            row,
            strict=True,
        )
    )


def claim_projection(session: AlertSession, target: dict[str, Any]) -> None:
    """Called inside the target freeze fence; no admitted history copy or scan."""
    expected = target["inherited_coverage"]
    current = coverage_binding(session)
    if current != expected or current["pending_target"] is not None:
        raise MonitoringStateError(
            "alert coverage changed", details={"reason": "coverage_conflict"}
        )
    session.conn.execute(
        "INSERT INTO alert_watch_heads(watch_id,pending_target) VALUES (?,?) "
        "ON CONFLICT(watch_id) DO UPDATE SET pending_target=excluded.pending_target",
        (session.watch_id, target["target_id"]),
    )


def require_projection(session: AlertSession, target: dict[str, Any]) -> None:
    expected = target["inherited_coverage"]
    current = coverage_binding(session)
    pending = current.pop("pending_target")
    current["pending_target"] = None
    if pending != target["target_id"] or current != expected:
        raise MonitoringStateError(
            "alert coverage changed", details={"reason": "coverage_conflict"}
        )


def prepare_terminal(
    session: AlertSession,
    target: dict[str, Any],
    progress: dict[str, Any],
    counts: dict[str, int],
    budget: AlertBudget,
) -> tuple[str, str]:
    """Admit immutable terminal control outside the completion writer fence."""
    require_projection(session, target)
    value = {
        "target_id": target["target_id"],
        "highwater": target["highwater"],
        "basis_sha256": target["basis_sha256"],
        "inherited_coverage": target["inherited_coverage"],
        "cutoff": target["cutoff"],
        "policy_sha256": identity("aip-", target["basis"].get("issue_policy")),
        "predecessor": progress["checkpoint_id"],
        "heads": progress["heads"],
        "counts": counts,
    }
    text = encode_control(value, budget, "terminal_checkpoint", passes=3)
    budget.admit("terminal_completion_control", derivations=1, input_bytes=1024)
    return identity("ack-terminal-", value), text
