"""Append-only alert windows and evidence-only transitions, owned by MonitoringStore."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from threadsense.errors import MonitoringStateError
from threadsense.monitoring.alert_budget import AlertBudget, text_size

ALERT_SCHEMA_VERSION = 8
_ALERT_TABLES = (
    "watch_alert_cohorts",
    "watch_alert_reports",
    "watch_alert_events",
    "watch_alert_seen",
    "watch_alert_completions",
)
ALERT_SCHEMA_STATEMENTS: tuple[str, ...] = (
    """CREATE TABLE watch_alert_cohorts (
        cohort_id TEXT PRIMARY KEY, watch_id TEXT NOT NULL REFERENCES watches(watch_id),
        basis_json TEXT NOT NULL, created_at_utc REAL NOT NULL)""",
    """CREATE TABLE watch_alert_reports (
        report_id TEXT PRIMARY KEY, watch_id TEXT NOT NULL REFERENCES watches(watch_id),
        sweep_run_id TEXT NOT NULL REFERENCES watch_sweeps(run_id),
        cohort_id TEXT NOT NULL REFERENCES watch_alert_cohorts(cohort_id),
        window_id TEXT NOT NULL UNIQUE, baseline_window_id TEXT NOT NULL,
        status TEXT NOT NULL, payload_json TEXT NOT NULL, payload_sha256 TEXT NOT NULL,
        json_path TEXT NOT NULL, markdown_path TEXT NOT NULL,
        run_id TEXT NOT NULL REFERENCES watch_runs(run_id), created_at_utc REAL NOT NULL,
        UNIQUE(watch_id, sweep_run_id))""",
    """CREATE TABLE watch_alert_events (
        alert_id TEXT PRIMARY KEY, watch_id TEXT NOT NULL, report_id TEXT NOT NULL
        REFERENCES watch_alert_reports(report_id), payload_json TEXT NOT NULL)""",
    """CREATE TABLE watch_alert_seen (
        watch_id TEXT NOT NULL, evidence_key TEXT NOT NULL, source_report_id TEXT,
        first_window_id TEXT NOT NULL REFERENCES watch_alert_reports(window_id),
        PRIMARY KEY(watch_id, evidence_key))""",
    """CREATE TABLE watch_alert_completions (
        report_id TEXT PRIMARY KEY REFERENCES watch_alert_reports(report_id),
        json_sha256 TEXT NOT NULL, markdown_sha256 TEXT NOT NULL,
        completed_at_utc REAL NOT NULL, run_id TEXT NOT NULL REFERENCES watch_runs(run_id))""",
    *tuple(
        statement
        for table in _ALERT_TABLES
        for statement in (
            f"CREATE TRIGGER {table}_no_update BEFORE UPDATE ON {table} "
            "BEGIN SELECT RAISE(ABORT, 'alert history is append-only'); END",
            f"CREATE TRIGGER {table}_no_delete BEFORE DELETE ON {table} "
            "BEGIN SELECT RAISE(ABORT, 'alert history is append-only'); END",
        )
    ),
)


@dataclass(frozen=True)
class PendingAlertReport:
    payload: dict[str, Any]
    json_path: str
    markdown_path: str
    resumed: bool
    completed: bool


def canonical_json(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )


def identity(prefix: str, value: Any) -> str:
    return prefix + hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()[:32]


def stored_report(
    conn: sqlite3.Connection, report_id: str, budget: AlertBudget | None = None
) -> PendingAlertReport:
    """Read one immutable legacy unit, admitting bytes before payload access."""
    admission = AlertBudget() if budget is None else budget
    admission.admit("legacy-report:" + report_id, source_rows=1)
    handle = conn.execute(
        "SELECT rowid FROM watch_alert_reports WHERE report_id=?", (report_id,)
    ).fetchone()
    if handle is None:
        raise MonitoringStateError(
            "alert report is missing", details={"reason": "alert_report_missing"}
        )
    size = text_size(conn, "watch_alert_reports", "payload_json", int(handle[0]))
    metadata_size = sum(
        text_size(conn, "watch_alert_reports", column, int(handle[0]))
        for column in ("payload_sha256", "json_path", "markdown_path")
    )
    admission.admit(
        "legacy-report:" + report_id, input_bytes=size * 3 + metadata_size + 4 * len(report_id)
    )
    row = conn.execute(
        "SELECT r.payload_json, r.payload_sha256, r.json_path, r.markdown_path, "
        "c.report_id FROM watch_alert_reports r LEFT JOIN watch_alert_completions c "
        "ON c.report_id = r.report_id WHERE r.report_id = ?",
        (report_id,),
    ).fetchone()
    if row is None:
        raise MonitoringStateError(
            "alert report is missing", details={"reason": "alert_report_missing"}
        )
    text = str(row[0])
    if hashlib.sha256(text.encode("utf-8")).hexdigest() != row[1]:
        raise MonitoringStateError(
            "alert report identity does not match its payload",
            details={"reason": "alert_report_hash_mismatch", "report_id": report_id},
        )
    try:
        payload = json.loads(text)
    except (ValueError, TypeError) as error:
        raise MonitoringStateError(
            "alert report cannot be decoded", details={"reason": "alert_report_unreadable"}
        ) from error
    if not isinstance(payload, dict) or payload.get("report_id") != report_id:
        raise MonitoringStateError(
            "alert report payload is invalid", details={"reason": "alert_report_unreadable"}
        )
    if payload.get("schema_version") != 1:
        raise MonitoringStateError(
            "legacy alert report schema is unsupported",
            details={
                "reason": "unsupported_alert_schema",
                "schema_version": payload.get("schema_version"),
            },
        )
    return PendingAlertReport(payload, str(row[2]), str(row[3]), True, row[4] is not None)
