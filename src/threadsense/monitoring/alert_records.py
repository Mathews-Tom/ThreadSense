"""Append-only alert windows and evidence-only transitions, owned by MonitoringStore."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from threadsense.errors import MonitoringStateError

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


def stored_report(conn: sqlite3.Connection, report_id: str) -> PendingAlertReport:
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
    return PendingAlertReport(payload, str(row[2]), str(row[3]), True, row[4] is not None)


def evidence_snapshot(
    conn: sqlite3.Connection, watch_id: str, cutoff: float, root: Path
) -> list[dict[str, Any]]:
    """Read history once per window, including comments without a finding.

    Local imports avoid a store/ingestion import cycle. Existing ingestion identity,
    cue and account rules apply to generic thread evidence as well.
    """
    from threadsense.models.canonical import load_canonical_thread
    from threadsense.monitoring.issue_records import report_id_for, update_id_for
    from threadsense.monitoring.issues import account_sha256, detect_update_cues
    from threadsense.monitoring.observations import comment_text_sha256, thread_text_sha256
    from threadsense.monitoring.sweep import _verify_stored_artifact

    reports = {
        str(row[0]): (row[1], row[2])
        for row in conn.execute(
            "SELECT report_id,lineage,copy_of_report_id FROM watch_issue_reports WHERE watch_id=?",
            (watch_id,),
        )
    }
    themes: dict[tuple[str, str], str] = {}
    for row in conn.execute(
        "SELECT e.report_id,e.text_sha256,d.theme_key FROM watch_issue_evidence e "
        "JOIN watch_digest_entries d ON d.entry_id=e.entry_id WHERE e.watch_id=? "
        "ORDER BY e.recorded_at_utc,e.entry_id",
        (watch_id,),
    ):
        themes.setdefault((str(row[0]), str(row[1])), str(row[2]))
    memberships: dict[str, str] = {}
    for row in conn.execute(
        "SELECT source_thread_id,issue_id FROM watch_issue_memberships "
        "WHERE watch_id=? ORDER BY event_id",
        (watch_id,),
    ):
        memberships[str(row[0])] = str(row[1])
    first_observed = {
        (str(row[0]), str(row[1])): row[2]
        for row in conn.execute(
            "SELECT source_thread_id,comment_id,first_observed_at_utc FROM watch_comments "
            "WHERE watch_id=?",
            (watch_id,),
        )
    }
    thread_observed = {
        str(row[0]): row[1]
        for row in conn.execute(
            "SELECT source_thread_id,MIN(observed_at_utc) FROM watch_acquisitions "
            "WHERE watch_id=? GROUP BY source_thread_id",
            (watch_id,),
        )
    }
    rows = conn.execute(
        "SELECT d.entry_id,d.source_thread_id,d.payload_json,r.revision_id,r.snapshot_path,"
        "r.snapshot_sha256,a.analysis_path,a.analysis_sha256,t.source_created_utc,"
        "ac.observed_at_utc FROM watch_digest_entries d "
        "JOIN watch_thread_revisions r ON r.revision_id=d.revision_id "
        "JOIN watch_revision_analyses a ON a.revision_id=r.revision_id "
        "JOIN watch_acquisitions ac ON ac.acquisition_id=r.acquisition_id "
        "JOIN watch_threads t ON t.watch_id=d.watch_id AND t.source_thread_id=d.source_thread_id "
        "WHERE d.watch_id=? AND d.kind='thread_update' AND ac.observed_at_utc<=? "
        "ORDER BY ac.observed_at_utc,r.sequence,d.entry_id",
        (watch_id, cutoff),
    ).fetchall()
    seen: set[tuple[str, str]] = set()
    result: list[dict[str, Any]] = []
    verified: set[tuple[str, str]] = set()
    for row in rows:
        for path, digest in ((str(row[4]), str(row[5])), (str(row[6]), str(row[7]))):
            artifact = root / path
            if not artifact.resolve().is_relative_to(root.resolve()):
                raise MonitoringStateError(
                    "alert evidence escapes monitoring storage",
                    details={"reason": "artifact_outside_state", "path": path},
                )
            if (path, digest) not in verified:
                _verify_stored_artifact(artifact, digest)
                verified.add((path, digest))
        snapshot = load_canonical_thread(root / str(row[4]))
        if snapshot.source.source_thread_id != row[1]:
            raise MonitoringStateError(
                "alert snapshot belongs to another thread",
                details={"reason": "alert_evidence_mismatch", "entry_id": row[0]},
            )
        comments = {comment.comment_id: comment for comment in snapshot.comments}
        entry = json.loads(row[2])
        thread_report = identity("atr-", [watch_id, row[1]])
        thread_text = thread_text_sha256(snapshot.title, snapshot.body)
        thread_key = (thread_report, thread_text)
        if thread_key not in seen:
            seen.add(thread_key)
            body = snapshot.title + "\n" + (snapshot.body or "")
            edited = bool(entry["thread_text_changed"])
            result.append(
                {
                    "source_report_id": thread_report,
                    "source_thread_id": row[1],
                    "source_kind": "thread_text",
                    "comment_id": None,
                    "community": snapshot.source.community,
                    "permalink": snapshot.permalink,
                    "source_created_utc": row[8],
                    "source_thread_created_utc": row[8],
                    "lineage": "firsthand",
                    "copy_of_report_id": None,
                    "author_sha256": account_sha256(
                        snapshot.source.source_name, snapshot.author.username
                    ),
                    "text_sha256": thread_text,
                    "entry_id": row[0],
                    "revision_id": row[3],
                    "snapshot_path": row[4],
                    "snapshot_sha256": row[5],
                    "analysis_path": row[6],
                    "analysis_sha256": row[7],
                    "first_observed_at_utc": thread_observed[str(row[1])],
                    "observed_at_utc": row[9],
                    "recorded_at_utc": row[9],
                    "observation_kind": "edited" if edited else "initial_observation",
                    "edit_observed_at_utc": row[9] if edited else None,
                    "source_edited_at_utc": None,
                    "excerpt": " ".join(body.split())[:280],
                    "theme_key": None,
                    "issue_id": memberships.get(str(row[1])),
                    "independent_person_verified": False,
                    "updates": [
                        {
                            "update_id": update_id_for(thread_report, thread_text, kind),
                            "kind": kind,
                            "cue": cue,
                        }
                        for kind, cue in detect_update_cues(body)
                    ],
                }
            )
        for change in entry["novel_comments"]:
            report = report_id_for(watch_id, str(row[1]), str(change["comment_id"]))
            key = (report, str(change["text_sha256"]))
            if key in seen:
                continue
            seen.add(key)
            comment = comments.get(str(change["comment_id"]))
            if comment is None or comment_text_sha256(comment.body) != key[1]:
                raise MonitoringStateError(
                    "alert citation does not match its recorded snapshot",
                    details={"reason": "alert_evidence_mismatch", "entry_id": row[0]},
                )
            lineage, copy_of = reports.get(report, ("firsthand", None))
            updates = [
                {"update_id": update_id_for(report, key[1], kind), "kind": kind, "cue": cue}
                for kind, cue in detect_update_cues(comment.body)
            ]
            result.append(
                {
                    "source_report_id": report,
                    "source_thread_id": row[1],
                    "source_kind": "comment",
                    "comment_id": comment.comment_id,
                    "community": snapshot.source.community,
                    "permalink": comment.permalink,
                    "source_created_utc": comment.created_utc,
                    "lineage": lineage,
                    "copy_of_report_id": copy_of,
                    "author_sha256": account_sha256(
                        snapshot.source.source_name, comment.author.username
                    ),
                    "text_sha256": key[1],
                    "entry_id": row[0],
                    "revision_id": row[3],
                    "snapshot_path": row[4],
                    "snapshot_sha256": row[5],
                    "analysis_path": row[6],
                    "analysis_sha256": row[7],
                    "first_observed_at_utc": first_observed[(str(row[1]), comment.comment_id)],
                    "source_thread_created_utc": row[8],
                    "observed_at_utc": row[9],
                    "recorded_at_utc": row[9],
                    "observation_kind": change["kind"],
                    "edit_observed_at_utc": row[9] if change["kind"] == "edited" else None,
                    "source_edited_at_utc": None,
                    "excerpt": " ".join(comment.body.split())[:280],
                    "theme_key": themes.get(key),
                    "issue_id": memberships.get(str(row[1])),
                    "independent_person_verified": False,
                    "updates": updates,
                }
            )
    return result


def build_report(
    *,
    conn: sqlite3.Connection,
    watch_id: str,
    sweep: tuple[Any, ...],
    basis: dict[str, Any],
    evidence: list[dict[str, Any]],
    now: float,
) -> tuple[dict[str, Any], list[tuple[str, str | None]]]:
    sweep_id, outcome, summary_text, finished = sweep
    summary = json.loads(summary_text)
    discovery = summary.get("discovery", {})
    sampling = {
        key: discovery.get(key)
        for key in ("queries", "subreddits", "sort", "time_window", "per_request_limit", "limit")
    }
    basis = {
        **basis,
        "collection_basis": summary.get("collection_basis"),
        "discovery_sampling": sampling,
    }
    comparable = {
        key: basis[key]
        for key in (
            "profile_revision",
            "profile_content_sha256",
            "profile",
            "collection_basis",
            "discovery_sampling",
            "issue_policy",
            "ranking_policy",
        )
    }
    previous = conn.execute(
        "SELECT cohort_id FROM watch_alert_reports WHERE watch_id=? ORDER BY rowid DESC LIMIT 1",
        (watch_id,),
    ).fetchone()
    basis_text = canonical_json(comparable)
    prior_cohort = (
        None
        if previous is None
        else conn.execute(
            "SELECT basis_json FROM watch_alert_cohorts WHERE cohort_id=?",
            (previous[0],),
        ).fetchone()
    )
    # Reverting a setting is a new chronological cohort, not a return to an old baseline.
    cohort_id = (
        str(previous[0])
        if prior_cohort is not None and prior_cohort[0] == basis_text
        else identity(
            "aco-", [watch_id, comparable, None if previous is None else previous[0], sweep_id]
        )
    )
    window_id = identity("awi-", [watch_id, sweep_id])
    report_id = identity("alr-", [window_id, cohort_id])
    baseline_row = conn.execute(
        "SELECT report_id,window_id,payload_json FROM watch_alert_reports "
        "WHERE watch_id=? AND cohort_id=? AND status='baseline' "
        "ORDER BY rowid LIMIT 1",
        (watch_id, cohort_id),
    ).fetchone()
    baseline = (
        None
        if baseline_row is None
        else {
            "report_id": baseline_row[0],
            "window_id": baseline_row[1],
            "evidence_count": len(json.loads(baseline_row[2])["evidence"]),
        }
    )
    baseline_window = window_id if baseline is None else str(baseline["window_id"])
    known_rows = conn.execute(
        "SELECT evidence_key,source_report_id FROM watch_alert_seen WHERE watch_id=?",
        (watch_id,),
    ).fetchall()
    known = {str(row[0]) for row in known_rows}
    known_reports = {str(row[1]) for row in known_rows if row[1] is not None}
    to_record: dict[str, str | None] = {}
    alerts: list[dict[str, Any]] = []
    suppression: set[str] = set()
    refusals = {"population_denominator_unknown", "rate_trend_refused_unknown_coverage"}
    if summary.get("collection_basis") is None:
        refusals.add("collection_basis_unknown")
    if outcome in ("partial", "failed"):
        refusals.add("partial_sweep" if outcome == "partial" else "failed_sweep")
    if baseline is None:
        suppression.add("initial_baseline" if previous is None else "cohort_baseline")
    text_origins: dict[str, str] = {}
    for item in evidence:
        text_origins.setdefault(str(item["text_sha256"]), str(item["source_report_id"]))
    report_texts = {(str(item["source_report_id"]), str(item["text_sha256"])) for item in evidence}

    def event(kind: str, key: str, citation: dict[str, Any], **extra: Any) -> None:
        if key in known or key in to_record:
            return
        to_record[key] = None
        if baseline is not None and outcome != "failed":
            alerts.append(
                {
                    "alert_id": identity("alt-", [watch_id, key]),
                    "kind": kind,
                    "evidence": [citation],
                    "claims_are_verified": False,
                    **extra,
                }
            )

    for item in evidence:
        report = str(item["source_report_id"])
        text = str(item["text_sha256"])
        key = "evidence:" + report + ":" + text
        copy = (
            item["lineage"] == "exact_copy"
            and (str(item["copy_of_report_id"]), text) in report_texts
        )
        copy = copy or (len(str(item["excerpt"])) >= 40 and text_origins[text] != report)
        item["is_exact_copy"] = bool(copy)
        if copy and item["copy_of_report_id"] is None:
            item["copy_of_report_id"] = text_origins[text]
            item["lineage"] = "exact_copy"
        citation = dict(item)
        if key in known:
            suppression.add("unchanged_evidence")
        elif copy:
            suppression.add("exact_copy_not_independent_evidence")
        elif baseline is not None and outcome != "failed":
            alerts.append(
                {
                    "alert_id": identity("alt-", [watch_id, key]),
                    "kind": "evidence_update" if report in known_reports else "new_evidence",
                    "evidence": [citation],
                    "claims_are_verified": False,
                    "interpretation": (
                        "newly observed cited evidence, not a population demand increase"
                    ),
                }
            )
        to_record[key] = report
        known_reports.add(report)
        community_key = "community:" + str(item["community"]).casefold()
        if not copy:
            event(
                "new_community",
                community_key,
                citation,
                interpretation="another observed community in this watch; not independent people",
            )
            for update in item["updates"]:
                event(
                    str(update["kind"]),
                    "update:" + str(update["update_id"]),
                    citation,
                    update_id=update["update_id"],
                    cue=update["cue"],
                    interpretation=(
                        "unverified source claim; no inferred reproduction or resolution"
                    ),
                )
    if not evidence:
        suppression.add("no_cited_evidence")
    if baseline is not None and not alerts:
        suppression.add("no_new_evidence")
    acquisitions = conn.execute(
        "SELECT completeness,parsed_comment_count,reported_comment_count,"
        "absent_known_comment_count "
        "FROM watch_acquisitions WHERE run_id=?",
        (sweep_id,),
    ).fetchall()
    if discovery.get("truncated"):
        refusals.add("discovery_truncated")
    if any(row[0] == "truncated" for row in acquisitions):
        refusals.add("comment_coverage_truncated")
    if not acquisitions or any(row[0] == "unknown" for row in acquisitions):
        refusals.add("comment_coverage_unknown")
    coverage = {
        "search_completeness": discovery.get("search_completeness", "unknown"),
        "comment_completeness": sorted({str(row[0]) for row in acquisitions}) or ["unknown"],
        "sweep_outcome": outcome,
        "rate_trend_eligible": False,
        "failed_queries": discovery.get("failed_queries", []),
        "failures": summary.get("failures", []),
        "refresh": summary.get("refresh", {}),
        "analysis": summary.get("analysis", {}),
        "absence_interpretation": "not evidence of deletion, resolution, or reduced demand",
    }
    denominators = {
        "population_denominator": None,
        "independent_people_denominator": None,
        "discovered_candidates": discovery.get("persisted_count"),
        "acquired_threads": len(acquisitions),
        "parsed_comments": sum(int(row[1]) for row in acquisitions),
        "source_reported_comments": sum(int(row[2]) for row in acquisitions),
        "absent_known_comments": sum(int(row[3]) for row in acquisitions),
        "cited_evidence_versions": len(evidence),
        "interpretation": (
            "bounded observations only; neither candidates nor comments are population denominators"
        ),
    }
    status = (
        "unavailable"
        if outcome == "failed"
        else "baseline"
        if baseline is None
        else "compared"
        if alerts
        else "no_changes"
    )
    if baseline is None and status == "baseline":
        baseline = {"report_id": report_id, "window_id": window_id, "evidence_count": len(evidence)}
    payload = {
        "artifact_type": "watch_alert_report",
        "schema_version": 1,
        "watch_id": watch_id,
        "report_id": report_id,
        "cohort_id": cohort_id,
        "window_id": window_id,
        "baseline_window_id": baseline_window,
        "baseline": baseline,
        "status": status,
        "created_at_utc": now,
        "window": {
            "sweep_run_id": sweep_id,
            "finished_at_utc": finished,
            "comparison": "durable cumulative evidence through this bounded sweep",
        },
        "basis": basis,
        "coverage": coverage,
        "bounded_denominators": denominators,
        "alerts": alerts,
        "alert_ids": [item["alert_id"] for item in alerts],
        "evidence": evidence,
        "suppression_reasons": sorted(suppression),
        "refusal_reasons": sorted(refusals),
    }
    conn.execute(
        "INSERT OR IGNORE INTO watch_alert_cohorts VALUES (?,?,?,?)",
        (cohort_id, watch_id, basis_text, now),
    )
    return payload, [] if outcome == "failed" else list(to_record.items())
