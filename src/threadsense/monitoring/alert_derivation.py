"""Target-complete, admitted evidence expansion and checkpointed alert decisions.

Source order is not citation order.  Gathering, snapshot expansion, and classification
are separate durable phases: in particular no prefix of a target can establish that
an explicitly copied report is independent evidence.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from threadsense.errors import MonitoringStateError
from threadsense.models.canonical import Thread, canonical_thread_from_payload
from threadsense.monitoring import alert_projection_work as work
from threadsense.monitoring.alert_budget import (
    AdmissionStop,
    AlertBudget,
    AlertSession,
    read_json_text,
    text_size,
)
from threadsense.monitoring.alert_projection import require_projection
from threadsense.monitoring.alert_records import canonical_json, identity
from threadsense.monitoring.alert_sources import get_version

DERIVATION_SCHEMA_STATEMENTS: tuple[str, ...] = (
    """CREATE TABLE alert_derivation_targets (
        target_id TEXT PRIMARY KEY, watch_id TEXT NOT NULL, highwater INTEGER NOT NULL,
        basis_sha256 TEXT NOT NULL, phase TEXT NOT NULL, source_cursor INTEGER NOT NULL,
        evidence_count INTEGER NOT NULL DEFAULT 0, event_count INTEGER NOT NULL DEFAULT 0,
        acquired_threads INTEGER NOT NULL DEFAULT 0, parsed_comments INTEGER NOT NULL DEFAULT 0,
        source_reported_comments INTEGER NOT NULL DEFAULT 0,
        absent_known_comments INTEGER NOT NULL DEFAULT 0,
        comment_coverage_truncated INTEGER NOT NULL DEFAULT 0,
        comment_coverage_unknown INTEGER NOT NULL DEFAULT 0)""",
    """CREATE TABLE alert_target_acquisitions (
        target_id TEXT NOT NULL, acquisition_id TEXT NOT NULL,
        PRIMARY KEY(target_id,acquisition_id))""",
    """CREATE TABLE alert_presence_complete (
        target_id TEXT PRIMARY KEY, highwater INTEGER NOT NULL, basis_sha256 TEXT NOT NULL)""",
    """CREATE TRIGGER alert_presence_complete_no_update BEFORE UPDATE ON alert_presence_complete
        BEGIN SELECT RAISE(ABORT, 'alert presence is immutable'); END""",
    """CREATE TRIGGER alert_presence_complete_no_delete BEFORE DELETE ON alert_presence_complete
        BEGIN SELECT RAISE(ABORT, 'alert presence is immutable'); END""",
    """CREATE TABLE alert_target_entries (
        target_id TEXT NOT NULL, entry_id TEXT NOT NULL, source_thread_id TEXT NOT NULL,
        revision_id TEXT NOT NULL, observed REAL NOT NULL, revision_sequence INTEGER NOT NULL,
        stage INTEGER NOT NULL DEFAULT 0, comment_cursor INTEGER NOT NULL DEFAULT 0,
        novel_cursor INTEGER NOT NULL DEFAULT 0, info_json TEXT,
        PRIMARY KEY(target_id,entry_id))""",
    """CREATE INDEX alert_target_entries_work ON alert_target_entries
        (target_id,stage,observed,revision_sequence,entry_id)""",
    """CREATE TABLE alert_target_comments (
        target_id TEXT NOT NULL, entry_id TEXT NOT NULL, comment_id TEXT NOT NULL,
        payload_json TEXT NOT NULL, PRIMARY KEY(target_id,entry_id,comment_id))""",
    """CREATE TABLE alert_target_memberships (
        target_id TEXT NOT NULL, source_thread_id TEXT NOT NULL, event_id INTEGER NOT NULL,
        issue_id TEXT NOT NULL, PRIMARY KEY(target_id,source_thread_id))""",
    """CREATE TABLE alert_target_observations (
        target_id TEXT NOT NULL, source_thread_id TEXT NOT NULL, first_observed REAL NOT NULL,
        PRIMARY KEY(target_id,source_thread_id))""",
    """CREATE TABLE alert_target_themes (
        target_id TEXT NOT NULL, report_id TEXT NOT NULL, text_sha256 TEXT NOT NULL,
        recorded REAL NOT NULL, entry_id TEXT NOT NULL, theme_key TEXT,
        PRIMARY KEY(target_id,report_id,text_sha256))""",
    """CREATE TABLE alert_target_evidence (
        target_id TEXT NOT NULL, evidence_key TEXT NOT NULL, report_id TEXT NOT NULL,
        text_sha256 TEXT NOT NULL, observed REAL NOT NULL, revision_sequence INTEGER NOT NULL,
        entry_id TEXT NOT NULL, slot INTEGER NOT NULL, citation_json TEXT NOT NULL,
        raw_sha256 TEXT NOT NULL, copy_of_report_id TEXT, community_key TEXT NOT NULL,
        done INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(target_id,evidence_key),
        UNIQUE(target_id,report_id,text_sha256))""",
    """CREATE INDEX alert_target_evidence_work ON alert_target_evidence
        (target_id,done,observed,revision_sequence,entry_id,slot,evidence_key)""",
    """CREATE INDEX alert_target_text_origins ON alert_target_evidence
        (target_id,text_sha256,observed,revision_sequence,entry_id,slot,report_id)""",
    """CREATE INDEX alert_target_text_dependencies ON alert_target_evidence
        (target_id,text_sha256,evidence_key)""",
    """CREATE INDEX alert_target_copy_dependencies ON alert_target_evidence
        (target_id,copy_of_report_id,text_sha256,evidence_key)""",
    """CREATE TABLE alert_target_revisits (
        target_id TEXT NOT NULL, kind TEXT NOT NULL, report_id TEXT NOT NULL,
        text_sha256 TEXT NOT NULL, cursor TEXT NOT NULL DEFAULT '',
        done INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(target_id,kind,report_id,text_sha256))""",
    """CREATE INDEX alert_target_revisits_work ON alert_target_revisits
        (target_id,done,kind,report_id,text_sha256)""",
    """CREATE TABLE alert_text_origin_projection (
        watch_id TEXT NOT NULL, text_sha256 TEXT NOT NULL, report_id TEXT NOT NULL,
        checkpoint_id TEXT NOT NULL, PRIMARY KEY(watch_id,text_sha256))""",
    """CREATE TABLE alert_evidence_projection (
        watch_id TEXT NOT NULL, evidence_key TEXT NOT NULL, report_id TEXT NOT NULL,
        citation_sha256 TEXT NOT NULL, raw_sha256 TEXT NOT NULL, checkpoint_id TEXT NOT NULL,
        PRIMARY KEY(watch_id,evidence_key))""",
    """CREATE TABLE alert_seen_projection (
        watch_id TEXT NOT NULL, evidence_key TEXT NOT NULL, source_report_id TEXT,
        checkpoint_id TEXT NOT NULL, PRIMARY KEY(watch_id,evidence_key))""",
    """CREATE TABLE alert_known_reports (
        watch_id TEXT NOT NULL, report_id TEXT NOT NULL, PRIMARY KEY(watch_id,report_id))""",
    """CREATE TABLE alert_derivation_decisions (
        target_id TEXT NOT NULL, evidence_key TEXT NOT NULL, checkpoint_id TEXT NOT NULL,
        citation_sha256 TEXT NOT NULL, overlay INTEGER NOT NULL,
        event_count INTEGER NOT NULL, PRIMARY KEY(target_id,evidence_key))""",
    """CREATE TABLE alert_v2_event_identities (
        watch_id TEXT NOT NULL, identity_key TEXT NOT NULL, alert_id TEXT NOT NULL,
        target_id TEXT NOT NULL, checkpoint_id TEXT NOT NULL,
        PRIMARY KEY(watch_id,identity_key))""",
    """CREATE TABLE alert_derivation_seen (
        target_id TEXT NOT NULL, evidence_key TEXT NOT NULL, source_report_id TEXT,
        checkpoint_id TEXT NOT NULL, PRIMARY KEY(target_id,evidence_key))""",
    *tuple(
        f"CREATE TRIGGER {table}_no_{operation.lower()} BEFORE {operation} ON {table} "
        "BEGIN SELECT RAISE(ABORT, 'alert decisions are append-only'); END"
        for table in (
            "alert_derivation_decisions",
            "alert_v2_event_identities",
            "alert_derivation_seen",
        )
        for operation in ("UPDATE", "DELETE")
    ),
)


@dataclass(frozen=True)
class DerivedUnit:
    key: str
    citation: dict[str, Any]
    events: list[dict[str, Any]]
    seen: list[tuple[str, str | None]]
    overlay: bool


def _fail(reason: str, **details: Any) -> MonitoringStateError:
    return MonitoringStateError(
        "alert derivation cannot continue", details={"reason": reason, **details}
    )


def _binding(session: AlertSession, target: dict[str, Any]) -> tuple[str, int, str]:
    if str(target["watch_id"]) != session.watch_id:
        raise _fail("alert_target_watch_mismatch")
    return str(target["target_id"]), int(target["highwater"]), str(target["basis_sha256"])


def _barrier(session: AlertSession, target: dict[str, Any]) -> None:
    target_id, highwater, basis = _binding(session, target)
    require_projection(session, target)
    row = session.conn.execute(
        "SELECT watch_id,highwater,basis_sha256,expected_target,"
        "expected_highwater,inherited_sha256 "
        "FROM alert_presence_bindings WHERE target_id=?",
        (target_id,),
    ).fetchone()
    inherited = target["inherited_coverage"]
    if row is None or tuple(row) != (
        session.watch_id,
        highwater,
        basis,
        inherited["target_id"],
        inherited["highwater"],
        identity("ainherited-", inherited),
    ):
        raise _fail("alert_presence_incomplete", target_id=target_id)


def _version(
    session: AlertSession,
    target: dict[str, Any],
    budget: AlertBudget,
    kind: str,
    key: list[Any],
    *,
    required: bool = True,
) -> dict[str, Any] | None:
    value = get_version(session, kind, canonical_json(key), int(target["highwater"]), budget)
    if value is None and required:
        raise _fail("alert_source_version_missing", source_kind=kind, source_key=key)
    if value is not None and value.get("watch_id", session.watch_id) != session.watch_id:
        raise _fail("alert_source_watch_mismatch", source_kind=kind, source_key=key)
    return value


def _required(value: dict[str, Any] | None) -> dict[str, Any]:
    if value is None:
        raise _fail("alert_source_version_missing")
    return value


def _decode(text: str, budget: AlertBudget, unit: str) -> dict[str, Any]:
    # Six bytes per character bounds UTF-8 plus JSON escape processing before decoding.
    budget.admit(unit, input_bytes=len(text) * 6)
    try:
        result = json.loads(text)
    except (TypeError, ValueError) as error:
        raise _fail("alert_source_unreadable", unit=unit) from error
    if not isinstance(result, dict):
        raise _fail("alert_source_unreadable", unit=unit)
    return result


def _json_bound(value: Any) -> int:
    """Allocation-free conservative JSON byte bound for an already admitted unit."""
    if isinstance(value, str):
        return 6 * len(value) + 2
    if isinstance(value, dict):
        return 2 + sum(_json_bound(key) + _json_bound(item) + 2 for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return 2 + sum(_json_bound(item) + 1 for item in value)
    return 32


def _encode(value: dict[str, Any], budget: AlertBudget, unit: str) -> str:
    budget.admit(unit, input_bytes=_json_bound(value))
    return canonical_json(value)


def _read_snapshot(
    session: AlertSession,
    revision: dict[str, Any],
    analysis: dict[str, Any],
    budget: AlertBudget,
) -> Thread:
    unit = "alert-snapshot:" + str(revision["revision_id"])
    try:
        root = session.root.resolve()
        snapshot_path = (root / str(revision["snapshot_path"])).resolve()
        analysis_path = (root / str(analysis["analysis_path"])).resolve()
        if not snapshot_path.is_relative_to(root) or not analysis_path.is_relative_to(root):
            raise _fail("artifact_outside_state", unit=unit)
        snapshot_size = snapshot_path.stat().st_size
        analysis_size = analysis_path.stat().st_size
        # Admit both leaves together.  Individually small leaves must not cause an
        # unchanged-prefix retry when their indivisible combined unit is oversized.
        budget.admit(unit, input_bytes=4 * snapshot_size + 2 * analysis_size)
        session.check()
        snapshot_bytes = snapshot_path.read_bytes()
        analysis_bytes = analysis_path.read_bytes()
        if len(snapshot_bytes) != snapshot_size or len(analysis_bytes) != analysis_size:
            raise _fail("alert_artifact_changed", unit=unit)
        if (
            hashlib.sha256(snapshot_bytes).hexdigest() != revision["snapshot_sha256"]
            or hashlib.sha256(analysis_bytes).hexdigest() != analysis["analysis_sha256"]
        ):
            raise _fail("alert_artifact_hash_mismatch", unit=unit)
        payload = json.loads(snapshot_bytes)
        if not isinstance(payload, dict):
            raise _fail("alert_source_unreadable", unit=unit)
        result = canonical_thread_from_payload(payload)
        if result.source.source_thread_id != revision["source_thread_id"]:
            raise _fail("alert_evidence_mismatch", unit=unit)
        session.check()
        return result
    except OSError as error:
        raise _fail("alert_artifact_unreadable", unit=unit) from error
    except (ValueError, TypeError, KeyError) as error:
        raise _fail("alert_source_unreadable", unit=unit) from error


def _gather_one(
    session: AlertSession, target: dict[str, Any], budget: AlertBudget, after: int
) -> bool:
    return work.gather_one(session, target, budget, after)


def _entry_sources(
    session: AlertSession,
    target: dict[str, Any],
    budget: AlertBudget,
    entry_id: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    entry = _required(_version(session, target, budget, "watch_digest_entries", [entry_id]))
    revision = _required(
        _version(session, target, budget, "watch_thread_revisions", [entry["revision_id"]])
    )
    analysis = _required(
        _version(session, target, budget, "watch_revision_analyses", [entry["revision_id"]])
    )
    acquisition = _required(
        _version(session, target, budget, "watch_acquisitions", [revision["acquisition_id"]])
    )
    return entry, revision, analysis, acquisition


def _metadata_text(
    session: AlertSession,
    budget: AlertBudget,
    table: str,
    column: str,
    where: str,
    values: tuple[Any, ...],
    unit: str,
) -> str | None:
    # All identifiers/clauses are private constants; source values stay parameterized.
    budget.admit(unit, source_rows=1, input_bytes=64)
    row = session.conn.execute(
        f"SELECT rowid,typeof({column}) FROM {table} WHERE {where}", values
    ).fetchone()
    if row is None or row[1] == "null":
        return None
    size = text_size(session.conn, table, column, int(row[0]))
    budget.admit(unit, input_bytes=size)
    result = session.conn.execute(
        f"SELECT {column} FROM {table} WHERE rowid=?", (int(row[0]),)
    ).fetchone()
    if result is None:
        raise _fail("alert_metadata_missing", unit=unit)
    return str(result[0])


def _membership(
    session: AlertSession, target_id: str, thread_id: str, budget: AlertBudget
) -> str | None:
    return _metadata_text(
        session,
        budget,
        "alert_watch_memberships",
        "issue_id",
        "watch_id=? AND source_thread_id=?",
        (session.watch_id, thread_id),
        "alert-membership:" + thread_id,
    )


class _AdmissionUnit:
    """Translate an indivisible cumulative shortfall, not just its final subread."""

    def __init__(self, budget: AlertBudget, name: str):
        self.budget = budget
        self.name = name
        self.start = dict(budget.used)

    def __enter__(self) -> _AdmissionUnit:
        return self

    def checkpoint(self) -> None:
        self.start = dict(self.budget.used)

    def __exit__(self, error_type: Any, error: Any, traceback: Any) -> None:
        if isinstance(error, AdmissionStop):
            dimension = str(error.details["reason"])
            required = (
                int(error.details["required"]) + self.budget.used[dimension] - self.start[dimension]
            )
            configured = int(getattr(self.budget, dimension))
            if required > configured:
                raise AdmissionStop(
                    "resource_refused",
                    self.name + ":" + str(error.details["unit"]),
                    dimension,
                    required,
                    configured,
                ) from error


def _account(source: str, username: str, budget: AlertBudget) -> str | None:
    from threadsense.monitoring.issues import account_sha256

    budget.admit("alert-source-account", input_bytes=12 * (len(source) + len(username) + 1))
    return account_sha256(source, username)


def _updates(report: str, text: str, body: str, budget: AlertBudget) -> list[dict[str, Any]]:
    # Local imports preserve MonitoringStore's existing import-cycle boundary.
    from threadsense.monitoring.issue_records import update_id_for
    from threadsense.monitoring.issues import detect_update_cues

    budget.admit("alert-update-cues:" + report, derivations=4, input_bytes=16 * len(body) + 2048)

    return [
        {"update_id": update_id_for(report, text, kind), "kind": kind, "cue": cue}
        for kind, cue in detect_update_cues(body)
    ]


def _start_entry(
    session: AlertSession, target: dict[str, Any], budget: AlertBudget, entry_id: str
) -> None:
    work.start_entry(session, target, budget, entry_id)


def _candidate_text(
    target: dict[str, Any],
    citation: dict[str, Any],
    budget: AlertBudget,
) -> tuple[str, str]:
    policy = target.get("basis", {}).get("issue_policy")
    budget.admit("alert-candidate-policy", input_bytes=_json_bound(policy) * 2)
    citation["issue_policy_sha256"] = identity("aip-", policy)
    encoded = _encode(citation, budget, str(citation["evidence_key"]))
    budget.admit(
        "alert-candidate-projection:" + str(citation["evidence_key"]),
        source_rows=4,
        input_bytes=8 * len(encoded) + 512,
    )
    return encoded, hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _insert_candidate(
    session: AlertSession,
    target: dict[str, Any],
    citation: dict[str, Any],
    revision_sequence: int,
    slot: int,
    citation_text: str,
    raw_digest: str,
) -> None:
    work.insert_candidate(
        session, target, citation, revision_sequence, slot, citation_text, raw_digest
    )


def _index_comments(
    session: AlertSession,
    target: dict[str, Any],
    budget: AlertBudget,
    entry_id: str,
    cursor: int,
    admission: _AdmissionUnit,
) -> None:
    entry, revision, analysis, _ = _entry_sources(session, target, budget, entry_id)
    snapshot = _read_snapshot(session, revision, analysis, budget)
    for position in range(cursor, len(snapshot.comments)):
        session.check()
        comment = snapshot.comments[position]
        budget.admit("alert-comment-index:" + entry_id + ":" + str(position), derivations=1)
        payload = {
            "comment_id": comment.comment_id,
            "body": comment.body,
            "username": comment.author.username,
            "permalink": comment.permalink,
            "created_utc": comment.created_utc,
        }
        encoded = _encode(payload, budget, "alert-comment-index:" + entry_id)
        with session.fence():
            require_projection(session, target)
            session.conn.execute(
                "INSERT INTO alert_watch_comments VALUES (?,?,?,?) "
                "ON CONFLICT(watch_id,entry_id,comment_id) "
                "DO UPDATE SET payload_json=excluded.payload_json",
                (session.watch_id, entry_id, comment.comment_id, encoded),
            )
            session.conn.execute(
                "UPDATE alert_watch_entries SET comment_cursor=? WHERE watch_id=? AND entry_id=?",
                (position + 1, session.watch_id, entry_id),
            )
        admission.checkpoint()
    with session.fence():
        require_projection(session, target)
        session.conn.execute(
            "UPDATE alert_watch_entries SET stage=2 WHERE watch_id=? AND entry_id=?",
            (session.watch_id, entry["entry_id"]),
        )


def _expand_novel(
    session: AlertSession,
    target: dict[str, Any],
    budget: AlertBudget,
    entry_id: str,
    cursor: int,
    info_rowid: int,
    admission: _AdmissionUnit,
) -> None:
    from threadsense.monitoring.issue_records import report_id_for
    from threadsense.monitoring.observations import comment_text_sha256

    entry = _required(_version(session, target, budget, "watch_digest_entries", [entry_id]))
    revision = _required(
        _version(session, target, budget, "watch_thread_revisions", [entry["revision_id"]])
    )
    payload = _decode(str(entry["payload_json"]), budget, "alert-entry:" + entry_id)
    info = read_json_text(
        session.conn,
        "alert_watch_entries",
        "info_json",
        info_rowid,
        budget,
        "alert-entry-info:" + entry_id,
    )
    changes = payload["novel_comments"]
    if not isinstance(changes, list) or not isinstance(info, dict):
        raise _fail("alert_source_unreadable", entry_id=entry_id)
    for position in range(cursor, len(changes)):
        change = changes[position]
        budget.admit("alert-novel:" + entry_id + ":" + str(position), derivations=1, source_rows=1)
        comment_id = str(change["comment_id"])
        handle = session.conn.execute(
            "SELECT rowid FROM alert_watch_comments "
            "WHERE watch_id=? AND entry_id=? AND comment_id=?",
            (session.watch_id, entry_id, comment_id),
        ).fetchone()
        if handle is None:
            raise _fail("alert_evidence_mismatch", entry_id=entry_id, comment_id=comment_id)
        comment = read_json_text(
            session.conn,
            "alert_watch_comments",
            "payload_json",
            int(handle[0]),
            budget,
            "alert-comment:" + comment_id,
        )
        if not isinstance(comment, dict):
            raise _fail("alert_source_unreadable", entry_id=entry_id)
        body = str(comment["body"])
        budget.admit("alert-comment-text:" + comment_id, input_bytes=16 * len(body))
        text = str(change["text_sha256"])
        if comment_text_sha256(body) != text:
            raise _fail("alert_evidence_mismatch", entry_id=entry_id, comment_id=comment_id)
        budget.admit(
            "alert-comment-report:" + comment_id,
            input_bytes=12
            * (len(session.watch_id) + len(str(entry["source_thread_id"])) + len(comment_id)),
        )
        report = report_id_for(session.watch_id, str(entry["source_thread_id"]), comment_id)
        lineage = _version(session, target, budget, "watch_issue_reports", [report], required=False)
        observed = _required(
            _version(
                session,
                target,
                budget,
                "watch_comments",
                [session.watch_id, entry["source_thread_id"], comment_id],
            )
        )
        theme = _metadata_text(
            session,
            budget,
            "alert_watch_themes",
            "theme_key",
            "watch_id=? AND report_id=? AND text_sha256=?",
            (session.watch_id, report, text),
            "alert-theme:" + report,
        )
        citation = {
            key: value
            for key, value in info.items()
            if not key.startswith("_") and key != "source_name"
        }
        citation.update(
            {
                "evidence_key": "evidence:" + report + ":" + text,
                "source_report_id": report,
                "source_kind": "comment",
                "comment_id": comment_id,
                "permalink": comment["permalink"],
                "source_created_utc": comment["created_utc"],
                "lineage": "firsthand" if lineage is None else lineage["lineage"],
                "copy_of_report_id": None if lineage is None else lineage["copy_of_report_id"],
                "author_sha256": _account(
                    str(info["source_name"]), str(comment["username"]), budget
                ),
                "text_sha256": text,
                "first_observed_at_utc": observed["first_observed_at_utc"],
                "observation_kind": change["kind"],
                "edit_observed_at_utc": info["observed_at_utc"]
                if change["kind"] == "edited"
                else None,
                "source_edited_at_utc": None,
                "excerpt": " ".join(body.split())[:280],
                "theme_key": theme,
                "issue_id": _membership(
                    session, str(target["target_id"]), str(entry["source_thread_id"]), budget
                ),
                "independent_person_verified": False,
                "updates": _updates(report, text, body, budget),
            }
        )
        encoded, raw_digest = _candidate_text(target, citation, budget)
        with session.fence():
            require_projection(session, target)
            _insert_candidate(
                session,
                target,
                citation,
                int(revision["sequence"]),
                position + 1,
                encoded,
                raw_digest,
            )
            session.conn.execute(
                "UPDATE alert_watch_entries SET novel_cursor=? WHERE watch_id=? AND entry_id=?",
                (position + 1, session.watch_id, entry_id),
            )
        admission.checkpoint()
    with session.fence():
        require_projection(session, target)
        session.conn.execute(
            "UPDATE alert_watch_entries SET stage=3 WHERE watch_id=? AND entry_id=?",
            (session.watch_id, entry_id),
        )


def _revisit_one(session: AlertSession, target: dict[str, Any], budget: AlertBudget) -> bool:
    return work.revisit_one(session, target, budget)


def prepare_presence(session: AlertSession, target: dict[str, Any], budget: AlertBudget) -> bool:
    """Resume the watch's bounded delta/index/revisit work before classification."""
    session.check()
    target_id, highwater, basis = _binding(session, target)
    with _AdmissionUnit(budget, "alert-presence:" + target_id) as admission:
        budget.admit("alert-presence-control:" + target_id, source_rows=4, input_bytes=1024)
        work.initialize(session, target, budget)
        state = session.conn.execute(
            "SELECT highwater,basis_sha256,phase,source_cursor "
            "FROM alert_derivation_targets WHERE target_id=?",
            (target_id,),
        ).fetchone()
        if state is None or (int(state[0]), str(state[1])) != (highwater, basis):
            raise _fail("alert_target_binding_mismatch", target_id=target_id)
        if state[2] == "ready":
            _barrier(session, target)
            return True
        if state[2] == "gather":
            return _gather_one(session, target, budget, int(state[3]))
        if state[2] == "revisit":
            return _revisit_one(session, target, budget)
        budget.admit("alert-expand-select:" + target_id, source_rows=4, input_bytes=256)
        selected = None
        for stage in (0, 1, 2, 4):
            selected = session.conn.execute(
                "SELECT entry_id,stage,comment_cursor,novel_cursor,rowid "
                "FROM alert_watch_entries WHERE watch_id=? AND stage=? "
                "ORDER BY observed,revision_sequence,entry_id LIMIT 1",
                (session.watch_id, stage),
            ).fetchone()
            if selected is not None:
                break
        if selected is None:
            with session.fence():
                require_projection(session, target)
                session.conn.execute(
                    "UPDATE alert_derivation_targets SET phase='revisit' WHERE target_id=?",
                    (target_id,),
                )
            return False
        if int(selected[1]) in (0, 4):
            _start_entry(session, target, budget, str(selected[0]))
        elif int(selected[1]) == 1:
            _index_comments(session, target, budget, str(selected[0]), int(selected[2]), admission)
        else:
            _expand_novel(
                session,
                target,
                budget,
                str(selected[0]),
                int(selected[3]),
                int(selected[4]),
                admission,
            )
        return False


def _known(session: AlertSession, key: str) -> bool:
    return (
        session.conn.execute(
            "SELECT 1 FROM alert_seen_projection WHERE watch_id=? AND evidence_key=?",
            (session.watch_id, key),
        ).fetchone()
        is not None
    )


def _classify(
    session: AlertSession,
    target: dict[str, Any],
    citation: dict[str, Any],
    budget: AlertBudget,
) -> tuple[list[dict[str, Any]], list[tuple[str, str | None]]]:
    target_id = str(target["target_id"])
    report, text = str(citation["source_report_id"]), str(citation["text_sha256"])
    budget.admit(
        "alert-origin-presence:" + str(citation["evidence_key"]), source_rows=3, input_bytes=256
    )
    _barrier(session, target)
    origin = work.origin(session, text, float(target["cutoff"]))
    if origin is None:
        raise _fail("alert_text_origin_missing", evidence_key=citation["evidence_key"])
    explicit = (
        citation["lineage"] == "exact_copy"
        and session.conn.execute(
            "SELECT 1 FROM alert_watch_evidence "
            "WHERE watch_id=? AND report_id=? AND text_sha256=? AND eligible=1 AND observed<=?",
            (session.watch_id, citation["copy_of_report_id"], text, float(target["cutoff"])),
        ).fetchone()
        is not None
    )
    copied = explicit or (len(str(citation["excerpt"])) >= 40 and str(origin[0]) != report)
    citation["is_exact_copy"] = copied
    if copied and citation["copy_of_report_id"] is None:
        citation["copy_of_report_id"] = str(origin[0])
        citation["lineage"] = "exact_copy"
    policy = target.get("basis", {}).get("issue_policy")
    budget.admit("alert-policy-provenance:" + target_id, input_bytes=_json_bound(policy) * 2)
    # Policy provenance is a bounded frozen reference, never an all-history policy list.
    citation["issue_policy_sha256"] = identity("aip-", policy)
    events: list[dict[str, Any]] = []
    seen: list[tuple[str, str | None]] = []
    enabled = target.get("baseline") is not None and target.get("outcome") != "failed"
    can_record = target.get("outcome") != "failed"

    def reserve(key: str, kind: str, source_report: str | None = None, **extra: Any) -> None:
        budget.admit(
            "alert-identity:" + key, source_rows=1, derivations=1, input_bytes=128 + len(key) * 12
        )
        if _known(session, key):
            return
        if can_record:
            seen.append((key, source_report))
        if enabled and (kind not in ("new_evidence", "evidence_update") or not copied):
            # At most one community, one evidence, and four fixed cue events per unit.
            budget.admit(
                "alert-event:" + key, input_bytes=_json_bound(citation) + _json_bound(extra)
            )
            events.append(
                {
                    "alert_id": identity("alt-", [session.watch_id, key]),
                    "kind": kind,
                    "evidence": [citation],
                    "claims_are_verified": False,
                    **extra,
                }
            )

    prior_report = session.conn.execute(
        "SELECT 1 FROM alert_known_reports WHERE watch_id=? AND report_id=?",
        (session.watch_id, report),
    ).fetchone()
    reserve(
        str(citation["evidence_key"]),
        "new_evidence" if prior_report is None else "evidence_update",
        report,
        interpretation="newly observed cited evidence, not a population demand increase",
    )
    reserve(
        "community:" + str(citation["community"]).casefold(),
        "new_community",
        interpretation="another observed community in this watch; not independent people",
    )
    if not copied:
        for update in citation["updates"]:
            reserve(
                "update:" + str(update["update_id"]),
                str(update["kind"]),
                update_id=update["update_id"],
                cue=update["cue"],
                interpretation="unverified source claim; no inferred reproduction or resolution",
            )
    return events, seen


def next_derivation(
    session: AlertSession, target: dict[str, Any], budget: AlertBudget
) -> DerivedUnit | None:
    """Return a bounded new citation or affected provenance replacement.

    Unchanged candidates are admitted revisits with durable key completion.  They do
    not rewrite artifact prefixes, and missing copied-community identities are still
    reconsidered independently of evidence suppression.
    """
    session.check()
    with _AdmissionUnit(budget, "alert-derivation:" + str(target["target_id"])) as admission:
        budget.admit("alert-presence-check", source_rows=2, input_bytes=1024)
        _barrier(session, target)
        while True:
            budget.admit(
                "alert-derivation-select:" + str(target["target_id"]), source_rows=1, derivations=1
            )
            row = session.conn.execute(
                "SELECT rowid,evidence_key FROM alert_watch_evidence WHERE watch_id=? AND done=0 "
                "ORDER BY observed,revision_sequence,entry_id,slot,evidence_key LIMIT 1",
                (session.watch_id,),
            ).fetchone()
            if row is None:
                return None
            citation = read_json_text(
                session.conn,
                "alert_watch_evidence",
                "citation_json",
                int(row[0]),
                budget,
                "alert-citation:" + str(row[1]),
            )
            if not isinstance(citation, dict):
                raise _fail("alert_citation_unreadable", evidence_key=row[1])
            if not work.citation_eligible(session, target, citation, budget):
                with session.fence():
                    _barrier(session, target)
                    session.conn.execute(
                        "UPDATE alert_watch_evidence SET done=1 "
                        "WHERE watch_id=? AND evidence_key=?",
                        (session.watch_id, row[1]),
                    )
                admission.checkpoint()
                continue
            events, seen = _classify(session, target, citation, budget)
            encoded = _encode(citation, budget, "alert-decision:" + str(row[1]))
            budget.admit("alert-decision-hash:" + str(row[1]), input_bytes=len(encoded) * 8)
            digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
            budget.admit("alert-prior-decision:" + str(row[1]), source_rows=1, input_bytes=64)
            prior = session.conn.execute(
                "SELECT citation_sha256 FROM alert_evidence_projection "
                "WHERE watch_id=? AND evidence_key=?",
                (session.watch_id, row[1]),
            ).fetchone()
            if prior is None or str(prior[0]) != digest or events or seen:
                return DerivedUnit(str(row[1]), citation, events, seen, prior is not None)
            budget.admit("alert-unchanged-commit:" + str(row[1]), source_rows=1, input_bytes=256)
            with session.fence():
                _barrier(session, target)
                session.conn.execute(
                    "UPDATE alert_watch_evidence SET done=1 "
                    "WHERE watch_id=? AND evidence_key=? AND done=0",
                    (session.watch_id, row[1]),
                )
            admission.checkpoint()


def prepare_derivation_commit(
    session: AlertSession, target: dict[str, Any], unit: DerivedUnit, budget: AlertBudget
) -> tuple[str, dict[str, str]]:
    """Prepare the indivisible publication control unit before entering its fence."""
    if len(unit.events) > 6 or len(unit.seen) > 6:
        raise _fail("alert_derivation_unit_invalid", evidence_key=unit.key)
    budget.admit(
        "alert-commit:" + unit.key,
        # Point reads only; INSERT/UPDATE parameters are charged as input below.
        source_rows=8,
        derivations=1,
        input_bytes=_json_bound(unit.citation) * 4
        + _json_bound(unit.events)
        + _json_bound(unit.seen) * 3
        + 2048,
    )
    session.check()
    _barrier(session, target)
    digest = hashlib.sha256(canonical_json(unit.citation).encode("utf-8")).hexdigest()
    event_keys = (
        {identity("alt-", [session.watch_id, key]): key for key, _ in unit.seen}
        if unit.events
        else {}
    )
    return digest, event_keys


def commit_derivation(
    session: AlertSession,
    target: dict[str, Any],
    unit: DerivedUnit,
    checkpoint_id: str,
    *,
    prepared_digest: str,
    prepared_event_keys: dict[str, str],
) -> None:
    """Commit prepared control inside the engine's expected-old publication fence."""
    session.check()
    _barrier(session, target)
    row = session.conn.execute(
        "SELECT done,raw_sha256 FROM alert_watch_evidence WHERE watch_id=? AND evidence_key=?",
        (session.watch_id, unit.key),
    ).fetchone()
    if row is None or int(row[0]):
        raise _fail("alert_derivation_cursor_changed", evidence_key=unit.key)
    digest = prepared_digest
    session.conn.execute(
        "INSERT INTO alert_derivation_decisions VALUES (?,?,?,?,?,?)",
        (target["target_id"], unit.key, checkpoint_id, digest, int(unit.overlay), len(unit.events)),
    )
    first_publication = (
        session.conn.execute(
            "SELECT 1 FROM alert_evidence_projection WHERE watch_id=? AND evidence_key=?",
            (session.watch_id, unit.key),
        ).fetchone()
        is None
    )
    session.conn.execute(
        "INSERT INTO alert_evidence_projection VALUES (?,?,?,?,?,?) "
        "ON CONFLICT(watch_id,evidence_key) DO UPDATE SET citation_sha256=excluded.citation_sha256,"
        "raw_sha256=excluded.raw_sha256,checkpoint_id=excluded.checkpoint_id",
        (
            session.watch_id,
            unit.key,
            unit.citation["source_report_id"],
            digest,
            str(row[1]),
            checkpoint_id,
        ),
    )
    origin = work.origin(session, str(unit.citation["text_sha256"]), float(target["cutoff"]))
    if origin is None:
        raise _fail("alert_text_origin_missing", evidence_key=unit.key)
    session.conn.execute(
        "INSERT INTO alert_text_origin_projection VALUES (?,?,?,?) "
        "ON CONFLICT(watch_id,text_sha256) DO UPDATE SET report_id=excluded.report_id,"
        "checkpoint_id=excluded.checkpoint_id",
        (session.watch_id, unit.citation["text_sha256"], str(origin[0]), checkpoint_id),
    )
    for key, report in unit.seen:
        session.conn.execute(
            "INSERT OR IGNORE INTO alert_derivation_seen VALUES (?,?,?,?)",
            (target["target_id"], key, report, checkpoint_id),
        )
        session.conn.execute(
            "INSERT OR IGNORE INTO alert_seen_projection VALUES (?,?,?,?)",
            (session.watch_id, key, report, checkpoint_id),
        )
        if report is not None:
            session.conn.execute(
                "INSERT OR IGNORE INTO alert_known_reports VALUES (?,?)", (session.watch_id, report)
            )
    for event in unit.events:
        # All event identities use the same watch-global key as seen suppression.
        event_key = prepared_event_keys.get(str(event["alert_id"]))
        if event_key is None:
            raise _fail("alert_event_identity_mismatch", alert_id=event["alert_id"])
        session.conn.execute(
            "INSERT INTO alert_v2_event_identities VALUES (?,?,?,?,?)",
            (session.watch_id, event_key, event["alert_id"], target["target_id"], checkpoint_id),
        )
    session.conn.execute(
        "UPDATE alert_watch_evidence SET done=1 WHERE watch_id=? AND evidence_key=?",
        (session.watch_id, unit.key),
    )
    session.conn.execute(
        "UPDATE alert_derivation_targets SET event_count=event_count+?,"
        "evidence_count=evidence_count+? WHERE target_id=?",
        (len(unit.events), int(first_publication), target["target_id"]),
    )


def target_counts(session: AlertSession, target_id: str) -> dict[str, int]:
    names = (
        "evidence_count",
        "event_count",
        "acquired_threads",
        "parsed_comments",
        "source_reported_comments",
        "absent_known_comments",
        "comment_coverage_truncated",
        "comment_coverage_unknown",
    )
    row = session.conn.execute(
        "SELECT " + ",".join(names) + " FROM alert_derivation_targets WHERE target_id=?",
        (target_id,),
    ).fetchone()
    if row is None:
        raise _fail("alert_target_missing", target_id=target_id)
    return {name: int(row[index]) for index, name in enumerate(names)}


def target_complete(session: AlertSession, target_id: str) -> bool:
    ready = session.conn.execute(
        "SELECT 1 FROM alert_presence_bindings WHERE target_id=?", (target_id,)
    ).fetchone()
    return (
        ready is not None
        and session.conn.execute(
            "SELECT 1 FROM alert_watch_evidence WHERE watch_id=? AND done=0 LIMIT 1",
            (session.watch_id,),
        ).fetchone()
        is None
    )
