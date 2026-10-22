"""Fenced, resumable coordination of bounded cumulative alert work.

Source expansion and artifact IO run in autocommit. Only checked descriptors and
bounded semantic deltas enter the short writer fence. Historical objects are
never updated; the small progress rows are projections of immutable checkpoints.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from typing import Any

from threadsense.errors import MonitoringStateError
from threadsense.monitoring.alert_budget import (
    AdmissionStop,
    AlertBudget,
    AlertSession,
    ArtifactReference,
    encode_control,
    json_bound,
    read_json_text,
)
from threadsense.monitoring.alert_projection import (
    claim_projection,
    coverage_binding,
    prepare_terminal,
    require_projection,
)
from threadsense.monitoring.alert_records import PendingAlertReport, canonical_json, identity

ENGINE_SCHEMA_STATEMENTS: tuple[str, ...] = (
    """CREATE TABLE watch_alert2_targets (
        target_id TEXT PRIMARY KEY,watch_id TEXT NOT NULL,sweep_id TEXT NOT NULL,
        highwater INTEGER NOT NULL,basis_sha256 TEXT NOT NULL,target_json TEXT NOT NULL,
        UNIQUE(watch_id,sweep_id))""",
    "CREATE INDEX watch_alert2_targets_watch ON watch_alert2_targets(watch_id,sweep_id)",
    """CREATE TABLE watch_alert2_progress (
        target_id TEXT PRIMARY KEY,checkpoint_id TEXT,heads_json TEXT NOT NULL,
        preview_json TEXT NOT NULL,staged_id TEXT,finished INTEGER NOT NULL DEFAULT 0)""",
    """CREATE TABLE watch_alert2_checkpoints (
        checkpoint_id TEXT PRIMARY KEY,target_id TEXT NOT NULL,predecessor TEXT,
        generation INTEGER NOT NULL,heads_json TEXT NOT NULL,counts_json TEXT NOT NULL,
        unit_key TEXT NOT NULL,UNIQUE(target_id,generation))""",
    """CREATE TABLE watch_alert2_stages (
        stage_id TEXT PRIMARY KEY,target_id TEXT NOT NULL,expected_checkpoint TEXT,
        generation INTEGER NOT NULL,unit_json TEXT NOT NULL)""",
    """CREATE TABLE watch_alert2_stage_completions (
        stage_id TEXT PRIMARY KEY,checkpoint_id TEXT NOT NULL)""",
    """CREATE TABLE watch_alert2_reports (
        report_id TEXT PRIMARY KEY,watch_id TEXT NOT NULL,target_id TEXT NOT NULL UNIQUE,
        payload_json TEXT NOT NULL,payload_sha256 TEXT NOT NULL,json_path TEXT NOT NULL,
        markdown_path TEXT NOT NULL)""",
    "CREATE INDEX watch_alert2_reports_watch ON watch_alert2_reports(watch_id,target_id)",
    """CREATE TABLE watch_alert2_completions (
        report_id TEXT PRIMARY KEY,json_sha256 TEXT NOT NULL,markdown_sha256 TEXT NOT NULL)""",
    """CREATE TABLE watch_alert2_watches (
        watch_id TEXT PRIMARY KEY,pending_target TEXT,last_target TEXT,last_report TEXT,
        cohort_id TEXT,comparable_json TEXT,baseline_json TEXT,evidence_head_json TEXT)""",
    *tuple(
        f"CREATE TRIGGER {table}_no_{operation.lower()} BEFORE {operation} ON {table} "
        "BEGIN SELECT RAISE(ABORT,'alert history is append-only'); END"
        for table in (
            "watch_alert2_targets",
            "watch_alert2_checkpoints",
            "watch_alert2_stages",
            "watch_alert2_stage_completions",
            "watch_alert2_reports",
            "watch_alert2_completions",
        )
        for operation in ("UPDATE", "DELETE")
    ),
)


def _load(
    conn_session: AlertSession, table: str, column: str, rowid: int, budget: AlertBudget, unit: str
) -> Any:
    budget.admit(unit + ":metadata", source_rows=1, input_bytes=64)
    return read_json_text(conn_session.conn, table, column, rowid, budget, unit)


def _target(session: AlertSession, target_id: str, budget: AlertBudget) -> dict[str, Any]:
    row = session.conn.execute(
        "SELECT rowid FROM watch_alert2_targets WHERE target_id=?",
        (target_id,),
    ).fetchone()
    if row is None:
        raise MonitoringStateError(
            "pending alert target is missing", details={"reason": "target_missing"}
        )
    value: dict[str, Any] = _load(
        session, "watch_alert2_targets", "target_json", int(row[0]), budget, target_id
    )
    if value.get("target_id") != target_id or value.get("watch_id") != session.watch_id:
        raise MonitoringStateError(
            "alert target binding differs", details={"reason": "target_mismatch"}
        )
    if "inherited_coverage" not in value:
        # Bounded adoption of a pre-R41 pending target preserves its immutable JSON.
        inherited = _projection_coverage(session, value.get("previous_target"), budget)
        pending = inherited.pop("pending_target")
        inherited["pending_target"] = None
        value["inherited_coverage"] = inherited
        unfinished = session.conn.execute(
            "SELECT finished FROM watch_alert2_progress WHERE target_id=?", (target_id,)
        ).fetchone()
        if pending is None and unfinished is not None and not unfinished[0]:
            budget.admit("projection_adoption", input_bytes=512)
            with session.fence():
                claim_projection(session, value)
    return value


def _projection_coverage(
    session: AlertSession, previous_target: str | None, budget: AlertBudget
) -> dict[str, Any]:
    inherited = coverage_binding(session)
    if inherited["target_id"] is None and previous_target is not None:
        budget.admit("projection_adoption_metadata", input_bytes=256)
        prior_report = session.conn.execute(
            "SELECT rowid FROM watch_alert2_reports WHERE target_id=?", (previous_target,)
        ).fetchone()
        if prior_report is not None:
            prior_payload = _load(
                session,
                "watch_alert2_reports",
                "payload_json",
                int(prior_report[0]),
                budget,
                "projection_adoption_count",
            )
            inherited["evidence_count"] = int(prior_payload["evidence_count"])
            with session.fence():
                session.conn.execute(
                    "INSERT OR IGNORE INTO alert_watch_heads(watch_id,evidence_count) VALUES (?,?)",
                    (session.watch_id, inherited["evidence_count"]),
                )
    return inherited


def _progress(session: AlertSession, target_id: str, budget: AlertBudget) -> dict[str, Any]:
    row = session.conn.execute(
        "SELECT rowid,checkpoint_id,staged_id,finished "
        "FROM watch_alert2_progress WHERE target_id=?",
        (target_id,),
    ).fetchone()
    if row is None:
        raise MonitoringStateError(
            "alert checkpoint is missing", details={"reason": "checkpoint_missing"}
        )
    return {
        "checkpoint_id": row[1],
        "staged_id": row[2],
        "finished": bool(row[3]),
        "heads": _load(
            session, "watch_alert2_progress", "heads_json", int(row[0]), budget, "checkpoint_heads"
        ),
        "preview": _load(
            session, "watch_alert2_progress", "preview_json", int(row[0]), budget, "checkpoint_page"
        ),
    }


def _result(
    report: PendingAlertReport | None,
    session: AlertSession,
    budget: AlertBudget,
    *,
    status: str | None = None,
    details: dict[str, Any] | None = None,
    target_id: str | None = None,
    new_ids: list[str] | None = None,
) -> dict[str, Any]:
    session.exiting = True
    payload = {} if report is None else report.payload
    result: dict[str, Any] = {
        "artifact_type": "watch_alert_result",
        "schema_version": 2,
        "watch_id": session.watch_id,
        "status": status or payload.get("status", "unavailable"),
        "report_id": payload.get("report_id"),
        "target_id": target_id or payload.get("target_id"),
        "cohort_id": payload.get("cohort_id"),
        "window_id": payload.get("window_id"),
        "baseline_window_id": payload.get("baseline_window_id"),
        "alert_ids": payload.get("alert_ids", []),
        "alerts": payload.get("alerts", []),
        "new_alert_ids": new_ids or [],
        "suppression_reasons": payload.get("suppression_reasons", []),
        "refusal_reasons": payload.get(
            "refusal_reasons", ["no_durable_sweep"] if status is None else []
        ),
        "json_path": None if report is None else str(session.root / report.json_path),
        "markdown_path": None if report is None else str(session.root / report.markdown_path),
        "resumed": False if report is None else report.resumed,
        "evidence_count": payload.get("evidence_count", 0),
        "event_count": payload.get("event_count", 0),
        "evidence_head": payload.get("evidence_head"),
        "change_head": payload.get("change_head"),
        "provenance_head": payload.get("provenance_head"),
        "comparison_complete": report is not None,
        "continuation": None
        if status not in ("backlog", "resource_refused")
        else {
            "watch_id": session.watch_id,
            "target_id": target_id,
            "command": "watch alerts",
        },
        "resource": details,
    }
    # The normal CLI emitter uses ASCII JSON with indent=2; admit before escaping.
    budget.admit("cli_response", response_bytes=json_bound(result) + 1024)
    result["accounting"] = budget.accounting()
    return result


def stored_schema2(
    session: AlertSession, report_id: str, budget: AlertBudget
) -> PendingAlertReport:
    row = session.conn.execute(
        "SELECT r.rowid,r.payload_sha256,r.json_path,r.markdown_path,c.report_id "
        "FROM watch_alert2_reports r LEFT JOIN watch_alert2_completions c "
        "ON c.report_id=r.report_id "
        "WHERE r.report_id=?",
        (report_id,),
    ).fetchone()
    if row is None:
        raise MonitoringStateError(
            "alert report missing", details={"reason": "alert_report_missing"}
        )
    payload = _load(session, "watch_alert2_reports", "payload_json", int(row[0]), budget, report_id)
    # Database canonical payload identity is distinct from the rendered JSON artifact hash.
    text = canonical_json(payload)
    budget.admit("report_payload_hash", input_bytes=len(text.encode("utf-8")))
    if hashlib.sha256(text.encode("utf-8")).hexdigest() != row[1]:
        raise MonitoringStateError(
            "alert report corrupted", details={"reason": "alert_report_hash_mismatch"}
        )
    if payload.get("schema_version") != 2:
        raise MonitoringStateError(
            "unsupported alert report schema", details={"reason": "unsupported_alert_schema"}
        )
    return PendingAlertReport(payload, str(row[2]), str(row[3]), True, row[4] is not None)


def complete_schema2(
    session: AlertSession,
    report_id: str,
    json_hash: str,
    markdown_hash: str,
    budget: AlertBudget,
) -> bool:
    """Atomically bind durable publication and expected-old completed coverage."""
    from threadsense.monitoring.alert_derivation import target_complete, target_counts

    budget.admit("completion_metadata", input_bytes=512)
    row = session.conn.execute(
        "SELECT target_id FROM watch_alert2_reports WHERE report_id=? AND watch_id=?",
        (report_id, session.watch_id),
    ).fetchone()
    if row is None:
        return False
    existing = session.conn.execute(
        "SELECT json_sha256,markdown_sha256 FROM watch_alert2_completions WHERE report_id=?",
        (report_id,),
    ).fetchone()
    if existing is not None:
        if tuple(existing) != (json_hash, markdown_hash):
            raise MonitoringStateError(
                "completion hash differs", details={"reason": "artifact_hash_mismatch"}
            )
        return True
    target_id = str(row[0])
    target = _target(session, target_id, budget)
    progress = _progress(session, target_id, budget)
    budget.admit("terminal_counts", input_bytes=512)
    counts = target_counts(session, target_id)
    terminal_id, terminal_text = prepare_terminal(session, target, progress, counts, budget)
    heads_text = encode_control(progress["heads"], budget, "terminal_heads", passes=2)
    counts_text = encode_control(counts, budget, "terminal_counts", passes=2)
    previous = target["inherited_coverage"]
    generation = 1
    if progress["checkpoint_id"] is not None:
        generation = 1 + int(
            session.conn.execute(
                "SELECT generation FROM watch_alert2_checkpoints WHERE checkpoint_id=?",
                (progress["checkpoint_id"],),
            ).fetchone()[0]
        )
    with session.fence():
        require_projection(session, target)
        current = session.conn.execute(
            "SELECT checkpoint_id,staged_id FROM watch_alert2_progress WHERE target_id=?",
            (target_id,),
        ).fetchone()
        if (
            current is None
            or tuple(current) != (progress["checkpoint_id"], None)
            or not target_complete(session, target_id)
        ):
            raise MonitoringStateError(
                "alert presence incomplete", details={"reason": "alert_presence_incomplete"}
            )
        session.conn.execute(
            "INSERT INTO watch_alert2_checkpoints VALUES (?,?,?,?,?,?,?)",
            (
                terminal_id,
                target_id,
                progress["checkpoint_id"],
                generation,
                heads_text,
                counts_text,
                "terminal",
            ),
        )
        session.conn.execute(
            "INSERT INTO alert_coverage_completions VALUES (?,?,?,?,?,?,?,?,?)",
            (
                target_id,
                session.watch_id,
                previous["target_id"],
                previous["highwater"],
                terminal_id,
                target["highwater"],
                target["cutoff"],
                identity("aip-", target["basis"].get("issue_policy")),
                terminal_text,
            ),
        )
        changed = session.conn.execute(
            "UPDATE alert_watch_heads SET completed_highwater=?,completed_target=?,"
            "completed_checkpoint=?,pending_target=NULL,cutoff=?,policy_sha256=?,evidence_count=? "
            "WHERE watch_id=? AND completed_highwater=? "
            "AND completed_target IS ? AND pending_target=?",
            (
                target["highwater"],
                target_id,
                terminal_id,
                target["cutoff"],
                identity("aip-", target["basis"].get("issue_policy")),
                counts["evidence_count"],
                session.watch_id,
                previous["highwater"],
                previous["target_id"],
                target_id,
            ),
        ).rowcount
        if changed != 1:
            raise MonitoringStateError("coverage changed", details={"reason": "coverage_conflict"})
        session.conn.execute(
            "INSERT INTO watch_alert2_completions VALUES (?,?,?)",
            (report_id, json_hash, markdown_hash),
        )
        session.conn.execute(
            "UPDATE watch_alert2_progress SET finished=1,checkpoint_id=? WHERE target_id=?",
            (terminal_id, target_id),
        )
        session.conn.execute(
            "UPDATE watch_alert2_watches SET pending_target=NULL,last_target=?,last_report=? "
            "WHERE watch_id=? AND pending_target=?",
            (target_id, report_id, session.watch_id, target_id),
        )
    return True


def _freeze_target(
    session: AlertSession, basis: dict[str, Any], budget: AlertBudget, now: float
) -> dict[str, Any] | None:
    with budget.unit("target_control"):
        return _freeze_target_impl(session, basis, budget, now)


def _freeze_target_impl(
    session: AlertSession, basis: dict[str, Any], budget: AlertBudget, now: float
) -> dict[str, Any] | None:
    from threadsense.monitoring.alert_sources import get_version, latest_change, source_highwater

    highwater = source_highwater(session)
    change = latest_change(session, "watch_sweeps", budget, through=highwater)
    if change is None:
        return None
    sweep = change.data
    sweep_id = str(sweep["run_id"])
    existing = session.conn.execute(
        "SELECT target_id FROM watch_alert2_targets WHERE watch_id=? AND sweep_id=?",
        (session.watch_id, sweep_id),
    ).fetchone()
    if existing is not None:
        return _target(session, str(existing[0]), budget)
    summary_raw = str(sweep["summary_json"])
    budget.admit("sweep_summary_decode", input_bytes=len(summary_raw.encode("utf-8")))
    summary = json.loads(summary_raw)
    run = get_version(session, "watch_runs", canonical_json([sweep_id]), highwater, budget)
    if run is None:
        raise MonitoringStateError(
            "sweep run provenance missing", details={"reason": "source_provenance_missing"}
        )
    revision = int(run["profile_revision"])
    profile_row = get_version(
        session, "watch_revisions", canonical_json([session.watch_id, revision]), highwater, budget
    )
    if profile_row is None:
        raise MonitoringStateError(
            "sweep profile provenance missing", details={"reason": "source_provenance_missing"}
        )
    profile_raw = str(profile_row["profile_json"])
    budget.admit("profile_decode_hash", input_bytes=2 * len(profile_raw.encode("utf-8")))
    if hashlib.sha256(profile_raw.encode("utf-8")).hexdigest() != profile_row["content_sha256"]:
        raise MonitoringStateError(
            "profile identity differs", details={"reason": "profile_hash_mismatch"}
        )
    discovery = summary.get("discovery", {})
    ranking_selection = latest_change(
        session, "watch_ranking_selections", budget, through=highwater
    )
    preference = latest_change(session, "watch_preference_events", budget, through=highwater)
    policy_id = None if ranking_selection is None else ranking_selection.data["policy_id"]
    ranking_policy = (
        None
        if policy_id is None
        else get_version(
            session, "watch_ranking_policies", canonical_json([policy_id]), highwater, budget
        )
    )
    frozen_basis = {
        **basis,
        "profile_revision": revision,
        "profile_content_sha256": profile_row["content_sha256"],
        "profile": json.loads(profile_raw),
        "collection_basis": summary.get("collection_basis"),
        "discovery_sampling": {
            key: discovery.get(key)
            for key in (
                "queries",
                "subreddits",
                "sort",
                "time_window",
                "per_request_limit",
                "limit",
            )
        },
        "ranking_policy": {
            "source_highwater": highwater,
            "mode": "presentation_only",
            "selection_sequence": None if ranking_selection is None else ranking_selection.sequence,
            "policy_id": policy_id,
            "policy_reference": None
            if ranking_policy is None
            else identity("arp-", ranking_policy),
        },
        "feedback_basis": {
            "source_highwater": highwater,
            "mode": "presentation_only",
            "preference_sequence": 0 if preference is None else preference.data["sequence"],
            "source_sequence": None if preference is None else preference.sequence,
        },
    }
    comparable = {
        key: frozen_basis[key]
        for key in (
            "profile_revision",
            "profile_content_sha256",
            "profile",
            "collection_basis",
            "discovery_sampling",
            "issue_policy",
        )
    }
    comparable_text = canonical_json(comparable)
    prior = session.conn.execute(
        "SELECT cohort_id,comparable_json,baseline_json,evidence_head_json,last_target "
        "FROM watch_alert2_watches WHERE watch_id=?",
        (session.watch_id,),
    ).fetchone()
    provenance_head = None
    legacy_payload = None
    if prior is not None and prior[4] is not None:
        prior_progress = _progress(session, str(prior[4]), budget)
        provenance_head = prior_progress["heads"]["provenance"]
    if prior is None:
        from threadsense.monitoring.alert_records import stored_report
        from threadsense.monitoring.alert_sources import legacy_report

        legacy = legacy_report(session, budget, pending_only=False, latest=True)
        if legacy is not None:
            legacy_payload = stored_report(
                session.conn, str(legacy.data["report_id"]), budget=budget
            ).payload
            legacy_comparable = {key: legacy_payload["basis"][key] for key in comparable}
            prior = (
                legacy_payload["cohort_id"],
                canonical_json(legacy_comparable),
                canonical_json(legacy_payload["baseline"]),
                None,
                None,
            )
    same_cohort = prior is not None and prior[1] == comparable_text
    cohort = (
        str(prior[0])
        if same_cohort
        else identity(
            "aco-", [session.watch_id, comparable, None if prior is None else prior[0], sweep_id]
        )
    )
    window = identity("awi-", [session.watch_id, sweep_id])
    target_id = identity("atg-", [window, cohort, highwater])
    report_id = identity("alr-", [window, cohort])
    baseline = json.loads(prior[2]) if same_cohort and prior[2] else None
    evidence_head = json.loads(prior[3]) if prior is not None and prior[3] else None
    target = {
        "target_id": target_id,
        "watch_id": session.watch_id,
        "sweep_id": sweep_id,
        "highwater": highwater,
        "cutoff": float(sweep["finished_at_utc"]),
        "basis_sha256": hashlib.sha256(canonical_json(frozen_basis).encode()).hexdigest(),
        "basis": frozen_basis,
        "baseline": baseline,
        "cohort_id": cohort,
        "window_id": window,
        "report_id": report_id,
        "outcome": str(sweep["outcome"]),
        "summary": summary,
        "created_at_utc": now,
        "previous_target": None if prior is None else prior[4],
        "legacy_predecessor": None if legacy_payload is None else legacy_payload["report_id"],
    }
    target["inherited_coverage"] = _projection_coverage(
        session, None if prior is None else prior[4], budget
    )
    text = encode_control(target, budget, "target_freeze", passes=3)
    budget.admit("target_freeze_control", derivations=1)
    with session.fence():
        claim_projection(session, target)
        session.conn.execute(
            "INSERT INTO watch_alert2_targets VALUES (?,?,?,?,?,?)",
            (target_id, session.watch_id, sweep_id, highwater, target["basis_sha256"], text),
        )
        session.conn.execute(
            "INSERT INTO watch_alert2_progress VALUES (?,NULL,?,?,NULL,0)",
            (
                target_id,
                canonical_json(
                    {"evidence": evidence_head, "changes": None, "provenance": provenance_head}
                ),
                canonical_json({"evidence": [], "alerts": [], "truncated": False}),
            ),
        )
        session.conn.execute(
            "INSERT INTO watch_alert2_watches VALUES (?,?,NULL,NULL,?,?,?,?) "
            "ON CONFLICT(watch_id) DO UPDATE SET pending_target=excluded.pending_target,"
            "cohort_id=excluded.cohort_id,comparable_json=excluded.comparable_json,"
            "baseline_json=excluded.baseline_json",
            (
                session.watch_id,
                target_id,
                cohort,
                comparable_text,
                canonical_json(baseline),
                None if evidence_head is None else canonical_json(evidence_head),
            ),
        )
    return target


def _stage_unit(
    session: AlertSession,
    target: dict[str, Any],
    unit: Any,
    progress: dict[str, Any],
    budget: AlertBudget,
) -> tuple[str, int]:
    prior = progress["checkpoint_id"]
    generation = 1
    if prior is not None:
        generation = (
            int(
                session.conn.execute(
                    "SELECT generation FROM watch_alert2_checkpoints WHERE checkpoint_id=?",
                    (prior,),
                ).fetchone()[0]
            )
            + 1
        )
    value = {
        "key": unit.key,
        "citation": unit.citation,
        "events": unit.events,
        "seen": unit.seen,
        "overlay": unit.overlay,
    }
    text = encode_control(value, budget, "stage_control", passes=3)
    budget.admit("stage_control_metadata", derivations=1)
    stage_id = identity("ast-", [target["target_id"], prior, unit.key, value])
    with session.fence():
        session.conn.execute(
            "INSERT OR IGNORE INTO watch_alert2_stages VALUES (?,?,?,?,?)",
            (stage_id, target["target_id"], prior, generation, text),
        )
        session.conn.execute(
            "UPDATE watch_alert2_progress SET staged_id=? WHERE target_id=? AND checkpoint_id IS ?",
            (stage_id, target["target_id"], prior),
        )
    if (
        session.conn.execute(
            "SELECT staged_id FROM watch_alert2_progress WHERE target_id=?",
            (target["target_id"],),
        ).fetchone()[0]
        != stage_id
    ):
        raise MonitoringStateError(
            "alert stage predecessor differs", details={"reason": "checkpoint_conflict"}
        )
    return stage_id, generation


def _publish_unit(
    session: AlertSession,
    target: dict[str, Any],
    unit: Any,
    progress: dict[str, Any],
    budget: AlertBudget,
) -> list[str]:
    with budget.unit("publish:" + unit.key):
        return _publish_unit_impl(session, target, unit, progress, budget)


def _publish_unit_impl(
    session: AlertSession,
    target: dict[str, Any],
    unit: Any,
    progress: dict[str, Any],
    budget: AlertBudget,
) -> list[str]:
    from threadsense.monitoring.alert_artifacts import publish_segment
    from threadsense.monitoring.alert_derivation import (
        commit_derivation,
        prepare_derivation_commit,
        target_counts,
    )

    stage_id, generation = _stage_unit(session, target, unit, progress, budget)
    heads = dict(progress["heads"])

    def publish(chain: str, items: list[dict[str, Any]]) -> None:
        old = heads.get(chain)
        prior = None if old is None else ArtifactReference.from_dict(old)
        # Generations are chain-global even across inherited evidence targets.
        number = generation if prior is None else max(generation, prior.generation + 1)
        heads[chain] = publish_segment(
            session,
            budget,
            chain="change" if chain == "changes" else chain,
            generation=number,
            items=items,
            prior=prior,
            target_id=str(target["target_id"]),
        ).to_dict()

    if unit.citation:
        if unit.overlay:
            publish("provenance", [{"key": unit.key, "citation": unit.citation}])
        else:
            publish("evidence", [unit.citation])
    if unit.events:
        publish("changes", unit.events)
    budget.admit("checkpoint_identity", input_bytes=2 * json_bound([stage_id, heads]))
    checkpoint_id = identity("ack-", [stage_id, heads])
    preview = progress["preview"]
    for name, items, limit in (
        ("evidence", [unit.citation] if unit.citation else [], 16),
        ("alerts", unit.events, 32),
    ):
        for item in items:
            if len(preview[name]) < limit:
                preview[name].append(item)
            else:
                preview["truncated"] = True
    checkpoint_text = encode_control(heads, budget, "checkpoint_heads", passes=2)
    preview_text = encode_control(preview, budget, "checkpoint_page", passes=2)
    prepared_digest, prepared_event_keys = prepare_derivation_commit(session, target, unit, budget)
    budget.admit("checkpoint_commit", derivations=1, input_bytes=2048)
    with session.fence():
        current = session.conn.execute(
            "SELECT checkpoint_id,staged_id FROM watch_alert2_progress WHERE target_id=?",
            (target["target_id"],),
        ).fetchone()
        if tuple(current) != (progress["checkpoint_id"], stage_id):
            raise MonitoringStateError(
                "alert checkpoint changed", details={"reason": "checkpoint_conflict"}
            )
        commit_derivation(
            session,
            target,
            unit,
            checkpoint_id,
            prepared_digest=prepared_digest,
            prepared_event_keys=prepared_event_keys,
        )
        counts = target_counts(session, str(target["target_id"]))
        session.conn.execute(
            "INSERT INTO watch_alert2_checkpoints VALUES (?,?,?,?,?,?,?)",
            (
                checkpoint_id,
                target["target_id"],
                progress["checkpoint_id"],
                generation,
                checkpoint_text,
                canonical_json(counts),
                unit.key,
            ),
        )
        session.conn.execute(
            "INSERT INTO watch_alert2_stage_completions VALUES (?,?)", (stage_id, checkpoint_id)
        )
        session.conn.execute(
            "UPDATE watch_alert2_progress SET checkpoint_id=?,heads_json=?,"
            "preview_json=?,staged_id=NULL WHERE target_id=?",
            (checkpoint_id, checkpoint_text, preview_text, target["target_id"]),
        )
        session.conn.execute(
            "UPDATE watch_alert2_watches SET evidence_head_json=? WHERE watch_id=?",
            (canonical_json(heads["evidence"]), session.watch_id),
        )
    return [str(event["alert_id"]) for event in unit.events]


def _report_payload(
    session: AlertSession, target: dict[str, Any], progress: dict[str, Any], budget: AlertBudget
) -> dict[str, Any]:
    from threadsense.monitoring.alert_derivation import target_counts

    counts = target_counts(session, str(target["target_id"]))
    summary = target["summary"]
    discovery = summary.get("discovery", {})
    refusals = {"population_denominator_unknown", "rate_trend_refused_unknown_coverage"}
    if target["basis"].get("collection_basis") is None:
        refusals.add("collection_basis_unknown")
    if target["outcome"] in ("partial", "failed"):
        refusals.add(str(target["outcome"]) + "_sweep")
    if discovery.get("truncated"):
        refusals.add("discovery_truncated")
    comment_coverage = []
    if counts["comment_coverage_truncated"]:
        refusals.add("comment_coverage_truncated")
        comment_coverage.append("truncated")
    if counts["comment_coverage_unknown"] or not counts["acquired_threads"]:
        refusals.add("comment_coverage_unknown")
        comment_coverage.append("unknown")
    baseline = target["baseline"]
    evidence_count = counts.get("evidence_count", 0)
    event_count = counts.get("event_count", 0)
    if baseline is None:
        baseline = {
            "report_id": target["report_id"],
            "window_id": target["window_id"],
            "evidence_count": evidence_count,
        }
    status = (
        "unavailable"
        if target["outcome"] == "failed"
        else "baseline"
        if target["baseline"] is None
        else "compared"
        if event_count
        else "no_changes"
    )
    heads = progress["heads"]
    payload = {
        "artifact_type": "watch_alert_report",
        "schema_version": 2,
        **{
            key: target[key]
            for key in (
                "watch_id",
                "target_id",
                "report_id",
                "cohort_id",
                "window_id",
                "created_at_utc",
            )
        },
        "source_highwater": target["highwater"],
        "basis_sha256": target["basis_sha256"],
        "baseline_window_id": baseline["window_id"],
        "baseline": baseline,
        "status": status,
        "window": {
            "sweep_run_id": target["sweep_id"],
            "finished_at_utc": target["cutoff"],
            "comparison": "cumulative evidence through frozen source highwater",
        },
        "basis": target["basis"],
        "coverage": {
            "search_completeness": discovery.get("search_completeness", "unknown"),
            "comment_completeness": comment_coverage,
            "sweep_outcome": target["outcome"],
            "rate_trend_eligible": False,
            "absence_interpretation": "not evidence of deletion, resolution, or reduced demand",
        },
        "bounded_denominators": {
            "population_denominator": None,
            "independent_people_denominator": None,
            "discovered_candidates": discovery.get("persisted_count"),
            **{
                key: counts[key]
                for key in (
                    "acquired_threads",
                    "parsed_comments",
                    "source_reported_comments",
                    "absent_known_comments",
                )
            },
            "cited_evidence_versions": evidence_count,
            "interpretation": "bounded observations, not population denominators",
        },
        "evidence_count": evidence_count,
        "event_count": event_count,
        "evidence": progress["preview"]["evidence"],
        "alerts": progress["preview"]["alerts"],
        "alert_ids": [event["alert_id"] for event in progress["preview"]["alerts"]],
        "page_complete": not progress["preview"]["truncated"],
        "comparison_complete": True,
        "evidence_head": heads["evidence"],
        "change_head": heads["changes"],
        "provenance_head": heads["provenance"],
        "integrity_scope": (
            "new bytes and immediate references checked; "
            "inherited links and leaves checked on bounded access"
        ),
        "refusal_reasons": sorted(refusals),
        "suppression_reasons": (
            ["initial_baseline" if target["previous_target"] is None else "cohort_baseline"]
            if target["baseline"] is None
            else ["no_new_evidence"]
            if not event_count
            else []
        ),
    }
    budget.admit("report_finalize", derivations=1)
    return payload


def execute(
    session: AlertSession,
    basis: dict[str, Any],
    budget: AlertBudget,
    now: float,
    complete: Callable[[str, str, str], None],
) -> dict[str, Any]:
    from threadsense.monitoring.alert_artifacts import publish_report
    from threadsense.monitoring.alert_derivation import next_derivation, prepare_presence
    from threadsense.monitoring.alert_sources import bootstrap, bootstrap_legacy, legacy_report

    target: dict[str, Any] | None = None
    new_ids: list[str] = []
    try:
        if not bootstrap_legacy(session, budget):
            return _result(
                None,
                session,
                budget,
                status="backlog",
                details={"reason": "legacy_order_bootstrap"},
            )
        legacy = legacy_report(session, budget)
        if legacy is not None:
            from threadsense.monitoring.alert_records import stored_report

            report = stored_report(session.conn, str(legacy.data["report_id"]), budget=budget)
            json_hash, markdown_hash = publish_report(
                session,
                budget,
                payload=report.payload,
                json_path=report.json_path,
                markdown_path=report.markdown_path,
            )
            budget.admit("legacy_completion", derivations=1, input_bytes=256)
            complete(str(report.payload["report_id"]), json_hash, markdown_hash)
            return _result(report, session, budget)
        if not bootstrap(session, budget):
            return _result(
                None, session, budget, status="backlog", details={"reason": "source_bootstrap"}
            )
        completed_legacy = legacy_report(session, budget, pending_only=False, latest=True)
        if completed_legacy is not None:
            from threadsense.monitoring.alert_budget import verify_file
            from threadsense.monitoring.alert_records import stored_report
            from threadsense.monitoring.alert_sources import latest_change

            sweep = latest_change(session, "watch_sweeps", budget)
            if sweep is not None and sweep.data["run_id"] == completed_legacy.data["sweep_run_id"]:
                report = stored_report(
                    session.conn, str(completed_legacy.data["report_id"]), budget=budget
                )
                budget.admit("legacy_completion_metadata", source_rows=1, input_bytes=256)
                hashes = session.conn.execute(
                    "SELECT json_sha256,markdown_sha256 "
                    "FROM watch_alert_completions WHERE report_id=?",
                    (report.payload["report_id"],),
                ).fetchone()
                if hashes is None:
                    raise MonitoringStateError(
                        "legacy completion is missing", details={"reason": "completion_missing"}
                    )
                verify_file(session, budget, report.json_path, str(hashes[0]))
                verify_file(session, budget, report.markdown_path, str(hashes[1]))
                return _result(report, session, budget)
        watch = session.conn.execute(
            "SELECT pending_target,last_report FROM watch_alert2_watches WHERE watch_id=?",
            (session.watch_id,),
        ).fetchone()
        target = (
            _target(session, str(watch[0]), budget)
            if watch is not None and watch[0]
            else _freeze_target(session, basis, budget, now)
        )
        if target is None:
            return _result(None, session, budget)
        target_id = str(target["target_id"])
        existing = session.conn.execute(
            "SELECT report_id FROM watch_alert2_reports WHERE target_id=?", (target_id,)
        ).fetchone()
        if existing is not None:
            report = stored_schema2(session, str(existing[0]), budget)
            if report.completed:
                return _result(report, session, budget)
        else:
            while not prepare_presence(session, target, budget):
                # Every preparation/control unit admits work and checks the claim.
                # Exhaustion preserves its durable phase and cursor.
                pass
            while True:
                progress = _progress(session, target_id, budget)
                if progress["staged_id"] is not None:
                    from threadsense.monitoring.alert_derivation import DerivedUnit

                    stage_row = session.conn.execute(
                        "SELECT rowid,expected_checkpoint "
                        "FROM watch_alert2_stages WHERE stage_id=?",
                        (progress["staged_id"],),
                    ).fetchone()
                    if stage_row is None or stage_row[1] != progress["checkpoint_id"]:
                        raise MonitoringStateError(
                            "pending stage changed", details={"reason": "checkpoint_conflict"}
                        )
                    unit_data = _load(
                        session,
                        "watch_alert2_stages",
                        "unit_json",
                        int(stage_row[0]),
                        budget,
                        str(progress["staged_id"]),
                    )
                    unit = DerivedUnit(**unit_data)
                else:
                    candidate = next_derivation(session, target, budget)
                    if candidate is None:
                        break
                    unit = candidate
                new_ids.extend(_publish_unit(session, target, unit, progress, budget))
            progress = _progress(session, target_id, budget)
            payload = _report_payload(session, target, progress, budget)
            relative = f"{session.watch_id}/alerts/{payload['report_id']}"
            text = encode_control(payload, budget, "report_database", passes=3)
            with session.fence():
                session.conn.execute(
                    "INSERT INTO watch_alert2_reports VALUES (?,?,?,?,?,?,?)",
                    (
                        payload["report_id"],
                        session.watch_id,
                        target_id,
                        text,
                        hashlib.sha256(text.encode()).hexdigest(),
                        relative + ".json",
                        relative + ".md",
                    ),
                )
                if target["baseline"] is None:
                    session.conn.execute(
                        "UPDATE watch_alert2_watches SET baseline_json=? WHERE watch_id=?",
                        (canonical_json(payload["baseline"]), session.watch_id),
                    )
            report = PendingAlertReport(payload, relative + ".json", relative + ".md", False, False)
        with budget.unit("report_publication"):
            json_hash, markdown_hash = publish_report(
                session,
                budget,
                payload=report.payload,
                json_path=report.json_path,
                markdown_path=report.markdown_path,
            )
            budget.admit("report_completion", derivations=1, input_bytes=256)
            complete(str(report.payload["report_id"]), json_hash, markdown_hash)
        return _result(report, session, budget, new_ids=new_ids)
    except AdmissionStop as stop:
        return _result(
            None,
            session,
            budget,
            status=str(stop.details["status"]),
            details=stop.details,
            target_id=None if target is None else str(target["target_id"]),
            new_ids=new_ids,
        )
