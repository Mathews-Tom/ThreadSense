"""Bounded watch-local source deltas and reusable, intrinsic evidence facts."""

from __future__ import annotations

import json
from typing import Any

from threadsense.monitoring.alert_budget import AlertBudget, AlertSession, read_json_text
from threadsense.monitoring.alert_projection import require_projection
from threadsense.monitoring.alert_records import canonical_json, identity
from threadsense.monitoring.alert_sources import get_version, next_change

COUNTERS = (
    "acquired_threads",
    "parsed_comments",
    "source_reported_comments",
    "absent_known_comments",
    "comment_coverage_truncated",
    "comment_coverage_unknown",
)


def _derivation() -> Any:
    from threadsense.monitoring import alert_derivation

    return alert_derivation


def queue(
    session: AlertSession, target: dict[str, Any], kind: str, key: str = "", secondary: str = ""
) -> None:
    session.conn.execute(
        "INSERT INTO alert_target_revisits(target_id,kind,report_id,text_sha256) "
        "VALUES (?,?,?,?) ON CONFLICT(target_id,kind,report_id,text_sha256) DO UPDATE SET "
        "done=0,cursor='' WHERE alert_target_revisits.done=1",
        (target["target_id"], kind, key, secondary),
    )


def initialize(session: AlertSession, target: dict[str, Any], budget: AlertBudget) -> None:
    require_projection(session, target)
    if (
        session.conn.execute(
            "SELECT 1 FROM alert_projection_initializations WHERE target_id=?",
            (target["target_id"],),
        ).fetchone()
        is not None
    ):
        return
    inherited = target["inherited_coverage"]
    budget.admit(
        "alert-initial-policy",
        input_bytes=_derivation()._json_bound(target.get("basis", {}).get("issue_policy")) * 2,
    )
    policy = identity("aip-", target.get("basis", {}).get("issue_policy"))
    budget.admit("alert-projection-initialize", source_rows=8, input_bytes=2048)
    with session.fence():
        require_projection(session, target)
        inserted = session.conn.execute(
            "INSERT OR IGNORE INTO alert_projection_initializations VALUES (?)",
            (target["target_id"],),
        ).rowcount
        if not inserted:
            return
        prior_target = session.conn.execute(
            "SELECT 1 FROM alert_derivation_targets WHERE target_id=?", (target["target_id"],)
        ).fetchone()
        session.conn.execute(
            "INSERT INTO alert_derivation_targets(target_id,watch_id,highwater,basis_sha256,phase,"
            "source_cursor,evidence_count) VALUES (?,?,?,?,'gather',?,?) "
            "ON CONFLICT(target_id) DO UPDATE SET phase='gather',"
            "source_cursor=excluded.source_cursor,"
            "evidence_count=excluded.evidence_count",
            (
                target["target_id"],
                session.watch_id,
                target["highwater"],
                target["basis_sha256"],
                inherited["highwater"],
                inherited["evidence_count"],
            ),
        )
        if prior_target is not None:
            queue(session, target, "adopt")
        if inherited["cutoff"] is not None and float(inherited["cutoff"]) != float(
            target["cutoff"]
        ):
            queue(
                session,
                target,
                "cutoff",
                str(min(float(inherited["cutoff"]), float(target["cutoff"]))),
                str(max(float(inherited["cutoff"]), float(target["cutoff"]))),
            )
        if inherited["policy_sha256"] is not None and inherited["policy_sha256"] != policy:
            queue(session, target, "policy")


def _contribution(
    session: AlertSession, data: dict[str, Any], sequence: int, budget: AlertBudget
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    old = session.conn.execute(
        "SELECT rowid FROM alert_watch_acquisition_contributions "
        "WHERE watch_id=? AND acquisition_id=?",
        (session.watch_id, data["acquisition_id"]),
    ).fetchone()
    prior = (
        None
        if old is None
        else read_json_text(
            session.conn,
            "alert_watch_acquisition_contributions",
            "contribution_json",
            int(old[0]),
            budget,
            "alert-acquisition-prior:" + str(data["acquisition_id"]),
        )
    )
    current = {
        "sweep_id": str(data["run_id"]),
        "sequence": sequence,
        "values": [
            1,
            int(data["parsed_comment_count"]),
            int(data["reported_comment_count"]),
            int(data["absent_known_comment_count"]),
            int(data["completeness"] == "truncated"),
            int(data["completeness"] == "unknown"),
        ],
    }
    return prior, current


def _apply_contribution(
    session: AlertSession,
    data: dict[str, Any],
    sequence: int,
    prior: dict[str, Any] | None,
    current: dict[str, Any],
    encoded: str,
) -> None:
    if prior is not None and int(prior["sequence"]) >= sequence:
        return
    for sign, contribution in ((-1, prior), (1, current)):
        if contribution is None:
            continue
        values = [sign * int(value) for value in contribution["values"]]
        session.conn.execute(
            "INSERT INTO alert_watch_sweep_totals(watch_id,sweep_id," + ",".join(COUNTERS) + ") "
            "VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(watch_id,sweep_id) DO UPDATE SET "
            + ",".join(name + "=" + name + "+excluded." + name for name in COUNTERS),
            (session.watch_id, contribution["sweep_id"], *values),
        )
    session.conn.execute(
        "INSERT INTO alert_watch_acquisition_contributions VALUES (?,?,?,?,?) "
        "ON CONFLICT(watch_id,acquisition_id) DO UPDATE SET sweep_id=excluded.sweep_id,"
        "source_sequence=excluded.source_sequence,contribution_json=excluded.contribution_json",
        (session.watch_id, data["acquisition_id"], current["sweep_id"], sequence, encoded),
    )


def gather_one(
    session: AlertSession, target: dict[str, Any], budget: AlertBudget, after: int
) -> bool:
    d = _derivation()
    change = next_change(session, after, int(target["highwater"]), budget)
    # At most head/cursor, prior acquisition, and two old/new thread minima.
    budget.admit("alert-gather-control", source_rows=8, input_bytes=2048)
    if change is None:
        totals = session.conn.execute(
            "SELECT "
            + ",".join(COUNTERS)
            + " FROM alert_watch_sweep_totals WHERE watch_id=? AND sweep_id=?",
            (session.watch_id, target["sweep_id"]),
        ).fetchone()
        values = [0] * len(COUNTERS) if totals is None else list(totals)
        with session.fence():
            require_projection(session, target)
            session.conn.execute(
                "UPDATE alert_derivation_targets SET phase='expand',source_cursor=?,"
                + ",".join(name + "=?" for name in COUNTERS)
                + " WHERE target_id=? AND phase='gather'",
                (target["highwater"], *values, target["target_id"]),
            )
        return False
    if change.watch_id != session.watch_id:
        raise d._fail("alert_source_watch_mismatch")
    data = change.data
    if change.kind in {
        "watch_digest_entries",
        "watch_thread_revisions",
        "watch_revision_analyses",
        "watch_acquisitions",
        "watch_threads",
        "watch_comments",
        "watch_issue_reports",
        "watch_issue_evidence",
        "watch_issue_memberships",
        "watch_alert_seen",
    }:
        data = d._required(
            get_version(session, change.kind, change.key, int(target["highwater"]), budget)
        )
    kind = change.kind
    entry_data = None
    contribution = None
    if kind == "watch_digest_entries" and data["kind"] == "thread_update":
        revision = d._required(
            d._version(session, target, budget, "watch_thread_revisions", [data["revision_id"]])
        )
        acquisition = d._required(
            d._version(session, target, budget, "watch_acquisitions", [revision["acquisition_id"]])
        )
        analysis = d._version(
            session,
            target,
            budget,
            "watch_revision_analyses",
            [data["revision_id"]],
            required=False,
        )
        entry_data = (data, revision, acquisition, analysis)
    if kind == "watch_acquisitions":
        prior, current = _contribution(session, data, change.sequence, budget)
        contribution = (
            prior,
            current,
            d._encode(current, budget, "alert-acquisition-contribution"),
        )
    theme = None
    if kind == "watch_issue_evidence":
        entry = d._required(
            d._version(session, target, budget, "watch_digest_entries", [data["entry_id"]])
        )
        theme = (
            session.watch_id,
            data["report_id"],
            data["text_sha256"],
            data["recorded_at_utc"],
            data["entry_id"],
            entry["theme_key"],
        )
    with session.fence():
        require_projection(session, target)
        state = session.conn.execute(
            "SELECT source_cursor,phase FROM alert_derivation_targets WHERE target_id=?",
            (target["target_id"],),
        ).fetchone()
        if state is None or int(state[0]) != after or state[1] != "gather":
            raise d._fail("alert_derivation_cursor_changed")
        if kind == "watch_digest_entries":
            queue(session, target, "theme_entry", str(data["entry_id"]))
        if entry_data is not None:
            entry, revision, acquisition, analysis = entry_data
            eligible = int(analysis is not None and entry["kind"] == "thread_update")
            session.conn.execute(
                "INSERT INTO alert_watch_entries"
                "(watch_id,entry_id,source_thread_id,revision_id,observed,"
                "revision_sequence,acquisition_id,eligible,stage) VALUES (?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(watch_id,entry_id) DO UPDATE SET "
                "source_thread_id=excluded.source_thread_id,"
                "revision_id=excluded.revision_id,observed=excluded.observed,"
                "revision_sequence=excluded.revision_sequence,"
                "acquisition_id=excluded.acquisition_id,eligible=excluded.eligible,stage=4",
                (
                    session.watch_id,
                    entry["entry_id"],
                    entry["source_thread_id"],
                    entry["revision_id"],
                    acquisition["observed_at_utc"],
                    revision["sequence"],
                    revision["acquisition_id"],
                    eligible,
                    0
                    if eligible and float(acquisition["observed_at_utc"]) <= float(target["cutoff"])
                    else 5,
                ),
            )
        elif kind == "watch_digest_entries":
            queue(session, target, "entry", str(data["entry_id"]))
        if kind == "watch_thread_revisions":
            queue(session, target, "revision", str(data["revision_id"]))
        elif kind == "watch_revision_analyses":
            queue(session, target, "revision", str(data["revision_id"]))
        elif kind == "watch_acquisitions":
            old = session.conn.execute(
                "SELECT source_thread_id FROM alert_watch_acquisitions "
                "WHERE watch_id=? AND acquisition_id=?",
                (session.watch_id, data["acquisition_id"]),
            ).fetchone()
            session.conn.execute(
                "INSERT INTO alert_watch_acquisitions VALUES (?,?,?,?) "
                "ON CONFLICT(watch_id,acquisition_id) DO UPDATE SET "
                "source_thread_id=excluded.source_thread_id,observed=excluded.observed",
                (
                    session.watch_id,
                    data["acquisition_id"],
                    data["source_thread_id"],
                    data["observed_at_utc"],
                ),
            )
            for thread_id in {str(data["source_thread_id"])} | (
                set() if old is None else {str(old[0])}
            ):
                previous = session.conn.execute(
                    "SELECT first_observed FROM alert_watch_observations "
                    "WHERE watch_id=? AND source_thread_id=?",
                    (session.watch_id, thread_id),
                ).fetchone()
                first = session.conn.execute(
                    "SELECT observed FROM alert_watch_acquisitions "
                    "WHERE watch_id=? AND source_thread_id=? "
                    "ORDER BY observed,acquisition_id LIMIT 1",
                    (session.watch_id, thread_id),
                ).fetchone()
                if first is not None:
                    session.conn.execute(
                        "INSERT INTO alert_watch_observations VALUES (?,?,?) "
                        "ON CONFLICT(watch_id,source_thread_id) "
                        "DO UPDATE SET first_observed=excluded.first_observed",
                        (session.watch_id, thread_id, first[0]),
                    )
                if previous is not None and (
                    first is None or float(previous[0]) != float(first[0])
                ):
                    queue(session, target, "thread_evidence", thread_id)
            queue(session, target, "acquisition", str(data["acquisition_id"]))
            assert contribution is not None
            _apply_contribution(session, data, change.sequence, *contribution)
        elif kind == "watch_threads":
            previous = session.conn.execute(
                "SELECT community,source_created_utc FROM alert_watch_thread_metadata "
                "WHERE watch_id=? AND source_thread_id=?",
                (session.watch_id, data["source_thread_id"]),
            ).fetchone()
            metadata = (data["subreddit"], data["source_created_utc"])
            session.conn.execute(
                "INSERT INTO alert_watch_thread_metadata VALUES (?,?,?,?,0) "
                "ON CONFLICT(watch_id,source_thread_id) DO UPDATE SET "
                "community_corrected=alert_watch_thread_metadata.community_corrected OR "
                "alert_watch_thread_metadata.community<>excluded.community,"
                "community=excluded.community,source_created_utc=excluded.source_created_utc",
                (session.watch_id, data["source_thread_id"], *metadata),
            )
            if previous is not None and tuple(previous) != metadata:
                queue(session, target, "thread", str(data["source_thread_id"]))
        elif kind == "watch_comments":
            queue(
                session, target, "comment", str(data["source_thread_id"]), str(data["comment_id"])
            )
        elif kind == "watch_issue_memberships":
            previous = session.conn.execute(
                "SELECT issue_id,event_id FROM alert_watch_memberships "
                "WHERE watch_id=? AND source_thread_id=?",
                (session.watch_id, data["source_thread_id"]),
            ).fetchone()
            session.conn.execute(
                "INSERT INTO alert_watch_memberships VALUES (?,?,?,?) "
                "ON CONFLICT(watch_id,source_thread_id) "
                "DO UPDATE SET event_id=excluded.event_id,issue_id=excluded.issue_id "
                "WHERE excluded.event_id>=alert_watch_memberships.event_id",
                (session.watch_id, data["source_thread_id"], data["event_id"], data["issue_id"]),
            )
            if previous is None or (
                int(data["event_id"]) >= int(previous[1])
                and str(previous[0]) != str(data["issue_id"])
            ):
                queue(session, target, "thread_evidence", str(data["source_thread_id"]))
        elif kind == "watch_issue_reports":
            queue(session, target, "report", str(data["report_id"]))
        elif theme is not None:
            session.conn.execute(
                "INSERT INTO alert_watch_themes VALUES (?,?,?,?,?,?) "
                "ON CONFLICT(watch_id,report_id,text_sha256) DO UPDATE SET "
                "recorded=excluded.recorded,entry_id=excluded.entry_id,"
                "theme_key=excluded.theme_key "
                "WHERE (excluded.recorded,excluded.entry_id)<="
                "(alert_watch_themes.recorded,alert_watch_themes.entry_id) "
                "OR excluded.entry_id=alert_watch_themes.entry_id",
                theme,
            )
            queue(session, target, "report", str(data["report_id"]))
        elif kind == "watch_alert_seen":
            session.conn.execute(
                "INSERT OR IGNORE INTO alert_seen_projection VALUES (?,?,?,?)",
                (session.watch_id, data["evidence_key"], data["source_report_id"], "legacy"),
            )
            if data["source_report_id"] is not None:
                session.conn.execute(
                    "INSERT OR IGNORE INTO alert_known_reports VALUES (?,?)",
                    (session.watch_id, data["source_report_id"]),
                )
        session.conn.execute(
            "UPDATE alert_derivation_targets SET source_cursor=? WHERE target_id=?",
            (change.sequence, target["target_id"]),
        )
    return False


def _entry_state(
    session: AlertSession, target: dict[str, Any], budget: AlertBudget, entry_id: str
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None, dict[str, Any]]:
    d = _derivation()
    entry = d._required(d._version(session, target, budget, "watch_digest_entries", [entry_id]))
    revision = d._required(
        d._version(session, target, budget, "watch_thread_revisions", [entry["revision_id"]])
    )
    acquisition = d._required(
        d._version(session, target, budget, "watch_acquisitions", [revision["acquisition_id"]])
    )
    analysis = d._version(
        session,
        target,
        budget,
        "watch_revision_analyses",
        [revision["revision_id"]],
        required=False,
    )
    return entry, revision, analysis, acquisition


def _validate_analysis(
    session: AlertSession, analysis: dict[str, Any], budget: AlertBudget
) -> None:
    import hashlib

    d = _derivation()
    unit = "alert-analysis:" + str(analysis["revision_id"])
    try:
        root = session.root.resolve()
        path = (root / str(analysis["analysis_path"])).resolve()
        if not path.is_relative_to(root):
            raise d._fail("artifact_outside_state", unit=unit)
        size = path.stat().st_size
        budget.admit(unit, input_bytes=2 * size)
        session.check()
        content = path.read_bytes()
        if (
            len(content) != size
            or hashlib.sha256(content).hexdigest() != analysis["analysis_sha256"]
        ):
            raise d._fail("alert_artifact_hash_mismatch", unit=unit)
        session.check()
    except OSError as error:
        raise d._fail("alert_artifact_unreadable", unit=unit) from error


def start_entry(
    session: AlertSession, target: dict[str, Any], budget: AlertBudget, entry_id: str
) -> None:
    d = _derivation()
    # Entry cache point and the final expected-head check; version reads self-charge.
    budget.admit("alert-thread:" + entry_id, source_rows=2, derivations=1, input_bytes=2048)
    entry, revision, analysis, acquisition = _entry_state(session, target, budget, entry_id)
    eligible = analysis is not None and entry["kind"] == "thread_update"
    observed = float(acquisition["observed_at_utc"])
    if not eligible or observed > float(target["cutoff"]):
        with session.fence():
            require_projection(session, target)
            session.conn.execute(
                "UPDATE alert_watch_entries SET eligible=?,observed=?,"
                "revision_sequence=?,revision_id=?,"
                "acquisition_id=?,stage=5 WHERE watch_id=? AND entry_id=?",
                (
                    int(eligible),
                    observed,
                    revision["sequence"],
                    revision["revision_id"],
                    revision["acquisition_id"],
                    session.watch_id,
                    entry_id,
                ),
            )
            queue(session, target, "entry_evidence", entry_id)
        return
    assert analysis is not None
    row = session.conn.execute(
        "SELECT rowid,snapshot_binding FROM alert_watch_entries WHERE watch_id=? AND entry_id=?",
        (session.watch_id, entry_id),
    ).fetchone()
    if row is None:
        raise d._fail("alert_entry_missing", entry_id=entry_id)
    binding = canonical_json([revision["snapshot_path"], revision["snapshot_sha256"]])
    cached = row[1] == binding
    info = (
        read_json_text(
            session.conn,
            "alert_watch_entries",
            "info_json",
            int(row[0]),
            budget,
            "alert-entry-info:" + entry_id,
        )
        if cached
        else None
    )
    payload = d._decode(str(entry["payload_json"]), budget, "alert-entry:" + entry_id)
    budget.admit("alert-entry-payload-binding:" + entry_id, input_bytes=d._json_bound(payload) * 2)
    payload_binding = identity("aep-", [entry["revision_id"], payload])
    if cached and isinstance(info, dict):
        citation = dict(info["_thread_citation"])
    else:
        from threadsense.monitoring.observations import thread_text_sha256

        snapshot = d._read_snapshot(session, revision, analysis, budget)
        budget.admit(
            "alert-thread-text:" + entry_id,
            input_bytes=16 * (len(snapshot.title) + len(snapshot.body or "") + 1),
        )
        body = snapshot.title + "\n" + (snapshot.body or "")
        report = identity("atr-", [session.watch_id, entry["source_thread_id"]])
        text = thread_text_sha256(snapshot.title, snapshot.body)
        citation = {
            "evidence_key": "evidence:" + report + ":" + text,
            "source_report_id": report,
            "source_kind": "thread_text",
            "comment_id": None,
            "permalink": snapshot.permalink,
            "lineage": "firsthand",
            "copy_of_report_id": None,
            "author_sha256": d._account(
                snapshot.source.source_name, snapshot.author.username, budget
            ),
            "text_sha256": text,
            "source_edited_at_utc": None,
            "excerpt": " ".join(body.split())[:280],
            "theme_key": None,
            "independent_person_verified": False,
            "updates": d._updates(report, text, body, budget),
        }
        info = {"source_name": snapshot.source.source_name, "community": snapshot.source.community}
    assert isinstance(info, dict)
    analysis_binding = canonical_json([analysis["analysis_path"], analysis["analysis_sha256"]])
    if cached and info.get("_analysis_binding") != analysis_binding:
        _validate_analysis(session, analysis, budget)
    info["_analysis_binding"] = analysis_binding
    old_payload = info.get("_payload_binding")
    thread = d._required(
        d._version(
            session, target, budget, "watch_threads", [session.watch_id, entry["source_thread_id"]]
        )
    )
    info.update(
        {
            "source_thread_id": entry["source_thread_id"],
            "entry_id": entry_id,
            "revision_id": revision["revision_id"],
            "snapshot_path": revision["snapshot_path"],
            "snapshot_sha256": revision["snapshot_sha256"],
            "analysis_path": analysis["analysis_path"],
            "analysis_sha256": analysis["analysis_sha256"],
            "source_thread_created_utc": thread["source_created_utc"],
            "observed_at_utc": observed,
            "recorded_at_utc": observed,
        }
    )
    citation.update(
        {
            key: value
            for key, value in info.items()
            if not key.startswith("_") and key != "source_name"
        }
    )
    edited = bool(payload["thread_text_changed"])
    citation.update(
        {
            "source_created_utc": thread["source_created_utc"],
            "observation_kind": "edited" if edited else "initial_observation",
            "edit_observed_at_utc": observed if edited else None,
        }
    )
    _refresh_metadata(session, target, citation, budget)
    encoded, digest = d._candidate_text(target, citation, budget)
    info["_thread_citation"] = citation
    info["_payload_binding"] = payload_binding
    info_text = d._encode(info, budget, "alert-entry-info:" + entry_id)
    stage = 1 if not cached else (2 if old_payload != payload_binding else 3)
    with session.fence():
        require_projection(session, target)
        insert_candidate(session, target, citation, int(revision["sequence"]), 0, encoded, digest)
        session.conn.execute(
            "UPDATE alert_watch_entries SET stage=?,info_json=?,snapshot_binding=?,"
            "comment_cursor=?,novel_cursor=0,observed=?,revision_sequence=?,"
            "revision_id=?,acquisition_id=?,eligible=1 "
            "WHERE watch_id=? AND entry_id=?",
            (
                stage,
                info_text,
                binding,
                0 if not cached else -1,
                observed,
                revision["sequence"],
                revision["revision_id"],
                revision["acquisition_id"],
                session.watch_id,
                entry_id,
            ),
        )
        queue(session, target, "entry_evidence", entry_id)


def origin(session: AlertSession, text: str, cutoff: float) -> Any:
    return session.conn.execute(
        "SELECT report_id FROM alert_watch_evidence WHERE watch_id=? AND text_sha256=? "
        "AND eligible=1 AND observed<=? "
        "ORDER BY observed,revision_sequence,entry_id,slot,report_id LIMIT 1",
        (session.watch_id, text, cutoff),
    ).fetchone()


def insert_candidate(
    session: AlertSession,
    target: dict[str, Any],
    citation: dict[str, Any],
    sequence: int,
    slot: int,
    encoded: str,
    digest: str,
) -> None:
    require_projection(session, target)
    session.conn.execute(
        "INSERT INTO alert_watch_occurrences VALUES (?,?,?,?,?,?,?,?,1) "
        "ON CONFLICT(watch_id,evidence_key,entry_id,slot) DO UPDATE SET observed=excluded.observed,"
        "revision_sequence=excluded.revision_sequence,citation_json=excluded.citation_json,"
        "raw_sha256=excluded.raw_sha256,eligible=1",
        (
            session.watch_id,
            citation["evidence_key"],
            citation["entry_id"],
            slot,
            citation["observed_at_utc"],
            sequence,
            encoded,
            digest,
        ),
    )
    selected = session.conn.execute(
        "SELECT entry_id,slot FROM alert_watch_occurrences WHERE watch_id=? AND evidence_key=? "
        "ORDER BY eligible DESC,observed,revision_sequence,entry_id,slot LIMIT 1",
        (session.watch_id, citation["evidence_key"]),
    ).fetchone()
    if selected is None or tuple(selected) != (citation["entry_id"], slot):
        return
    _write_candidate(session, target, citation, sequence, slot, encoded, digest, 1)


def _write_candidate(
    session: AlertSession,
    target: dict[str, Any],
    citation: dict[str, Any],
    sequence: int,
    slot: int,
    encoded: str,
    digest: str,
    eligible: int,
) -> None:
    report, text = str(citation["source_report_id"]), str(citation["text_sha256"])
    previous = session.conn.execute(
        "SELECT raw_sha256,observed,revision_sequence,entry_id,slot,eligible "
        "FROM alert_watch_evidence "
        "WHERE watch_id=? AND evidence_key=?",
        (session.watch_id, citation["evidence_key"]),
    ).fetchone()
    session.conn.execute(
        "INSERT INTO alert_watch_evidence"
        "(watch_id,evidence_key,report_id,text_sha256,observed,revision_sequence,"
        "entry_id,slot,citation_json,raw_sha256,copy_of_report_id,community_key,"
        "done,source_thread_id,comment_id,eligible) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0,?,?,?) ON CONFLICT(watch_id,evidence_key) DO UPDATE SET "
        "observed=excluded.observed,revision_sequence=excluded.revision_sequence,"
        "entry_id=excluded.entry_id,"
        "slot=excluded.slot,citation_json=excluded.citation_json,raw_sha256=excluded.raw_sha256,"
        "copy_of_report_id=excluded.copy_of_report_id,community_key=excluded.community_key,"
        "source_thread_id=excluded.source_thread_id,comment_id=excluded.comment_id,"
        "eligible=excluded.eligible,"
        "done=CASE WHEN alert_watch_evidence.raw_sha256=excluded.raw_sha256 AND "
        "alert_watch_evidence.eligible=excluded.eligible THEN alert_watch_evidence.done ELSE 0 END",
        (
            session.watch_id,
            citation["evidence_key"],
            report,
            text,
            citation["observed_at_utc"],
            sequence,
            citation["entry_id"],
            slot,
            encoded,
            digest,
            citation["copy_of_report_id"],
            "community:" + str(citation["community"]).casefold(),
            citation["source_thread_id"],
            citation["comment_id"],
            eligible,
        ),
    )
    order = (citation["observed_at_utc"], sequence, citation["entry_id"], slot, eligible)
    if previous is None or tuple(previous[1:]) != order:
        queue(session, target, "text", "", text)
        queue(session, target, "copy", report, text)


def _refresh_metadata(
    session: AlertSession, target: dict[str, Any], citation: dict[str, Any], budget: AlertBudget
) -> None:
    d = _derivation()
    thread_id = str(citation["source_thread_id"])
    thread = d._required(
        d._version(session, target, budget, "watch_threads", [session.watch_id, thread_id])
    )
    budget.admit("alert-community-correction:" + thread_id, source_rows=1, input_bytes=32)
    metadata = session.conn.execute(
        "SELECT community_corrected FROM alert_watch_thread_metadata "
        "WHERE watch_id=? AND source_thread_id=?",
        (session.watch_id, thread_id),
    ).fetchone()
    if metadata is None:
        raise d._fail("alert_thread_metadata_missing", source_thread_id=thread_id)
    if metadata[0]:
        citation["community"] = thread["subreddit"]
    citation["source_thread_created_utc"] = thread["source_created_utc"]
    citation["issue_id"] = d._membership(session, str(target["target_id"]), thread_id, budget)
    if citation["source_kind"] == "thread_text":
        budget.admit("alert-first-observation:" + thread_id, source_rows=1, input_bytes=64)
        first = session.conn.execute(
            "SELECT observed FROM alert_watch_acquisitions WHERE watch_id=? AND source_thread_id=? "
            "ORDER BY observed,acquisition_id LIMIT 1",
            (session.watch_id, thread_id),
        ).fetchone()
        if first is None:
            raise d._fail("alert_first_observation_missing")
        citation["first_observed_at_utc"] = float(first[0])
        citation["source_created_utc"] = thread["source_created_utc"]
    else:
        observed = d._required(
            d._version(
                session,
                target,
                budget,
                "watch_comments",
                [session.watch_id, thread_id, citation["comment_id"]],
            )
        )
        citation["first_observed_at_utc"] = observed["first_observed_at_utc"]
        lineage = d._version(
            session,
            target,
            budget,
            "watch_issue_reports",
            [citation["source_report_id"]],
            required=False,
        )
        citation["lineage"] = "firsthand" if lineage is None else lineage["lineage"]
        citation["copy_of_report_id"] = None if lineage is None else lineage["copy_of_report_id"]
        budget.admit("alert-theme-dependency", source_rows=1, input_bytes=64)
        theme = session.conn.execute(
            "SELECT entry_id FROM alert_watch_themes "
            "WHERE watch_id=? AND report_id=? AND text_sha256=?",
            (session.watch_id, citation["source_report_id"], citation["text_sha256"]),
        ).fetchone()
        if theme is not None:
            themed = d._required(
                d._version(session, target, budget, "watch_digest_entries", [theme[0]])
            )
            citation["theme_key"] = themed["theme_key"]
    citation.pop("is_exact_copy", None)
    citation["issue_policy_sha256"] = identity("aip-", target.get("basis", {}).get("issue_policy"))


def citation_eligible(
    session: AlertSession, target: dict[str, Any], citation: dict[str, Any], budget: AlertBudget
) -> bool:
    budget.admit("alert-evidence-eligibility", source_rows=1, input_bytes=64)
    row = session.conn.execute(
        "SELECT eligible FROM alert_watch_evidence WHERE watch_id=? AND evidence_key=?",
        (session.watch_id, citation["evidence_key"]),
    ).fetchone()
    return (
        row is not None
        and bool(row[0])
        and float(citation["observed_at_utc"]) <= float(target["cutoff"])
    )


def _refresh_occurrence(
    session: AlertSession, target: dict[str, Any], budget: AlertBudget, rowid: int
) -> tuple[dict[str, Any], int, int, str, str, int]:
    d = _derivation()
    citation = read_json_text(
        session.conn,
        "alert_watch_occurrences",
        "citation_json",
        rowid,
        budget,
        "alert-occurrence-refresh",
    )
    if not isinstance(citation, dict):
        raise d._fail("alert_citation_unreadable")
    entry, revision, analysis, acquisition = _entry_state(
        session, target, budget, str(citation["entry_id"])
    )
    citation.update(
        {
            "observed_at_utc": acquisition["observed_at_utc"],
            "recorded_at_utc": acquisition["observed_at_utc"],
            "revision_id": revision["revision_id"],
            "snapshot_path": revision["snapshot_path"],
            "snapshot_sha256": revision["snapshot_sha256"],
        }
    )
    if analysis is not None:
        citation.update(
            {
                "analysis_path": analysis["analysis_path"],
                "analysis_sha256": analysis["analysis_sha256"],
            }
        )
    if citation["observation_kind"] == "edited":
        citation["edit_observed_at_utc"] = acquisition["observed_at_utc"]
    _refresh_metadata(session, target, citation, budget)
    encoded, digest = d._candidate_text(target, citation, budget)
    slot = session.conn.execute(
        "SELECT slot FROM alert_watch_occurrences WHERE rowid=?", (rowid,)
    ).fetchone()
    assert slot is not None
    return (
        citation,
        int(revision["sequence"]),
        int(slot[0]),
        encoded,
        digest,
        int(analysis is not None and entry["kind"] == "thread_update"),
    )


def _refresh_evidence(
    session: AlertSession, target: dict[str, Any], budget: AlertBudget, key: str
) -> tuple[dict[str, Any], int, int, str, str, int] | None:
    budget.admit("alert-evidence-minimum:" + key, source_rows=3, derivations=1, input_bytes=1024)
    selected = session.conn.execute(
        "SELECT rowid FROM alert_watch_occurrences WHERE watch_id=? AND evidence_key=? "
        "ORDER BY eligible DESC,observed,revision_sequence,entry_id,slot LIMIT 1",
        (session.watch_id, key),
    ).fetchone()
    if selected is None:
        return None
    return _refresh_occurrence(session, target, budget, int(selected[0]))


def revisit_one(session: AlertSession, target: dict[str, Any], budget: AlertBudget) -> bool:
    d = _derivation()
    budget.admit("alert-affected-select", source_rows=8, derivations=1, input_bytes=2048)
    job = session.conn.execute(
        "SELECT kind,report_id,text_sha256,cursor FROM alert_target_revisits "
        "WHERE target_id=? AND done=0 "
        "ORDER BY kind,report_id,text_sha256 LIMIT 1",
        (target["target_id"],),
    ).fetchone()
    if job is None:
        with session.fence():
            require_projection(session, target)
            expected = target["inherited_coverage"]
            session.conn.execute(
                "INSERT OR IGNORE INTO alert_presence_complete VALUES (?,?,?)",
                (target["target_id"], target["highwater"], target["basis_sha256"]),
            )
            session.conn.execute(
                "INSERT INTO alert_presence_bindings VALUES (?,?,?,?,?,?,?)",
                (
                    target["target_id"],
                    session.watch_id,
                    target["highwater"],
                    target["basis_sha256"],
                    expected["target_id"],
                    expected["highwater"],
                    identity("ainherited-", expected),
                ),
            )
            session.conn.execute(
                "UPDATE alert_derivation_targets SET phase='ready' WHERE target_id=?",
                (target["target_id"],),
            )
        return True
    kind, key, secondary, cursor = map(str, job)
    entry_kinds = {
        "entry": "entry_id",
        "revision": "revision_id",
        "acquisition": "acquisition_id",
        "thread": "source_thread_id",
    }
    row = None
    if kind in entry_kinds:
        row = session.conn.execute(
            "SELECT entry_id FROM alert_watch_entries WHERE watch_id=? AND "
            + entry_kinds[kind]
            + "=? AND entry_id>? ORDER BY entry_id LIMIT 1",
            (session.watch_id, key, cursor),
        ).fetchone()
    elif kind == "cutoff":
        cutoff_after = (float(key), "") if not cursor else json.loads(cursor)
        row = session.conn.execute(
            "SELECT entry_id,observed FROM alert_watch_entries "
            "WHERE watch_id=? AND observed>? AND observed<=? "
            "AND (observed,entry_id)>(?,?) ORDER BY observed,entry_id LIMIT 1",
            (
                session.watch_id,
                float(key),
                float(secondary),
                float(cutoff_after[0]),
                str(cutoff_after[1]),
            ),
        ).fetchone()
    elif kind == "entry_evidence":
        occurrence_after = ("", -1) if not cursor else json.loads(cursor)
        row = session.conn.execute(
            "SELECT evidence_key,slot,rowid FROM alert_watch_occurrences "
            "WHERE watch_id=? AND entry_id=? "
            "AND (evidence_key,slot)>(?,?) ORDER BY evidence_key,slot LIMIT 1",
            (session.watch_id, key, str(occurrence_after[0]), int(occurrence_after[1])),
        ).fetchone()
    elif kind == "adopt":
        row = session.conn.execute(
            "SELECT evidence_key,overlay FROM alert_derivation_decisions "
            "WHERE target_id=? AND evidence_key>? ORDER BY evidence_key LIMIT 1",
            (target["target_id"], cursor),
        ).fetchone()
    elif kind == "theme_entry":
        after = ["", ""] if not cursor else json.loads(cursor)
        row = session.conn.execute(
            "SELECT report_id,text_sha256 FROM alert_watch_themes WHERE watch_id=? AND entry_id=? "
            "AND (report_id,text_sha256)>(?,?) ORDER BY report_id,text_sha256 LIMIT 1",
            (session.watch_id, key, after[0], after[1]),
        ).fetchone()
    else:
        conditions, parameters = "", []
        if kind == "text":
            conditions, parameters = "text_sha256=? AND ", [secondary]
        elif kind == "copy":
            conditions, parameters = "copy_of_report_id=? AND text_sha256=? AND ", [key, secondary]
        elif kind == "report":
            conditions, parameters = "report_id=? AND ", [key]
        elif kind == "thread_evidence":
            conditions, parameters = "source_thread_id=? AND ", [key]
        elif kind == "comment":
            conditions, parameters = "source_thread_id=? AND comment_id=? AND ", [key, secondary]
        elif kind == "evidence":
            conditions, parameters = "evidence_key=? AND ", [key]
        elif kind != "policy":
            raise d._fail("alert_revisit_kind_invalid", kind=kind)
        row = session.conn.execute(
            "SELECT evidence_key FROM alert_watch_evidence WHERE watch_id=? AND "
            + conditions
            + "evidence_key>? ORDER BY evidence_key LIMIT 1",
            (session.watch_id, *parameters, cursor),
        ).fetchone()
    prepared = None
    if row is not None and kind == "entry_evidence":
        prepared = _refresh_occurrence(session, target, budget, int(row[2]))
    elif (
        row is not None
        and kind not in entry_kinds
        and kind not in {"cutoff", "adopt", "theme_entry"}
    ):
        prepared = _refresh_evidence(session, target, budget, str(row[0]))
    with session.fence():
        require_projection(session, target)
        if row is None:
            session.conn.execute(
                "UPDATE alert_target_revisits SET done=1 "
                "WHERE target_id=? AND kind=? AND report_id=? AND text_sha256=?",
                (target["target_id"], kind, key, secondary),
            )
        else:
            next_cursor = str(row[0])
            if kind == "adopt":
                if not int(row[1]):
                    session.conn.execute(
                        "UPDATE alert_derivation_targets SET evidence_count=evidence_count+1 "
                        "WHERE target_id=?",
                        (target["target_id"],),
                    )
            elif kind == "theme_entry":
                queue(session, target, "report", str(row[0]))
                next_cursor = canonical_json([row[0], row[1]])
            elif kind in entry_kinds or kind == "cutoff":
                session.conn.execute(
                    "UPDATE alert_watch_entries SET stage=4 WHERE watch_id=? AND entry_id=?",
                    (session.watch_id, row[0]),
                )
                session.conn.execute(
                    "UPDATE alert_derivation_targets SET phase='expand' WHERE target_id=?",
                    (target["target_id"],),
                )
                if kind == "cutoff":
                    next_cursor = canonical_json([row[1], row[0]])
            elif kind == "entry_evidence":
                assert prepared is not None
                citation, sequence, slot, encoded, digest, eligible = prepared
                session.conn.execute(
                    "UPDATE alert_watch_occurrences SET observed=?,revision_sequence=?,"
                    "citation_json=?,raw_sha256=?,eligible=? WHERE rowid=?",
                    (citation["observed_at_utc"], sequence, encoded, digest, eligible, row[2]),
                )
                queue(session, target, "evidence", str(row[0]))
                # Cutoff changes can remove an explicit origin without changing
                # its intrinsic bytes/order. Revisit both dependency families.
                queue(session, target, "text", "", str(citation["text_sha256"]))
                queue(
                    session,
                    target,
                    "copy",
                    str(citation["source_report_id"]),
                    str(citation["text_sha256"]),
                )
                next_cursor = canonical_json([row[0], row[1]])
            else:
                if prepared is not None:
                    _write_candidate(session, target, *prepared)
                session.conn.execute(
                    "UPDATE alert_watch_evidence SET done=0 WHERE watch_id=? AND evidence_key=?",
                    (session.watch_id, row[0]),
                )
            session.conn.execute(
                "UPDATE alert_target_revisits SET cursor=? "
                "WHERE target_id=? AND kind=? AND report_id=? AND text_sha256=?",
                (next_cursor, target["target_id"], kind, key, secondary),
            )
    return False
