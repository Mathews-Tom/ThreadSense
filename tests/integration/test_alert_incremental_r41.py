from __future__ import annotations

import builtins
import copy
import hashlib
import io
import json
import os
import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from recorded_reddit import ReplayPhase
from test_alert_resources import assert_accounting, config_file, fresh_alerts
from test_watch_alerts import artifact_bytes, changed_phase, read_collection, read_report, sweep
from test_watch_incremental import (
    PROFILE_PATH,
    WATCH_ID,
    create_watch,
    install_transport,
    isolate_environment,
    phases_by_name,
    run_cli,
)

from threadsense.monitoring.alert_sources import next_change
from threadsense.monitoring.alerts import present_watch_alerts
from threadsense.monitoring.issue_records import report_id_for
from threadsense.monitoring.observations import comment_text_sha256


@pytest.fixture
def watch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    isolate_environment(monkeypatch, tmp_path)
    return create_watch()


def rows(watch: Path, sql: str, args: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with closing(sqlite3.connect(watch)) as connection:
        connection.row_factory = sqlite3.Row
        return [dict(row) for row in connection.execute(sql, args)]


def mutate(watch: Path, sql: str, args: tuple[Any, ...]) -> None:
    # Change real recorded source metadata, allowing the normal capture triggers
    # to record the version. Never manufacture alert targets or reports.
    with closing(sqlite3.connect(watch)) as connection:
        connection.execute(sql, args)
        connection.commit()


def head(watch: Path) -> dict[str, Any]:
    return rows(watch, "SELECT * FROM alert_watch_heads WHERE watch_id=?", (WATCH_ID,))[0]


def assert_completed(watch: Path, result: dict[str, Any]) -> dict[str, Any]:
    current = head(watch)
    report = rows(
        watch,
        "SELECT target_id FROM watch_alert2_reports WHERE report_id=?",
        (result["report_id"],),
    )[0]
    completions = rows(
        watch,
        "SELECT * FROM alert_coverage_completions WHERE target_id=?",
        (report["target_id"],),
    )
    assert len(completions) == 1
    terminal = completions[0]
    assert current["pending_target"] is None
    assert current["completed_target"] == report["target_id"] == terminal["target_id"]
    assert current["completed_checkpoint"] == terminal["checkpoint_id"]
    assert current["completed_highwater"] == terminal["highwater"]
    assert current["evidence_count"] == result["evidence_count"] > 0
    document = json.loads(terminal["terminal_json"])
    assert document["counts"]["evidence_count"] == result["evidence_count"]
    assert document["counts"]["event_count"] == read_report(result)["event_count"]
    assert document["inherited_coverage"]["target_id"] == terminal["expected_target"]
    assert document["inherited_coverage"]["highwater"] == terminal["expected_highwater"]
    return terminal


def established_snapshots(watch: Path) -> dict[Path, bytes]:
    paths = {
        (watch.parent / row["snapshot_path"]).resolve()
        for row in rows(
            watch,
            "SELECT DISTINCT r.snapshot_path FROM watch_thread_revisions r "
            "JOIN alert_watch_entries e ON e.watch_id=r.watch_id AND e.revision_id=r.revision_id "
            "WHERE e.watch_id=? AND e.snapshot_binding IS NOT NULL",
            (WATCH_ID,),
        )
    }
    assert paths
    return {path: path.read_bytes() for path in paths}


@contextmanager
def forbid_old_source_io(
    monkeypatch: pytest.MonkeyPatch,
    snapshots: dict[Path, bytes],
) -> Iterator[None]:
    """Guard actual IO, decoding and hashing, not just a high-level cache method."""
    paths = set(snapshots)
    payloads = set(snapshots.values())
    text_payloads = {payload.decode("utf-8") for payload in payloads}
    original_open = builtins.open
    original_io_open = io.open
    original_read = Path.read_bytes
    original_loads = json.loads
    original_sha256 = hashlib.sha256

    def check_path(value: Any) -> None:
        if isinstance(value, (str, bytes, os.PathLike)):
            assert Path(os.fsdecode(value)).resolve() not in paths, "old snapshot reopened"

    def guarded_open(file: Any, *args: Any, **kwargs: Any) -> Any:
        check_path(file)
        return original_open(file, *args, **kwargs)

    def guarded_io_open(file: Any, *args: Any, **kwargs: Any) -> Any:
        check_path(file)
        return original_io_open(file, *args, **kwargs)

    def guarded_read(path: Path) -> bytes:
        check_path(path)
        return original_read(path)

    def guarded_loads(value: Any, *args: Any, **kwargs: Any) -> Any:
        if isinstance(value, str):
            assert value not in text_payloads, "old snapshot decoded again"
        elif isinstance(value, (bytes, bytearray)):
            assert bytes(value) not in payloads, "old snapshot decoded again"
        return original_loads(value, *args, **kwargs)

    def guarded_sha256(value: Any = b"", *args: Any, **kwargs: Any) -> Any:
        assert bytes(value) not in payloads, "old snapshot hashed again"
        return original_sha256(value, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(builtins, "open", guarded_open)
        patch.setattr(io, "open", guarded_io_open)
        patch.setattr(Path, "read_bytes", guarded_read)
        patch.setattr(json, "loads", guarded_loads)
        patch.setattr(hashlib, "sha256", guarded_sha256)
        yield


def protect_presenter(
    monkeypatch: pytest.MonkeyPatch,
    watch: Path,
) -> list[dict[str, Any]]:
    # Sweep collection may legitimately read old revisions. Only the alert
    # operation, invoked by the real CLI after collection, must reuse its facts.
    import threadsense.cli as cli

    original = present_watch_alerts
    snapshots = established_snapshots(watch)
    accounting: list[dict[str, Any]] = []

    def present(*args: Any, **kwargs: Any) -> dict[str, Any]:
        nonlocal snapshots
        with forbid_old_source_io(monkeypatch, snapshots):
            result = original(*args, **kwargs)
        accounting.append(result["accounting"])
        if result["status"] not in ("backlog", "resource_refused"):
            snapshots = established_snapshots(watch)
        return result

    monkeypatch.setattr(cli, "present_watch_alerts", present)
    return accounting


def identities(result: dict[str, Any]) -> set[tuple[str, str]]:
    evidence = read_collection(result, "evidence")
    keys = [(item["source_report_id"], item["text_sha256"]) for item in evidence]
    assert len(keys) == len(set(keys)) == result["evidence_count"]
    return set(keys)


def rewrite_comment_phase(
    tmp_path: Path,
    *,
    body: str,
    comment_id: str,
    later_origin: bool = False,
) -> ReplayPhase:
    phase = changed_phase(tmp_path, body=body)
    routes = []
    for index, route in enumerate(phase.routes):
        if route.response_path is None or "alert-source" not in route.response_path.name:
            routes.append(route)
            continue
        payload = json.loads(route.response_path.read_bytes())

        def visit(node: Any) -> None:
            if isinstance(node, list):
                for child in list(node):
                    if (
                        later_origin
                        and isinstance(child, dict)
                        and child.get("kind") == "t1"
                        and child.get("data", {}).get("id") == "ka4"
                    ):
                        origin = copy.deepcopy(child)
                        origin["data"].update(id="ka6", name="t1_ka6")
                        origin["data"]["permalink"] = origin["data"]["permalink"].replace(
                            "/ka4/", "/ka6/"
                        )
                        node.append(origin)
                    visit(child)
            elif isinstance(node, dict):
                data = node.get("data")
                if node.get("kind") == "t1" and isinstance(data, dict) and data.get("id") == "ka4":
                    data.update(id=comment_id, name="t1_" + comment_id)
                    data["permalink"] = data["permalink"].replace("/ka4/", f"/{comment_id}/")
                for child in node.values():
                    visit(child)

        visit(payload)
        path = tmp_path / f"r41-{comment_id}-{index}.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        routes.append(replace(route, response_path=path))
    return replace(phase, routes=tuple(routes))


def test_successive_targets_reuse_old_bytes_and_inherit_unique_evidence(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = sweep(monkeypatch, "initial")
    before = artifact_bytes(baseline)
    frozen_sources = established_snapshots(watch)
    frozen_artifacts = {
        path: path.read_bytes()
        for path in (watch.parent / WATCH_ID / "alerts").rglob("*")
        if path.is_file()
    }
    assert frozen_artifacts
    baseline_ids = identities(baseline)
    previous = assert_completed(watch, baseline)
    protect_presenter(monkeypatch, watch)
    # The guard includes the snapshots just established by the preceding target.
    for phase, expected_comment in (("search_omits_known", "reddit:ka4"), ("unchanged", None)):
        changed = sweep(monkeypatch, phase)
        current_ids = identities(changed)
        assert baseline_ids <= current_ids
        assert changed["evidence_count"] >= baseline["evidence_count"]
        terminal = assert_completed(watch, changed)
        assert terminal["expected_target"] == previous["target_id"]
        assert terminal["expected_highwater"] == previous["highwater"]
        if expected_comment is not None:
            citations = [
                item for event in read_collection(changed, "changes") for item in event["evidence"]
            ]
            assert any(item["comment_id"] == expected_comment for item in citations)
            assert all(
                item["revision_id"] and item["entry_id"] and item["text_sha256"]
                for item in citations
            )
        assert artifact_bytes(baseline) == before
        previous = terminal
        baseline_ids = current_ids
    unchanged = sweep(monkeypatch, "score_only")
    assert identities(unchanged) == baseline_ids
    assert read_collection(unchanged, "changes") == []
    assert_completed(watch, unchanged)
    assert all(path.read_bytes() == value for path, value in frozen_sources.items())
    assert all(path.read_bytes() == value for path, value in frozen_artifacts.items())


def test_unchanged_history_does_not_accumulate_admitted_derivation_work(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = sweep(monkeypatch, "initial")
    sweep(monkeypatch, "unchanged")
    accounting = protect_presenter(monkeypatch, watch)
    totals: list[tuple[int, int]] = []
    for _ in range(8):
        accounting.clear()
        result = sweep(monkeypatch, "unchanged")
        assert identities(result) == identities(baseline)
        assert read_collection(result, "changes") == []
        assert_completed(watch, result)
        assert accounting
        totals.append(
            (
                sum(item["used"]["source_rows"] for item in accounting),
                sum(item["used"]["derivations"] for item in accounting),
            )
        )
    # Aggregate across every invocation, not just the cheap final continuation.
    # Each target adds real source versions and immutable history; the same next
    # delta must cost the same after eight targets as after the first one.
    assert totals[0][0] > 0 and totals[0][1] > 0
    assert all(total == totals[0] for total in totals[1:])


def test_interleaved_other_watch_never_enters_gather_or_its_admitted_work(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import threadsense.monitoring.alert_projection_work as projection_work

    sweep(monkeypatch, "initial")
    sweep(monkeypatch, "unchanged")  # establish reusable entries before measurement
    profile = tmp_path / "other-watch.toml"
    profile.write_text(
        PROFILE_PATH.read_text().replace('id = "sync-conflicts"', 'id = "other-watch"')
    )
    create_watch(profile)
    measured: list[tuple[int, int]] = []
    original = next_change

    def next_own(session: Any, after: int, through: int, budget: Any) -> Any:
        before = budget.accounting()["used"]["source_rows"]
        result = original(session, after, through, budget)
        if session.watch_id == WATCH_ID:
            cost = budget.accounting()["used"]["source_rows"] - before
            assert result is None or result.watch_id == WATCH_ID
            measured.append((0 if result is None else 1, cost))
        return result

    monkeypatch.setattr(projection_work, "next_change", next_own)
    controls: list[tuple[int, int]] = []
    for noise in (0, 12):
        for _ in range(noise):
            install_transport(monkeypatch, phases_by_name()["unchanged"])
            code, payload = run_cli("watch", "run", "--id", "other-watch")
            assert code == 0, payload
        measured.clear()
        result = sweep(monkeypatch, "unchanged")
        assert read_collection(result, "changes") == []
        assert_completed(watch, result)
        assert measured
        controls.append((sum(item[0] for item in measured), sum(item[1] for item in measured)))
    # Both own sweeps produce the same kinds of source versions. Adding twelve
    # complete foreign sweeps must not add a candidate or an admitted source row.
    assert controls[0] == controls[1]


@pytest.mark.parametrize("correction", ["membership", "theme", "policy"])
def test_semantic_corrections_overlay_cached_facts_without_reopening_snapshots(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
    correction: str,
) -> None:
    baseline = sweep(monkeypatch, "initial")
    frozen = artifact_bytes(baseline)
    before = read_collection(baseline, "evidence")
    source = next(item for item in before if item["comment_id"] == "reddit:ka1")
    destination = None
    if correction == "membership":
        destination = rows(
            watch,
            "SELECT issue_id FROM watch_issues WHERE watch_id=? AND issue_id!=? "
            "ORDER BY issue_id LIMIT 1",
            (WATCH_ID, source["issue_id"]),
        )[0]["issue_id"]
        code, payload = run_cli(
            "issues",
            "link",
            "--watch",
            WATCH_ID,
            "--issue",
            destination,
            "--thread",
            source["source_thread_id"],
            "--reason",
            "R41 reviewed membership correction",
        )
        assert code == 0, payload
    elif correction == "theme":
        theme = rows(
            watch,
            "SELECT entry_id FROM alert_watch_themes "
            "WHERE watch_id=? AND report_id=? AND text_sha256=?",
            (WATCH_ID, source["source_report_id"], source["text_sha256"]),
        )[0]
        mutate(
            watch,
            "UPDATE watch_digest_entries SET theme_key=? WHERE watch_id=? AND entry_id=?",
            ("r41-reviewed-theme", WATCH_ID, theme["entry_id"]),
        )
    else:
        monkeypatch.setenv("THREADSENSE_ISSUES_CANDIDATE_WINDOW_DAYS", "14")
    protect_presenter(monkeypatch, watch)
    changed = sweep(monkeypatch, "unchanged")
    evidence = read_collection(changed, "evidence")
    corrected = next(item for item in evidence if item["evidence_key"] == source["evidence_key"])
    assert identities(changed) == identities(baseline)
    assert corrected["source_report_id"] == source["source_report_id"]
    if correction == "membership":
        assert corrected["issue_id"] == destination
    elif correction == "theme":
        assert corrected["theme_key"] == "r41-reviewed-theme"
    else:
        assert (
            read_report(changed)["basis"]["issue_policy"]
            != read_report(baseline)["basis"]["issue_policy"]
        )
        assert corrected["issue_policy_sha256"] != source["issue_policy_sha256"]
    assert_completed(watch, changed)
    repeated = sweep(monkeypatch, "unchanged")
    assert repeated["evidence_count"] == changed["evidence_count"]
    assert read_collection(repeated, "changes") == []
    assert read_collection(baseline, "evidence") == before
    assert artifact_bytes(baseline) == frozen


def test_backward_cutoff_defers_new_entries_then_reuses_them_when_eligible(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import threadsense.cli as cli

    baseline = sweep(monkeypatch, "initial")
    frozen = artifact_bytes(baseline)
    previous = head(watch)
    original = present_watch_alerts
    corrected = False

    def earlier_cutoff(*args: Any, **kwargs: Any) -> dict[str, Any]:
        nonlocal corrected
        if not corrected:
            newest = rows(
                watch,
                "SELECT run_id FROM watch_sweeps WHERE watch_id=? "
                "ORDER BY finished_at_utc DESC LIMIT 1",
                (WATCH_ID,),
            )[0]["run_id"]
            mutate(
                watch,
                "UPDATE watch_sweeps SET finished_at_utc=? WHERE run_id=?",
                (previous["cutoff"] - 1, newest),
            )
            corrected = True
        return original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(cli, "present_watch_alerts", earlier_cutoff)
        deferred = sweep(monkeypatch, "search_omits_known")
    assert head(watch)["cutoff"] < previous["cutoff"]
    assert deferred["evidence_count"] == baseline["evidence_count"]
    assert read_collection(deferred, "changes") == []
    assert not any(
        item["comment_id"] == "reddit:ka4" for item in read_collection(deferred, "evidence")
    )
    assert_completed(watch, deferred)
    protect_presenter(monkeypatch, watch)
    eligible = sweep(monkeypatch, "unchanged")
    ka4 = [
        item for item in read_collection(eligible, "evidence") if item["comment_id"] == "reddit:ka4"
    ]
    assert len(ka4) == 1
    assert any(
        item["comment_id"] == "reddit:ka4"
        for event in read_collection(eligible, "changes")
        for item in event["evidence"]
    )
    assert eligible["evidence_count"] > deferred["evidence_count"]
    assert identities(deferred) < identities(eligible)
    assert_completed(watch, eligible)
    assert artifact_bytes(baseline) == frozen


class BeforeAlertDeath(BaseException):
    """The real sweep is committed; the first alert invocation has not begun."""


def test_selected_sweep_counts_include_acquisitions_admitted_under_prior_coverage(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import threadsense.cli as cli

    baseline = sweep(monkeypatch, "initial")
    frozen = artifact_bytes(baseline)
    initial_runs = {item["run_id"] for item in rows(watch, "SELECT run_id FROM watch_sweeps")}

    def die(*args: Any, **kwargs: Any) -> Any:
        raise BeforeAlertDeath

    with monkeypatch.context() as patch:
        patch.setattr(cli, "present_watch_alerts", die)
        install_transport(monkeypatch, phases_by_name()["search_omits_known"])
        with pytest.raises(BeforeAlertDeath):
            run_cli("watch", "run", "--id", WATCH_ID)
    delayed = next(
        item
        for item in rows(watch, "SELECT * FROM watch_sweeps")
        if item["run_id"] not in initial_runs
    )
    # A later real sweep publishes all evidence and admits the delayed sweep's
    # acquisitions, even though that sweep has never been the selected target.
    current = sweep(monkeypatch, "unchanged")
    current_count = current["evidence_count"]
    contributions = rows(
        watch,
        "SELECT * FROM alert_watch_acquisition_contributions WHERE watch_id=? AND sweep_id=?",
        (WATCH_ID, delayed["run_id"]),
    )
    assert contributions
    assert not rows(
        watch,
        "SELECT target_id FROM watch_alert2_targets WHERE watch_id=? AND sweep_id=?",
        (WATCH_ID, delayed["run_id"]),
    )
    old_head = head(watch)
    # A corrected source sweep completion becomes the most recent version, with
    # an intentionally nonmonotone cutoff; there is no synthetic alert report.
    mutate(
        watch,
        "UPDATE watch_sweeps SET finished_at_utc=? WHERE run_id=?",
        (delayed["finished_at_utc"] + 0.001, delayed["run_id"]),
    )
    protect_presenter(monkeypatch, watch)
    code, result = run_cli("watch", "alerts", "--id", WATCH_ID)
    assert code == 0, result
    from test_watch_alerts import drain_alerts

    result = drain_alerts(result)
    report = read_report(result)
    assert report["window"]["sweep_run_id"] == delayed["run_id"]
    expected = rows(
        watch,
        "SELECT COUNT(*) AS acquired_threads,SUM(parsed_comment_count) AS parsed_comments,"
        "SUM(COALESCE(reported_comment_count,0)) AS source_reported_comments,"
        "SUM(absent_known_comment_count) AS absent_known_comments "
        "FROM watch_acquisitions WHERE watch_id=? AND run_id=?",
        (WATCH_ID, delayed["run_id"]),
    )[0]
    assert expected["acquired_threads"] > 0 and expected["parsed_comments"] > 0
    for key, value in expected.items():
        assert report["bounded_denominators"][key] == value
    assert result["evidence_count"] == current_count
    assert report["event_count"] == len(read_collection(result, "changes")) == 0
    terminal = assert_completed(watch, result)
    assert terminal["expected_target"] == old_head["completed_target"]
    assert artifact_bytes(baseline) == frozen


def test_feedback_only_target_has_nonzero_inherited_count_and_one_terminal(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = sweep(monkeypatch, "initial")
    frozen = artifact_bytes(baseline)
    old_head = head(watch)
    finding = rows(
        watch,
        "SELECT entry_id FROM watch_digest_entries WHERE watch_id=? AND kind='finding' "
        "ORDER BY entry_id LIMIT 1",
        (WATCH_ID,),
    )[0]["entry_id"]
    code, feedback = run_cli(
        "feedback",
        "record",
        "--watch",
        WATCH_ID,
        "--target",
        finding,
        "--target-kind",
        "finding",
        "--kind",
        "preference",
        "--value",
        "useful",
        "--key",
        "r41-terminal-feedback",
    )
    assert code == 0, feedback
    protect_presenter(monkeypatch, watch)
    result = sweep(monkeypatch, "unchanged")
    assert result["evidence_count"] == baseline["evidence_count"] > 0
    assert read_report(result)["event_count"] == 0
    assert read_collection(result, "changes") == []
    assert identities(result) == identities(baseline)
    terminal = assert_completed(watch, result)
    assert terminal["expected_target"] == old_head["completed_target"]
    assert terminal["highwater"] > old_head["completed_highwater"]
    assert not rows(
        watch,
        "SELECT evidence_key FROM alert_derivation_decisions WHERE target_id=?",
        (terminal["target_id"],),
    )
    assert read_report(result)["evidence_head"] == read_report(baseline)["evidence_head"]
    completed_head = head(watch)
    completed_rows = rows(
        watch, "SELECT * FROM alert_coverage_completions WHERE watch_id=?", (WATCH_ID,)
    )
    for _ in range(2):
        code, repeated = run_cli("watch", "alerts", "--id", WATCH_ID)
        assert code == 0 and repeated["report_id"] == result["report_id"]
        assert repeated["new_alert_ids"] == []
        assert head(watch) == completed_head
        assert (
            rows(watch, "SELECT * FROM alert_coverage_completions WHERE watch_id=?", (WATCH_ID,))
            == completed_rows
        )
    assert artifact_bytes(baseline) == frozen


def test_late_earlier_origin_reclassifies_old_heuristic_copy_without_old_io(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import threadsense.cli as cli

    baseline = sweep(monkeypatch, "initial")
    frozen = artifact_bytes(baseline)
    source = next(
        item for item in read_collection(baseline, "evidence") if item["comment_id"] == "reddit:ka1"
    )
    phase = rewrite_comment_phase(tmp_path, body=source["excerpt"], comment_id="aa0")
    original = present_watch_alerts
    earlier = source["first_observed_at_utc"] - 1
    existing_acquisitions = {
        item["acquisition_id"]
        for item in rows(watch, "SELECT acquisition_id FROM watch_acquisitions")
    }
    snapshots = established_snapshots(watch)

    def correct_observation(*args: Any, **kwargs: Any) -> dict[str, Any]:
        new = [
            item
            for item in rows(watch, "SELECT acquisition_id FROM watch_acquisitions")
            if item["acquisition_id"] not in existing_acquisitions
        ]
        for acquisition in new:
            mutate(
                watch,
                "UPDATE watch_acquisitions SET observed_at_utc=? WHERE acquisition_id=?",
                (earlier, acquisition["acquisition_id"]),
            )
        mutate(
            watch,
            "UPDATE watch_comments SET first_observed_at_utc=? WHERE watch_id=? AND comment_id=?",
            (earlier, WATCH_ID, "reddit:aa0"),
        )
        with forbid_old_source_io(monkeypatch, snapshots):
            return original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(cli, "present_watch_alerts", correct_observation)
        changed = sweep(monkeypatch, phase)
    evidence = read_collection(changed, "evidence")
    origin = next(item for item in evidence if item["comment_id"] == "reddit:aa0")
    former = next(item for item in evidence if item["evidence_key"] == source["evidence_key"])
    assert not origin["is_exact_copy"] and origin["copy_of_report_id"] is None
    assert former["is_exact_copy"] and former["copy_of_report_id"] == origin["source_report_id"]
    assert identities(baseline) < identities(changed)
    assert_completed(watch, changed)
    protect_presenter(monkeypatch, watch)
    repeated = sweep(monkeypatch, phase)
    assert repeated["evidence_count"] == changed["evidence_count"]
    assert read_collection(repeated, "changes") == []
    assert artifact_bytes(baseline) == frozen


def test_fresh_process_delta_resume_waits_for_complete_presence_before_copy_events(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    baseline = sweep(monkeypatch, "initial")
    frozen = artifact_bytes(baseline)
    old_head = head(watch)
    source = next(
        item for item in read_collection(baseline, "evidence") if item["comment_id"] == "reddit:ka1"
    )
    body = "I can reproduce."
    phase = rewrite_comment_phase(tmp_path, body=body, comment_id="ka4", later_origin=True)
    origin_id = report_id_for(WATCH_ID, source["source_thread_id"], "reddit:ka6")
    copy_id = report_id_for(WATCH_ID, source["source_thread_id"], "reddit:ka4")
    with closing(sqlite3.connect(watch)) as connection:
        entry_id, run_id = connection.execute(
            "SELECT entry_id,created_run_id FROM watch_digest_entries "
            "WHERE watch_id=? AND source_thread_id=? AND kind='thread_update' "
            "ORDER BY entry_id LIMIT 1",
            (WATCH_ID, source["source_thread_id"]),
        ).fetchone()
        sequence = connection.execute(
            "SELECT COALESCE(MAX(record_sequence),0) FROM watch_issue_reports WHERE watch_id=?",
            (WATCH_ID,),
        ).fetchone()[0]
        for offset, (comment_id, report_id, origin) in enumerate(
            (("reddit:ka6", origin_id, None), ("reddit:ka4", copy_id, origin_id)),
            start=1,
        ):
            connection.execute(
                "INSERT INTO watch_issue_reports VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    report_id,
                    WATCH_ID,
                    source["source_thread_id"],
                    comment_id,
                    sequence + offset,
                    comment_text_sha256(body),
                    None,
                    source["community"],
                    source["permalink"].replace("/ka1/", f"/{comment_id.split(':')[1]}/"),
                    source["source_created_utc"],
                    "firsthand" if origin is None else "exact_copy",
                    origin,
                    entry_id,
                    source["first_observed_at_utc"],
                    run_id,
                ),
            )
        connection.commit()
    config = config_file(tmp_path, source_rows=32, derivations=16)
    install_transport(monkeypatch, phase)
    code, payload = run_cli("watch", "run", "--id", WATCH_ID, "--config", str(config))
    assert code == 0, payload
    result = payload["alerts"]
    assert result["status"] == "backlog", result
    target_id = None
    cursor = -1
    delivered: set[str] = set()
    saw_unready = False
    for _ in range(4096):
        assert_accounting(result)
        assert delivered.isdisjoint(result["new_alert_ids"])
        delivered.update(result["new_alert_ids"])
        current_head = head(watch)
        if current_head["pending_target"] is not None:
            if target_id is None:
                target_id = current_head["pending_target"]
            assert current_head["pending_target"] == target_id
            assert current_head["completed_target"] == old_head["completed_target"]
            assert current_head["completed_highwater"] == old_head["completed_highwater"]
            assert not rows(
                watch,
                "SELECT target_id FROM alert_coverage_completions WHERE target_id=?",
                (target_id,),
            )
            progress = rows(
                watch,
                "SELECT source_cursor FROM alert_derivation_targets WHERE target_id=?",
                (target_id,),
            )
            if progress:
                assert progress[0]["source_cursor"] >= cursor
                cursor = progress[0]["source_cursor"]
            presence = rows(
                watch,
                "SELECT target_id FROM alert_presence_bindings WHERE target_id=?",
                (target_id,),
            )
            if not presence:
                saw_unready = True
                assert not rows(
                    watch,
                    "SELECT evidence_key FROM alert_derivation_decisions WHERE target_id=?",
                    (target_id,),
                )
                assert not rows(
                    watch,
                    "SELECT alert_id FROM alert_v2_event_identities WHERE target_id=?",
                    (target_id,),
                )
                assert not delivered
        if result["status"] != "backlog":
            break
        code, result = fresh_alerts(config)
        assert code == 0, result
    else:
        pytest.fail("finite incremental target did not finish across fresh processes")
    assert saw_unready and target_id is not None
    terminal = assert_completed(watch, result)
    assert terminal["target_id"] == target_id
    evidence = read_collection(result, "evidence")
    copied = [item for item in evidence if item["comment_id"] == "reddit:ka4"]
    assert len(copied) == 1 and copied[0]["is_exact_copy"]
    assert copied[0]["copy_of_report_id"] == origin_id
    events = read_collection(result, "changes")
    assert not any(
        event["kind"] in ("new_evidence", "reproduction")
        and any(item["comment_id"] == "reddit:ka4" for item in event["evidence"])
        for event in events
    )
    assert any(
        event["kind"] == "reproduction"
        and any(item["comment_id"] == "reddit:ka6" for item in event["evidence"])
        for event in events
    )
    assert delivered == {event["alert_id"] for event in events}
    assert result["evidence_count"] == len(identities(result)) > baseline["evidence_count"]
    assert read_report(result)["event_count"] == len(events)
    code, replay = fresh_alerts(config)
    assert (
        code == 0 and replay["report_id"] == result["report_id"] and replay["new_alert_ids"] == []
    )
    assert artifact_bytes(baseline) == frozen


def test_ineligible_first_occurrence_keeps_later_valid_citation(
    watch: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    baseline = sweep(monkeypatch, "initial")
    expanded = sweep(monkeypatch, "edit")
    historical = artifact_bytes(expanded)
    duplicates = rows(
        watch,
        "SELECT evidence_key FROM alert_watch_occurrences WHERE watch_id=? "
        "GROUP BY evidence_key HAVING COUNT(DISTINCT entry_id)>1 ORDER BY evidence_key",
        (WATCH_ID,),
    )
    assert duplicates, "the real revised snapshot must retain an earlier report/text"
    key = duplicates[0]["evidence_key"]
    occurrences = rows(
        watch,
        "SELECT entry_id FROM alert_watch_occurrences WHERE watch_id=? AND evidence_key=? "
        "ORDER BY observed,revision_sequence,entry_id,slot",
        (WATCH_ID, key),
    )
    earliest = occurrences[0]["entry_id"]
    later = next(item["entry_id"] for item in occurrences if item["entry_id"] != earliest)
    before = next(
        item for item in read_collection(expanded, "evidence") if item["evidence_key"] == key
    )
    assert before["entry_id"] == earliest
    entry = rows(
        watch, "SELECT payload_json FROM watch_digest_entries WHERE entry_id=?", (earliest,)
    )[0]
    payload = json.loads(entry["payload_json"])
    comment = next(
        item
        for item in read_collection(expanded, "evidence")
        if item["source_thread_id"] == before["source_thread_id"] and item["comment_id"] is not None
    )
    payload["novel_evidence"] = [
        {"comment_id": comment["comment_id"], "text_sha256": comment["text_sha256"]}
    ]
    import threadsense.cli as cli

    snapshots = established_snapshots(watch)

    def correct_kind(*args: Any, **kwargs: Any) -> dict[str, Any]:
        mutate(
            watch,
            "UPDATE watch_digest_entries SET kind='finding',theme_key='eligibility-correction',"
            "payload_json=? WHERE watch_id=? AND entry_id=?",
            (json.dumps(payload), WATCH_ID, earliest),
        )
        with forbid_old_source_io(monkeypatch, snapshots):
            return present_watch_alerts(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(cli, "present_watch_alerts", correct_kind)
        corrected = sweep(monkeypatch, "unchanged")
    citation = next(
        item for item in read_collection(corrected, "evidence") if item["evidence_key"] == key
    )
    assert citation["entry_id"] == later
    assert citation["source_report_id"] == before["source_report_id"]
    assert citation["is_exact_copy"] == before["is_exact_copy"]
    assert identities(corrected) == identities(expanded)
    assert corrected["evidence_count"] == expanded["evidence_count"]
    assert_completed(watch, corrected)
    assert artifact_bytes(expanded) == historical
    assert identities(baseline) <= identities(corrected)
