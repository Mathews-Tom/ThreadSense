from __future__ import annotations

import copy
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError
from test_watch_alerts import (
    artifact_bytes,
    changed_phase,
    drain_alerts,
    read_collection,
    read_report,
    sweep,
)
from test_watch_incremental import (
    PROFILE_PATH,
    WATCH_ID,
    create_watch,
    install_transport,
    isolate_environment,
    phases_by_name,
    run_cli,
)

from threadsense.config import AppConfig
from threadsense.errors import MonitoringStateError
from threadsense.monitoring.alert_budget import AdmissionStop, AlertBudget
from threadsense.monitoring.alert_reader import read_alert_page
from threadsense.monitoring.issue_records import report_id_for
from threadsense.monitoring.observations import comment_text_sha256


@pytest.fixture
def watch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    isolate_environment(monkeypatch, tmp_path)
    return create_watch()


def config_file(tmp_path: Path, **limits: int) -> Path:
    path = tmp_path / (
        "alerts-" + "-".join(f"{key}-{value}" for key, value in limits.items()) + ".toml"
    )
    path.write_text("[alerts]\n" + "".join(f"{key} = {value}\n" for key, value in limits.items()))
    return path


def assert_accounting(payload: dict[str, Any]) -> None:
    accounting = payload["accounting"]
    assert set(accounting["used"]) == {
        "source_rows",
        "derivations",
        "input_bytes",
        "artifact_bytes",
        "response_bytes",
    }
    for dimension, used in accounting["used"].items():
        assert type(used) is int and 0 <= used <= accounting["limits"][dimension]


def fresh_alerts(config: Path) -> tuple[int, dict[str, Any]]:
    """A new interpreter has no shared AlertBudget, cache or continuation object."""
    process = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; from threadsense.cli import main; sys.exit(main(sys.argv[1:]))",
            "--output-format",
            "json",
            "watch",
            "alerts",
            "--id",
            WATCH_ID,
            "--config",
            str(config),
        ],
        env=dict(os.environ),
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    assert process.stdout.strip(), process.stderr
    return process.returncode, json.loads(process.stdout)


@pytest.mark.parametrize(
    "dimension", ["source_rows", "derivations", "input_bytes", "artifact_bytes", "response_bytes"]
)
@pytest.mark.parametrize("invalid", [0, -1, True, 1.5, "32"])
def test_alert_configuration_requires_positive_strict_integers(
    dimension: str, invalid: Any
) -> None:
    with pytest.raises(ValidationError):
        AppConfig.model_validate({"alerts": {dimension: invalid}})


def test_full_envelope_is_distinct_from_remaining_allowance() -> None:
    budget = AlertBudget(source_rows=3, input_bytes=10)
    budget.admit("first", source_rows=2, input_bytes=6)
    before = budget.accounting()
    with pytest.raises(AdmissionStop) as remaining:
        budget.admit("next", source_rows=2, input_bytes=5)
    assert remaining.value.details == {
        "status": "backlog",
        "unit": "next",
        "reason": "source_rows",
        "required": 2,
        "configured": 3,
    }
    assert budget.accounting() == before  # rejected compound admission consumes nothing
    with pytest.raises(AdmissionStop) as oversized:
        budget.admit("oversized", input_bytes=11)
    assert oversized.value.details["status"] == "resource_refused"
    assert oversized.value.details["required"] == 11
    assert oversized.value.details["configured"] == 10
    assert budget.accounting() == before
    fresh = AlertBudget(source_rows=3, input_bytes=10)
    fresh.admit("next", source_rows=2, input_bytes=5)
    assert fresh.accounting()["used"]["input_bytes"] == 5


def test_low_budget_cli_continues_across_many_fresh_processes_without_redelivery(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = config_file(tmp_path, source_rows=32, derivations=16)
    install_transport(monkeypatch, phases_by_name()["initial"])
    code, first = run_cli("watch", "run", "--id", WATCH_ID, "--config", str(config))
    assert code == 0, first
    current = first["alerts"]
    assert current["status"] == "backlog", current
    invocations = [current]
    delivered: set[str] = set()
    for _ in range(4096):
        assert_accounting(current)
        assert delivered.isdisjoint(current["new_alert_ids"])
        delivered.update(current["new_alert_ids"])
        if current["status"] != "backlog":
            break
        assert current["status"] != "no_changes"
        code, current = fresh_alerts(config)
        assert code == 0, current
        invocations.append(current)
    else:
        pytest.fail("bounded fresh-process continuation failed to exhaust the saved target")
    assert len(invocations) >= 8, invocations
    assert current["status"] == "baseline" and not delivered
    assert all(item["accounting"]["limits"]["source_rows"] == 32 for item in invocations)
    report = read_report(current)
    assert report["schema_version"] == 2
    assert report["evidence_count"] == len(report["evidence"]) > 0
    frozen = artifact_bytes(current)
    code, reused = fresh_alerts(config)
    assert code == 0 and reused["report_id"] == current["report_id"]
    assert not reused["new_alert_ids"] and artifact_bytes(reused) == frozen

    install_transport(monkeypatch, phases_by_name()["search_omits_known"])
    code, changed = run_cli("watch", "run", "--id", WATCH_ID, "--config", str(config))
    assert code == 0, changed
    result = changed["alerts"]
    delivered.clear()
    for _ in range(4096):
        assert_accounting(result)
        assert delivered.isdisjoint(result["new_alert_ids"])
        delivered.update(result["new_alert_ids"])
        if result["status"] != "backlog":
            break
        code, result = fresh_alerts(config)
        assert code == 0, result
    else:
        pytest.fail("changed target did not terminate")
    changes = read_collection(result, "changes")
    ids = [event["alert_id"] for event in changes]
    assert ids and len(ids) == len(set(ids))
    assert delivered == set(ids)
    assert any(
        item["comment_id"] == "reddit:ka4" for event in changes for item in event["evidence"]
    )
    assert (
        result["cohort_id"] == current["cohort_id"]
    )  # operational envelope is not cohort identity
    assert artifact_bytes(current) == frozen
    code, reused = fresh_alerts(config)
    assert code == 0 and reused["new_alert_ids"] == []


def legacy_file(tmp_path: Path, report: dict[str, Any]) -> Path:
    payload = {**report, "schema_version": 1}
    for key in ("evidence_head", "change_head", "provenance_head"):
        payload.pop(key, None)
    path = tmp_path / "legacy-alert-report.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def test_schema1_reader_preserves_old_bytes_and_preflights_oversized_payload(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = sweep(monkeypatch, "initial")
    path = legacy_file(watch.parent, read_report(baseline))
    frozen = path.read_bytes()
    original = Path.read_bytes
    original_open = Path.open
    touched: list[Path] = []

    def guarded_read(target: Path) -> bytes:
        if target == path:
            touched.append(target)
            pytest.fail("oversized legacy report was read before admission")
        return original(target)

    def guarded_open(target: Path, *args: Any, **kwargs: Any) -> Any:
        if target == path:
            touched.append(target)
            pytest.fail("oversized legacy report was opened before admission")
        return original_open(target, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "read_bytes", guarded_read)
        patch.setattr(Path, "open", guarded_open)
        refused = read_alert_page(
            root=watch.parent, report_path=path, budget=AlertBudget(input_bytes=1)
        )
    assert refused["status"] == "resource_refused"
    assert refused["complete"] is False and refused["items"] == []
    assert refused["refusal"]["required"] > refused["refusal"]["configured"]
    assert not touched
    page = read_alert_page(root=watch.parent, report_path=path)
    assert page["complete"] and page["items"] == read_report(baseline)["evidence"]
    assert path.read_bytes() == frozen


@pytest.mark.parametrize("schema", [0, 3, 999])
def test_reader_refuses_unsupported_schemas_without_empty_success(
    tmp_path: Path, schema: int
) -> None:
    path = tmp_path / "future.json"
    path.write_text(
        json.dumps(
            {
                "artifact_type": "watch_alert_report",
                "schema_version": schema,
                "watch_id": WATCH_ID,
                "report_id": "future",
                "evidence": [],
                "alerts": [],
            }
        )
    )
    with pytest.raises(MonitoringStateError) as refused:
        read_alert_page(root=tmp_path, report_path=path)
    assert refused.value.details["reason"] == "artifact_schema_unsupported"


@pytest.mark.parametrize("damage", ["missing", "corrupt"])
def test_reader_refuses_accessed_raw_leaf_instead_of_losing_citations(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
    damage: str,
) -> None:
    result = sweep(monkeypatch, "initial")
    evidence = read_collection(result, "evidence")
    leaf = watch.parent / evidence[0]["snapshot_path"]
    frozen = artifact_bytes(result)
    if damage == "missing":
        leaf.unlink()
    else:
        leaf.write_bytes(leaf.read_bytes() + b" ")
    with pytest.raises(MonitoringStateError):
        read_collection(result, "evidence")
    assert artifact_bytes(result) == frozen


def test_reader_refuses_corrupt_immediate_manifest_and_preserves_counts(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = sweep(monkeypatch, "initial")
    path = Path(result["json_path"])
    document = json.loads(path.read_bytes())
    reference = document["evidence_head"]
    head = watch.parent / reference["path"]
    before = path.read_bytes()
    head.write_bytes(head.read_bytes() + b" ")
    with pytest.raises(MonitoringStateError):
        read_alert_page(root=watch.parent, report_path=path)
    assert path.read_bytes() == before
    assert json.loads(before)["evidence_count"] > 0


def test_profile_a_b_a_resets_each_chronological_cohort_without_rewriting_history(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    first = sweep(monkeypatch, "initial")
    frozen = artifact_bytes(first)
    revised = tmp_path / "cohort-b.toml"
    revised.write_text(
        PROFILE_PATH.read_text().replace("discovered_candidates = 4", "discovered_candidates = 5")
    )
    create_watch(revised)
    second = sweep(monkeypatch, "search_omits_known")
    assert second["status"] == "baseline" and not second["new_alert_ids"]
    create_watch(PROFILE_PATH)
    third = sweep(monkeypatch, "edit")
    assert third["status"] == "baseline" and not third["new_alert_ids"]
    assert first["cohort_id"] != second["cohort_id"]
    assert third["cohort_id"] == first["cohort_id"]
    assert third["window_id"] != first["window_id"]
    assert artifact_bytes(first) == frozen


def test_oversized_legacy_database_payload_refuses_before_decode(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from threadsense.monitoring.alert_records import stored_report

    baseline = sweep(monkeypatch, "initial")
    report = read_report(baseline)
    text = json.dumps(
        {
            **report,
            "report_id": "legacy-admission-boundary",
            "schema_version": 1,
            "padding": "é" * 10000,
        },
        ensure_ascii=False,
    )
    report_id = "legacy-admission-boundary"
    with closing(sqlite3.connect(watch)) as connection:
        source = connection.execute(
            "SELECT run_id,finished_at_utc FROM watch_sweeps WHERE watch_id=? LIMIT 1", (WATCH_ID,)
        ).fetchone()
        assert source is not None
        connection.execute(
            "INSERT OR IGNORE INTO watch_alert_cohorts VALUES (?,?,?,?)",
            (report["cohort_id"], WATCH_ID, "{}", source[1]),
        )
        # This legacy row is deliberately indivisible and pending: no completion is inserted.
        connection.execute(
            "INSERT INTO watch_alert_reports VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                report_id,
                WATCH_ID,
                source[0],
                report["cohort_id"],
                "legacy-window",
                "legacy-window",
                "baseline",
                text,
                hashlib.sha256(text.encode()).hexdigest(),
                "legacy.json",
                "legacy.md",
                source[0],
                source[1],
            ),
        )
        connection.commit()
        with monkeypatch.context() as patch:

            def refuse_decode(*args: Any, **kwargs: Any) -> None:
                pytest.fail("oversized legacy payload reached JSON decode")

            patch.setattr("threadsense.monitoring.alert_records.json.loads", refuse_decode)
            with pytest.raises(AdmissionStop) as stopped:
                stored_report(connection, report_id, AlertBudget(input_bytes=32))
        assert stopped.value.details["status"] == "resource_refused"
        assert (
            connection.execute(
                "SELECT payload_json FROM watch_alert_reports WHERE report_id=?", (report_id,)
            ).fetchone()[0]
            == text
        )


def test_short_explicit_copy_uses_later_canonical_origin_before_classification(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    baseline = sweep(monkeypatch, "initial")
    old_bytes = artifact_bytes(baseline)
    body = "I can reproduce."
    phase = changed_phase(tmp_path, body=body)
    routes = []
    for index, route in enumerate(phase.routes):
        if route.response_path is None or "alert-source" not in route.response_path.name:
            routes.append(route)
            continue
        payload = json.loads(route.response_path.read_bytes())

        def add_later_origin(node: Any) -> None:
            if isinstance(node, list):
                for child in list(node):
                    if (
                        isinstance(child, dict)
                        and child.get("kind") == "t1"
                        and child.get("data", {}).get("id") == "ka4"
                    ):
                        origin = copy.deepcopy(child)
                        origin["data"]["id"] = "ka6"
                        origin["data"]["name"] = "t1_ka6"
                        origin["data"]["permalink"] = origin["data"]["permalink"].replace(
                            "/ka4/", "/ka6/"
                        )
                        node.append(origin)  # same timestamp; later canonical comment identity
                    add_later_origin(child)
            elif isinstance(node, dict):
                for child in node.values():
                    add_later_origin(child)

        add_later_origin(payload)
        path = tmp_path / f"short-copy-origin-{index}.json"
        path.write_text(json.dumps(payload))
        routes.append(replace(route, response_path=path))
    phase = replace(phase, routes=tuple(routes))
    origin_evidence = next(
        item for item in read_report(baseline)["evidence"] if item["comment_id"] == "reddit:ka1"
    )
    thread_id = origin_evidence["source_thread_id"]
    origin_id = report_id_for(WATCH_ID, thread_id, "reddit:ka6")
    copy_id = report_id_for(WATCH_ID, thread_id, "reddit:ka4")
    with closing(sqlite3.connect(watch)) as connection:
        entry_id, run_id = connection.execute(
            "SELECT entry_id,created_run_id FROM watch_digest_entries WHERE watch_id=? "
            "AND source_thread_id=? AND kind='thread_update' ORDER BY entry_id LIMIT 1",
            (WATCH_ID, thread_id),
        ).fetchone()
        sequence = connection.execute(
            "SELECT COALESCE(MAX(record_sequence),0) FROM watch_issue_reports WHERE watch_id=?",
            (WATCH_ID,),
        ).fetchone()[0]
        for offset, (comment_id, report_id, origin) in enumerate(
            (("reddit:ka6", origin_id, None), ("reddit:ka4", copy_id, origin_id)), start=1
        ):
            connection.execute(
                "INSERT INTO watch_issue_reports VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    report_id,
                    WATCH_ID,
                    thread_id,
                    comment_id,
                    sequence + offset,
                    comment_text_sha256(body),
                    None,
                    origin_evidence["community"],
                    origin_evidence["permalink"].replace("/ka1/", f"/{comment_id.split(':')[1]}/"),
                    origin_evidence["source_created_utc"],
                    "firsthand" if origin is None else "exact_copy",
                    origin,
                    entry_id,
                    origin_evidence["first_observed_at_utc"],
                    run_id,
                ),
            )
        connection.commit()
    changed = sweep(monkeypatch, phase)
    evidence = read_collection(changed, "evidence")
    copied = [item for item in evidence if item["comment_id"] == "reddit:ka4"]
    assert len(copied) == 1 and copied[0]["is_exact_copy"]
    assert copied[0]["copy_of_report_id"] == origin_id
    assert any(item["comment_id"] == "reddit:ka6" for item in evidence)
    events = read_collection(changed, "changes")
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
    assert artifact_bytes(baseline) == old_bytes


@pytest.mark.parametrize("older", [False, True])
def test_late_lower_key_observation_with_tied_or_older_source_time_is_not_skipped(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    older: bool,
) -> None:
    baseline = sweep(monkeypatch, "initial")
    prior = sweep(monkeypatch, "search_omits_known")
    frozen = artifact_bytes(prior)
    original = next(
        item for item in read_report(prior)["evidence"] if item["comment_id"] == "reddit:ka4"
    )
    source_time = original["source_created_utc"] - (86400 if older else 0)
    phase = changed_phase(
        tmp_path, body="I can reproduce a distinct offline database lock and sync conflict."
    )
    routes = []
    for index, route in enumerate(phase.routes):
        if route.response_path is None or "alert-source" not in route.response_path.name:
            routes.append(route)
            continue
        payload = json.loads(route.response_path.read_bytes())

        def replace_with_late_lower_key(node: Any) -> None:
            if isinstance(node, list):
                for child in node:
                    replace_with_late_lower_key(child)
            elif isinstance(node, dict):
                data = node.get("data")
                if node.get("kind") == "t1" and isinstance(data, dict) and data.get("id") == "ka4":
                    data["id"] = "aa0"
                    data["name"] = "t1_aa0"
                    data["permalink"] = data["permalink"].replace("/ka4/", "/aa0/")
                    data["created_utc"] = source_time
                for child in node.values():
                    replace_with_late_lower_key(child)

        replace_with_late_lower_key(payload)
        path = tmp_path / f"late-lower-key-{index}.json"
        path.write_text(json.dumps(payload))
        routes.append(replace(route, response_path=path))
    phase = replace(phase, routes=tuple(routes))
    changed = sweep(monkeypatch, phase)
    evidence = read_collection(changed, "evidence")
    late = [item for item in evidence if item["comment_id"] == "reddit:aa0"]
    assert len(late) == 1
    assert late[0]["source_created_utc"] == source_time
    assert late[0]["first_observed_at_utc"] > original["first_observed_at_utc"]
    assert changed["cohort_id"] == baseline["cohort_id"]
    assert any(
        item["comment_id"] == "reddit:aa0"
        for event in read_collection(changed, "changes")
        for item in event["evidence"]
    )
    repeated = sweep(monkeypatch, phase)
    assert not repeated["new_alert_ids"]
    assert not read_collection(repeated, "changes")
    assert artifact_bytes(prior) == frozen


@pytest.mark.parametrize("schema", [1, 3])
def test_pending_legacy_unit_is_not_bypassed_and_recovers_or_refuses_future_schema(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    schema: int,
) -> None:
    from threadsense.monitoring.alert_records import canonical_json
    from threadsense.monitoring.alerts import render_alert_json, render_alert_markdown

    completed = sweep(monkeypatch, "initial")
    historical = artifact_bytes(completed)
    payload = {
        **read_report(completed),
        "schema_version": schema,
        "report_id": "pending-legacy",
        "window_id": "pending-legacy-window",
        "baseline_window_id": "pending-legacy-window",
    }
    text = canonical_json(payload)
    relative = f"{WATCH_ID}/alerts/pending-legacy"
    with closing(sqlite3.connect(watch)) as connection:
        sweep_id, when = connection.execute(
            "SELECT run_id,finished_at_utc FROM watch_sweeps WHERE watch_id=? LIMIT 1", (WATCH_ID,)
        ).fetchone()
        connection.execute(
            "INSERT OR IGNORE INTO watch_alert_cohorts VALUES (?,?,?,?)",
            (payload["cohort_id"], WATCH_ID, "{}", when),
        )
        connection.execute(
            "INSERT INTO watch_alert_reports VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                payload["report_id"],
                WATCH_ID,
                sweep_id,
                payload["cohort_id"],
                payload["window_id"],
                payload["baseline_window_id"],
                payload["status"],
                text,
                hashlib.sha256(text.encode()).hexdigest(),
                relative + ".json",
                relative + ".md",
                sweep_id,
                when,
            ),
        )
        connection.commit()
    tiny = config_file(tmp_path, input_bytes=32)
    code, refused = run_cli("watch", "alerts", "--id", WATCH_ID, "--config", str(tiny))
    assert code == 0 and refused["status"] == "resource_refused", refused
    assert refused["resource"]["required"] > refused["resource"]["configured"]
    assert not (watch.parent / (relative + ".json")).exists()
    code, recovered = run_cli("watch", "alerts", "--id", WATCH_ID)
    if schema == 3:
        assert code != 0 and recovered["error"]["code"] == "monitoring_state_error"
        assert not (watch.parent / (relative + ".json")).exists()
    else:
        assert code == 0 and recovered["report_id"] == payload["report_id"], recovered
        assert Path(recovered["json_path"]).read_bytes() == render_alert_json(payload)
        assert Path(recovered["markdown_path"]).read_bytes() == render_alert_markdown(payload)
        code, restarted = run_cli("watch", "alerts", "--id", WATCH_ID)
        assert code == 0 and restarted["report_id"] == payload["report_id"]
        assert restarted["new_alert_ids"] == []
    with closing(sqlite3.connect(watch)) as connection:
        assert (
            connection.execute(
                "SELECT payload_json FROM watch_alert_reports WHERE report_id=?",
                (payload["report_id"],),
            ).fetchone()[0]
            == text
        )
    assert artifact_bytes(completed) == historical


def test_reader_cursor_binds_immutable_report_path_hash_and_collection(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = sweep(monkeypatch, "initial")
    path = Path(result["json_path"])
    page = read_alert_page(
        root=watch.parent,
        report_path=path,
        budget=AlertBudget(derivations=1),
    )
    assert not page["complete"] and page["status"] == "backlog"
    cursor = page["cursor"]
    assert cursor is not None
    assert_accounting(page)
    with pytest.raises(MonitoringStateError):
        read_alert_page(root=watch.parent, report_path=path, collection="changes", cursor=cursor)
    relocated = path.with_name("relocated-report.json")
    original = path.read_bytes()
    relocated.write_bytes(original)
    with pytest.raises(MonitoringStateError):
        read_alert_page(root=watch.parent, report_path=relocated, cursor=cursor)
    resumed = read_alert_page(root=watch.parent, report_path=path, cursor=cursor)
    assert resumed["complete"] and resumed["items"]
    assert_accounting(resumed)
    path.write_bytes(original + b" ")
    with pytest.raises(MonitoringStateError):
        read_alert_page(root=watch.parent, report_path=path, cursor=cursor)


def test_oversized_source_snapshot_is_refused_before_open_and_recovers_same_target(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    baseline = sweep(monkeypatch, "initial")
    original = artifact_bytes(baseline)
    phase = changed_phase(
        tmp_path,
        body="I can reproduce a unique offline sync conflict. " + "detail " * 20000,
    )
    config = config_file(tmp_path, source_rows=32, derivations=16, input_bytes=131072)
    install_transport(monkeypatch, phase)
    code, payload = run_cli("watch", "run", "--id", WATCH_ID, "--config", str(config))
    assert code == 0, payload
    result = payload["alerts"]
    with closing(sqlite3.connect(watch)) as connection:
        snapshots = [
            watch.parent / row[0]
            for row in connection.execute(
                "SELECT snapshot_path FROM watch_thread_revisions WHERE watch_id=?", (WATCH_ID,)
            )
        ]
    oversized = {path.resolve() for path in snapshots if path.stat().st_size > 131072}
    assert oversized
    original_open = Path.open

    def guard(target: Path, *args: Any, **kwargs: Any) -> Any:
        if target.resolve() in oversized:
            pytest.fail("oversized source snapshot was opened before byte admission")
        return original_open(target, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "open", guard)
        code, result = run_cli("watch", "alerts", "--id", WATCH_ID, "--config", str(config))
        assert code == 0, result
        for _ in range(4096):
            assert_accounting(result)
            if result["status"] != "backlog":
                break
            code, result = run_cli("watch", "alerts", "--id", WATCH_ID, "--config", str(config))
            assert code == 0, result
        else:
            pytest.fail("oversized unit spun through unchanged backlog")
    assert result["status"] == "resource_refused", result
    assert result["resource"]["reason"] == "input_bytes"
    assert result["resource"]["required"] > result["resource"]["configured"]
    pending = result["continuation"]["target_id"]
    code, recovered = run_cli("watch", "alerts", "--id", WATCH_ID)
    assert code == 0, recovered
    recovered = drain_alerts(recovered)
    assert recovered["target_id"] == pending
    assert any(
        item["comment_id"] == "reddit:ka4"
        for event in read_collection(recovered, "changes")
        for item in event["evidence"]
    )
    assert artifact_bytes(baseline) == original


def test_artifact_envelope_refuses_before_publication_and_resumes_same_pending_target(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    baseline = sweep(monkeypatch, "initial")
    before = artifact_bytes(baseline)
    config = config_file(tmp_path, artifact_bytes=1)
    install_transport(monkeypatch, phases_by_name()["search_omits_known"])
    code, payload = run_cli("watch", "run", "--id", WATCH_ID, "--config", str(config))
    assert code == 0, payload
    result = payload["alerts"]
    for _ in range(4096):
        assert_accounting(result)
        assert result["new_alert_ids"] == []
        if result["status"] != "backlog":
            break
        code, result = run_cli("watch", "alerts", "--id", WATCH_ID, "--config", str(config))
        assert code == 0, result
    else:
        pytest.fail("artifact full-envelope refusal spun as backlog")
    assert result["status"] == "resource_refused"
    assert result["resource"]["reason"] == "artifact_bytes"
    assert result["resource"]["required"] > 1 == result["resource"]["configured"]
    target_id = result["continuation"]["target_id"]
    code, recovered = run_cli("watch", "alerts", "--id", WATCH_ID)
    assert code == 0, recovered
    recovered = drain_alerts(recovered)
    assert recovered["target_id"] == target_id
    assert recovered["new_alert_ids"]
    assert artifact_bytes(baseline) == before


def test_response_envelope_too_small_for_control_is_typed_refusal_not_oversized_success(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    baseline = sweep(monkeypatch, "initial")
    before = artifact_bytes(baseline)
    config = config_file(tmp_path, response_bytes=1)
    code, payload = run_cli("watch", "alerts", "--id", WATCH_ID, "--config", str(config))
    assert code != 0 and payload["error"]["code"] == "monitoring_state_error", payload
    details = payload["error"]["details"]
    assert "response_bytes" in json.dumps(details)
    assert "resource_refused" in json.dumps(details)
    assert artifact_bytes(baseline) == before
    code, recovered = run_cli("watch", "alerts", "--id", WATCH_ID)
    assert code == 0 and recovered["report_id"] == baseline["report_id"]
    assert recovered["new_alert_ids"] == []


def test_membership_correction_resolves_current_overlay_without_rewriting_old_report(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = sweep(monkeypatch, "initial")
    frozen = artifact_bytes(baseline)
    before = read_collection(baseline, "evidence")
    source = next(item for item in before if item["comment_id"] == "reddit:ka1")
    with closing(sqlite3.connect(watch)) as connection:
        destination = connection.execute(
            "SELECT issue_id FROM watch_issues WHERE watch_id=? AND issue_id!=? "
            "ORDER BY issue_id LIMIT 1",
            (WATCH_ID, source["issue_id"]),
        ).fetchone()
    assert destination is not None
    code, linked = run_cli(
        "issues",
        "link",
        "--watch",
        WATCH_ID,
        "--issue",
        destination[0],
        "--thread",
        source["source_thread_id"],
        "--reason",
        "reviewed membership correction",
    )
    assert code == 0, linked
    changed = sweep(monkeypatch, "unchanged")
    current = read_collection(changed, "evidence")
    corrected = next(item for item in current if item["evidence_key"] == source["evidence_key"])
    assert corrected["issue_id"] == destination[0]
    assert corrected["source_report_id"] == source["source_report_id"]
    assert changed["evidence_count"] == baseline["evidence_count"]
    assert read_collection(changed, "changes") == []
    assert read_collection(baseline, "evidence") == before
    assert artifact_bytes(baseline) == frozen


def test_source_envelope_too_small_for_indivisible_control_refuses_without_spin(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    baseline = sweep(monkeypatch, "initial")
    frozen = artifact_bytes(baseline)
    config = config_file(tmp_path, source_rows=1)
    install_transport(monkeypatch, phases_by_name()["search_omits_known"])
    code, payload = run_cli("watch", "run", "--id", WATCH_ID, "--config", str(config))
    assert code == 0, payload
    result = payload["alerts"]
    for _ in range(4096):
        assert_accounting(result)
        if result["status"] != "backlog":
            break
        code, result = run_cli("watch", "alerts", "--id", WATCH_ID, "--config", str(config))
        assert code == 0, result
    else:
        pytest.fail("indivisible source/control unit spun through unchanged backlog")
    assert result["status"] == "resource_refused", result
    assert result["resource"]["reason"] == "source_rows"
    assert result["resource"]["required"] > 1 == result["resource"]["configured"]
    assert artifact_bytes(baseline) == frozen
    code, recovered = run_cli("watch", "alerts", "--id", WATCH_ID)
    assert code == 0, recovered
    assert read_collection(drain_alerts(recovered), "changes")
