from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from contextlib import closing
from dataclasses import replace
from functools import partial
from pathlib import Path
from typing import Any

import pytest
from recorded_reddit import ReplayPhase
from test_watch_incremental import (
    PROFILE_PATH,
    WATCH_ID,
    create_watch,
    install_transport,
    isolate_environment,
    phases_by_name,
    run_cli,
)

from threadsense.monitoring import MonitoringStore


@pytest.fixture
def watch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    isolate_environment(monkeypatch, tmp_path)
    return create_watch()


def sweep(monkeypatch: pytest.MonkeyPatch, phase: str | ReplayPhase) -> dict[str, Any]:
    install_transport(monkeypatch, phases_by_name()[phase] if isinstance(phase, str) else phase)
    code, payload = run_cli("watch", "run", "--id", WATCH_ID)
    assert code == (1 if phase == "partial" else 0), payload
    result: dict[str, Any] = payload["alerts"]
    return result


def read_report(result: dict[str, Any]) -> dict[str, Any]:
    report: dict[str, Any] = json.loads(Path(result["json_path"]).read_text(encoding="utf-8"))
    assert report["report_id"] == result["report_id"]
    assert report["artifact_type"] == "watch_alert_report"
    return report


def artifact_bytes(result: dict[str, Any]) -> tuple[bytes, bytes]:
    return Path(result["json_path"]).read_bytes(), Path(result["markdown_path"]).read_bytes()


def cited(result: dict[str, Any], comment_id: str) -> list[dict[str, Any]]:
    return [
        citation
        for event in result["alerts"]
        for citation in event["evidence"]
        if citation["comment_id"] == comment_id
    ]


def changed_phase(tmp_path: Path, *, body: str, community: str | None = None) -> ReplayPhase:
    """Keep actual fixture transport/normalization; change only recorded ka4 source evidence."""
    phase = phases_by_name()["search_omits_known"]
    routes = []
    for index, route in enumerate(phase.routes):
        if route.response_path is None or "thread_a_new_comment" not in route.response_path.name:
            routes.append(route)
            continue
        payload = json.loads(route.response_path.read_text(encoding="utf-8"))

        def mutate(node: Any) -> None:
            if isinstance(node, list):
                for item in node:
                    mutate(item)
            elif isinstance(node, dict):
                data = node.get("data")
                if isinstance(data, dict):
                    if node.get("kind") == "t1" and data.get("id") == "ka4":
                        data["body"] = body
                    if node.get("kind") == "t3" and community is not None:
                        data["subreddit"] = community
                for value in node.values():
                    mutate(value)

        mutate(payload)
        path = (
            tmp_path / f"alert-source-{index}-{hashlib.sha256(body.encode()).hexdigest()[:8]}.json"
        )
        path.write_text(json.dumps(payload), encoding="utf-8")
        routes.append(replace(route, response_path=path))
    return replace(phase, routes=tuple(routes))


def test_baseline_restart_and_digest_preserve_cited_comparison(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    code, unavailable = run_cli("watch", "alerts", "--id", WATCH_ID)
    assert code == 0 and unavailable["status"] == "unavailable"
    assert unavailable["report_id"] is None and unavailable["alert_ids"] == []
    baseline = sweep(monkeypatch, "initial")
    assert baseline["status"] == "baseline" and baseline["new_alert_ids"] == []
    before = artifact_bytes(baseline)
    changed = sweep(monkeypatch, "search_omits_known")
    assert changed["new_alert_ids"]
    evidence = cited(changed, "reddit:ka4")
    assert evidence
    assert all(item["permalink"].endswith("/ka4/") for item in evidence)
    assert all(
        item["text_sha256"] and item["revision_id"] and item["entry_id"] for item in evidence
    )
    with closing(sqlite3.connect(watch)) as connection:
        source_created, first_observed = connection.execute(
            "SELECT source_created_utc,first_observed_at_utc FROM watch_comments "
            "WHERE watch_id=? AND comment_id=?",
            (WATCH_ID, "reddit:ka4"),
        ).fetchone()
    assert all(item["source_created_utc"] == source_created for item in evidence)
    assert all(item["first_observed_at_utc"] == first_observed for item in evidence)
    assert source_created != first_observed
    assert all(item["independent_person_verified"] is False for item in evidence)
    assert all(event["claims_are_verified"] is False for event in changed["alerts"])
    report = read_report(changed)
    assert report["baseline"]["window_id"] == baseline["window_id"]
    assert report["baseline"]["report_id"] == baseline["report_id"]
    assert report["basis"]["profile"]["queries"] == ["sync conflict"]
    assert report["basis"]["profile_content_sha256"]
    persisted = artifact_bytes(changed)

    def refuse_source(*args: Any, **kwargs: Any) -> None:
        pytest.fail("alerts/digest must use durable evidence without requesting a source")

    monkeypatch.setattr("threadsense.cli.build_reddit_connector", refuse_source)
    for command in ("alerts", "digest", "alerts"):
        code, payload = run_cli("watch", command, "--id", WATCH_ID)
        result = payload["alerts"] if command == "digest" else payload
        assert code == 0 and result["resumed"]
        assert result["report_id"] == changed["report_id"]
        assert result["alert_ids"] == changed["alert_ids"]
        assert result["new_alert_ids"] == []
        assert artifact_bytes(result) == persisted
    assert artifact_bytes(baseline) == before


def test_scores_disappearance_and_reappearance_are_not_new_demand(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sweep(monkeypatch, "initial")
    for phase in ("unchanged", "score_only"):
        result = sweep(monkeypatch, phase)
        assert result["new_alert_ids"] == []
        assert result["suppression_reasons"]
    changed = sweep(monkeypatch, "search_omits_known")
    assert cited(changed, "reddit:ka4")  # Missing from search is not deletion.
    sweep(monkeypatch, "edit")
    partial_result = sweep(monkeypatch, "partial")
    assert partial_result["new_alert_ids"] == []
    assert not any(event["kind"] == "resolution_claim" for event in partial_result["alerts"])
    reappeared = sweep(monkeypatch, "reappear")
    assert not cited(reappeared, "reddit:ka2")  # Previously known, temporarily hidden by more.
    assert cited(
        reappeared, "reddit:ka5"
    )  # Distinct newly observed historical evidence remains visible.
    assert set(changed["alert_ids"]).isdisjoint(reappeared["alert_ids"])
    assert sweep(monkeypatch, "backlog")["new_alert_ids"] == []


@pytest.mark.parametrize(
    ("cue", "kind"),
    [
        ("I can reproduce", "reproduction"),
        ("Cannot reproduce", "contradiction"),
        ("This is fixed in version 3", "resolution_claim"),
    ],
)
def test_update_claims_are_cited_not_asserted_as_verified(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    cue: str,
    kind: str,
) -> None:
    sweep(monkeypatch, "initial")
    body = f"{cue}. The merge conflict duplicates notes after offline sync; manual cleanup is slow."
    result = sweep(monkeypatch, changed_phase(tmp_path, body=body))
    events = [event for event in result["alerts"] if event["kind"] == kind]
    assert events, result
    assert all(event["claims_are_verified"] is False for event in events)
    citations = [item for event in events for item in event["evidence"]]
    assert any(item["comment_id"] == "reddit:ka4" and cue in item["excerpt"] for item in citations)
    assert "unverified" in Path(result["markdown_path"]).read_text(encoding="utf-8").lower()


def test_exact_copy_does_not_become_independent_new_evidence(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    baseline = sweep(monkeypatch, "initial")
    original = (
        "The merge tool keeps both versions and renames one with a conflict suffix, "
        "which is slow to clean up."
    )
    result = sweep(monkeypatch, changed_phase(tmp_path, body=original))
    assert not any(
        event["kind"] == "new_evidence"
        and any(item["comment_id"] == "reddit:ka4" for item in event["evidence"])
        for event in result["alerts"]
    )
    assert result["suppression_reasons"]
    assert read_report(result)["baseline"]["window_id"] == baseline["window_id"]


def test_new_community_is_observed_not_independent_people(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    sweep(monkeypatch, "initial")
    result = sweep(
        monkeypatch,
        changed_phase(
            tmp_path,
            community="OtherSyncTools",
            body=(
                "I can reproduce the merge conflict on a second desktop; "
                "it duplicates offline notes."
            ),
        ),
    )
    events = [event for event in result["alerts"] if event["kind"] == "new_community"]
    assert events, result
    assert any(
        item["community"] == "OtherSyncTools" for event in events for item in event["evidence"]
    )
    assert all(
        item["independent_person_verified"] is False
        for event in events
        for item in event["evidence"]
    )


def test_bounded_and_partial_coverage_refuse_rates_without_hiding_additions(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = sweep(monkeypatch, "initial")
    changed = sweep(monkeypatch, "search_omits_known")
    partial_result = sweep(monkeypatch, "partial")
    for result in (baseline, changed, partial_result):
        report = read_report(result)
        assert result["refusal_reasons"]
        assert report["coverage"]["rate_trend_eligible"] is False
        assert report["bounded_denominators"]["population_denominator"] is None
    assert changed["new_alert_ids"] and cited(changed, "reddit:ka4")
    assert read_report(partial_result)["coverage"]["sweep_outcome"] == "partial"


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("THREADSENSE_REDDIT_LISTING_LIMIT", "7"),
        ("THREADSENSE_ENABLED_SOURCES", "reddit,hackernews"),
        ("THREADSENSE_ANALYSIS_DUPLICATE_THRESHOLD", "0.75"),
    ],
)
def test_collection_policy_drift_binds_actual_sweep_not_later_invocation(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
    variable: str,
    value: str,
) -> None:
    baseline = sweep(monkeypatch, "initial")
    frozen = artifact_bytes(baseline)
    monkeypatch.setenv(variable, value)
    code, reused = run_cli("watch", "alerts", "--id", WATCH_ID)
    assert code == 0 and reused["report_id"] == baseline["report_id"]
    assert artifact_bytes(reused) == frozen
    changed = sweep(monkeypatch, "unchanged")
    assert changed["cohort_id"] != baseline["cohort_id"]
    assert changed["status"] == "baseline" and changed["new_alert_ids"] == []
    assert (
        read_report(changed)["basis"]["collection_basis"]
        != read_report(baseline)["basis"]["collection_basis"]
    )
    assert artifact_bytes(baseline) == frozen


def test_profile_limit_change_establishes_new_baseline(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    first = sweep(monkeypatch, "initial")
    profile = tmp_path / "revised.toml"
    profile.write_text(
        PROFILE_PATH.read_text(encoding="utf-8").replace(
            "discovered_candidates = 4", "discovered_candidates = 5"
        ),
        encoding="utf-8",
    )
    create_watch(profile)
    changed = sweep(monkeypatch, "search_omits_known")
    assert changed["cohort_id"] != first["cohort_id"]
    assert changed["status"] == "baseline" and changed["alert_ids"] == []


def test_edited_evidence_preserves_source_time_and_records_observation_time(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sweep(monkeypatch, "initial")
    sweep(monkeypatch, "search_omits_known")
    result = sweep(monkeypatch, "edit")
    edits = [event for event in result["alerts"] if event["kind"] == "evidence_update"]
    assert edits, result
    evidence = [
        item for event in edits for item in event["evidence"] if item["comment_id"] == "reddit:kb2"
    ]
    assert evidence
    assert all(item["edit_observed_at_utc"] >= item["first_observed_at_utc"] for item in evidence)
    assert all(item["source_edited_at_utc"] is None for item in evidence)
    assert all(item["source_created_utc"] < item["edit_observed_at_utc"] for item in evidence)
    assert all(item["source_thread_created_utc"] != item["source_created_utc"] for item in evidence)


class InterruptedAlertWrite(BaseException):
    pass


@pytest.mark.parametrize("tamper", [False, True])
def test_interrupted_publication_recovers_frozen_report_or_refuses_tampering(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
    tamper: bool,
) -> None:
    sweep(monkeypatch, "initial")
    install_transport(monkeypatch, phases_by_name()["search_omits_known"])
    with monkeypatch.context() as patch:

        def interrupt(*args: Any, **kwargs: Any) -> None:
            raise InterruptedAlertWrite()

        patch.setattr(MonitoringStore, "complete_alert_report", interrupt)
        with pytest.raises(InterruptedAlertWrite):
            run_cli("watch", "run", "--id", WATCH_ID, "--lease-seconds", "1")
    files = sorted(watch.parent.rglob("*alerts/*.json"))
    pending = [path for path in files if json.loads(path.read_text())["status"] != "baseline"]
    assert len(pending) == 1
    path = pending[0]
    frozen = path.read_bytes()
    document = json.loads(frozen)
    if tamper:
        path.write_bytes(frozen + b" ")
    monkeypatch.setattr(
        "threadsense.cli.MonitoringStore",
        partial(
            MonitoringStore,
            clock=lambda: time.time() + 120,
        ),
    )
    code, result = run_cli("watch", "alerts", "--id", WATCH_ID)
    if tamper:
        assert code != 0 and result["error"]["code"] == "monitoring_state_error"
        assert path.read_bytes() == frozen + b" "
    else:
        assert code == 0 and result["report_id"] == document["report_id"]
        assert path.read_bytes() == frozen
        code, restarted = run_cli("watch", "alerts", "--id", WATCH_ID)
        assert code == 0 and restarted["new_alert_ids"] == []
        assert restarted["alert_ids"] == result["alert_ids"]


def test_preference_only_reordering_does_not_redeliver_evidence(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sqlite3
    from contextlib import closing

    baseline = sweep(monkeypatch, "initial")
    frozen = artifact_bytes(baseline)
    with closing(sqlite3.connect(watch)) as conn:
        row = conn.execute(
            "SELECT entry_id FROM watch_digest_entries WHERE watch_id=? AND kind='finding' "
            "ORDER BY entry_id LIMIT 1",
            (WATCH_ID,),
        ).fetchone()
    assert row is not None
    code, feedback = run_cli(
        "feedback",
        "record",
        "--watch",
        WATCH_ID,
        "--target",
        row[0],
        "--target-kind",
        "finding",
        "--kind",
        "preference",
        "--value",
        "useful",
        "--key",
        "alerts-preference",
    )
    assert code == 0, feedback
    code, reused = run_cli("watch", "alerts", "--id", WATCH_ID)
    assert code == 0 and reused["report_id"] == baseline["report_id"]
    assert reused["new_alert_ids"] == [] and artifact_bytes(reused) == frozen
    next_window = sweep(monkeypatch, "unchanged")
    assert next_window["new_alert_ids"] == []
    assert artifact_bytes(baseline) == frozen


def test_alert_presentation_refuses_live_competing_claim_without_rewriting_history(
    watch: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = sweep(monkeypatch, "initial")
    before = artifact_bytes(baseline)
    claim = MonitoringStore(watch).claim_run(WATCH_ID, lease_seconds=600)
    code, refusal = run_cli("watch", "alerts", "--id", WATCH_ID)
    assert code == 1 and refusal["error"]["code"] == "run_claim_error"
    assert refusal["error"]["details"]["reason"] == "claim_held"
    assert artifact_bytes(baseline) == before
    MonitoringStore(watch).finalize_run(claim.run_id, claim.owner_token, "succeeded")
    code, reused = run_cli("watch", "alerts", "--id", WATCH_ID)
    assert code == 0 and reused["report_id"] == baseline["report_id"]
    assert reused["new_alert_ids"] == []
