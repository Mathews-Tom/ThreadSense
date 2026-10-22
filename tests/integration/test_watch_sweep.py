from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import time
from collections.abc import Iterator
from contextlib import closing
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pytest
from recorded_reddit import (
    PhaseReplayTransport,
    ReplayPhase,
    load_replay_phases,
    replay_connector_factory,
    reroute,
)

from threadsense.config import AppConfig, load_config
from threadsense.errors import MonitoringStateError, RunClaimError
from threadsense.logging_config import configure_logging
from threadsense.models.analysis import load_analysis_artifact_file
from threadsense.models.canonical import load_canonical_thread
from threadsense.monitoring import MonitoringStore, load_watch_profile_file, monitoring_state_path
from threadsense.monitoring.state import RunClaim
from threadsense.monitoring.sweep import SweepResult, run_watch_sweep
from threadsense.observability import TraceContext

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "watch" / "incremental"
PROFILE_PATH = FIXTURE_DIR / "profile.toml"
PHASES_PATH = FIXTURE_DIR / "phases.json"
WATCH_ID = "sync-conflicts"
THREAD_A = "sc1aaa"
THREAD_B = "sc1bbb"
QUERY = "sync conflict"
SEARCH_URL_MARKER = "/search.json"
UNSAFE_POST_ID = "../x"
SWEEP_LEASE_SECONDS = 60
STATE_TABLES = (
    "watch_runs",
    "watch_threads",
    "watch_discoveries",
    "watch_acquisitions",
    "watch_fetch_failures",
    "watch_comments",
    "watch_comment_texts",
    "watch_thread_revisions",
    "watch_revision_analyses",
    "watch_sweeps",
)


# Sweep harness.


def build_config(monkeypatch: pytest.MonkeyPatch, storage_root: Path) -> AppConfig:
    monkeypatch.setenv("THREADSENSE_STORAGE_ROOT", str(storage_root))
    monkeypatch.setenv("THREADSENSE_RUNTIME_ENABLED", "false")
    monkeypatch.setenv("THREADSENSE_REDDIT_MAX_RETRIES", "0")
    monkeypatch.setenv("THREADSENSE_REDDIT_REQUEST_DELAY", "0")
    return load_config()


def create_watch_store(config: AppConfig) -> MonitoringStore:
    store = MonitoringStore(monitoring_state_path(config.storage))
    store.save_profile(load_watch_profile_file(PROFILE_PATH))
    return store


def run_phase_sweep(
    config: AppConfig,
    store: MonitoringStore,
    phase: ReplayPhase,
    *,
    lease_seconds: int = SWEEP_LEASE_SECONDS,
) -> tuple[SweepResult, PhaseReplayTransport]:
    transport = PhaseReplayTransport(phase)
    result = run_watch_sweep(
        config=config,
        store=store,
        watch_id=WATCH_ID,
        lease_seconds=lease_seconds,
        connector_factory=replay_connector_factory(transport),
        logger=configure_logging(),
        trace=TraceContext.create(run_id=f"watch-{phase.name}", source_name="watch"),
    )
    return result, transport


def read_tables(state_path: Path) -> dict[str, list[dict[str, Any]]]:
    uri = f"{state_path.as_uri()}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        return {
            table: [dict(row) for row in conn.execute(f"SELECT * FROM {table}")]
            for table in STATE_TABLES
        }


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def artifact_hashes(monitoring_root: Path) -> dict[str, str]:
    """Hash every sweep artifact under `watches/`, keyed by monitoring-root-relative path."""
    return {
        path.relative_to(monitoring_root).as_posix(): sha256_file(path)
        for path in sorted((monitoring_root / "watches").rglob("*"))
        if path.is_file()
    }


@dataclass(frozen=True)
class PhaseRecord:
    result: SweepResult
    requested_urls: list[str]
    status: dict[str, Any]
    tables: dict[str, list[dict[str, Any]]]
    artifacts: dict[str, str]

    def run_rows(self, table: str) -> list[dict[str, Any]]:
        return [row for row in self.tables[table] if row["run_id"] == self.result.run_id]

    def comment(self, comment_id: str) -> dict[str, Any]:
        rows = [row for row in self.tables["watch_comments"] if row["comment_id"] == comment_id]
        assert len(rows) == 1
        return rows[0]


def record_phase(config: AppConfig, store: MonitoringStore, phase: ReplayPhase) -> PhaseRecord:
    result, transport = run_phase_sweep(config, store, phase)
    return PhaseRecord(
        result=result,
        requested_urls=transport.requested_urls,
        status=store.sweep_status(WATCH_ID),
        tables=read_tables(store.path),
        artifacts=artifact_hashes(store.monitoring_root()),
    )


@dataclass(frozen=True)
class SequenceRun:
    monitoring_root: Path
    store: MonitoringStore
    phases: dict[str, PhaseRecord]

    @property
    def final(self) -> PhaseRecord:
        return self.phases["backlog"]


@pytest.fixture(scope="module")
def sequence(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SequenceRun]:
    phases = load_replay_phases(PHASES_PATH)
    with pytest.MonkeyPatch.context() as monkeypatch:
        config = build_config(monkeypatch, tmp_path_factory.mktemp("sequence") / ".threadsense")
        store = create_watch_store(config)
        records: dict[str, PhaseRecord] = {}
        for phase in phases:
            records[phase.name] = record_phase(config, store, phase)
        yield SequenceRun(monitoring_root=store.monitoring_root(), store=store, phases=records)


def thread_url(source_thread_id: str) -> str:
    phase = load_replay_phases(PHASES_PATH)[0]
    marker = f"/comments/{source_thread_id}/"
    return next(route.url for route in phase.routes if marker in route.url)


# Phase sequence behavior.


def test_run_watch_sweep_initial_outcome_is_changes(sequence: SequenceRun) -> None:
    assert sequence.phases["initial"].result.outcome == "changes"


def test_run_watch_sweep_initial_creates_first_revision_per_selected_thread(
    sequence: SequenceRun,
) -> None:
    revisions = sequence.phases["initial"].run_rows("watch_thread_revisions")

    assert sorted((row["source_thread_id"], row["sequence"]) for row in revisions) == [
        (THREAD_A, 1),
        (THREAD_B, 1),
    ]
    assert all(row["previous_revision_id"] is None for row in revisions)


def test_run_watch_sweep_initial_classifies_only_initial_observations(
    sequence: SequenceRun,
) -> None:
    record = sequence.phases["initial"]

    changes = record.result.summary["changes"]

    assert changes["initial_observation"] == 5
    assert changes["new_source_event"] == 0
    assert changes["newly_observed_historical"] == 0
    assert changes["edited"] == 0
    assert {row["novelty"] for row in record.tables["watch_comments"]} == {"initial_observation"}


def test_run_watch_sweep_initial_rejects_thread_with_exclude_term(sequence: SequenceRun) -> None:
    record = sequence.phases["initial"]

    rejected = [row for row in record.run_rows("watch_discoveries") if row["selected"] == 0]

    assert [(row["source_thread_id"], row["rejection_reason"]) for row in rejected] == [
        ("sc1ccc", "excluded_term")
    ]
    assert record.result.summary["discovery"]["matched_count"] == 3
    assert record.result.summary["discovery"]["selected_count"] == 2


def test_run_watch_sweep_initial_analyzes_every_new_revision(sequence: SequenceRun) -> None:
    record = sequence.phases["initial"]

    analyses = record.run_rows("watch_revision_analyses")

    assert record.result.summary["analysis"]["analyzed"] == 2
    assert {row["revision_id"] for row in analyses} == {
        row["revision_id"] for row in record.run_rows("watch_thread_revisions")
    }


@pytest.mark.parametrize("phase_name", ["unchanged", "score_only"])
def test_run_watch_sweep_non_substantive_refresh_outcome_is_no_changes(
    sequence: SequenceRun, phase_name: str
) -> None:
    assert sequence.phases[phase_name].result.outcome == "no_changes"


@pytest.mark.parametrize("phase_name", ["unchanged", "score_only"])
def test_run_watch_sweep_non_substantive_refresh_creates_no_revision(
    sequence: SequenceRun, phase_name: str
) -> None:
    record = sequence.phases[phase_name]

    assert record.run_rows("watch_thread_revisions") == []
    assert record.result.summary["changes"]["revisions_created"] == 0


@pytest.mark.parametrize(
    ("phase_name", "previous_name"), [("unchanged", "initial"), ("score_only", "unchanged")]
)
def test_run_watch_sweep_non_substantive_refresh_adds_no_comment_text_rows(
    sequence: SequenceRun, phase_name: str, previous_name: str
) -> None:
    current = sequence.phases[phase_name].tables["watch_comment_texts"]
    previous = sequence.phases[previous_name].tables["watch_comment_texts"]

    assert current == previous


@pytest.mark.parametrize("phase_name", ["unchanged", "score_only"])
def test_run_watch_sweep_non_substantive_refresh_records_both_acquisitions(
    sequence: SequenceRun, phase_name: str
) -> None:
    acquisitions = sequence.phases[phase_name].run_rows("watch_acquisitions")

    assert sorted(row["source_thread_id"] for row in acquisitions) == [THREAD_A, THREAD_B]
    assert all(row["revision_id"] is None for row in acquisitions)


def test_run_watch_sweep_known_thread_omitted_by_search_is_still_refreshed(
    sequence: SequenceRun,
) -> None:
    record = sequence.phases["search_omits_known"]

    discovered = {row["source_thread_id"] for row in record.run_rows("watch_discoveries")}
    refreshed = {row["source_thread_id"] for row in record.run_rows("watch_acquisitions")}

    assert THREAD_A not in discovered
    assert THREAD_A in refreshed
    assert thread_url(THREAD_A) in record.requested_urls


def test_run_watch_sweep_reply_after_prior_observation_is_new_source_event(
    sequence: SequenceRun,
) -> None:
    record = sequence.phases["search_omits_known"]

    revisions = record.run_rows("watch_thread_revisions")

    assert record.comment("reddit:ka4")["novelty"] == "new_source_event"
    assert record.result.summary["changes"]["new_source_event"] == 1
    assert [row["source_thread_id"] for row in revisions] == [THREAD_A]
    assert record.result.outcome == "changes"


def test_run_watch_sweep_edited_comment_creates_revision_for_its_thread(
    sequence: SequenceRun,
) -> None:
    record = sequence.phases["edit"]

    revisions = record.run_rows("watch_thread_revisions")

    assert [(row["source_thread_id"], row["sequence"]) for row in revisions] == [(THREAD_B, 2)]
    assert record.result.summary["changes"]["edited"] == 1


def test_run_watch_sweep_edited_comment_records_edited_text_row(sequence: SequenceRun) -> None:
    record = sequence.phases["edit"]
    edited_body = (
        "Check whether two machines run the nightly job at the same minute; "
        "staggering them by ten minutes fixed it for us."
    )
    acquisition_ids = {row["acquisition_id"] for row in record.run_rows("watch_acquisitions")}

    edits = [
        row
        for row in record.tables["watch_comment_texts"]
        if row["comment_id"] == "reddit:kb2" and row["change"] == "edited"
    ]

    assert len(edits) == 1
    assert edits[0]["acquisition_id"] in acquisition_ids
    assert edits[0]["text_sha256"] == hashlib.sha256(edited_body.encode("utf-8")).hexdigest()
    assert record.comment("reddit:kb2")["current_text_sha256"] == edits[0]["text_sha256"]


def test_run_watch_sweep_fetch_failure_with_other_success_outcome_is_partial(
    sequence: SequenceRun,
) -> None:
    assert sequence.phases["partial"].result.outcome == "partial"


def test_run_watch_sweep_fetch_failure_is_recorded_for_failing_thread(
    sequence: SequenceRun,
) -> None:
    record = sequence.phases["partial"]

    failures = record.run_rows("watch_fetch_failures")
    summary_failures = record.result.summary["failures"]

    assert [(row["stage"], row["target"]) for row in failures] == [("fetch", THREAD_B)]
    assert [(item["stage"], item["target"]) for item in summary_failures] == [("fetch", THREAD_B)]
    assert summary_failures[0]["error"]["details"]["status_code"] == 503
    assert record.result.summary["refresh"]["failed"] == 1


def test_run_watch_sweep_nested_more_node_records_truncated_acquisition(
    sequence: SequenceRun,
) -> None:
    record = sequence.phases["partial"]

    acquisitions = record.run_rows("watch_acquisitions")

    assert [(row["source_thread_id"], row["completeness"]) for row in acquisitions] == [
        (THREAD_A, "truncated")
    ]
    assert acquisitions[0]["absent_known_comment_count"] == 1
    assert record.result.summary["refresh"]["truncated_acquisitions"] == 1


def test_run_watch_sweep_truncated_acquisition_creates_no_revision(sequence: SequenceRun) -> None:
    record = sequence.phases["partial"]

    assert record.run_rows("watch_thread_revisions") == []
    assert record.run_rows("watch_acquisitions")[0]["revision_id"] is None


def test_run_watch_sweep_comment_hidden_by_truncation_stays_known(sequence: SequenceRun) -> None:
    record = sequence.phases["partial"]
    partial_acquisition = record.run_rows("watch_acquisitions")[0]["acquisition_id"]

    initial = sequence.phases["initial"].comment("reddit:ka2")

    hidden = record.comment("reddit:ka2")

    assert hidden["last_seen_acquisition_id"] != partial_acquisition
    assert hidden["current_text_sha256"] == initial["current_text_sha256"]


def test_run_watch_sweep_reappearing_comment_adds_no_text_row(sequence: SequenceRun) -> None:
    record = sequence.phases["reappear"]

    texts = [
        row for row in record.tables["watch_comment_texts"] if row["comment_id"] == "reddit:ka2"
    ]

    assert [row["change"] for row in texts] == ["first_observed"]
    assert record.comment("reddit:ka2")["novelty"] == "initial_observation"


def test_run_watch_sweep_first_seen_old_comment_is_newly_observed_historical(
    sequence: SequenceRun,
) -> None:
    record = sequence.phases["reappear"]

    revisions = record.run_rows("watch_thread_revisions")
    changes = record.result.summary["changes"]

    assert record.comment("reddit:ka5")["novelty"] == "newly_observed_historical"
    assert changes["newly_observed_historical"] == 1
    assert changes["new_source_event"] == 0
    assert changes["edited"] == 0
    assert [(row["source_thread_id"], row["sequence"]) for row in revisions] == [(THREAD_A, 3)]


def test_run_watch_sweep_revision_snapshot_keeps_comments_absent_from_acquisition(
    sequence: SequenceRun,
) -> None:
    record = sequence.phases["reappear"]
    revision = record.run_rows("watch_thread_revisions")[0]

    snapshot = load_canonical_thread(sequence.monitoring_root / revision["snapshot_path"])
    bodies = {comment.comment_id: comment.body for comment in snapshot.comments}

    assert set(bodies) == {f"reddit:ka{index}" for index in range(1, 6)}
    assert bodies["reddit:ka3"] == (
        "Is there a setting to prefer the newest edit instead of keeping both copies?"
    )
    assert snapshot.comment_count == 5


def test_run_watch_sweep_backlog_reports_pending_refresh(sequence: SequenceRun) -> None:
    record = sequence.final

    refresh = record.result.summary["refresh"]

    assert refresh["tracked_threads"] == 4
    assert refresh["attempted"] == 2
    assert refresh["pending"] == 2
    assert record.status["refresh_due_never_refreshed"] == 2


def test_run_watch_sweep_backlog_refreshes_only_budgeted_threads(sequence: SequenceRun) -> None:
    record = sequence.final

    fetched = sorted(row["source_thread_id"] for row in record.run_rows("watch_acquisitions"))

    assert fetched == [THREAD_A, THREAD_B]
    assert not any("sc1ddd" in url or "sc1eee" in url for url in record.requested_urls)


def test_run_watch_sweep_backlog_reports_analysis_pending(sequence: SequenceRun) -> None:
    record = sequence.final

    analysis = record.result.summary["analysis"]

    assert analysis["limit"] == 2
    assert analysis["pending"] == record.status["pending_analyses"]
    assert analysis["pending"] == sequence.store.pending_analysis_count(WATCH_ID)


def test_run_watch_sweep_backlog_outcome_is_no_changes(sequence: SequenceRun) -> None:
    assert sequence.final.result.outcome == "no_changes"


def test_sweep_status_latest_sweep_matches_last_result(sequence: SequenceRun) -> None:
    latest = sequence.final.status["latest_sweep"]

    assert latest["run_id"] == sequence.final.result.run_id
    assert latest["outcome"] == "no_changes"
    assert latest["summary"] == sequence.final.result.summary


def test_run_watch_sweep_acquisition_observed_at_does_not_follow_raw_fetched_at(
    sequence: SequenceRun,
) -> None:
    acquisitions = sequence.final.tables["watch_acquisitions"]

    fetched_at = {
        row["acquisition_id"]: json.loads(
            (sequence.monitoring_root / row["raw_path"]).read_text(encoding="utf-8")
        )["fetched_at_utc"]
        for row in acquisitions
    }

    assert acquisitions
    assert all(row["observed_at_utc"] <= fetched_at[row["acquisition_id"]] for row in acquisitions)


# Immutable artifacts.


def test_run_watch_sweep_recorded_artifacts_match_recorded_sha256(sequence: SequenceRun) -> None:
    tables = sequence.final.tables
    root = sequence.monitoring_root
    recorded = [
        *[(row["raw_path"], row["raw_sha256"]) for row in tables["watch_acquisitions"]],
        *[
            (row["normalized_path"], row["normalized_sha256"])
            for row in tables["watch_acquisitions"]
        ],
        *[
            (row["snapshot_path"], row["snapshot_sha256"])
            for row in tables["watch_thread_revisions"]
        ],
        *[
            (row["analysis_path"], row["analysis_sha256"])
            for row in tables["watch_revision_analyses"]
        ],
    ]

    mismatched = [path for path, digest in recorded if sha256_file(root / path) != digest]

    assert len(recorded) == 2 * 15 + 5 + 5
    assert mismatched == []


def test_run_watch_sweep_earlier_artifacts_stay_byte_identical(sequence: SequenceRun) -> None:
    final = sequence.final.artifacts

    changed = [
        (name, path)
        for name, record in sequence.phases.items()
        for path, digest in record.artifacts.items()
        if final.get(path) != digest
    ]

    assert changed == []


def test_run_watch_sweep_revision_snapshot_provenance_points_to_its_raw_acquisition(
    sequence: SequenceRun,
) -> None:
    tables = sequence.final.tables
    acquisitions = {row["acquisition_id"]: row for row in tables["watch_acquisitions"]}

    for revision in tables["watch_thread_revisions"]:
        acquisition = acquisitions[revision["acquisition_id"]]
        snapshot = load_canonical_thread(sequence.monitoring_root / revision["snapshot_path"])
        raw_path = sequence.monitoring_root / acquisition["raw_path"]
        assert Path(snapshot.provenance.raw_artifact_path).resolve() == raw_path.resolve()
        assert snapshot.provenance.raw_sha256 == acquisition["raw_sha256"]


def test_run_watch_sweep_analysis_references_immutable_snapshot(sequence: SequenceRun) -> None:
    tables = sequence.final.tables
    snapshots = {
        row["revision_id"]: row["snapshot_path"] for row in tables["watch_thread_revisions"]
    }

    for row in tables["watch_revision_analyses"]:
        analysis = load_analysis_artifact_file(sequence.monitoring_root / row["analysis_path"])
        expected = (sequence.monitoring_root / snapshots[row["revision_id"]]).resolve()
        assert Path(analysis.provenance.normalized_artifact_path).resolve() == expected


def test_run_watch_sweep_revisions_chain_previous_revision_per_thread(
    sequence: SequenceRun,
) -> None:
    revisions = sequence.final.tables["watch_thread_revisions"]
    chains: dict[str, list[dict[str, Any]]] = {}
    for row in sorted(revisions, key=lambda item: item["sequence"]):
        chains.setdefault(row["source_thread_id"], []).append(row)

    links = {
        thread: [(row["sequence"], row["previous_revision_id"]) for row in rows]
        for thread, rows in chains.items()
    }

    assert links == {
        thread: [
            (index + 1, rows[index - 1]["revision_id"] if index else None)
            for index in range(len(rows))
        ]
        for thread, rows in chains.items()
    }
    assert {thread: len(rows) for thread, rows in chains.items()} == {THREAD_A: 3, THREAD_B: 2}


# Source failures, unsafe source ids and thread text changes.


@dataclass(frozen=True)
class BaselinedWatch:
    config: AppConfig
    store: MonitoringStore
    phases: dict[str, ReplayPhase]


@pytest.fixture
def baselined_watch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> BaselinedWatch:
    config = build_config(monkeypatch, tmp_path / ".threadsense")
    store = create_watch_store(config)
    phases = {phase.name: phase for phase in load_replay_phases(PHASES_PATH)}
    run_phase_sweep(config, store, phases["initial"])
    return BaselinedWatch(config=config, store=store, phases=phases)


@pytest.fixture
def search_failed_sweep(baselined_watch: BaselinedWatch) -> PhaseRecord:
    phase = reroute(baselined_watch.phases["unchanged"], SEARCH_URL_MARKER, status=503)
    return record_phase(baselined_watch.config, baselined_watch.store, phase)


def test_run_watch_sweep_search_failure_with_refreshed_threads_outcome_is_partial(
    search_failed_sweep: PhaseRecord,
) -> None:
    assert search_failed_sweep.result.outcome == "partial"
    assert search_failed_sweep.result.summary["refresh"]["fetched"] == 2


def test_run_watch_sweep_search_failure_is_recorded_for_failing_query(
    search_failed_sweep: PhaseRecord,
) -> None:
    failures = search_failed_sweep.run_rows("watch_fetch_failures")

    assert [(row["stage"], row["target"]) for row in failures] == [("search", QUERY)]
    assert search_failed_sweep.result.summary["discovery"]["failed_queries"] == [QUERY]


def test_run_watch_sweep_every_search_and_fetch_failing_outcome_is_failed(
    baselined_watch: BaselinedWatch,
) -> None:
    phase = baselined_watch.phases["unchanged"]
    for url_marker in (SEARCH_URL_MARKER, f"/comments/{THREAD_A}/", f"/comments/{THREAD_B}/"):
        phase = reroute(phase, url_marker, status=503)

    record = record_phase(baselined_watch.config, baselined_watch.store, phase)

    runs = [row for row in record.tables["watch_runs"] if row["run_id"] == record.result.run_id]
    assert record.result.outcome == "failed"
    assert [row["status"] for row in runs] == ["failed"]


def test_run_watch_sweep_thread_title_change_creates_revision_with_thread_text_changed(
    baselined_watch: BaselinedWatch, tmp_path: Path
) -> None:
    payload = json.loads(
        (FIXTURE_DIR / "responses" / "thread_a_initial.json").read_text(encoding="utf-8")
    )
    payload[0]["data"]["children"][0]["data"]["title"] = "Sync conflict after offline edit, solved"
    retitled_path = tmp_path / "thread_a_retitled.json"
    retitled_path.write_text(json.dumps(payload), encoding="utf-8")
    phase = reroute(
        baselined_watch.phases["unchanged"], f"/comments/{THREAD_A}/", response_path=retitled_path
    )

    record = record_phase(baselined_watch.config, baselined_watch.store, phase)

    revisions = record.run_rows("watch_thread_revisions")
    assert [(row["source_thread_id"], row["sequence"]) for row in revisions] == [(THREAD_A, 2)]
    assert json.loads(revisions[0]["changes_json"])["thread_text_changed"] is True
    assert record.result.summary["changes"]["thread_text_changed"] == 1


def test_run_watch_sweep_thread_repeating_comment_id_is_failed_fetch_not_failed_sweep(
    baselined_watch: BaselinedWatch, tmp_path: Path
) -> None:
    payload = json.loads(
        (FIXTURE_DIR / "responses" / "thread_a_initial.json").read_text(encoding="utf-8")
    )
    comments = payload[1]["data"]["children"]
    comments.append(comments[0])
    repeating_path = tmp_path / "thread_a_repeating.json"
    repeating_path.write_text(json.dumps(payload), encoding="utf-8")
    phase = reroute(
        baselined_watch.phases["unchanged"], f"/comments/{THREAD_A}/", response_path=repeating_path
    )

    record = record_phase(baselined_watch.config, baselined_watch.store, phase)

    failures = record.run_rows("watch_fetch_failures")
    assert record.result.outcome == "partial"
    assert [(row["stage"], row["target"]) for row in failures] == [("fetch", THREAD_A)]
    assert record.run_rows("watch_thread_revisions") == []
    assert [row["source_thread_id"] for row in record.run_rows("watch_acquisitions")] == [THREAD_B]


@pytest.mark.parametrize("malformation", ["comment_child", "reply_child", "replies_list"])
def test_run_watch_sweep_malformed_thread_cannot_starve_refresh_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, malformation: str
) -> None:
    config = build_config(monkeypatch, tmp_path / ".threadsense")
    now = [time.time()]
    store = MonitoringStore(monitoring_state_path(config.storage), clock=lambda: now[0])
    profile = load_watch_profile_file(PROFILE_PATH)
    store.save_profile(profile)
    phases = {phase.name: phase for phase in load_replay_phases(PHASES_PATH)}
    payload = json.loads(
        (FIXTURE_DIR / "responses" / "thread_a_initial.json").read_text(encoding="utf-8")
    )
    children = payload[1]["data"]["children"]
    if malformation == "comment_child":
        children.append(3)
    elif malformation == "reply_child":
        children[0]["data"]["replies"] = {"kind": "Listing", "data": {"children": [3]}}
    else:
        children[0]["data"]["replies"] = []
    malformed_path = tmp_path / "thread_a_malformed.json"
    malformed_path.write_text(json.dumps(payload), encoding="utf-8")
    phase = reroute(phases["initial"], f"/comments/{THREAD_A}/", response_path=malformed_path)

    first = record_phase(config, store, phase)

    assert first.result.outcome == "partial"
    failures = first.run_rows("watch_fetch_failures")
    assert [
        (row["stage"], row["target"], json.loads(row["error_json"])["code"]) for row in failures
    ] == [("fetch", THREAD_A, "reddit_response_error")]
    assert [row["source_thread_id"] for row in first.run_rows("watch_acquisitions")] == [THREAD_B]
    assert [
        (row["source_thread_id"], row["sequence"])
        for row in first.run_rows("watch_thread_revisions")
    ] == [(THREAD_B, 1)]
    assert len(first.run_rows("watch_revision_analyses")) == 1
    assert first.result.summary["analysis"]["analyzed"] == 1
    assert len(first.tables["watch_comments"]) == 2
    assert len(first.tables["watch_comment_texts"]) == 2
    before = next(
        row for row in first.tables["watch_threads"] if row["source_thread_id"] == THREAD_A
    )

    store.save_profile(replace(profile, limits=replace(profile.limits, refreshed_threads=1)))
    now[0] += 10
    second = record_phase(config, store, phase)

    assert second.result.outcome == "partial"
    assert second.run_rows("watch_acquisitions") == []
    assert [
        (row["stage"], row["target"], json.loads(row["error_json"])["code"])
        for row in second.run_rows("watch_fetch_failures")
    ] == [("fetch", THREAD_A, "reddit_response_error")]
    after = next(
        row for row in second.tables["watch_threads"] if row["source_thread_id"] == THREAD_A
    )
    assert after["last_attempted_at_utc"] > before["last_attempted_at_utc"]
    assert [item.source_thread_id for item in store.refresh_queue(WATCH_ID, 2)] == [
        THREAD_B,
        THREAD_A,
    ]

    now[0] += 10
    third = record_phase(config, store, phase)

    assert third.result.outcome == "no_changes"
    assert [row["source_thread_id"] for row in third.run_rows("watch_acquisitions")] == [THREAD_B]
    assert third.run_rows("watch_fetch_failures") == []
    assert third.tables["watch_thread_revisions"] == first.tables["watch_thread_revisions"]
    assert third.tables["watch_revision_analyses"] == first.tables["watch_revision_analyses"]
    assert third.tables["watch_comment_texts"] == first.tables["watch_comment_texts"]
    assert all(third.artifacts[path] == digest for path, digest in first.artifacts.items())


def test_run_watch_sweep_analysis_budget_drains_durable_pending_revision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = build_config(monkeypatch, tmp_path / ".threadsense")
    store = create_watch_store(config)
    profile = load_watch_profile_file(PROFILE_PATH)
    store.save_profile(replace(profile, limits=replace(profile.limits, analyzed_revisions=1)))
    phases = {phase.name: phase for phase in load_replay_phases(PHASES_PATH)}

    first = record_phase(config, store, phases["initial"])

    assert first.result.summary["changes"]["revisions_created"] == 2
    assert first.result.summary["analysis"]["analyzed"] == 1
    assert first.result.summary["analysis"]["pending"] == 1
    assert len(first.tables["watch_revision_analyses"]) == 1
    assert sorted(
        (row["source_thread_id"], row["sequence"]) for row in first.tables["watch_thread_revisions"]
    ) == [(THREAD_A, 1), (THREAD_B, 1)]
    assert len(first.tables["watch_comments"]) == 5
    assert {row["novelty"] for row in first.tables["watch_comments"]} == {"initial_observation"}

    second = record_phase(config, store, phases["unchanged"])

    assert second.result.summary["analysis"]["analyzed"] == 1
    assert second.result.summary["analysis"]["pending"] == 0
    assert len(second.tables["watch_revision_analyses"]) == 2
    assert second.run_rows("watch_thread_revisions") == []
    assert second.result.summary["changes"]["revisions_created"] == 0
    assert second.result.summary["changes"]["new_source_event"] == 0
    assert second.tables["watch_thread_revisions"] == first.tables["watch_thread_revisions"]
    assert second.tables["watch_comment_texts"] == first.tables["watch_comment_texts"]
    assert len(second.tables["watch_comments"]) == 5
    assert {row["novelty"] for row in second.tables["watch_comments"]} == {"initial_observation"}
    assert all(second.artifacts[path] == digest for path, digest in first.artifacts.items())


def test_run_watch_sweep_discovery_budget_bounds_persisted_candidates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = build_config(monkeypatch, tmp_path / ".threadsense")
    store = create_watch_store(config)
    profile = load_watch_profile_file(PROFILE_PATH)
    store.save_profile(replace(profile, limits=replace(profile.limits, discovered_candidates=1)))
    payload = json.loads(
        (FIXTURE_DIR / "responses" / "search_initial.json").read_text(encoding="utf-8")
    )
    children = payload["data"]["children"]
    for child in children[1:]:
        child["data"]["title"] = children[0]["data"]["title"]
        child["data"]["selftext"] = children[0]["data"]["selftext"]
        child["data"]["score"] = 1
    search_path = tmp_path / "search_bounded.json"
    search_path.write_text(json.dumps(payload), encoding="utf-8")
    phase = reroute(
        load_replay_phases(PHASES_PATH)[0], SEARCH_URL_MARKER, response_path=search_path
    )

    record = record_phase(config, store, phase)

    discovery = record.result.summary["discovery"]
    assert discovery["limit"] == 1
    assert discovery["matched_count"] == 3
    assert discovery["persisted_count"] == 1
    assert discovery["truncated"] is True
    assert discovery["newly_tracked_count"] == 1
    assert [row["source_thread_id"] for row in record.run_rows("watch_discoveries")] == [THREAD_A]
    assert [row["source_thread_id"] for row in record.tables["watch_threads"]] == [THREAD_A]
    assert [row["source_thread_id"] for row in record.run_rows("watch_acquisitions")] == [THREAD_A]


def test_run_watch_sweep_malformed_search_preserves_known_thread_progress(
    baselined_watch: BaselinedWatch, tmp_path: Path
) -> None:
    payload = json.loads(
        (FIXTURE_DIR / "responses" / "search_initial.json").read_text(encoding="utf-8")
    )
    payload["data"]["children"].append(3)
    search_path = tmp_path / "search_malformed.json"
    search_path.write_text(json.dumps(payload), encoding="utf-8")
    phase = reroute(
        baselined_watch.phases["unchanged"], SEARCH_URL_MARKER, response_path=search_path
    )

    record = record_phase(baselined_watch.config, baselined_watch.store, phase)

    assert record.result.outcome == "partial"
    failures = record.run_rows("watch_fetch_failures")
    assert [
        (row["stage"], row["target"], json.loads(row["error_json"])["code"]) for row in failures
    ] == [("search", QUERY, "reddit_response_error")]
    assert sorted(row["source_thread_id"] for row in record.run_rows("watch_acquisitions")) == [
        THREAD_A,
        THREAD_B,
    ]
    assert record.result.summary["refresh"]["fetched"] == 2
    assert record.run_rows("watch_thread_revisions") == []


def test_run_watch_sweep_search_hit_with_unsafe_post_id_is_skipped_and_counted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = build_config(monkeypatch, tmp_path / ".threadsense")
    store = create_watch_store(config)
    payload = json.loads(
        (FIXTURE_DIR / "responses" / "search_initial.json").read_text(encoding="utf-8")
    )
    unsafe_hit = copy.deepcopy(payload["data"]["children"][0])
    unsafe_hit["data"]["id"] = UNSAFE_POST_ID
    unsafe_hit["data"]["permalink"] = "/r/SyncTools/comments/x/unsafe_sync_conflict/"
    payload["data"]["children"].append(unsafe_hit)
    search_path = tmp_path / "search_with_unsafe_id.json"
    search_path.write_text(json.dumps(payload), encoding="utf-8")
    initial = load_replay_phases(PHASES_PATH)[0]
    phase = replace(
        reroute(initial, SEARCH_URL_MARKER, response_path=search_path),
        fresh_post_ids=initial.fresh_post_ids | {UNSAFE_POST_ID},
    )

    record = record_phase(config, store, phase)

    recorded_ids = {
        row["source_thread_id"]
        for table in ("watch_discoveries", "watch_threads")
        for row in record.tables[table]
    }
    assert record.result.summary["discovery"]["invalid_source_ids"] == 1
    assert recorded_ids == {THREAD_A, THREAD_B, "sc1ccc"}


@pytest.fixture
def sweep_failed_on_store_invariant(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> MonitoringStore:
    config = build_config(monkeypatch, tmp_path / ".threadsense")
    store = create_watch_store(config)
    initial = load_replay_phases(PHASES_PATH)[0]

    def reject_acquisition(*args: Any, **kwargs: Any) -> None:
        raise MonitoringStateError(
            "injected store invariant failure", details={"reason": "inconsistent_changes"}
        )

    with monkeypatch.context() as patch:
        patch.setattr(MonitoringStore, "record_acquisition", reject_acquisition)
        with pytest.raises(MonitoringStateError, match="injected store invariant failure"):
            run_phase_sweep(config, store, initial)
    return store


def test_run_watch_sweep_store_invariant_failure_records_failed_sweep(
    sweep_failed_on_store_invariant: MonitoringStore,
) -> None:
    latest = sweep_failed_on_store_invariant.sweep_status(WATCH_ID)["latest_sweep"]

    assert latest["outcome"] == "failed"
    assert latest["summary"]["error"]["details"]["reason"] == "inconsistent_changes"


def test_run_watch_sweep_store_invariant_failure_records_no_fetch_failure(
    sweep_failed_on_store_invariant: MonitoringStore,
) -> None:
    tables = read_tables(sweep_failed_on_store_invariant.path)

    assert tables["watch_fetch_failures"] == []


# Crash boundaries.


@dataclass(frozen=True)
class CrashedSweep:
    config: AppConfig
    store: MonitoringStore
    initial: ReplayPhase
    orphan_artifacts: dict[str, str]


@pytest.fixture
def sweep_failed_after_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> CrashedSweep:
    config = build_config(monkeypatch, tmp_path / ".threadsense")
    store = create_watch_store(config)
    initial = load_replay_phases(PHASES_PATH)[0]

    def fail_record_acquisition(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("injected failure after artifact write")

    with monkeypatch.context() as patch:
        patch.setattr(MonitoringStore, "record_acquisition", fail_record_acquisition)
        with pytest.raises(RuntimeError, match="injected failure"):
            run_phase_sweep(config, store, initial)
    return CrashedSweep(
        config=config,
        store=store,
        initial=initial,
        orphan_artifacts=artifact_hashes(store.monitoring_root()),
    )


def test_run_watch_sweep_store_error_after_artifacts_records_failed_sweep(
    sweep_failed_after_artifacts: CrashedSweep,
) -> None:
    latest = sweep_failed_after_artifacts.store.sweep_status(WATCH_ID)["latest_sweep"]

    assert latest["outcome"] == "failed"
    assert "error" in latest["summary"]


def test_run_watch_sweep_store_error_after_artifacts_leaves_artifact_files(
    sweep_failed_after_artifacts: CrashedSweep,
) -> None:
    orphans = sweep_failed_after_artifacts.orphan_artifacts

    assert any(
        path.startswith(f"watches/{WATCH_ID}/threads/{THREAD_A}/acquisitions/")
        and path.endswith(".raw.json")
        for path in orphans
    )


def test_run_watch_sweep_retry_after_store_error_keeps_orphan_files(
    sweep_failed_after_artifacts: CrashedSweep,
) -> None:
    crashed = sweep_failed_after_artifacts
    run_phase_sweep(crashed.config, crashed.store, crashed.initial)

    after = artifact_hashes(crashed.store.monitoring_root())

    assert all(after.get(path) == digest for path, digest in crashed.orphan_artifacts.items())


def test_run_watch_sweep_retry_after_store_error_records_each_revision_once(
    sweep_failed_after_artifacts: CrashedSweep,
) -> None:
    crashed = sweep_failed_after_artifacts

    result, _ = run_phase_sweep(crashed.config, crashed.store, crashed.initial)
    tables = read_tables(crashed.store.path)

    revision_ids = [row["revision_id"] for row in tables["watch_thread_revisions"]]
    comment_keys = [
        (row["source_thread_id"], row["comment_id"]) for row in tables["watch_comments"]
    ]
    assert result.outcome == "changes"
    assert len(revision_ids) == len(set(revision_ids)) == 2
    assert len(comment_keys) == len(set(comment_keys)) == 5
    assert len(tables["watch_comment_texts"]) == 5
    assert len(tables["watch_revision_analyses"]) == 2


class ProcessDeathError(BaseException):
    """Simulates the process dying mid-sweep: nothing in the sweep may handle it."""


@dataclass(frozen=True)
class InterruptedSweep:
    config: AppConfig
    store: MonitoringStore
    initial: ReplayPhase
    dead_claim: RunClaim


@pytest.fixture
def interrupted_sweep(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> InterruptedSweep:
    config = build_config(monkeypatch, tmp_path / ".threadsense")
    store = create_watch_store(config)
    observed_at = time.time()
    store = MonitoringStore(store.path, clock=lambda: observed_at)
    initial = load_replay_phases(PHASES_PATH)[0]
    claims: list[RunClaim] = []
    original_claim_run = MonitoringStore.claim_run

    def recording_claim_run(self: MonitoringStore, *args: Any, **kwargs: Any) -> RunClaim:
        claim = original_claim_run(self, *args, **kwargs)
        claims.append(claim)
        return claim

    def die(*args: Any, **kwargs: Any) -> None:
        raise ProcessDeathError

    with monkeypatch.context() as patch:
        patch.setattr(MonitoringStore, "claim_run", recording_claim_run)
        patch.setattr(MonitoringStore, "record_acquisition", die)
        patch.setattr(MonitoringStore, "complete_sweep", die)
        with pytest.raises(ProcessDeathError):
            run_phase_sweep(config, store, initial)
    assert len(claims) == 1
    return InterruptedSweep(config=config, store=store, initial=initial, dead_claim=claims[0])


@pytest.fixture
def recovered_sweep(interrupted_sweep: InterruptedSweep) -> SweepResult:
    # A store whose clock is past the dead lease recovers it without waiting on wall time.
    recovered_at = interrupted_sweep.dead_claim.lease_expires_at_utc + 1.0
    later_store = MonitoringStore(interrupted_sweep.store.path, clock=lambda: recovered_at)
    result, _ = run_phase_sweep(interrupted_sweep.config, later_store, interrupted_sweep.initial)
    return result


def test_run_watch_sweep_hard_interruption_leaves_claim_running(
    interrupted_sweep: InterruptedSweep,
) -> None:
    runs = read_tables(interrupted_sweep.store.path)["watch_runs"]

    dead = [row for row in runs if row["run_id"] == interrupted_sweep.dead_claim.run_id]

    assert [row["status"] for row in dead] == ["running"]


def test_run_watch_sweep_during_live_dead_claim_is_refused(
    interrupted_sweep: InterruptedSweep,
) -> None:
    with pytest.raises(RunClaimError) as raised:
        run_phase_sweep(
            interrupted_sweep.config, interrupted_sweep.store, interrupted_sweep.initial
        )

    assert raised.value.details["reason"] == "claim_held"


def test_run_watch_sweep_after_dead_lease_expires_recovers_dead_run(
    interrupted_sweep: InterruptedSweep, recovered_sweep: SweepResult
) -> None:
    assert recovered_sweep.recovered_run_id == interrupted_sweep.dead_claim.run_id
    assert recovered_sweep.outcome == "changes"


def test_finalize_run_dead_run_after_recovery_raises_stale_owner(
    interrupted_sweep: InterruptedSweep, recovered_sweep: SweepResult
) -> None:
    dead = interrupted_sweep.dead_claim

    with pytest.raises(RunClaimError) as raised:
        interrupted_sweep.store.finalize_run(dead.run_id, dead.owner_token, "succeeded")

    assert raised.value.details["reason"] == "stale_owner"
    assert raised.value.details["recovered_by_run_id"] == recovered_sweep.run_id
