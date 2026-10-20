"""Cross-thread issue history over real recorded sweeps, real artifacts and real SQLite.

Replays tests/fixtures/watch/issues/ (synthesized Reddit payloads; see its README.md)
through the watch sweep and digest, then ingests the materialized finding entries into
issue history and drives the operator link and unlink commands.
"""

from __future__ import annotations

import hashlib
import logging
import re
import sqlite3
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass, replace
from functools import partial
from pathlib import Path
from typing import Any

import pytest
from recorded_reddit import (
    PhaseReplayTransport,
    ReplayPhase,
    load_replay_phases,
    replay_connector_factory,
)

from threadsense.config import AppConfig, load_config
from threadsense.errors import (
    MonitoringInputError,
    MonitoringStateError,
    RunClaimError,
    ThreadSenseError,
)
from threadsense.monitoring import (
    IssueEntryInput,
    IssueIngestion,
    IssuePolicy,
    IssueReportInput,
    build_issue_entry_input,
    ingest_issue_evidence,
    issue_policy,
    load_watch_profile_file,
    present_watch_digest,
    run_watch_sweep,
)
from threadsense.monitoring.digest import build_digest_entries
from threadsense.monitoring.issue_records import LINEAGE_MIN_CHARS, issue_id_for, report_id_for
from threadsense.monitoring.state import (
    _SCHEMA_STATEMENTS,
    STATE_SCHEMA_VERSION,
    MonitoringStore,
    monitoring_state_path,
)
from threadsense.observability import TraceContext

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "watch" / "issues"
PROFILE_PATH = FIXTURE_DIR / "profile.toml"
PHASES_PATH = FIXTURE_DIR / "phases.json"
WATCH_ID = "export-issues"
OTHER_WATCH_ID = "export-issues-copy"
THREAD_A = "ix1aaa"
THREAD_B = "ix1bbb"
THREAD_C = "ix1ccc"
THREAD_D = "ix1ddd"
THREAD_E = "ix1eee"
THREAD_F = "ix1fff"
INITIAL_THREADS = (THREAD_A, THREAD_B, THREAD_D, THREAD_E, THREAD_F)
ISSUE_A = issue_id_for(WATCH_ID, THREAD_A)
ISSUE_B = issue_id_for(WATCH_ID, THREAD_B)
ISSUE_C = issue_id_for(WATCH_ID, THREAD_C)
ISSUE_D = issue_id_for(WATCH_ID, THREAD_D)
ISSUE_F = issue_id_for(WATCH_ID, THREAD_F)
SHARED_LINK = "https://status.example.com/incidents/77"
LEASE_SECONDS = 600
PRE_ISSUE_SCHEMA_VERSION = 3
DIGEST_ISSUE_VIEW_SCHEMA_VERSION = 5
LOGGER = logging.getLogger("threadsense.tests.issue_store")


@dataclass(frozen=True)
class IssueHarness:
    config: AppConfig
    store: MonitoringStore
    phases: dict[str, ReplayPhase]


def build_harness(storage_root: Path) -> IssueHarness:
    config = load_config(
        None,
        env={
            "THREADSENSE_STORAGE_ROOT": str(storage_root),
            "THREADSENSE_REDDIT_REQUEST_DELAY": "0",
            "THREADSENSE_REDDIT_MAX_RETRIES": "0",
            "THREADSENSE_RUNTIME_ENABLED": "false",
        },
    )
    store = MonitoringStore(monitoring_state_path(config.storage))
    store.save_profile(load_watch_profile_file(PROFILE_PATH))
    phases = {phase.name: phase for phase in load_replay_phases(PHASES_PATH)}
    return IssueHarness(config=config, store=store, phases=phases)


@pytest.fixture
def harness(tmp_path: Path) -> IssueHarness:
    return build_harness(tmp_path / "store")


def sweep_phase(harness: IssueHarness, phase_name: str, *, watch_id: str) -> TraceContext:
    """Replay one recorded sweep and return its trace."""
    transport = PhaseReplayTransport(harness.phases[phase_name])
    trace = TraceContext.create(run_id=f"issues-{watch_id}-{phase_name}", source_name="watch")
    sweep = run_watch_sweep(
        config=harness.config,
        store=harness.store,
        watch_id=watch_id,
        lease_seconds=LEASE_SECONDS,
        connector_factory=replay_connector_factory(transport),
        logger=LOGGER,
        trace=trace,
    )
    assert sweep.outcome == "changes"
    return trace


def run_phase(
    harness: IssueHarness, phase_name: str, policy: IssuePolicy, *, watch_id: str = WATCH_ID
) -> IssueIngestion:
    """Replay one recorded sweep, then present a digest that ingests evidence with `policy`."""
    trace = sweep_phase(harness, phase_name, watch_id=watch_id)
    digest = present_watch_digest(
        store=harness.store,
        watch_id=watch_id,
        lease_seconds=LEASE_SECONDS,
        issue_policy=policy,
        logger=LOGGER,
        trace=trace,
    )
    return IssueIngestion(**digest.issues)


def materialize_phase(harness: IssueHarness, phase_name: str, *, watch_id: str = WATCH_ID) -> int:
    """Replay one recorded sweep and materialize its digest entries without ingesting issues."""
    sweep_phase(harness, phase_name, watch_id=watch_id)
    store = harness.store
    claim = store.claim_run(watch_id, lease_seconds=LEASE_SECONDS)
    materialized = 0
    for item in store.unmaterialized_revisions(watch_id):
        entries = build_digest_entries(watch_id, item)
        materialized += store.materialize_digest_entries(claim, entries)
    store.finalize_run(claim.run_id, claim.owner_token, "succeeded")
    return materialized


def ingest(
    harness: IssueHarness, policy: IssuePolicy, *, watch_id: str = WATCH_ID
) -> IssueIngestion:
    claim = harness.store.claim_run(watch_id, lease_seconds=LEASE_SECONDS)
    result = ingest_issue_evidence(harness.store, claim, policy)
    harness.store.finalize_run(claim.run_id, claim.owner_token, "succeeded")
    return result


def default_policy(harness: IssueHarness) -> IssuePolicy:
    return issue_policy(harness.config.issues)


def advance(harness: IssueHarness, *phase_names: str) -> list[IssueIngestion]:
    """Run each phase; its digest ingests the evidence with the configured policy."""
    return [run_phase(harness, phase_name, default_policy(harness)) for phase_name in phase_names]


def query_rows(state_path: Path, sql: str, params: tuple[Any, ...]) -> list[tuple[Any, ...]]:
    uri = f"{state_path.as_uri()}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as conn:
        return list(conn.execute(sql, params).fetchall())


def count_rows(state_path: Path, table: str) -> int:
    return int(query_rows(state_path, f"SELECT COUNT(*) FROM {table}", ())[0][0])


def finding_entry_count(state_path: Path) -> int:
    rows = query_rows(
        state_path,
        "SELECT COUNT(*) FROM watch_digest_entries WHERE watch_id = ? AND kind = 'finding'",
        (WATCH_ID,),
    )
    return int(rows[0][0])


def artifact_digests(store: MonitoringStore) -> dict[str, str]:
    """sha256 of every file under the monitoring root except the state database."""
    root = store.monitoring_root()
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file() and not path.name.startswith(store.path.name)
    }


def member_ids(view: dict[str, Any]) -> list[str]:
    return [member["source_thread_id"] for member in view["members"]]


def suggestions_between(view: dict[str, Any], first: str, second: str) -> list[dict[str, Any]]:
    return [
        suggestion
        for suggestion in view["suggestions"]
        if {suggestion["source_thread_id"], suggestion["candidate_thread_id"]} == {first, second}
    ]


def history_shape(view: dict[str, Any]) -> list[tuple[str, str, str, str, str | None]]:
    return [
        (
            event["action"],
            event["basis"],
            event["source_thread_id"],
            event["issue_id"],
            event["previous_issue_id"],
        )
        for event in view["membership_history"]
    ]


def assert_refused(
    store: MonitoringStore,
    action: Callable[[], object],
    error_type: type[ThreadSenseError],
    reason: str,
) -> dict[str, Any]:
    """The call fails with `reason` and leaves the state file and memberships untouched."""
    state_bytes = store.path.read_bytes()
    memberships = count_rows(store.path, "watch_issue_memberships")
    with pytest.raises(error_type) as caught:
        action()
    assert caught.value.details["reason"] == reason
    assert store.path.read_bytes() == state_bytes
    assert count_rows(store.path, "watch_issue_memberships") == memberships
    return caught.value.details


def test_ingest_issue_evidence_initial_phase_creates_separate_issues_with_signals(
    harness: IssueHarness,
) -> None:
    assert default_policy(harness) == IssuePolicy(30, 25, 3)
    result = run_phase(harness, "initial", default_policy(harness))

    store = harness.store
    assert result.threads_created == 5
    assert (result.lineage_assignments, result.exact_copies) == (0, 0)
    assert result.policy_sha256 == default_policy(harness).sha256()
    issues = store.list_issues(WATCH_ID)
    assert sorted(
        (issue["home_thread_id"], issue["state"], issue["member_thread_ids"]) for issue in issues
    ) == sorted((thread, "active", [thread]) for thread in INITIAL_THREADS)
    view_a = store.inspect_issue(WATCH_ID, ISSUE_A)
    view_b = store.inspect_issue(WATCH_ID, ISSUE_B)
    (pair,) = suggestions_between(view_a, THREAD_A, THREAD_B)
    assert (pair["status"], pair["refusal_reason"]) == ("unconfirmed", None)
    assert pair["signals"]["themes"] == ["reliability"]
    assert {"export crash", "markdown export"} <= set(pair["signals"]["key_phrases"])
    assert pair["currently_grouped"] is False
    (refusal,) = suggestions_between(store.inspect_issue(WATCH_ID, ISSUE_D), THREAD_D, THREAD_E)
    assert (refusal["status"], refusal["refusal_reason"]) == ("refused", "shared_link_only")
    assert refusal["signals"]["links"] == [SHARED_LINK]
    assert store.inspect_issue(WATCH_ID, ISSUE_F)["suggestions"] == []
    assert (member_ids(view_a), member_ids(view_b)) == ([THREAD_A], [THREAD_B])
    assert view_a["counts"]["firsthand_reports"] == 2
    assert view_b["counts"]["firsthand_reports"] == 2
    assert view_a["state"] == view_b["state"] == "active"


def test_record_issue_evidence_wide_window_compares_out_of_window_thread(
    harness: IssueHarness,
) -> None:
    result = run_phase(harness, "initial", IssuePolicy(365, 25, 3))

    # Every thread is compared with every earlier thread: 0 + 1 + 2 + 3 + 4.
    assert result.comparisons == 10
    view_f = harness.store.inspect_issue(WATCH_ID, ISSUE_F)
    assert view_f["grouping_policy"]["candidates"]["window_days"] == 365
    with_f = [
        suggestion
        for suggestion in view_f["suggestions"]
        if suggestion["status"] == "unconfirmed"
        and {suggestion["source_thread_id"], suggestion["candidate_thread_id"]}
        & {THREAD_A, THREAD_B}
    ]
    assert with_f
    assert all(suggestion["signals"]["themes"] == ["reliability"] for suggestion in with_f)


def test_record_issue_evidence_comparison_bound_limits_comparisons_and_suggestions(
    tmp_path: Path,
) -> None:
    unbounded = build_harness(tmp_path / "default")
    bounded = build_harness(tmp_path / "bounded")
    default_result = run_phase(unbounded, "initial", default_policy(unbounded))
    bounded_result = run_phase(bounded, "initial", IssuePolicy(30, 1, 1))

    # A, B, D and E share the 30-day window; F is outside it for every thread.
    assert default_result.comparisons == 6
    assert bounded_result.comparisons == 3
    assert bounded_result.comparisons <= bounded_result.threads_created
    assert bounded_result.suggestions >= 1
    for thread in INITIAL_THREADS:
        view = bounded.store.inspect_issue(WATCH_ID, issue_id_for(WATCH_ID, thread))
        sourced = [
            suggestion
            for suggestion in view["suggestions"]
            if suggestion["source_thread_id"] == thread and suggestion["status"] == "unconfirmed"
        ]
        assert len(sourced) <= 1


def test_record_issue_evidence_repost_assigns_exact_copy_to_origin_issue(
    harness: IssueHarness,
) -> None:
    (_, repost) = advance(harness, "initial", "repost")

    store = harness.store
    assert (repost.threads_created, repost.lineage_assignments, repost.exact_copies) == (1, 1, 1)
    view_a = store.inspect_issue(WATCH_ID, ISSUE_A)
    reports = {report["comment_id"]: report for report in view_a["reports"]}
    assert set(reports) == {"reddit:ia1", "reddit:ia2", "reddit:ic1"}
    origin_id = report_id_for(WATCH_ID, THREAD_A, "reddit:ia1")
    assert (reports["reddit:ia1"]["report_id"], reports["reddit:ia1"]["lineage"]) == (
        origin_id,
        "firsthand",
    )
    copy = reports["reddit:ic1"]
    assert (copy["source_thread_id"], copy["lineage"]) == (THREAD_C, "exact_copy")
    assert copy["copy_of_report_id"] == origin_id
    assert [(member["source_thread_id"], member["basis"]) for member in view_a["members"]] == [
        (THREAD_A, "thread_seed"),
        (THREAD_C, "exact_content_lineage"),
    ]
    counts = view_a["counts"]
    expected_counts = {
        "observed_threads": 2,
        "observed_communities": 2,
        "observed_accounts": 3,
        "firsthand_reports": 2,
        "exact_copies": 1,
        "reports": 3,
    }
    assert {key: counts[key] for key in expected_counts} == expected_counts
    assert counts["independence"]["status"] == "not_verified"
    assert counts["independence"]["verified_independent_reports"] is None
    view_c = store.inspect_issue(WATCH_ID, ISSUE_C)
    assert (view_c["state"], member_ids(view_c)) == ("inactive", [])
    (c_to_b,) = suggestions_between(view_a, THREAD_C, THREAD_B)
    assert c_to_b["status"] == "unconfirmed"


def test_record_issue_evidence_original_and_repost_in_one_batch_records_origin_first(
    harness: IssueHarness,
) -> None:
    materialize_phase(harness, "initial")
    materialize_phase(harness, "repost")

    result = ingest(harness, default_policy(harness))

    assert (result.threads_created, result.lineage_assignments, result.exact_copies) == (6, 1, 1)
    view_a = harness.store.inspect_issue(WATCH_ID, ISSUE_A)
    reports = {report["comment_id"]: report for report in view_a["reports"]}
    assert reports["reddit:ia1"]["lineage"] == "firsthand"
    assert (reports["reddit:ic1"]["lineage"], reports["reddit:ic1"]["copy_of_report_id"]) == (
        "exact_copy",
        report_id_for(WATCH_ID, THREAD_A, "reddit:ia1"),
    )
    assert [(member["source_thread_id"], member["basis"]) for member in view_a["members"]] == [
        (THREAD_A, "thread_seed"),
        (THREAD_C, "exact_content_lineage"),
    ]


def repost_inputs_with_extra_report(
    harness: IssueHarness, extra: Callable[[IssueReportInput], IssueReportInput]
) -> list[IssueEntryInput]:
    """Pending repost-phase inputs with one synthetic report appended to C's entry."""
    inputs = [
        build_issue_entry_input(pending)
        for pending in harness.store.pending_issue_entries(WATCH_ID)
    ]
    (c_index,) = [index for index, entry in enumerate(inputs) if entry.source_thread_id == THREAD_C]
    entry_c = inputs[c_index]
    (copy,) = [report for report in entry_c.reports if report.comment_id == "reddit:ic1"]
    inputs[c_index] = replace(entry_c, reports=(*entry_c.reports, extra(copy)))
    return inputs


def record_inputs(harness: IssueHarness, inputs: list[IssueEntryInput]) -> IssueIngestion:
    claim = harness.store.claim_run(WATCH_ID, lease_seconds=LEASE_SECONDS)
    result = harness.store.record_issue_evidence(claim, inputs, default_policy(harness))
    harness.store.finalize_run(claim.run_id, claim.owner_token, "succeeded")
    return result


def test_record_issue_evidence_duplicate_second_entry_rolls_back_first_pending_write(
    tmp_path: Path,
) -> None:
    harness = build_harness(tmp_path / "store")
    materialize_phase(harness, "initial")
    store = harness.store
    inputs = sorted(
        (build_issue_entry_input(pending) for pending in store.pending_issue_entries(WATCH_ID)),
        key=lambda entry: (
            min(report.source_created_utc for report in entry.reports),
            entry.entry_id,
        ),
    )
    first, duplicate = inputs[0], inputs[-1]
    assert first.entry_id != duplicate.entry_id
    # The recorder sorts by earliest report time, then entry ID. Preserve that order
    # so the pending entry is inserted before the second entry's duplicate refusal.
    assert (
        min(report.source_created_utc for report in first.reports),
        first.entry_id,
    ) < (
        min(report.source_created_utc for report in duplicate.reports),
        duplicate.entry_id,
    )
    seeded = record_inputs(harness, [duplicate])
    assert seeded.entries == 1
    assert count_rows(store.path, "watch_issue_entries") == 1
    pending_before = store.pending_issue_entries(WATCH_ID)
    pending_ids = {entry.entry_id for entry in pending_before}
    assert first.entry_id in pending_ids
    assert duplicate.entry_id not in pending_ids
    claim = store.claim_run(WATCH_ID, lease_seconds=LEASE_SECONDS)
    tables = [
        str(row[0])
        for row in query_rows(
            store.path,
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND (name LIKE 'watch_issue%' OR name IN "
            "('watch_digest_entries', 'watch_digest_issue_views')) ORDER BY name",
            (),
        )
    ]
    rows_before = {
        table: sorted(query_rows(store.path, f"SELECT * FROM {table}", ()), key=repr)
        for table in tables
    }

    with pytest.raises(MonitoringStateError) as caught:
        store.record_issue_evidence(claim, [first, duplicate], default_policy(harness))

    assert caught.value.details["reason"] == "issue_entry_already_recorded"
    assert caught.value.details["entry_id"] == duplicate.entry_id
    assert {
        table: sorted(query_rows(store.path, f"SELECT * FROM {table}", ()), key=repr)
        for table in tables
    } == rows_before
    assert store.pending_issue_entries(WATCH_ID) == pending_before
    assert count_rows(store.path, "watch_issue_entries") == 1


def test_record_issue_evidence_invalid_report_text_hash_preserves_pending_state(
    tmp_path: Path,
) -> None:
    harness = build_harness(tmp_path / "store")
    materialize_phase(harness, "initial")
    store = harness.store
    pending_before = store.pending_issue_entries(WATCH_ID)
    entry = build_issue_entry_input(pending_before[0])
    assert entry.reports
    invalid = replace(
        entry,
        reports=(replace(entry.reports[0], text_sha256="x"), *entry.reports[1:]),
    )
    claim = store.claim_run(WATCH_ID, lease_seconds=LEASE_SECONDS)
    tables = [
        str(row[0])
        for row in query_rows(
            store.path,
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND (name LIKE 'watch_issue%' OR name IN "
            "('watch_digest_entries', 'watch_digest_issue_views')) ORDER BY name",
            (),
        )
    ]
    rows_before = {
        table: sorted(query_rows(store.path, f"SELECT * FROM {table}", ()), key=repr)
        for table in tables
    }

    with pytest.raises(MonitoringInputError) as caught:
        store.record_issue_evidence(claim, [invalid], default_policy(harness))

    assert caught.value.details["reason"] == "invalid_issue_input"
    assert caught.value.details["field"] == "text_sha256"
    assert caught.value.details["entry_id"] == entry.entry_id
    assert {
        table: sorted(query_rows(store.path, f"SELECT * FROM {table}", ()), key=repr)
        for table in tables
    } == rows_before
    assert store.pending_issue_entries(WATCH_ID) == pending_before
    assert count_rows(store.path, "watch_issue_entries") == 0


def test_record_issue_evidence_partial_copy_seeds_own_issue_with_copy_signal(
    harness: IssueHarness,
) -> None:
    advance(harness, "initial")
    counts_a = harness.store.inspect_issue(WATCH_ID, ISSUE_A)["counts"]
    materialize_phase(harness, "repost")
    inputs = repost_inputs_with_extra_report(
        harness,
        lambda copy: replace(
            copy,
            comment_id="reddit:icx",
            text_sha256=hashlib.sha256(b"reposter adds a firsthand note").hexdigest(),
            content_chars=80,
            source_created_utc=copy.source_created_utc + 60,
            links=(),
            cues=(),
        ),
    )

    result = record_inputs(harness, inputs)

    assert (result.threads_created, result.lineage_assignments, result.exact_copies) == (1, 0, 1)
    view_c = harness.store.inspect_issue(WATCH_ID, ISSUE_C)
    assert view_c["state"] == "active"
    assert [(member["source_thread_id"], member["basis"]) for member in view_c["members"]] == [
        (THREAD_C, "thread_seed")
    ]
    (c_to_a,) = suggestions_between(view_c, THREAD_C, THREAD_A)
    assert c_to_a["status"] == "unconfirmed"
    assert report_id_for(WATCH_ID, THREAD_C, "reddit:ic1") in c_to_a["signals"]["exact_copies"]
    assert harness.store.inspect_issue(WATCH_ID, ISSUE_A)["counts"] == counts_a


def test_record_issue_evidence_short_matching_text_stays_firsthand(
    harness: IssueHarness,
) -> None:
    advance(harness, "initial")
    ((origin_text,),) = query_rows(
        harness.store.path,
        "SELECT first_text_sha256 FROM watch_issue_reports WHERE report_id = ?",
        (report_id_for(WATCH_ID, THREAD_A, "reddit:ia1"),),
    )
    materialize_phase(harness, "repost")
    inputs = repost_inputs_with_extra_report(
        harness,
        lambda copy: replace(
            copy,
            comment_id="reddit:icy",
            text_sha256=str(origin_text),
            content_chars=LINEAGE_MIN_CHARS - 1,
            source_created_utc=copy.source_created_utc + 60,
            links=(),
            cues=(),
        ),
    )

    result = record_inputs(harness, inputs)

    assert result.exact_copies == 1
    view_c = harness.store.inspect_issue(WATCH_ID, ISSUE_C)
    reports = {report["comment_id"]: report for report in view_c["reports"]}
    assert (reports["reddit:icy"]["lineage"], reports["reddit:icy"]["copy_of_report_id"]) == (
        "firsthand",
        None,
    )
    assert member_ids(view_c) == [THREAD_C]


def test_record_issue_evidence_suggestion_cap_records_truncated_count(
    harness: IssueHarness,
) -> None:
    result = run_phase(harness, "initial", IssuePolicy(365, 25, 1))

    assert result.suggestions_truncated >= 1
    for thread in INITIAL_THREADS:
        view = harness.store.inspect_issue(WATCH_ID, issue_id_for(WATCH_ID, thread))
        sourced = [
            suggestion
            for suggestion in view["suggestions"]
            if suggestion["source_thread_id"] == thread and suggestion["status"] == "unconfirmed"
        ]
        assert len(sourced) <= 1


def test_record_issue_evidence_empty_batch_stale_claim_raises_run_claim_error(
    harness: IssueHarness,
) -> None:
    advance(harness, "initial")
    store = harness.store
    stale = store.claim_run(WATCH_ID, lease_seconds=LEASE_SECONDS)
    store.finalize_run(stale.run_id, stale.owner_token, "succeeded")
    ingestions = count_rows(store.path, "watch_issue_ingestions")

    with pytest.raises(RunClaimError):
        store.record_issue_evidence(stale, [], default_policy(harness))

    assert count_rows(store.path, "watch_issue_ingestions") == ingestions


def test_list_and_inspect_issue_unknown_watch_raise_unknown_watch(
    harness: IssueHarness,
) -> None:
    advance(harness, "initial")
    unknown = "export-issues-unknown"
    store = harness.store
    state_bytes = store.path.read_bytes()

    for action in (
        lambda: store.list_issues(unknown),
        lambda: store.inspect_issue(unknown, issue_id_for(unknown, THREAD_A)),
    ):
        with pytest.raises(MonitoringStateError) as caught:
            action()
        assert caught.value.details["reason"] == "unknown_watch"

    assert store.path.read_bytes() == state_bytes


def test_list_and_inspect_issue_missing_state_raise_unknown_watch(tmp_path: Path) -> None:
    store = MonitoringStore(tmp_path / "absent" / "monitoring.sqlite3")

    for action in (
        lambda: store.list_issues(WATCH_ID),
        lambda: store.inspect_issue(WATCH_ID, ISSUE_A),
    ):
        with pytest.raises(MonitoringStateError) as caught:
            action()
        assert caught.value.details["reason"] == "unknown_watch"

    assert not store.path.exists()


def test_record_issue_evidence_updates_phase_records_unverified_update_claims(
    harness: IssueHarness,
) -> None:
    advance(harness, "initial", "repost", "updates")

    view_a = harness.store.inspect_issue(WATCH_ID, ISSUE_A)

    assert view_a["update_counts"] == {
        "reproduction": 1,
        "contradiction": 1,
        "workaround": 1,
        "resolution_claim": 1,
    }
    assert sorted(
        (update["comment_id"], update["kind"], update["cue"]) for update in view_a["updates"]
    ) == [
        ("reddit:ia3", "reproduction", "steps to reproduce"),
        ("reddit:ia4", "contradiction", "cannot reproduce"),
        ("reddit:ia5", "workaround", "workaround"),
        ("reddit:ia6", "resolution_claim", "fixed in"),
    ]
    assert all(
        update["report_id"] == report_id_for(WATCH_ID, THREAD_A, update["comment_id"])
        for update in view_a["updates"]
    )
    assert view_a["status"] == "open"
    assert view_a["resolution"]["resolved"] is False
    assert view_a["resolution"]["resolution_claims"] == 1
    assert view_a["counts"]["firsthand_reports"] == 6
    assert view_a["counts"]["exact_copies"] == 1
    assert view_a["counts"]["observed_accounts"] == 5


def test_link_issue_thread_link_then_unlink_moves_counts_and_restores_home(
    harness: IssueHarness,
) -> None:
    advance(harness, "initial", "repost", "updates")
    store = harness.store

    linked = store.link_issue_thread(WATCH_ID, ISSUE_A, THREAD_B, "  same markdown export crash  ")

    assert linked["membership"] == {
        "source_thread_id": THREAD_B,
        "issue_id": ISSUE_A,
        "home_issue_id": ISSUE_B,
    }
    event = linked["event"]
    assert (event["action"], event["basis"], event["previous_issue_id"]) == (
        "linked",
        "operator",
        ISSUE_B,
    )
    assert event["reason"] == "same markdown export crash"
    assert (event["report_id"], event["policy_sha256"], event["run_id"]) == (None, None, None)
    after_link = store.inspect_issue(WATCH_ID, ISSUE_A)
    assert member_ids(after_link) == [THREAD_A, THREAD_C, THREAD_B]
    assert after_link["counts"]["observed_threads"] == 3
    assert after_link["counts"]["firsthand_reports"] == 8
    assert store.inspect_issue(WATCH_ID, ISSUE_B)["state"] == "inactive"

    unlinked = store.unlink_issue_thread(WATCH_ID, ISSUE_A, THREAD_B, "different root cause")

    assert unlinked["membership"] == {
        "source_thread_id": THREAD_B,
        "issue_id": ISSUE_B,
        "home_issue_id": ISSUE_B,
    }
    after_unlink = store.inspect_issue(WATCH_ID, ISSUE_A)
    assert member_ids(after_unlink) == [THREAD_A, THREAD_C]
    assert after_unlink["counts"]["observed_threads"] == 2
    assert after_unlink["counts"]["firsthand_reports"] == 6
    view_b = store.inspect_issue(WATCH_ID, ISSUE_B)
    assert (view_b["state"], member_ids(view_b)) == ("active", [THREAD_B])
    assert history_shape(after_unlink) == [
        ("assigned", "thread_seed", THREAD_A, ISSUE_A, None),
        ("assigned", "exact_content_lineage", THREAD_C, ISSUE_A, None),
        ("linked", "operator", THREAD_B, ISSUE_A, ISSUE_B),
        ("unlinked", "operator", THREAD_B, ISSUE_B, ISSUE_A),
    ]

    lineage_unlinked = store.unlink_issue_thread(WATCH_ID, ISSUE_A, THREAD_C, "tracked alone")

    assert lineage_unlinked["membership"]["issue_id"] == ISSUE_C
    view_c = store.inspect_issue(WATCH_ID, ISSUE_C)
    assert (view_c["state"], member_ids(view_c)) == ("active", [THREAD_C])
    assert member_ids(store.inspect_issue(WATCH_ID, ISSUE_A)) == [THREAD_A]


def test_link_issue_thread_invalid_or_conflicting_requests_refused_without_writes(
    harness: IssueHarness,
) -> None:
    advance(harness, "initial", "repost")
    store = harness.store
    link = partial(store.link_issue_thread, WATCH_ID)
    unlink = partial(store.unlink_issue_thread, WATCH_ID)
    invalid_requests: list[Callable[[], object]] = [
        partial(link, "iss-xyz", THREAD_B, "reason"),
        partial(link, ISSUE_A, THREAD_B, ""),
        partial(link, ISSUE_A, THREAD_B, "   "),
        partial(link, ISSUE_A, THREAD_B, "bad\x07reason"),
        partial(unlink, ISSUE_A, THREAD_C, "bad\x7freason"),
    ]
    state_refusals: list[tuple[Callable[[], object], str]] = [
        (partial(link, "iss-" + "0" * 32, THREAD_B, "reason"), "unknown_issue"),
        (partial(link, ISSUE_A, "zz9999", "reason"), "unknown_issue_thread"),
        (partial(link, ISSUE_A, THREAD_C, "reason"), "already_member"),
        (partial(link, ISSUE_C, THREAD_B, "reason"), "inactive_issue"),
        (partial(unlink, ISSUE_A, THREAD_B, "reason"), "not_member"),
        (partial(unlink, ISSUE_A, THREAD_A, "reason"), "not_linked"),
    ]

    for action in invalid_requests:
        assert_refused(store, action, MonitoringInputError, "invalid_issue_request")
    for action, reason in state_refusals:
        assert_refused(store, action, MonitoringStateError, reason)
    details = assert_refused(
        store,
        partial(link, ISSUE_B, THREAD_A, "reason"),
        MonitoringStateError,
        "issue_has_other_members",
    )

    assert THREAD_C in details["member_thread_ids"]
    assert member_ids(store.inspect_issue(WATCH_ID, ISSUE_A)) == [THREAD_A, THREAD_C]


def test_link_issue_thread_other_watch_issue_refused_as_unknown(
    harness: IssueHarness, tmp_path: Path
) -> None:
    advance(harness, "initial")
    other_profile = tmp_path / "other_profile.toml"
    other_profile.write_text(
        PROFILE_PATH.read_text(encoding="utf-8").replace(
            f'id = "{WATCH_ID}"', f'id = "{OTHER_WATCH_ID}"'
        ),
        encoding="utf-8",
    )
    harness.store.save_profile(load_watch_profile_file(other_profile))
    run_phase(harness, "initial", default_policy(harness), watch_id=OTHER_WATCH_ID)
    other_issue = issue_id_for(OTHER_WATCH_ID, THREAD_A)
    assert harness.store.inspect_issue(OTHER_WATCH_ID, other_issue)["home_thread_id"] == THREAD_A

    assert_refused(
        harness.store,
        partial(harness.store.link_issue_thread, WATCH_ID, other_issue, THREAD_B, "reason"),
        MonitoringStateError,
        "unknown_issue",
    )
    assert_refused(
        harness.store,
        partial(harness.store.inspect_issue, WATCH_ID, other_issue),
        MonitoringStateError,
        "unknown_issue",
    )


def test_issue_tables_raw_update_or_delete_aborted_as_append_only(
    harness: IssueHarness,
) -> None:
    advance(harness, "initial", "repost")
    before = harness.store.inspect_issue(WATCH_ID, ISSUE_A)

    with closing(sqlite3.connect(harness.store.path)) as conn:
        with pytest.raises(sqlite3.DatabaseError, match="append-only"):
            conn.execute("UPDATE watch_issue_memberships SET reason = 'x'")
        with pytest.raises(sqlite3.DatabaseError, match="append-only"):
            conn.execute("DELETE FROM watch_issue_reports")
        conn.rollback()

    assert harness.store.inspect_issue(WATCH_ID, ISSUE_A) == before


def test_link_issue_thread_link_and_unlink_leave_prior_artifacts_unchanged(
    harness: IssueHarness,
) -> None:
    advance(harness, "initial", "repost")
    before = artifact_digests(harness.store)
    assert before

    harness.store.link_issue_thread(WATCH_ID, ISSUE_A, THREAD_B, "same exporter crash")
    harness.store.unlink_issue_thread(WATCH_ID, ISSUE_A, THREAD_B, "different root cause")

    assert artifact_digests(harness.store) == before


def test_ingest_issue_evidence_repeat_and_stale_claim_record_nothing(
    harness: IssueHarness,
) -> None:
    advance(harness, "initial")
    store = harness.store
    ingestions = count_rows(store.path, "watch_issue_ingestions")

    repeat = ingest(harness, default_policy(harness))

    assert repeat == IssueIngestion(
        run_id=repeat.run_id,
        policy_sha256=None,
        entries=0,
        threads_created=0,
        lineage_assignments=0,
        reports_recorded=0,
        exact_copies=0,
        evidence_recorded=0,
        updates_recorded=0,
        comparisons=0,
        suggestions=0,
        suggestions_truncated=0,
        refusals=0,
    )
    assert count_rows(store.path, "watch_issue_ingestions") == ingestions

    materialize_phase(harness, "repost")
    pending = len(store.pending_issue_entries(WATCH_ID))
    assert pending >= 1
    reports = count_rows(store.path, "watch_issue_reports")
    stale = store.claim_run(WATCH_ID, lease_seconds=LEASE_SECONDS)
    store.finalize_run(stale.run_id, stale.owner_token, "succeeded")

    with pytest.raises(RunClaimError):
        ingest_issue_evidence(store, stale, default_policy(harness))

    assert len(store.pending_issue_entries(WATCH_ID)) == pending
    assert count_rows(store.path, "watch_issue_reports") == reports
    assert count_rows(store.path, "watch_issue_ingestions") == ingestions
    assert ingest(harness, default_policy(harness)).entries == pending


def test_monitoring_store_pre_issue_schema_upgrades_and_ingests_prior_entries(
    harness: IssueHarness,
) -> None:
    run_phase(harness, "initial", default_policy(harness))
    store = harness.store
    kept_tables = ("watch_thread_revisions", "watch_digest_entries", "watch_digests")
    kept_before = {
        table: sorted(query_rows(store.path, f"SELECT * FROM {table}", ()), key=repr)
        for table in kept_tables
    }
    with closing(sqlite3.connect(store.path)) as conn:
        objects = conn.execute(
            "SELECT type, name FROM sqlite_master "
            "WHERE name GLOB 'watch_issue*' AND type IN ('trigger', 'table') "
            "ORDER BY type = 'table', name"
        ).fetchall()
        assert ("table", "watch_issues") in objects
        for kind, name in objects:
            conn.execute(f'DROP {kind.upper()} "{name}"')
        assert conn.execute("SELECT COUNT(*) FROM watch_digest_issue_views").fetchone() == (1,)
        conn.execute("DROP TABLE watch_digest_issue_views")
        # Tables of versions after the issue-view version did not exist in a pre-issue state.
        for schema_version in range(DIGEST_ISSUE_VIEW_SCHEMA_VERSION + 1, STATE_SCHEMA_VERSION + 1):
            for statement in _SCHEMA_STATEMENTS[schema_version]:
                created = re.match(r"\s*CREATE TABLE (\w+)", statement)
                if created is not None:
                    conn.execute(f"DROP TABLE {created.group(1)}")
        conn.execute(f"PRAGMA user_version = {PRE_ISSUE_SCHEMA_VERSION}")
        conn.commit()
    pre_issue_bytes = store.path.read_bytes()

    assert store.list_issues(WATCH_ID) == []
    assert store.path.read_bytes() == pre_issue_bytes

    claim = store.claim_run(WATCH_ID, lease_seconds=LEASE_SECONDS)

    assert query_rows(store.path, "PRAGMA user_version", ()) == [(STATE_SCHEMA_VERSION,)]
    assert STATE_SCHEMA_VERSION >= DIGEST_ISSUE_VIEW_SCHEMA_VERSION
    assert count_rows(store.path, "watch_digest_issue_views") == 0
    assert query_rows(
        store.path,
        "SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ? ORDER BY name",
        ("watch_digest_issue_views",),
    ) == [("watch_digest_issue_views_no_delete",), ("watch_digest_issue_views_no_update",)]
    assert {
        table: sorted(query_rows(store.path, f"SELECT * FROM {table}", ()), key=repr)
        for table in kept_tables
    } == kept_before
    findings = finding_entry_count(store.path)
    assert findings >= 5
    assert len(store.pending_issue_entries(WATCH_ID)) == findings
    result = ingest_issue_evidence(store, claim, default_policy(harness))
    store.finalize_run(claim.run_id, claim.owner_token, "succeeded")
    assert (result.entries, result.threads_created) == (findings, 5)
    assert store.pending_issue_entries(WATCH_ID) == []
    assert len(store.list_issues(WATCH_ID)) == 5
