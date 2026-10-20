"""Late-discovered exact-copy lineage over real recorded sweeps, artifacts and SQLite.

Replays tests/fixtures/watch/issues/ through the watch sweep, materializes its finding
digest entries with the store, and records issue evidence in a chosen batch order so a
repost is recorded before the earlier-posted original it copies.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from collections.abc import Callable
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
)

from threadsense.config import AppConfig, load_config
from threadsense.errors import MonitoringStateError
from threadsense.monitoring import (
    IssueEntryInput,
    IssueIngestion,
    IssueReportInput,
    build_issue_entry_input,
    issue_policy,
    load_watch_profile_file,
    run_watch_sweep,
)
from threadsense.monitoring.digest import build_digest_entries
from threadsense.monitoring.issue_corrections import LINEAGE_CORRECTIONS_KEY, LINEAGE_EVENTS_KEY
from threadsense.monitoring.issue_records import LINEAGE_MIN_CHARS, issue_id_for, report_id_for
from threadsense.monitoring.state import MonitoringStore, monitoring_state_path
from threadsense.observability import TraceContext

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "watch" / "issues"
PROFILE_PATH = FIXTURE_DIR / "profile.toml"
PHASES_PATH = FIXTURE_DIR / "phases.json"
WATCH_ID = "export-issues"
OTHER_WATCH_ID = "export-issues-copy"
THREAD_A = "ix1aaa"
THREAD_B = "ix1bbb"
THREAD_C = "ix1ccc"
THREAD_F = "ix1fff"
ISSUE_A = issue_id_for(WATCH_ID, THREAD_A)
ISSUE_C = issue_id_for(WATCH_ID, THREAD_C)
ISSUE_F = issue_id_for(WATCH_ID, THREAD_F)
IA1 = "reddit:ia1"
IC1 = "reddit:ic1"
LEASE_SECONDS = 600
UNCORRECTED_FIRSTHAND = ("firsthand", None, "firsthand", None)
LOGGER = logging.getLogger("threadsense.tests.issue_lineage_corrections")
ISSUE_TABLES = (
    "watch_issue_policies",
    "watch_issues",
    "watch_issue_entries",
    "watch_issue_reports",
    "watch_issue_evidence",
    "watch_issue_updates",
    "watch_issue_memberships",
    "watch_issue_suggestions",
    "watch_issue_ingestions",
)


@dataclass(frozen=True)
class Harness:
    config: AppConfig
    store: MonitoringStore
    phases: dict[str, ReplayPhase]


def build_harness(storage_root: Path) -> Harness:
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
    return Harness(config=config, store=store, phases=phases)


@pytest.fixture
def harness(tmp_path: Path) -> Harness:
    return build_harness(tmp_path / "store")


def add_other_watch(harness: Harness, tmp_path: Path) -> None:
    """Save the same profile under a second watch id."""
    other_profile = tmp_path / "other_profile.toml"
    other_profile.write_text(
        PROFILE_PATH.read_text(encoding="utf-8").replace(
            f'id = "{WATCH_ID}"', f'id = "{OTHER_WATCH_ID}"'
        ),
        encoding="utf-8",
    )
    harness.store.save_profile(load_watch_profile_file(other_profile))


def sweep(harness: Harness, phase_name: str, *, watch_id: str = WATCH_ID) -> None:
    """Replay one recorded sweep, then materialize its finding digest entries."""
    trace = TraceContext.create(run_id=f"lineage-{watch_id}-{phase_name}", source_name="watch")
    result = run_watch_sweep(
        config=harness.config,
        store=harness.store,
        watch_id=watch_id,
        lease_seconds=LEASE_SECONDS,
        connector_factory=replay_connector_factory(
            PhaseReplayTransport(harness.phases[phase_name])
        ),
        logger=LOGGER,
        trace=trace,
    )
    assert result.outcome == "changes"
    store = harness.store
    claim = store.claim_run(watch_id, lease_seconds=LEASE_SECONDS)
    for item in store.unmaterialized_revisions(watch_id):
        store.materialize_digest_entries(claim, build_digest_entries(watch_id, item))
    store.finalize_run(claim.run_id, claim.owner_token, "succeeded")


def pending_inputs(harness: Harness, watch_id: str = WATCH_ID) -> list[IssueEntryInput]:
    return [build_issue_entry_input(item) for item in harness.store.pending_issue_entries(watch_id)]


def record(
    harness: Harness, inputs: list[IssueEntryInput], *, watch_id: str = WATCH_ID
) -> IssueIngestion:
    store = harness.store
    claim = store.claim_run(watch_id, lease_seconds=LEASE_SECONDS)
    result = store.record_issue_evidence(claim, inputs, issue_policy(harness.config.issues))
    store.finalize_run(claim.run_id, claim.owner_token, "succeeded")
    return result


def only(inputs: list[IssueEntryInput], thread_id: str) -> list[IssueEntryInput]:
    return [entry for entry in inputs if entry.source_thread_id == thread_id]


def without(inputs: list[IssueEntryInput], thread_id: str) -> list[IssueEntryInput]:
    return [entry for entry in inputs if entry.source_thread_id != thread_id]


def entry_citing(inputs: list[IssueEntryInput], comment_id: str) -> IssueEntryInput:
    (entry,) = [e for e in inputs if any(r.comment_id == comment_id for r in e.reports)]
    return entry


def report_in(inputs: list[IssueEntryInput], comment_id: str) -> IssueReportInput:
    (report,) = [r for r in entry_citing(inputs, comment_id).reports if r.comment_id == comment_id]
    return report


def with_extra(
    inputs: list[IssueEntryInput], thread_id: str, *extras: IssueReportInput
) -> list[IssueEntryInput]:
    """The inputs with synthetic reports appended to the thread's first pending entry."""
    index = next(i for i, entry in enumerate(inputs) if entry.source_thread_id == thread_id)
    changed = list(inputs)
    changed[index] = replace(inputs[index], reports=(*inputs[index].reports, *extras))
    return changed


def copy_of(
    template: IssueReportInput, origin: IssueReportInput, comment_id: str, posted: float
) -> IssueReportInput:
    """A report carrying exactly the origin's eligible text, posted at `posted`."""
    return replace(
        template,
        comment_id=comment_id,
        text_sha256=origin.text_sha256,
        content_chars=origin.content_chars,
        source_created_utc=posted,
        links=(),
        cues=(),
    )


def rid(thread_id: str, comment_id: str, watch_id: str = WATCH_ID) -> str:
    return report_id_for(watch_id, thread_id, comment_id)


def query(path: Path, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    with closing(sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)) as conn:
        return list(conn.execute(sql, params).fetchall())


def table_counts(path: Path) -> dict[str, int]:
    return {table: len(query(path, f"SELECT rowid FROM {table}")) for table in ISSUE_TABLES}


def report_row(store: MonitoringStore, report_id: str) -> list[tuple[Any, ...]]:
    return query(store.path, "SELECT * FROM watch_issue_reports WHERE report_id = ?", (report_id,))


def all_rows(store: MonitoringStore, table: str, order: str) -> list[tuple[Any, ...]]:
    return query(store.path, f"SELECT * FROM {table} ORDER BY {order}")


def ledger(store: MonitoringStore, watch_id: str = WATCH_ID) -> list[tuple[Any, ...]]:
    return query(
        store.path,
        "SELECT run_id, summary_json FROM watch_issue_ingestions WHERE watch_id = ? ORDER BY rowid",
        (watch_id,),
    )


def summary_of(store: MonitoringStore, run_id: str) -> dict[str, Any]:
    ((summary_json,),) = query(
        store.path, "SELECT summary_json FROM watch_issue_ingestions WHERE run_id = ?", (run_id,)
    )
    summary: dict[str, Any] = json.loads(str(summary_json))
    return summary


def events_of(store: MonitoringStore, run_id: str) -> list[dict[str, Any]]:
    summary = summary_of(store, run_id)
    assert summary[LINEAGE_CORRECTIONS_KEY] == len(summary[LINEAGE_EVENTS_KEY])
    events: list[dict[str, Any]] = summary[LINEAGE_EVENTS_KEY]
    return events


def report_view(
    store: MonitoringStore, issue_id: str, comment_id: str, watch_id: str = WATCH_ID
) -> dict[str, Any]:
    reports = store.inspect_issue(watch_id, issue_id)["reports"]
    (report,) = [report for report in reports if report["comment_id"] == comment_id]
    return dict(report)


def lineage_of(report: dict[str, Any]) -> tuple[Any, ...]:
    return (
        report["lineage"],
        report["copy_of_report_id"],
        report["recorded_lineage"],
        report["recorded_copy_of_report_id"],
    )


def member_ids(view: dict[str, Any]) -> list[str]:
    return [member["source_thread_id"] for member in view["members"]]


def expected_event(
    result: IssueIngestion,
    entry: IssueEntryInput,
    discovered: IssueReportInput,
    *,
    report_id: str,
    origin_report_id: str,
    previous: str | None,
) -> dict[str, Any]:
    return {
        "watch_id": WATCH_ID,
        "run_id": result.run_id,
        "policy_sha256": result.policy_sha256,
        "report_id": report_id,
        "origin_report_id": origin_report_id,
        "previous_origin_report_id": previous,
        "text_sha256": discovered.text_sha256,
        "discovered_report_id": rid(entry.source_thread_id, discovered.comment_id),
        "discovery_entry_id": entry.entry_id,
    }


def assert_unreadable(action: Callable[[], object]) -> None:
    with pytest.raises(MonitoringStateError) as caught:
        action()
    assert caught.value.details["reason"] == "issue_state_unreadable"


def c_before_a(
    harness: Harness,
) -> tuple[list[IssueEntryInput], IssueIngestion, IssueIngestion]:
    """Record repost C alone, then A (and the other initial threads) in a later ingestion."""
    sweep(harness, "initial")
    sweep(harness, "repost")
    inputs = pending_inputs(harness)
    first = record(harness, only(inputs, THREAD_C))
    second = record(harness, without(inputs, THREAD_C))
    return inputs, first, second


def test_record_issue_evidence_repost_recorded_before_original_corrects_lineage_and_counts(
    harness: Harness, tmp_path: Path
) -> None:
    # Arrange
    store = harness.store
    sweep(harness, "initial")
    sweep(harness, "repost")
    inputs = pending_inputs(harness)
    ia1, ic1 = report_in(inputs, IA1), report_in(inputs, IC1)
    assert ic1.text_sha256 == ia1.text_sha256
    assert ia1.source_created_utc < ic1.source_created_utc
    assert ia1.content_chars >= LINEAGE_MIN_CHARS
    first = record(harness, only(inputs, THREAD_C))
    assert first.lineage_corrections == 0
    copy_id, origin_id = rid(THREAD_C, IC1), rid(THREAD_A, IA1)
    c_row = report_row(store, copy_id)
    evidence = all_rows(store, "watch_issue_evidence", "rowid")
    memberships = all_rows(store, "watch_issue_memberships", "event_id")
    earlier_ledger = ledger(store)

    # Act
    second = record(harness, without(inputs, THREAD_C))

    # Assert
    assert second.lineage_corrections == 1
    assert second.to_dict()["lineage_corrections"] == 1
    event = expected_event(
        second,
        entry_citing(inputs, IA1),
        ia1,
        report_id=copy_id,
        origin_report_id=origin_id,
        previous=None,
    )
    assert events_of(store, second.run_id) == [event]
    assert ledger(store)[: len(earlier_ledger)] == earlier_ledger
    assert report_row(store, copy_id) == c_row
    assert all_rows(store, "watch_issue_evidence", "rowid")[: len(evidence)] == evidence
    after = all_rows(store, "watch_issue_memberships", "event_id")
    assert after[: len(memberships)] == memberships
    regrouped = query(
        store.path,
        "SELECT event_id FROM watch_issue_memberships WHERE run_id = ? AND source_thread_id = ?",
        (second.run_id, THREAD_C),
    )
    assert regrouped == []
    assert member_ids(store.inspect_issue(WATCH_ID, ISSUE_C)) == [THREAD_C]
    (a_to_c,) = [
        suggestion
        for suggestion in store.inspect_issue(WATCH_ID, ISSUE_A)["suggestions"]
        if {suggestion["source_thread_id"], suggestion["candidate_thread_id"]}
        == {THREAD_A, THREAD_C}
    ]
    assert copy_id in a_to_c["signals"]["exact_copies"]

    store.link_issue_thread(WATCH_ID, ISSUE_A, THREAD_C, "same repost")

    linked = store.inspect_issue(WATCH_ID, ISSUE_A)
    copy = report_view(store, ISSUE_A, IC1)
    assert lineage_of(copy) == ("exact_copy", origin_id, "firsthand", None)
    assert copy["lineage_correction"] == event
    expected_counts = {"firsthand_reports": 2, "exact_copies": 1, "reports": 3}
    assert {key: linked["counts"][key] for key in expected_counts} == expected_counts
    for update in linked["updates"]:
        copied = update["report_id"] == copy_id and update["text_sha256"] == ia1.text_sha256
        assert update["copied_from_report_id"] == (origin_id if copied else None)
    assert report_row(store, copy_id) == c_row
    baseline = build_harness(tmp_path / "baseline")
    sweep(baseline, "initial")
    record(baseline, pending_inputs(baseline))
    sweep(baseline, "repost")
    record(baseline, pending_inputs(baseline))
    in_order = baseline.store.inspect_issue(WATCH_ID, ISSUE_A)
    assert member_ids(in_order) == member_ids(linked) == [THREAD_A, THREAD_C]
    assert linked["counts"] == in_order["counts"]
    assert linked["update_counts"] == in_order["update_counts"]


def test_record_issue_evidence_even_earlier_origin_corrects_original_and_corrected_copy(
    harness: Harness,
) -> None:
    # Arrange
    store = harness.store
    inputs, _, _ = c_before_a(harness)
    ia1 = report_in(inputs, IA1)
    earlier_ledger = ledger(store)
    sweep(harness, "updates")
    updates = pending_inputs(harness)
    z = copy_of(ia1, ia1, "reddit:ia0", ia1.source_created_utc - 600)
    batch = with_extra(updates, THREAD_A, z)
    entry_z = next(entry for entry in batch if entry.source_thread_id == THREAD_A)

    # Act
    result = record(harness, batch)

    # Assert
    assert result.lineage_corrections == 2
    z_id = rid(THREAD_A, "reddit:ia0")
    assert events_of(store, result.run_id) == [
        expected_event(
            result, entry_z, z, report_id=rid(THREAD_A, IA1), origin_report_id=z_id, previous=None
        ),
        expected_event(
            result,
            entry_z,
            z,
            report_id=rid(THREAD_C, IC1),
            origin_report_id=z_id,
            previous=rid(THREAD_A, IA1),
        ),
    ]
    assert ledger(store)[: len(earlier_ledger)] == earlier_ledger
    assert lineage_of(report_view(store, ISSUE_A, "reddit:ia0")) == UNCORRECTED_FIRSTHAND
    assert lineage_of(report_view(store, ISSUE_A, IA1)) == ("exact_copy", z_id, "firsthand", None)
    assert lineage_of(report_view(store, ISSUE_C, IC1)) == ("exact_copy", z_id, "firsthand", None)


def test_record_issue_evidence_copy_chain_before_original_corrects_every_copy_to_original(
    harness: Harness,
) -> None:
    # Arrange
    store = harness.store
    sweep(harness, "initial")
    sweep(harness, "repost")
    inputs = pending_inputs(harness)
    ia1, ic1 = report_in(inputs, IA1), report_in(inputs, IC1)
    chain = copy_of(ic1, ic1, "reddit:ic2", ic1.source_created_utc + 60)
    first = record(harness, with_extra(only(inputs, THREAD_C), THREAD_C, chain))
    assert first.exact_copies == 1
    ic2 = rid(THREAD_C, "reddit:ic2")
    recorded_chain = lineage_of(report_view(store, ISSUE_C, "reddit:ic2"))[2:]
    assert recorded_chain == ("exact_copy", rid(THREAD_C, IC1))

    # Act
    second = record(harness, without(inputs, THREAD_C))

    # Assert
    entry_a = entry_citing(inputs, IA1)
    origin, ic1_id = rid(THREAD_A, IA1), rid(THREAD_C, IC1)
    assert second.lineage_corrections == 2
    assert events_of(store, second.run_id) == [
        expected_event(
            second, entry_a, ia1, report_id=ic1_id, origin_report_id=origin, previous=None
        ),
        expected_event(
            second, entry_a, ia1, report_id=ic2, origin_report_id=origin, previous=ic1_id
        ),
    ]
    assert lineage_of(report_view(store, ISSUE_C, IC1)) == ("exact_copy", origin, "firsthand", None)
    assert lineage_of(report_view(store, ISSUE_C, "reddit:ic2")) == (
        "exact_copy",
        origin,
        "exact_copy",
        rid(THREAD_C, IC1),
    )
    counts = store.inspect_issue(WATCH_ID, ISSUE_C)["counts"]
    assert (counts["firsthand_reports"], counts["exact_copies"]) == (0, 2)


def test_record_issue_evidence_later_copy_inserted_first_in_batch_corrected_in_same_ingestion(
    harness: Harness,
) -> None:
    # Arrange
    store = harness.store
    sweep(harness, "initial")
    inputs = pending_inputs(harness)
    ia1 = report_in(inputs, IA1)
    f_template = only(inputs, THREAD_F)[0].reports[0]
    assert f_template.source_created_utc < ia1.source_created_utc
    later = copy_of(f_template, ia1, "reddit:ifl", ia1.source_created_utc + 3600)
    batch = with_extra(inputs, THREAD_F, later)

    # Act
    result = record(harness, batch)

    # Assert
    assert result.lineage_corrections == 1
    assert len(ledger(store)) == 1
    assert events_of(store, result.run_id) == [
        expected_event(
            result,
            entry_citing(inputs, IA1),
            ia1,
            report_id=rid(THREAD_F, "reddit:ifl"),
            origin_report_id=rid(THREAD_A, IA1),
            previous=None,
        )
    ]
    view_f = store.inspect_issue(WATCH_ID, ISSUE_F)
    assert member_ids(view_f) == [THREAD_F]
    assert lineage_of(report_view(store, ISSUE_F, "reddit:ifl")) == (
        "exact_copy",
        rid(THREAD_A, IA1),
        "firsthand",
        None,
    )
    assert view_f["counts"]["exact_copies"] == 1
    assert THREAD_F not in member_ids(store.inspect_issue(WATCH_ID, ISSUE_A))


def test_record_issue_evidence_short_or_different_earlier_text_records_no_correction(
    harness: Harness,
) -> None:
    # Arrange
    store = harness.store
    sweep(harness, "initial")
    sweep(harness, "repost")
    inputs = pending_inputs(harness)
    ic1 = report_in(inputs, IC1)
    assert ic1.content_chars >= LINEAGE_MIN_CHARS
    record(harness, only(inputs, THREAD_C))
    b_template = only(inputs, THREAD_B)[0].reports[0]
    short = replace(
        copy_of(b_template, ic1, "reddit:ibs", ic1.source_created_utc - 3600),
        content_chars=LINEAGE_MIN_CHARS - 1,
    )
    edited = replace(
        copy_of(b_template, ic1, "reddit:ibe", ic1.source_created_utc - 1800),
        text_sha256=hashlib.sha256(b"an edited repost of the export crash report").hexdigest(),
    )

    # Act
    result = record(harness, with_extra(only(inputs, THREAD_B), THREAD_B, short, edited))

    # Assert
    assert result.lineage_corrections == 0
    assert events_of(store, result.run_id) == []
    copy = report_view(store, ISSUE_C, IC1)
    assert lineage_of(copy) == UNCORRECTED_FIRSTHAND
    assert copy["lineage_correction"] is None
    issue_b = issue_id_for(WATCH_ID, THREAD_B)
    for comment_id in ("reddit:ibs", "reddit:ibe"):
        assert lineage_of(report_view(store, issue_b, comment_id)) == UNCORRECTED_FIRSTHAND


def test_record_issue_evidence_same_post_time_copy_recorded_later_copies_earlier_record(
    harness: Harness,
) -> None:
    # Arrange
    store = harness.store
    sweep(harness, "initial")
    sweep(harness, "repost")
    inputs = pending_inputs(harness)
    ic1 = report_in(inputs, IC1)
    record(harness, only(inputs, THREAD_C))
    b_template = only(inputs, THREAD_B)[0].reports[0]
    tied = copy_of(b_template, ic1, "reddit:ibt", ic1.source_created_utc)

    # Act
    result = record(harness, with_extra(only(inputs, THREAD_B), THREAD_B, tied))

    # Assert
    assert result.lineage_corrections == 0
    assert events_of(store, result.run_id) == []
    assert lineage_of(report_view(store, ISSUE_C, IC1)) == UNCORRECTED_FIRSTHAND
    copy = report_view(store, issue_id_for(WATCH_ID, THREAD_B), "reddit:ibt")
    assert lineage_of(copy) == (
        "exact_copy",
        rid(THREAD_C, IC1),
        "exact_copy",
        rid(THREAD_C, IC1),
    )


def test_record_issue_evidence_after_correction_adds_no_events_and_views_stay_stable(
    harness: Harness,
) -> None:
    # Arrange
    store = harness.store
    inputs, _, second = c_before_a(harness)
    ia1, ic1 = report_in(inputs, IA1), report_in(inputs, IC1)
    correction = report_view(store, ISSUE_C, IC1)["lineage_correction"]
    earlier_ledger = ledger(store)
    sweep(harness, "updates")
    later = copy_of(ia1, ia1, "reddit:ia9", ic1.source_created_utc + 600)

    # Act
    result = record(harness, with_extra(pending_inputs(harness), THREAD_A, later))

    # Assert
    assert result.lineage_corrections == 0
    assert events_of(store, result.run_id) == []
    assert ledger(store)[: len(earlier_ledger)] == earlier_ledger
    assert len(events_of(store, second.run_id)) == 1
    origin = rid(THREAD_A, IA1)
    later_copy = report_view(store, ISSUE_A, "reddit:ia9")
    assert lineage_of(later_copy) == ("exact_copy", origin, "exact_copy", origin)
    assert later_copy["lineage_correction"] is None
    assert report_view(store, ISSUE_C, IC1)["lineage_correction"] == correction
    first_inspect = json.dumps(store.inspect_issue(WATCH_ID, ISSUE_A), sort_keys=True)
    first_list = json.dumps(store.list_issues(WATCH_ID), sort_keys=True)
    assert json.dumps(store.inspect_issue(WATCH_ID, ISSUE_A), sort_keys=True) == first_inspect
    assert json.dumps(store.list_issues(WATCH_ID), sort_keys=True) == first_list


def insert_ledger_row(store: MonitoringStore, watch_id: str, summary: dict[str, Any]) -> None:
    """Append an ingestion ledger row for a fresh finalized run, bypassing the store."""
    ((policy_sha256, created),) = query(
        store.path,
        "SELECT policy_sha256, MAX(created_at_utc) FROM watch_issue_ingestions WHERE watch_id = ?",
        (watch_id,),
    )
    with closing(sqlite3.connect(store.path)) as conn, conn:
        conn.execute(
            "INSERT INTO watch_issue_ingestions (run_id, watch_id, policy_sha256, summary_json, "
            "created_at_utc) VALUES (?, ?, ?, ?, ?)",
            (summary["run_id"], watch_id, policy_sha256, json.dumps(summary), float(created)),
        )


def fresh_run(store: MonitoringStore, watch_id: str) -> str:
    claim = store.claim_run(watch_id, lease_seconds=LEASE_SECONDS)
    store.finalize_run(claim.run_id, claim.owner_token, "succeeded")
    return claim.run_id


def test_record_issue_evidence_identical_text_in_other_watch_neither_creates_nor_consumes(
    harness: Harness, tmp_path: Path
) -> None:
    # Arrange
    store = harness.store
    sweep(harness, "initial")
    sweep(harness, "repost")
    inputs = pending_inputs(harness)
    record(harness, only(inputs, THREAD_C))
    add_other_watch(harness, tmp_path)
    sweep(harness, "initial", watch_id=OTHER_WATCH_ID)

    # Act
    other = record(harness, pending_inputs(harness, OTHER_WATCH_ID), watch_id=OTHER_WATCH_ID)
    still_firsthand = report_view(store, ISSUE_C, IC1)
    own = record(harness, without(inputs, THREAD_C))

    # Assert
    assert other.lineage_corrections == 0
    assert events_of(store, other.run_id) == []
    assert lineage_of(still_firsthand) == UNCORRECTED_FIRSTHAND
    assert own.lineage_corrections == 1
    (event,) = events_of(store, own.run_id)
    assert (event["origin_report_id"], event["watch_id"]) == (rid(THREAD_A, IA1), WATCH_ID)
    other_issue = issue_id_for(OTHER_WATCH_ID, THREAD_A)
    other_ia1 = report_view(store, other_issue, IA1, OTHER_WATCH_ID)
    assert lineage_of(other_ia1) == UNCORRECTED_FIRSTHAND
    assert other_ia1["lineage_correction"] is None

    # A watch-2 ledger event that names watch-1 reports is refused for watch 2 only.
    run_id = fresh_run(store, OTHER_WATCH_ID)
    stolen = {**event, "watch_id": OTHER_WATCH_ID, "run_id": run_id}
    insert_ledger_row(
        store,
        OTHER_WATCH_ID,
        {"run_id": run_id, LINEAGE_CORRECTIONS_KEY: 1, LINEAGE_EVENTS_KEY: [stolen]},
    )
    assert_unreadable(lambda: store.inspect_issue(OTHER_WATCH_ID, other_issue))
    assert report_view(store, ISSUE_C, IC1)["lineage_correction"] == event


LedgerCase = Callable[[dict[str, Any], str], dict[str, Any]]
MALFORMED_LEDGER_CASES: list[Any] = [
    pytest.param(lambda e, r: {LINEAGE_CORRECTIONS_KEY: 0}, id="count_without_events"),
    pytest.param(lambda e, r: {LINEAGE_EVENTS_KEY: []}, id="events_without_count"),
    pytest.param(
        lambda e, r: {LINEAGE_CORRECTIONS_KEY: 2, LINEAGE_EVENTS_KEY: [{**e, "run_id": r}]},
        id="count_disagrees_with_events",
    ),
    pytest.param(
        lambda e, r: {LINEAGE_CORRECTIONS_KEY: 1, LINEAGE_EVENTS_KEY: [e]}, id="foreign_run"
    ),
    pytest.param(
        lambda e, r: {
            LINEAGE_CORRECTIONS_KEY: 1,
            LINEAGE_EVENTS_KEY: [{**e, "run_id": r, "watch_id": OTHER_WATCH_ID}],
        },
        id="foreign_watch",
    ),
    pytest.param(
        lambda e, r: {
            LINEAGE_CORRECTIONS_KEY: 1,
            LINEAGE_EVENTS_KEY: [{**e, "run_id": r, "report_id": rid(THREAD_C, "reddit:icx")}],
        },
        id="unrecorded_report",
    ),
    pytest.param(
        lambda e, r: {LINEAGE_CORRECTIONS_KEY: 1, LINEAGE_EVENTS_KEY: [{**e, "run_id": r}]},
        id="replayed_event",
    ),
    pytest.param(
        lambda e, r: {
            LINEAGE_CORRECTIONS_KEY: 1,
            LINEAGE_EVENTS_KEY: [{**e, "run_id": r, "note": "extra"}],
        },
        id="extra_event_key",
    ),
]


def ledger_ready(harness: Harness) -> tuple[dict[str, Any], dict[str, Any], str]:
    """A corrected watch with pending update entries and a fresh run awaiting a ledger row."""
    _, _, second = c_before_a(harness)
    (event,) = events_of(harness.store, second.run_id)
    sweep(harness, "updates")
    run_id = fresh_run(harness.store, WATCH_ID)
    summary = {
        key: value
        for key, value in summary_of(harness.store, second.run_id).items()
        if key not in (LINEAGE_CORRECTIONS_KEY, LINEAGE_EVENTS_KEY)
    }
    return event, {**summary, "run_id": run_id}, run_id


@pytest.mark.parametrize("case", MALFORMED_LEDGER_CASES)
def test_record_issue_evidence_malformed_ledger_events_refused_without_partial_record(
    harness: Harness, case: LedgerCase
) -> None:
    # Arrange
    store = harness.store
    event, base, run_id = ledger_ready(harness)
    insert_ledger_row(store, WATCH_ID, {**base, **case(event, run_id)})
    counts = table_counts(store.path)
    rows = ledger(store)
    claim = store.claim_run(WATCH_ID, lease_seconds=LEASE_SECONDS)
    inputs = pending_inputs(harness)
    assert inputs

    # Act / Assert
    assert_unreadable(
        lambda: store.record_issue_evidence(claim, inputs, issue_policy(harness.config.issues))
    )
    store.finalize_run(claim.run_id, claim.owner_token, "failed")
    assert table_counts(store.path) == counts
    assert ledger(store) == rows
    assert_unreadable(lambda: store.inspect_issue(WATCH_ID, ISSUE_A))
    assert_unreadable(lambda: store.list_issues(WATCH_ID))


def test_record_issue_evidence_legacy_ledger_summary_without_lineage_keys_accepted(
    harness: Harness,
) -> None:
    # Arrange
    store = harness.store
    event, base, _ = ledger_ready(harness)
    insert_ledger_row(store, WATCH_ID, base)

    # Act
    result = record(harness, pending_inputs(harness))

    # Assert
    assert result.entries >= 1
    assert result.lineage_corrections == 0
    assert report_view(store, ISSUE_C, IC1)["lineage_correction"] == event


def assert_ingestion_refused(harness: Harness, inputs: list[IssueEntryInput], reason: str) -> None:
    store = harness.store
    counts = table_counts(store.path)
    claim = store.claim_run(WATCH_ID, lease_seconds=LEASE_SECONDS)
    with pytest.raises(MonitoringStateError) as caught:
        store.record_issue_evidence(claim, inputs, issue_policy(harness.config.issues))
    store.finalize_run(claim.run_id, claim.owner_token, "failed")
    assert caught.value.details["reason"] == reason
    assert table_counts(store.path) == counts


def test_record_issue_evidence_already_recorded_repost_entry_refused_without_partial_record(
    harness: Harness,
) -> None:
    # Arrange
    inputs, _, _ = c_before_a(harness)

    # Act / Assert
    assert_ingestion_refused(harness, only(inputs, THREAD_C), "issue_entry_already_recorded")


def test_record_issue_evidence_other_watch_entry_refused_as_unknown_without_partial_record(
    harness: Harness, tmp_path: Path
) -> None:
    # Arrange
    c_before_a(harness)
    add_other_watch(harness, tmp_path)
    sweep(harness, "initial", watch_id=OTHER_WATCH_ID)
    foreign = only(pending_inputs(harness, OTHER_WATCH_ID), THREAD_A)

    # Act / Assert
    assert_ingestion_refused(harness, foreign, "unknown_issue_entry")
