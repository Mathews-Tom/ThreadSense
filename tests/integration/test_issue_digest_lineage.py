"""Digest consumers of cue-bearing reports discovered in reverse source-time order."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from recorded_reddit import reroute
from test_issue_store import (
    FIXTURE_DIR,
    ISSUE_A,
    LEASE_SECONDS,
    LOGGER,
    THREAD_A,
    THREAD_C,
    WATCH_ID,
    IssueHarness,
    build_harness,
    default_policy,
    materialize_phase,
    query_rows,
    record_inputs,
)

from threadsense.monitoring import IssueEntryInput, build_issue_entry_input, present_watch_digest
from threadsense.monitoring.issue_records import UPDATE_KINDS, report_id_for
from threadsense.observability import TraceContext

COPIED_TEXT = (
    "The markdown export crash happens every time my vault has more than five thousand "
    "notes; the exporter throws an error and writes nothing. My workaround is to export "
    "smaller vaults until the crash is diagnosed."
)
EDITED_COPY_TEXT = (
    COPIED_TEXT + " The export crash is fixed in version 9.9 according to this edited report."
)
COPIED_TEXT_SHA256 = hashlib.sha256(COPIED_TEXT.encode("utf-8")).hexdigest()
EDITED_TEXT_SHA256 = hashlib.sha256(EDITED_COPY_TEXT.encode("utf-8")).hexdigest()
ORIGIN_REPORT = report_id_for(WATCH_ID, THREAD_A, "reddit:ia1")
COPY_REPORT = report_id_for(WATCH_ID, THREAD_C, "reddit:ic1")


@dataclass(frozen=True)
class LateOriginScenario:
    harness: IssueHarness
    copy_entry: IssueEntryInput
    raw_copy_rows: dict[str, list[tuple[Any, ...]]]


@dataclass(frozen=True)
class CapturedDigest:
    digest_id: str
    json_path: Path
    markdown_path: Path
    json_bytes: bytes
    markdown_bytes: bytes
    payload: dict[str, Any]
    issue_view_rows: list[tuple[Any, ...]]


def write_report_fixture(destination: Path, response_name: str, comment_id: str, text: str) -> Path:
    """Change a synthesized replay body before the real sweep records its artifacts."""
    source = FIXTURE_DIR / "responses" / response_name
    payload: list[dict[str, Any]] = json.loads(source.read_text(encoding="utf-8"))
    [comment] = [
        child["data"]
        for child in payload[1]["data"]["children"]
        if child["kind"] == "t1" and child["data"]["id"] == comment_id
    ]
    comment["body"] = text
    destination.write_text(json.dumps(payload), encoding="utf-8")
    return destination


def raw_copy_rows(harness: IssueHarness, copy_entry_id: str) -> dict[str, list[tuple[Any, ...]]]:
    """The original copy's immutable report, evidence and cue rows, not later additions."""
    return {
        "report": query_rows(
            harness.store.path,
            "SELECT * FROM watch_issue_reports WHERE report_id = ?",
            (COPY_REPORT,),
        ),
        "evidence": query_rows(
            harness.store.path,
            "SELECT * FROM watch_issue_evidence WHERE report_id = ? AND entry_id = ?",
            (COPY_REPORT, copy_entry_id),
        ),
        "updates": query_rows(
            harness.store.path,
            "SELECT * FROM watch_issue_updates WHERE report_id = ? AND entry_id = ? "
            "ORDER BY update_id",
            (COPY_REPORT, copy_entry_id),
        ),
    }


def capture_digest(harness: IssueHarness, name: str) -> CapturedDigest:
    result = present_watch_digest(
        store=harness.store,
        watch_id=WATCH_ID,
        lease_seconds=LEASE_SECONDS,
        issue_policy=default_policy(harness),
        logger=LOGGER,
        trace=TraceContext.create(run_id=name, source_name="watch"),
    )
    assert result.status == "presented"
    assert result.digest_id is not None
    assert result.json_path is not None
    assert result.markdown_path is not None
    json_bytes = result.json_path.read_bytes()
    return CapturedDigest(
        digest_id=result.digest_id,
        json_path=result.json_path,
        markdown_path=result.markdown_path,
        json_bytes=json_bytes,
        markdown_bytes=result.markdown_path.read_bytes(),
        payload=json.loads(json_bytes),
        issue_view_rows=query_rows(
            harness.store.path,
            "SELECT * FROM watch_digest_issue_views WHERE digest_id = ?",
            (result.digest_id,),
        ),
    )


@pytest.fixture
def late_origin(tmp_path: Path) -> LateOriginScenario:
    harness = build_harness(tmp_path / "store")
    origin_response = write_report_fixture(
        tmp_path / "origin.json", "thread_a.json", "ia1", COPIED_TEXT
    )
    copy_response = write_report_fixture(
        tmp_path / "copy.json", "thread_c.json", "ic1", COPIED_TEXT
    )
    for name in ("initial", "repost"):
        harness.phases[name] = reroute(
            harness.phases[name], f"/comments/{THREAD_A}/", response_path=origin_response
        )
    harness.phases["repost"] = reroute(
        harness.phases["repost"], f"/comments/{THREAD_C}/", response_path=copy_response
    )
    materialize_phase(harness, "initial")
    materialize_phase(harness, "repost")
    inputs = [
        build_issue_entry_input(pending)
        for pending in harness.store.pending_issue_entries(WATCH_ID)
    ]
    [copy_entry] = [entry for entry in inputs if entry.source_thread_id == THREAD_C]
    [copy] = [report for report in copy_entry.reports if report.comment_id == "reddit:ic1"]
    [origin] = [
        report
        for entry in inputs
        if entry.source_thread_id == THREAD_A
        for report in entry.reports
        if report.comment_id == "reddit:ia1"
    ]
    assert len(COPIED_TEXT) >= 40
    assert origin.content_chars == copy.content_chars == len(COPIED_TEXT)
    assert origin.text_sha256 == copy.text_sha256 == COPIED_TEXT_SHA256
    assert origin.cues == copy.cues == (("workaround", "workaround"),)
    assert origin.source_created_utc < copy.source_created_utc

    # Discovery is deliberately the reverse of source time, in separate real transactions.
    first = record_inputs(harness, [copy_entry])
    assert first.updates_recorded == 1
    assert query_rows(
        harness.store.path,
        "SELECT lineage, copy_of_report_id FROM watch_issue_reports WHERE report_id = ?",
        (COPY_REPORT,),
    ) == [("firsthand", None)]
    assert query_rows(
        harness.store.path,
        "SELECT report_id, entry_id, text_sha256, kind, cue FROM watch_issue_updates "
        "WHERE report_id = ?",
        (COPY_REPORT,),
    ) == [(COPY_REPORT, copy_entry.entry_id, COPIED_TEXT_SHA256, "workaround", "workaround")]
    original_rows = raw_copy_rows(harness, copy_entry.entry_id)
    assert all(len(rows) == 1 for rows in original_rows.values())
    record_inputs(harness, [entry for entry in inputs if entry.entry_id != copy_entry.entry_id])
    assert query_rows(
        harness.store.path,
        "SELECT report_id FROM watch_issue_reports WHERE report_id IN (?, ?) "
        "ORDER BY record_sequence",
        (ORIGIN_REPORT, COPY_REPORT),
    ) == [(COPY_REPORT,), (ORIGIN_REPORT,)]
    assert query_rows(
        harness.store.path,
        "SELECT digest_id, presented_at_utc FROM watch_digest_entries WHERE entry_id = ?",
        (copy_entry.entry_id,),
    ) == [(None, None)]
    harness.store.link_issue_thread(
        WATCH_ID, ISSUE_A, THREAD_C, "Compare the late-discovered original and its repost"
    )
    assert {
        member["source_thread_id"]
        for member in harness.store.inspect_issue(WATCH_ID, ISSUE_A)["members"]
    } == {THREAD_A, THREAD_C}
    assert raw_copy_rows(harness, copy_entry.entry_id) == original_rows
    return LateOriginScenario(harness, copy_entry, original_rows)


def test_new_digest_cue_counts_and_report_refs_match_current_issue_after_late_origin(
    late_origin: LateOriginScenario,
) -> None:
    harness = late_origin.harness
    current = harness.store.inspect_issue(WATCH_ID, ISSUE_A)
    digest = capture_digest(harness, "late-origin-digest")
    refs = [ref for ref in digest.payload["issues"]["entry_refs"] if ref["issue_id"] == ISSUE_A]
    assert {ref["source_thread_id"] for ref in refs} == {THREAD_A, THREAD_C}
    assert late_origin.copy_entry.entry_id in {ref["entry_id"] for ref in refs}
    reports = {report["report_id"]: report for report in current["reports"]}
    digest_reports = {report["report_id"]: report for ref in refs for report in ref["reports"]}
    assert (
        set(digest_reports)
        == set(reports)
        == {
            ORIGIN_REPORT,
            report_id_for(WATCH_ID, THREAD_A, "reddit:ia2"),
            COPY_REPORT,
        }
    )
    assert (reports[ORIGIN_REPORT]["lineage"], reports[ORIGIN_REPORT]["copy_of_report_id"]) == (
        "firsthand",
        None,
    )
    for report_id, report in reports.items():
        ref = digest_reports[report_id]
        assert (ref["lineage"], ref["copy_of_report_id"], ref["text_sha256"]) == (
            report["lineage"],
            report["copy_of_report_id"],
            report["first_text_sha256"],
        )

    # Counts are projected by the issue API, independently of the digest's entry-update query.
    # Old forks retain both firsthand cues; corrected lineage excludes the copied first text.
    counts = Counter(update["kind"] for ref in refs for update in ref["updates"])
    assert counts["workaround"] >= 1
    assert {kind: counts[kind] for kind in UPDATE_KINDS} == current["update_counts"]
    [summary] = [
        issue for issue in digest.payload["issues"]["issues"] if issue["issue_id"] == ISSUE_A
    ]
    assert summary["update_counts"] == current["update_counts"]
    assert raw_copy_rows(harness, late_origin.copy_entry.entry_id) == late_origin.raw_copy_rows


def test_edited_copy_cues_survive_new_revision_without_rewriting_prior_digest(
    late_origin: LateOriginScenario, tmp_path: Path
) -> None:
    harness = late_origin.harness
    earlier_digest = capture_digest(harness, "before-copy-edit")
    edited_response = write_report_fixture(
        tmp_path / "edited-copy.json", "thread_c.json", "ic1", EDITED_COPY_TEXT
    )
    harness.phases["edited-copy"] = reroute(
        harness.phases["repost"], f"/comments/{THREAD_C}/", response_path=edited_response
    )
    materialize_phase(harness, "edited-copy")
    [edited_entry] = [
        build_issue_entry_input(pending)
        for pending in harness.store.pending_issue_entries(WATCH_ID)
    ]
    assert edited_entry.source_thread_id == THREAD_C
    assert edited_entry.entry_id != late_origin.copy_entry.entry_id
    assert edited_entry.revision_id != late_origin.copy_entry.revision_id
    [edited_report] = list(edited_entry.reports)
    assert edited_report.comment_id == "reddit:ic1"
    assert edited_report.text_sha256 == EDITED_TEXT_SHA256 != COPIED_TEXT_SHA256
    assert edited_report.cues == (
        ("workaround", "workaround"),
        ("resolution_claim", "fixed in"),
    )
    recorded = record_inputs(harness, [edited_entry])
    assert (recorded.reports_recorded, recorded.updates_recorded) == (0, 2)
    digest = capture_digest(harness, "after-copy-edit")
    assert digest.digest_id != earlier_digest.digest_id
    [ref] = [
        ref
        for ref in digest.payload["issues"]["entry_refs"]
        if ref["source_thread_id"] == THREAD_C and ref["kind"] == "finding"
    ]
    assert ref["entry_id"] == edited_entry.entry_id
    assert ref["issue_id"] == ISSUE_A
    current = harness.store.inspect_issue(WATCH_ID, ISSUE_A)
    [copy] = [report for report in current["reports"] if report["report_id"] == COPY_REPORT]
    [report_ref] = ref["reports"]
    assert (
        report_ref["report_id"],
        report_ref["lineage"],
        report_ref["copy_of_report_id"],
        report_ref["text_sha256"],
    ) == (COPY_REPORT, copy["lineage"], copy["copy_of_report_id"], EDITED_TEXT_SHA256)
    assert copy["first_text_sha256"] == COPIED_TEXT_SHA256
    assert len(ref["updates"]) == 2
    assert {
        (update["kind"], update["cue"], update["report_id"], update["comment_id"])
        for update in ref["updates"]
    } == {
        ("workaround", "workaround", COPY_REPORT, "reddit:ic1"),
        ("resolution_claim", "fixed in", COPY_REPORT, "reddit:ic1"),
    }
    assert query_rows(
        harness.store.path,
        "SELECT text_sha256, kind, cue FROM watch_issue_updates "
        "WHERE report_id = ? AND entry_id = ? ORDER BY kind",
        (COPY_REPORT, edited_entry.entry_id),
    ) == [
        (EDITED_TEXT_SHA256, "resolution_claim", "fixed in"),
        (EDITED_TEXT_SHA256, "workaround", "workaround"),
    ]
    [summary] = digest.payload["issues"]["issues"]
    assert summary["update_counts"] == current["update_counts"]
    assert current["update_counts"]["resolution_claim"] == 1
    assert "- Update: workaround in reddit:ic1" in digest.markdown_bytes.decode("utf-8")
    assert raw_copy_rows(harness, late_origin.copy_entry.entry_id) == late_origin.raw_copy_rows
    assert earlier_digest.json_path.read_bytes() == earlier_digest.json_bytes
    assert earlier_digest.markdown_path.read_bytes() == earlier_digest.markdown_bytes
    assert (
        query_rows(
            harness.store.path,
            "SELECT * FROM watch_digest_issue_views WHERE digest_id = ?",
            (earlier_digest.digest_id,),
        )
        == earlier_digest.issue_view_rows
    )
