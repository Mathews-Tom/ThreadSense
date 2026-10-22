from __future__ import annotations

import hashlib
import io
import json
import plistlib
import re
import shlex
import sqlite3
import sys
import time
from collections import Counter
from collections.abc import Iterator
from contextlib import closing, redirect_stdout
from dataclasses import dataclass
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

from threadsense.cli import main
from threadsense.cli_display import set_output_mode
from threadsense.models.analysis import load_analysis_artifact_file
from threadsense.monitoring import MonitoringStore

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "watch" / "incremental"
PROFILE_PATH = FIXTURE_DIR / "profile.toml"
PHASES_PATH = FIXTURE_DIR / "phases.json"
WATCH_ID = "sync-conflicts"
THREAD_A = "sc1aaa"
THREAD_B = "sc1bbb"
# profile.toml limits.presented_findings and refresh_interval_minutes.
PRESENTATION_LIMIT = 3
REFRESH_INTERVAL_SECONDS = 3600
HELD_CLAIM_LEASE_SECONDS = 600
UNREPRESENTABLE_CRON_MINUTES = 45
HOSTILE_WORKSPACE_NAME = "work space 'single' \"double\" 100%"
NOVELTY_KINDS = {"initial_observation", "new_source_event", "newly_observed_historical", "edited"}
BACKLOG_KEYS = {
    "presentation_limit",
    "presented_in_digest",
    "unpresented_entries_after",
    "pending_analyses",
    "tracked_threads",
    "refresh_pending",
    "latest_sweep_outcome",
    "search_completeness",
}
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
    "watch_digests",
    "watch_digest_entries",
)


class ProcessDeathError(BaseException):
    """Simulates the process dying mid-sweep: nothing in the CLI may handle it."""


# CLI harness: in-process `threadsense.cli.main` against isolated storage.


def isolate_environment(monkeypatch: pytest.MonkeyPatch, workspace: Path) -> None:
    monkeypatch.chdir(workspace)
    monkeypatch.delenv("THREADSENSE_CONFIG", raising=False)
    monkeypatch.delenv("THREADSENSE_STORAGE_MONITORING_DIR", raising=False)
    monkeypatch.setenv("THREADSENSE_STORAGE_ROOT", str(workspace / ".threadsense"))
    monkeypatch.setenv("THREADSENSE_RUNTIME_ENABLED", "false")
    monkeypatch.setenv("THREADSENSE_REDDIT_MAX_RETRIES", "0")
    monkeypatch.setenv("THREADSENSE_REDDIT_REQUEST_DELAY", "0")


def run_cli(*args: str) -> tuple[int, dict[str, Any]]:
    buffer = io.StringIO()
    try:
        with redirect_stdout(buffer):
            exit_code = main(["--output-format", "json", *args])
    finally:
        set_output_mode(None)
    return exit_code, json.loads(buffer.getvalue())


def create_watch(profile_path: Path = PROFILE_PATH) -> Path:
    exit_code, created = run_cli("watch", "create", "--profile", str(profile_path))
    assert exit_code == 0, created
    return Path(created["state_path"])


def install_transport(monkeypatch: pytest.MonkeyPatch, phase: ReplayPhase) -> PhaseReplayTransport:
    transport = PhaseReplayTransport(phase)
    monkeypatch.setattr(
        "threadsense.cli.build_reddit_connector", replay_connector_factory(transport)
    )
    return transport


def phases_by_name() -> dict[str, ReplayPhase]:
    return {phase.name: phase for phase in load_replay_phases(PHASES_PATH)}


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
    """Hash every file under `watches/`, keyed by monitoring-root-relative path."""
    return {
        path.relative_to(monitoring_root).as_posix(): sha256_file(path)
        for path in sorted((monitoring_root / "watches").rglob("*"))
        if path.is_file()
    }


def tree_digest(root: Path) -> dict[str, str]:
    """Every directory and file under `root`, files by content hash."""
    return {
        path.relative_to(root).as_posix(): sha256_file(path) if path.is_file() else "<dir>"
        for path in sorted(root.rglob("*"))
    }


def entry_id(*parts: str) -> str:
    return "ent-" + hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()[:32]


def digest_id(sequence: int, entry_ids: list[str]) -> str:
    text = f"{WATCH_ID}\n{sequence}\n" + "\n".join(entry_ids)
    return "dig-" + hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]


def entry_payloads(tables: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    return [json.loads(row["payload_json"]) for row in tables["watch_digest_entries"]]


def written_digests(tables: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    rows = [row for row in tables["watch_digests"] if row["status"] == "written"]
    return sorted(rows, key=lambda row: row["sequence"])


def presented_entry_ids(tables: dict[str, list[dict[str, Any]]]) -> list[str]:
    return [entry for row in written_digests(tables) for entry in json.loads(row["entry_ids_json"])]


def novelty(payload: dict[str, Any]) -> list[tuple[str, str]]:
    return [(comment["comment_id"], comment["kind"]) for comment in payload["novel_comments"]]


def cited_comment_ids(payload: dict[str, Any]) -> set[str]:
    """Comment ids an entry presents: all novel comments, or a finding's novel evidence."""
    key = "novel_comments" if payload["kind"] == "thread_update" else "novel_evidence"
    return {comment["comment_id"] for comment in payload[key]}


def artifact_tree(monitoring_root: Path) -> dict[str, str]:
    """Monitoring-root files other than the state database and its journal."""
    return {
        path: digest
        for path, digest in tree_digest(monitoring_root).items()
        if not path.startswith("state.sqlite3")
    }


# Journey over every recorded phase, one `watch run` per phase.


@dataclass(frozen=True)
class Step:
    name: str
    exit_code: int
    payload: dict[str, Any]
    inspection: dict[str, Any]
    tables: dict[str, list[dict[str, Any]]]
    artifacts: dict[str, str]
    requested_urls: list[str]

    @property
    def sweep(self) -> dict[str, Any]:
        sweep: dict[str, Any] = self.payload["sweep"]
        return sweep

    @property
    def digest(self) -> dict[str, Any]:
        digest: dict[str, Any] = self.payload["digest"]
        return digest

    def entries(self) -> dict[str, dict[str, Any]]:
        return {row["entry_id"]: row for row in self.tables["watch_digest_entries"]}

    def unpresented_count(self) -> int:
        return sum(1 for row in self.tables["watch_digest_entries"] if row["digest_id"] is None)

    def revision(self, source_thread_id: str, sequence: int) -> dict[str, Any]:
        rows = [
            row
            for row in self.tables["watch_thread_revisions"]
            if row["source_thread_id"] == source_thread_id and row["sequence"] == sequence
        ]
        assert len(rows) == 1
        return rows[0]

    def comment(self, comment_id: str) -> dict[str, Any]:
        rows = [row for row in self.tables["watch_comments"] if row["comment_id"] == comment_id]
        assert len(rows) == 1
        return rows[0]


@dataclass(frozen=True)
class Journey:
    monitoring_root: Path
    steps: dict[str, Step]

    @property
    def final(self) -> Step:
        return self.steps["backlog"]

    def new_entries(self, name: str) -> dict[str, dict[str, Any]]:
        names = list(self.steps)
        index = names.index(name)
        earlier = set(self.steps[names[index - 1]].entries()) if index else set()
        return {key: row for key, row in self.steps[name].entries().items() if key not in earlier}

    def thread_update(self, name: str, source_thread_id: str, sequence: int) -> dict[str, Any]:
        step = self.steps[name]
        revision_id = step.revision(source_thread_id, sequence)["revision_id"]
        row = step.entries()[entry_id(WATCH_ID, revision_id, "thread_update")]
        payload: dict[str, Any] = json.loads(row["payload_json"])
        return payload


def run_phase(monkeypatch: pytest.MonkeyPatch, state_path: Path, phase: ReplayPhase) -> Step:
    transport = install_transport(monkeypatch, phase)
    exit_code, payload = run_cli("watch", "run", "--id", WATCH_ID)
    inspect_code, inspection = run_cli("watch", "inspect", "--id", WATCH_ID)
    assert inspect_code == 0, inspection
    return Step(
        name=phase.name,
        exit_code=exit_code,
        payload=payload,
        inspection=inspection,
        tables=read_tables(state_path),
        artifacts=artifact_hashes(state_path.parent),
        requested_urls=transport.requested_urls,
    )


@pytest.fixture(scope="module")
def journey(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Journey]:
    with pytest.MonkeyPatch.context() as monkeypatch:
        isolate_environment(monkeypatch, tmp_path_factory.mktemp("journey"))
        state_path = create_watch()
        steps = {
            phase.name: run_phase(monkeypatch, state_path, phase)
            for phase in load_replay_phases(PHASES_PATH)
        }
        yield Journey(monitoring_root=state_path.parent, steps=steps)


def thread_url(source_thread_id: str) -> str:
    phase = load_replay_phases(PHASES_PATH)[0]
    marker = f"/comments/{source_thread_id}/"
    return next(route.url for route in phase.routes if marker in route.url)


@pytest.mark.parametrize(
    ("phase_name", "outcome", "exit_code"),
    [
        ("initial", "changes", 0),
        ("unchanged", "no_changes", 0),
        ("score_only", "no_changes", 0),
        ("search_omits_known", "changes", 0),
        ("edit", "changes", 0),
        ("partial", "partial", 1),
        ("reappear", "changes", 0),
        ("backlog", "no_changes", 0),
    ],
)
def test_watch_run_phase_reports_sweep_outcome_and_exit_code(
    journey: Journey, phase_name: str, outcome: str, exit_code: int
) -> None:
    step = journey.steps[phase_name]

    assert step.exit_code == exit_code
    assert step.payload["artifact_type"] == "watch_run"
    assert step.payload["status"] == outcome
    assert step.sweep["outcome"] == outcome


@pytest.mark.parametrize(
    ("phase_name", "source_thread_id", "sequence", "expected"),
    [
        (
            "initial",
            THREAD_A,
            1,
            [(f"reddit:ka{index}", "initial_observation") for index in (1, 2, 3)],
        ),
        (
            "initial",
            THREAD_B,
            1,
            [(f"reddit:kb{index}", "initial_observation") for index in (1, 2)],
        ),
        ("search_omits_known", THREAD_A, 2, [("reddit:ka4", "new_source_event")]),
        ("edit", THREAD_B, 2, [("reddit:kb2", "edited")]),
        ("reappear", THREAD_A, 3, [("reddit:ka5", "newly_observed_historical")]),
    ],
)
def test_watch_run_revision_materializes_thread_update_with_its_novelty(
    journey: Journey,
    phase_name: str,
    source_thread_id: str,
    sequence: int,
    expected: list[tuple[str, str]],
) -> None:
    payload = journey.thread_update(phase_name, source_thread_id, sequence)

    assert payload["entry_id"] in journey.new_entries(phase_name)
    assert novelty(payload) == expected
    assert payload["counts"] == {kind: 0 for kind in NOVELTY_KINDS} | Counter(
        kind for _, kind in expected
    )
    assert payload["revision_sequence"] == sequence


def test_watch_run_materializes_one_thread_update_per_analyzed_revision(
    journey: Journey,
) -> None:
    tables = journey.final.tables

    updates = [row for row in tables["watch_digest_entries"] if row["kind"] == "thread_update"]

    assert sorted(row["revision_id"] for row in updates) == sorted(
        row["revision_id"] for row in tables["watch_revision_analyses"]
    )
    assert len(updates) == 5


def test_watch_run_thread_update_links_recorded_snapshot_and_analysis(journey: Journey) -> None:
    tables = journey.final.tables
    revisions = {row["revision_id"]: row for row in tables["watch_thread_revisions"]}
    analyses = {row["revision_id"]: row for row in tables["watch_revision_analyses"]}

    for payload in entry_payloads(tables):
        revision = revisions[payload["revision_id"]]
        analysis = analyses[payload["revision_id"]]
        assert payload["snapshot_path"] == revision["snapshot_path"]
        assert payload["snapshot_sha256"] == revision["snapshot_sha256"]
        assert payload["analysis_path"] == analysis["analysis_path"]
        assert payload["analysis_sha256"] == analysis["analysis_sha256"]
        assert (
            sha256_file(journey.monitoring_root / payload["snapshot_path"])
            == (payload["snapshot_sha256"])
        )
        assert (
            sha256_file(journey.monitoring_root / payload["analysis_path"])
            == (payload["analysis_sha256"])
        )


def test_watch_run_finding_entries_are_analysis_findings_citing_novel_comments(
    journey: Journey,
) -> None:
    tables = journey.final.tables
    payloads = entry_payloads(tables)

    for update in (payload for payload in payloads if payload["kind"] == "thread_update"):
        novel_ids = {comment["comment_id"] for comment in update["novel_comments"]}
        analysis = load_analysis_artifact_file(journey.monitoring_root / update["analysis_path"])
        expected = {
            entry_id(WATCH_ID, update["revision_id"], "finding", finding.theme_key): sorted(
                novel_ids & set(finding.evidence_comment_ids)
            )
            for finding in analysis.findings
            if novel_ids & set(finding.evidence_comment_ids)
        }
        actual = {
            payload["entry_id"]: payload["novel_evidence_comment_ids"]
            for payload in payloads
            if payload["kind"] == "finding" and payload["revision_id"] == update["revision_id"]
        }
        assert actual == expected
        for payload in payloads:
            if payload["kind"] == "finding" and payload["revision_id"] == update["revision_id"]:
                cited = [comment["comment_id"] for comment in payload["novel_evidence"]]
                assert cited == payload["novel_evidence_comment_ids"]


def test_watch_run_initial_first_observations_carry_observation_time_and_excerpt(
    journey: Journey,
) -> None:
    payload = journey.thread_update("initial", THREAD_A, 1)

    comments = {comment["comment_id"]: comment for comment in payload["novel_comments"]}

    assert all(isinstance(item["first_observed_at_utc"], float) for item in comments.values())
    assert comments["reddit:ka3"]["excerpt"] == (
        "Is there a setting to prefer the newest edit instead of keeping both copies?"
    )


@pytest.mark.parametrize("phase_name", ["unchanged", "score_only", "partial", "backlog"])
def test_watch_run_phase_without_new_revision_materializes_no_entry(
    journey: Journey, phase_name: str
) -> None:
    step = journey.steps[phase_name]
    names = list(journey.steps)
    earlier = journey.steps[names[names.index(phase_name) - 1]].entries()

    assert journey.new_entries(phase_name) == {}
    assert set(step.digest["entry_ids"]) <= set(earlier)


def test_watch_run_known_thread_omitted_by_search_presents_new_source_event(
    journey: Journey,
) -> None:
    step = journey.steps["search_omits_known"]
    discovered = {
        row["source_thread_id"]
        for row in step.tables["watch_discoveries"]
        if row["run_id"] == step.sweep["run_id"]
    }

    payload = journey.thread_update("search_omits_known", THREAD_A, 2)

    assert THREAD_A not in discovered
    assert thread_url(THREAD_A) in step.requested_urls
    assert payload["novel_comments"][0]["excerpt"] == (
        "Version 2.4 added a prefer-newest option under advanced sync settings."
    )
    assert isinstance(payload["novel_comments"][0]["first_observed_at_utc"], float)


def test_watch_run_edit_entry_records_edited_change_with_null_source_edit_time(
    journey: Journey,
) -> None:
    payload = journey.thread_update("edit", THREAD_B, 2)
    initial_text = journey.steps["initial"].comment("reddit:kb2")["current_text_sha256"]
    edited_text = journey.steps["edit"].comment("reddit:kb2")["current_text_sha256"]

    [change] = payload["novel_comments"]

    assert change["source_edited_at_utc"] is None
    assert change["previous_text_sha256"] == initial_text
    assert change["text_sha256"] == edited_text != initial_text
    assert isinstance(change["observed_at_utc"], float)
    assert "first_observed_at_utc" not in change


def test_watch_run_partial_sweep_hides_absent_comment_without_phantom_deletion(
    journey: Journey,
) -> None:
    partial = journey.steps["partial"]
    initial_revision = journey.steps["initial"].revision(THREAD_A, 1)["revision_id"]

    citing_ka2 = {
        payload["revision_id"]
        for payload in entry_payloads(journey.final.tables)
        if "reddit:ka2" in cited_comment_ids(payload)
    }
    kinds = {
        comment["kind"]
        for payload in entry_payloads(journey.final.tables)
        if payload["kind"] == "thread_update"
        for comment in payload["novel_comments"]
    }

    assert partial.sweep["outcome"] == "partial"
    assert partial.exit_code == 1
    assert journey.new_entries("partial") == {}
    assert (
        partial.comment("reddit:ka2")["current_text_sha256"]
        == (journey.steps["initial"].comment("reddit:ka2")["current_text_sha256"])
    )
    assert citing_ka2 == {initial_revision}
    assert kinds <= NOVELTY_KINDS


def test_watch_run_reappear_labels_historical_comment_and_skips_reappearing_comment(
    journey: Journey,
) -> None:
    payload = journey.thread_update("reappear", THREAD_A, 3)

    cited = {
        comment_id
        for row in journey.new_entries("reappear").values()
        for comment_id in cited_comment_ids(json.loads(row["payload_json"]))
    }

    assert novelty(payload) == [("reddit:ka5", "newly_observed_historical")]
    assert "reddit:ka2" not in cited
    assert "reddit:ka3" not in cited


def test_watch_run_backlog_phase_reports_refresh_pending(journey: Journey) -> None:
    step = journey.final

    refresh = step.sweep["summary"]["refresh"]

    assert (refresh["tracked_threads"], refresh["attempted"], refresh["pending"]) == (4, 2, 2)
    assert step.inspection["sweep"]["refresh_due_never_refreshed"] == 2
    assert not any("sc1ddd" in url or "sc1eee" in url for url in step.requested_urls)
    assert step.digest["status"] == "no_entries"
    assert step.inspection["digest"]["unpresented_entries"] == 0


def test_watch_run_digest_backlog_reports_state_after_its_run(journey: Journey) -> None:
    for step in journey.steps.values():
        digest = step.digest
        if digest["status"] == "no_entries":
            assert (digest["digest_id"], digest["entry_ids"], digest["backlog"]) == (
                None,
                [],
                None,
            ), step.name
            assert step.unpresented_count() == 0, step.name
            continue
        backlog = digest["backlog"]
        assert set(backlog) == BACKLOG_KEYS, step.name
        assert backlog["presentation_limit"] == PRESENTATION_LIMIT, step.name
        assert backlog["presented_in_digest"] == len(digest["entry_ids"]), step.name
        assert backlog["unpresented_entries_after"] == step.unpresented_count(), step.name
        assert backlog["refresh_pending"] == step.sweep["summary"]["refresh"]["pending"]
        assert backlog["latest_sweep_outcome"] == step.sweep["outcome"], step.name
        assert backlog["pending_analyses"] == step.inspection["sweep"]["pending_analyses"]
        assert backlog["search_completeness"] == "unknown", step.name


def test_watch_run_digest_presents_at_most_presentation_limit(journey: Journey) -> None:
    digests = written_digests(journey.final.tables)

    for row in digests:
        entry_ids = json.loads(row["entry_ids_json"])
        backlog = json.loads(row["backlog_json"])
        assert 1 <= len(entry_ids) <= PRESENTATION_LIMIT
        assert len(entry_ids) == PRESENTATION_LIMIT or backlog["unpresented_entries_after"] == 0
    assert digests


def test_watch_run_digests_present_each_entry_once_in_creation_order(journey: Journey) -> None:
    tables = journey.final.tables
    entries = journey.final.entries()
    digests = written_digests(tables)

    presented = presented_entry_ids(tables)
    linked = {
        entry: row["digest_id"] for row in digests for entry in json.loads(row["entry_ids_json"])
    }

    assert [row["sequence"] for row in digests] == list(range(1, len(digests) + 1))
    assert len(presented) == len(set(presented))
    assert {key: row["digest_id"] for key, row in entries.items() if row["digest_id"]} == linked
    assert presented == sorted(presented, key=lambda key: (entries[key]["created_at_utc"], key))
    assert not [row for row in tables["watch_digests"] if row["status"] == "pending"]


def test_watch_run_new_entries_are_created_by_that_run_digest(journey: Journey) -> None:
    for name, step in journey.steps.items():
        created_by = {row["created_run_id"] for row in journey.new_entries(name).values()}

        assert created_by <= {step.digest["run_id"]}, name
        assert step.digest["materialized_entries"] == len(journey.new_entries(name)), name


def test_watch_run_digest_artifacts_match_recorded_identity_and_hashes(journey: Journey) -> None:
    entries = journey.final.entries()

    for row in written_digests(journey.final.tables):
        json_path = journey.monitoring_root / row["json_path"]
        markdown_path = journey.monitoring_root / row["markdown_path"]
        entry_ids = json.loads(row["entry_ids_json"])
        document = json.loads(json_path.read_text(encoding="utf-8"))

        assert row["digest_id"] == digest_id(row["sequence"], entry_ids)
        assert json_path.parent == journey.monitoring_root / "watches" / WATCH_ID / "digests"
        assert json_path.name == f"{row['sequence']:06d}-{row['digest_id']}.json"
        assert markdown_path.name == f"{row['sequence']:06d}-{row['digest_id']}.md"
        assert sha256_file(json_path) == row["json_sha256"]
        assert sha256_file(markdown_path) == row["markdown_sha256"]
        assert document["artifact_type"] == "watch_digest"
        assert (document["digest_id"], document["sequence"]) == (row["digest_id"], row["sequence"])
        assert document["entries"] == [
            json.loads(entries[key]["payload_json"]) for key in entry_ids
        ]
        assert document["backlog"] == json.loads(row["backlog_json"])


def test_watch_run_digest_payload_matches_written_digest(journey: Journey) -> None:
    for step in journey.steps.values():
        if step.digest["status"] != "presented":
            continue
        [row] = [
            row
            for row in step.tables["watch_digests"]
            if row["digest_id"] == step.digest["digest_id"]
        ]

        assert row["status"] == "written", step.name
        assert step.digest["sequence"] == row["sequence"], step.name
        assert step.digest["entry_ids"] == json.loads(row["entry_ids_json"]), step.name
        assert step.digest["resumed"] is False, step.name
        assert Path(step.digest["json_path"]).resolve() == (
            (journey.monitoring_root / row["json_path"]).resolve()
        )


def test_watch_run_earlier_artifacts_stay_byte_identical(journey: Journey) -> None:
    final = journey.final.artifacts

    changed = [
        (name, path)
        for name, step in journey.steps.items()
        for path, digest in step.artifacts.items()
        if final.get(path) != digest
    ]

    assert changed == []
    assert any("/digests/" in path for path in final)


def test_watch_inspect_digest_status_matches_durable_state(journey: Journey) -> None:
    for step in journey.steps.values():
        status = step.inspection["digest"]
        digests = written_digests(step.tables)

        assert status["entry_count"] == len(step.tables["watch_digest_entries"]), step.name
        assert status["unpresented_entries"] == step.unpresented_count(), step.name
        assert status["pending_digest_id"] is None, step.name
        assert status["digest_count"] == len(digests), step.name
        if digests:
            assert status["latest_digest"]["digest_id"] == digests[-1]["digest_id"], step.name
            assert Path(status["latest_digest"]["json_path"]).is_absolute(), step.name


# Crash between digest artifact write and complete_digest.


@dataclass(frozen=True)
class CrashedDigest:
    state_path: Path
    pending: dict[str, Any]
    file_hashes: tuple[str, str]
    tables: dict[str, list[dict[str, Any]]]
    inspection: dict[str, Any]


def pending_file_hashes(state_path: Path, row: dict[str, Any]) -> tuple[str, str]:
    root = state_path.parent
    return sha256_file(root / row["json_path"]), sha256_file(root / row["markdown_path"])


@pytest.fixture
def crashed_digest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> CrashedDigest:
    isolate_environment(monkeypatch, tmp_path)
    state_path = create_watch()
    install_transport(monkeypatch, phases_by_name()["initial"])

    def fail_complete_digest(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("injected failure after digest artifact write")

    with monkeypatch.context() as patch:
        patch.setattr(MonitoringStore, "complete_digest", fail_complete_digest)
        with pytest.raises(RuntimeError, match="injected failure"):
            run_cli("watch", "run", "--id", WATCH_ID)
    tables = read_tables(state_path)
    [pending] = tables["watch_digests"]
    _, inspection = run_cli("watch", "inspect", "--id", WATCH_ID)
    return CrashedDigest(
        state_path=state_path,
        pending=pending,
        file_hashes=pending_file_hashes(state_path, pending),
        tables=tables,
        inspection=inspection,
    )


def test_watch_run_digest_crash_leaves_pending_digest_with_unpresented_entries(
    crashed_digest: CrashedDigest,
) -> None:
    pending = crashed_digest.pending

    assert pending["status"] == "pending"
    assert (pending["json_sha256"], pending["markdown_sha256"]) == (None, None)
    assert all(row["digest_id"] is None for row in crashed_digest.tables["watch_digest_entries"])
    assert crashed_digest.inspection["digest"]["pending_digest_id"] == pending["digest_id"]
    assert crashed_digest.inspection["active_run"] is None


def test_watch_digest_after_crash_resumes_same_digest_byte_identical(
    crashed_digest: CrashedDigest,
) -> None:
    pending = crashed_digest.pending

    exit_code, payload = run_cli("watch", "digest", "--id", WATCH_ID)
    [row] = [
        row
        for row in read_tables(crashed_digest.state_path)["watch_digests"]
        if row["digest_id"] == pending["digest_id"]
    ]

    assert exit_code == 0
    assert payload["artifact_type"] == "watch_digest_result"
    assert (payload["status"], payload["resumed"]) == ("presented", True)
    assert payload["digest_id"] == pending["digest_id"]
    assert payload["sequence"] == pending["sequence"]
    assert payload["entry_ids"] == json.loads(pending["entry_ids_json"])
    assert pending_file_hashes(crashed_digest.state_path, row) == crashed_digest.file_hashes
    assert row["status"] == "written"
    assert (row["json_sha256"], row["markdown_sha256"]) == crashed_digest.file_hashes


def test_watch_digest_after_crash_presents_every_entry_exactly_once(
    crashed_digest: CrashedDigest,
) -> None:
    entry_count = len(crashed_digest.tables["watch_digest_entries"])
    statuses = [run_cli("watch", "digest", "--id", WATCH_ID)[1]["status"]]
    while statuses[-1] == "presented" and len(statuses) <= entry_count:
        statuses.append(run_cli("watch", "digest", "--id", WATCH_ID)[1]["status"])
    tables = read_tables(crashed_digest.state_path)

    presented = presented_entry_ids(tables)

    assert statuses[-1] == "no_entries"
    assert sorted(presented) == sorted(row["entry_id"] for row in tables["watch_digest_entries"])
    assert len(presented) == len(set(presented)) == entry_count
    assert presented[: len(json.loads(crashed_digest.pending["entry_ids_json"]))] == json.loads(
        crashed_digest.pending["entry_ids_json"]
    )


# Process death between sweep artifact write and record_acquisition.


@dataclass(frozen=True)
class RecoveredSweep:
    dead_run_id: str
    orphan_artifacts: dict[str, str]
    exit_code: int
    payload: dict[str, Any]
    tables: dict[str, list[dict[str, Any]]]
    artifacts: dict[str, str]


@pytest.fixture
def recovered_sweep(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> RecoveredSweep:
    isolate_environment(monkeypatch, tmp_path)
    state_path = create_watch()
    initial = phases_by_name()["initial"]

    def die(*args: Any, **kwargs: Any) -> None:
        raise ProcessDeathError

    install_transport(monkeypatch, initial)
    with monkeypatch.context() as patch:
        patch.setattr(MonitoringStore, "record_acquisition", die)
        patch.setattr(MonitoringStore, "complete_sweep", die)
        with pytest.raises(ProcessDeathError):
            run_cli("watch", "run", "--id", WATCH_ID)
    [dead] = read_tables(state_path)["watch_runs"]
    orphans = artifact_hashes(state_path.parent)
    # A store clock past the dead lease recovers it without waiting on wall time.
    offset = dead["lease_expires_at_utc"] + 1.0 - time.time()
    monkeypatch.setattr(
        "threadsense.cli.MonitoringStore",
        partial(MonitoringStore, clock=lambda: time.time() + offset),
    )
    install_transport(monkeypatch, initial)
    exit_code, payload = run_cli("watch", "run", "--id", WATCH_ID)
    return RecoveredSweep(
        dead_run_id=dead["run_id"],
        orphan_artifacts=orphans,
        exit_code=exit_code,
        payload=payload,
        tables=read_tables(state_path),
        artifacts=artifact_hashes(state_path.parent),
    )


def test_watch_run_after_sweep_death_recovers_expired_claim(
    recovered_sweep: RecoveredSweep,
) -> None:
    sweep = recovered_sweep.payload["sweep"]

    assert recovered_sweep.exit_code == 0
    assert sweep["recovered_run_id"] == recovered_sweep.dead_run_id
    assert sweep["outcome"] == "changes"
    assert recovered_sweep.orphan_artifacts


def test_watch_run_after_sweep_death_records_each_revision_and_entry_once(
    recovered_sweep: RecoveredSweep,
) -> None:
    tables = recovered_sweep.tables

    revision_ids = [row["revision_id"] for row in tables["watch_thread_revisions"]]
    updates = [row for row in tables["watch_digest_entries"] if row["kind"] == "thread_update"]
    presented = presented_entry_ids(tables)

    assert len(revision_ids) == len(set(revision_ids)) == 2
    assert sorted(row["revision_id"] for row in updates) == sorted(revision_ids)
    assert len(presented) == len(set(presented))
    assert presented == recovered_sweep.payload["digest"]["entry_ids"]


def test_watch_run_after_sweep_death_keeps_orphan_files_byte_identical(
    recovered_sweep: RecoveredSweep,
) -> None:
    after = recovered_sweep.artifacts

    assert all(
        after.get(path) == digest for path, digest in recovered_sweep.orphan_artifacts.items()
    )


# Non-overlap: another owner's live claim.


def test_watch_run_with_claim_held_by_other_owner_is_refused_without_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    isolate_environment(monkeypatch, tmp_path)
    state_path = create_watch()
    MonitoringStore(state_path).claim_run(WATCH_ID, lease_seconds=HELD_CLAIM_LEASE_SECONDS)
    transport = install_transport(monkeypatch, phases_by_name()["initial"])
    tables_before = read_tables(state_path)
    tree_before = artifact_tree(state_path.parent)

    exit_code, payload = run_cli("watch", "run", "--id", WATCH_ID)

    assert exit_code == 1
    assert payload["error"]["code"] == "run_claim_error"
    assert payload["error"]["details"]["reason"] == "claim_held"
    assert transport.requests == []
    assert read_tables(state_path) == tables_before
    assert artifact_tree(state_path.parent) == tree_before


# Render-only scheduling.


@dataclass(frozen=True)
class ScheduledWatch:
    workspace: Path
    storage_root: Path
    state_path: Path


@pytest.fixture
def scheduled_watch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ScheduledWatch:
    workspace = tmp_path / HOSTILE_WORKSPACE_NAME
    workspace.mkdir()
    isolate_environment(monkeypatch, workspace)
    state_path = create_watch()
    return ScheduledWatch(
        workspace=workspace.resolve(),
        storage_root=(workspace / ".threadsense").resolve(),
        state_path=state_path,
    )


def render_schedule(watch: ScheduledWatch, schedule_format: str) -> tuple[int, dict[str, Any]]:
    return run_cli(
        "watch",
        "schedule",
        "--id",
        WATCH_ID,
        "--format",
        schedule_format,
        "--workspace",
        str(watch.workspace),
    )


def expected_argv(lease_seconds: int) -> list[str]:
    return [
        str(Path(sys.executable).absolute()),
        "-m",
        "threadsense",
        "--output-format",
        "json",
        "watch",
        "run",
        "--id",
        WATCH_ID,
        "--lease-seconds",
        str(lease_seconds),
    ]


def expected_log_path(watch: ScheduledWatch) -> Path:
    return watch.state_path.parent.resolve() / "watches" / WATCH_ID / "schedule" / "watch-run.log"


def test_watch_schedule_launchd_renders_absolute_hostile_workspace_paths(
    scheduled_watch: ScheduledWatch,
) -> None:
    exit_code, payload = render_schedule(scheduled_watch, "launchd")

    plist = plistlib.loads(payload["content"].encode("utf-8"))

    assert exit_code == 0
    assert plist["Label"] == f"com.threadsense.watch.{WATCH_ID}"
    assert plist["ProgramArguments"] == payload["argv"] == expected_argv(REFRESH_INTERVAL_SECONDS)
    assert Path(plist["ProgramArguments"][0]).is_absolute()
    assert plist["WorkingDirectory"] == str(scheduled_watch.workspace)
    assert plist["StartInterval"] == REFRESH_INTERVAL_SECONDS == payload["interval_seconds"]
    assert plist["EnvironmentVariables"] == {
        "THREADSENSE_STORAGE_ROOT": str(scheduled_watch.storage_root)
    }
    assert Path(plist["StandardOutPath"]).resolve() == expected_log_path(scheduled_watch)
    assert plist["StandardErrorPath"] == plist["StandardOutPath"] == payload["log_path"]
    assert plist["RunAtLoad"] is False


def test_watch_schedule_cron_line_quotes_hostile_workspace_and_escapes_percent(
    scheduled_watch: ScheduledWatch,
) -> None:
    exit_code, payload = render_schedule(scheduled_watch, "cron")

    [line] = payload["content"].splitlines()
    tokens = shlex.split(line.replace("\\%", "%"))

    assert exit_code == 0
    assert re.search(r"(?<!\\)%", line) is None
    assert tokens[:5] == ["0", "*/1", "*", "*", "*"]
    assert tokens[5:] == [
        "cd",
        str(scheduled_watch.workspace),
        "&&",
        f"THREADSENSE_STORAGE_ROOT={scheduled_watch.storage_root}",
        "exec",
        *expected_argv(REFRESH_INTERVAL_SECONDS),
        ">>",
        payload["log_path"],
        "2>&1",
    ]
    assert Path(payload["log_path"]).resolve() == expected_log_path(scheduled_watch)


@pytest.mark.parametrize("schedule_format", ["launchd", "cron"])
def test_watch_schedule_renders_without_writing_or_installing(
    scheduled_watch: ScheduledWatch, schedule_format: str
) -> None:
    before = tree_digest(scheduled_watch.workspace)

    exit_code, payload = render_schedule(scheduled_watch, schedule_format)

    assert exit_code == 0
    assert payload["artifact_type"] == "watch_schedule"
    assert (payload["status"], payload["format"]) == ("rendered", schedule_format)
    assert (payload["installed"], payload["enabled"]) == (False, False)
    assert payload["overlap_policy"] == "run_claim"
    assert payload["lease_seconds"] == REFRESH_INTERVAL_SECONDS
    assert Path(payload["executable"]).is_absolute()
    assert Path(payload["storage_root"]) == scheduled_watch.storage_root
    assert tree_digest(scheduled_watch.workspace) == before


def test_watch_schedule_pins_implicitly_loaded_config_in_argv(
    scheduled_watch: ScheduledWatch, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "implicit config.toml"
    config_path.write_text("", encoding="utf-8")
    monkeypatch.setenv("THREADSENSE_CONFIG", str(config_path))

    exit_code, payload = render_schedule(scheduled_watch, "launchd")

    assert exit_code == 0
    assert payload["config_path"] == str(config_path.resolve())
    assert payload["argv"][-2:] == ["--config", str(config_path.resolve())]


def test_watch_schedule_unrepresentable_cron_interval_is_refused(
    scheduled_watch: ScheduledWatch, tmp_path: Path
) -> None:
    profile = tmp_path / "interval.toml"
    profile.write_text(
        PROFILE_PATH.read_text(encoding="utf-8").replace(
            "refresh_interval_minutes = 60",
            f"refresh_interval_minutes = {UNREPRESENTABLE_CRON_MINUTES}",
        ),
        encoding="utf-8",
    )
    create_watch(profile)
    before = tree_digest(scheduled_watch.workspace)

    cron_code, refused = render_schedule(scheduled_watch, "cron")
    launchd_code, rendered = render_schedule(scheduled_watch, "launchd")

    assert cron_code == 1
    assert refused["error"]["code"] == "monitoring_input_error"
    assert refused["error"]["details"]["reason"] == "cron_interval_unrepresentable"
    assert refused["error"]["details"]["interval_minutes"] == UNREPRESENTABLE_CRON_MINUTES
    assert launchd_code == 0
    assert plistlib.loads(rendered["content"].encode("utf-8"))["StartInterval"] == (
        UNREPRESENTABLE_CRON_MINUTES * 60
    )
    assert tree_digest(scheduled_watch.workspace) == before
