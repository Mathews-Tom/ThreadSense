"""V5 cross-thread issue history through the real CLI, recorded sweeps and real SQLite.

Replays tests/fixtures/watch/issues/ (synthesized Reddit payloads; see its README.md, never
live data) through in-process `threadsense.cli.main` with JSON output: `watch run`, `watch
digest` and the `issues inspect|link|unlink` commands.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import shutil
import sqlite3
import time
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
from threadsense.monitoring import IssuePolicy, MonitoringStore
from threadsense.monitoring.issue_records import issue_id_for, report_id_for
from threadsense.monitoring.state import _SCHEMA_STATEMENTS, STATE_SCHEMA_VERSION

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "watch" / "issues"
PROFILE_PATH = FIXTURE_DIR / "profile.toml"
PHASES_PATH = FIXTURE_DIR / "phases.json"
WATCH_ID = "export-issues"
OTHER_WATCH_ID = "export-issues-copy"
UNKNOWN_WATCH_ID = "no-such-watch"
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
ORIGIN_REPORT = report_id_for(WATCH_ID, THREAD_A, "reddit:ia1")
MISSING_ISSUE = "iss-" + "0" * 32
PRE_VIEW_SCHEMA_VERSION = 4
DEFAULT_POLICY = IssuePolicy(30, 25, 3)
PRESENTING_PHASES = ("initial", "repost", "updates")
UPDATE_COMMENTS = {
    "reproduction": "reddit:ia3",
    "contradiction": "reddit:ia4",
    "workaround": "reddit:ia5",
    "resolution_claim": "reddit:ia6",
}


class ProcessDeathError(BaseException):
    """Simulates the process dying mid-digest: nothing in the CLI may handle it."""


# CLI harness: in-process `threadsense.cli.main` against isolated storage.


def isolate_environment(monkeypatch: pytest.MonkeyPatch, workspace: Path) -> None:
    monkeypatch.chdir(workspace)
    monkeypatch.delenv("THREADSENSE_CONFIG", raising=False)
    monkeypatch.delenv("THREADSENSE_STORAGE_MONITORING_DIR", raising=False)
    for name in (
        "THREADSENSE_ISSUES_CANDIDATE_WINDOW_DAYS",
        "THREADSENSE_ISSUES_MAX_CANDIDATE_COMPARISONS",
        "THREADSENSE_ISSUES_MAX_SUGGESTIONS_PER_THREAD",
    ):
        monkeypatch.delenv(name, raising=False)
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
    payload: dict[str, Any] = json.loads(buffer.getvalue())
    return exit_code, payload


def create_watch(profile_path: Path = PROFILE_PATH) -> Path:
    exit_code, created = run_cli("watch", "create", "--profile", str(profile_path))
    assert exit_code == 0, created
    return Path(created["state_path"])


def write_other_profile(directory: Path) -> Path:
    """The fixture profile under a different watch id."""
    text = PROFILE_PATH.read_text(encoding="utf-8")
    original = f'\nid = "{WATCH_ID}"\n'
    assert text.count(original) == 1
    path = directory / "other_profile.toml"
    path.write_text(text.replace(original, f'\nid = "{OTHER_WATCH_ID}"\n'), encoding="utf-8")
    return path


def install_transport(monkeypatch: pytest.MonkeyPatch, phase: ReplayPhase) -> None:
    monkeypatch.setattr(
        "threadsense.cli.build_reddit_connector",
        replay_connector_factory(PhaseReplayTransport(phase)),
    )


def phases_by_name() -> dict[str, ReplayPhase]:
    return {phase.name: phase for phase in load_replay_phases(PHASES_PATH)}


def watch_run(monkeypatch: pytest.MonkeyPatch, phase_name: str) -> dict[str, Any]:
    install_transport(monkeypatch, phases_by_name()[phase_name])
    exit_code, payload = run_cli("watch", "run", "--id", WATCH_ID)
    assert exit_code == 0, payload
    return payload


def inspect_issue(issue_id: str) -> dict[str, Any]:
    exit_code, payload = run_cli("issues", "inspect", "--watch", WATCH_ID, "--issue", issue_id)
    assert exit_code == 0, payload
    return payload


def membership(command: str, issue_id: str, thread_id: str, reason: str) -> dict[str, Any]:
    exit_code, payload = run_cli(
        "issues",
        command,
        "--watch",
        WATCH_ID,
        "--issue",
        issue_id,
        "--thread",
        thread_id,
        "--reason",
        reason,
    )
    assert exit_code == 0, payload
    return payload


def query_rows(state_path: Path, sql: str) -> list[tuple[Any, ...]]:
    uri = f"{state_path.as_uri()}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as conn:
        return [tuple(row) for row in conn.execute(sql).fetchall()]


def membership_event_count(state_path: Path) -> int:
    [(count,)] = query_rows(state_path, "SELECT COUNT(*) FROM watch_issue_memberships")
    return int(count)


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def artifact_hashes(monitoring_root: Path) -> dict[str, str]:
    """Hash every file under `watches/`, keyed by monitoring-root-relative path."""
    return {
        path.relative_to(monitoring_root).as_posix(): sha256_file(path)
        for path in sorted((monitoring_root / "watches").rglob("*"))
        if path.is_file()
    }


def suggestions_between(view: dict[str, Any], first: str, second: str) -> list[dict[str, Any]]:
    return [
        suggestion
        for suggestion in view["suggestions"]
        if {suggestion["source_thread_id"], suggestion["candidate_thread_id"]} == {first, second}
    ]


def observation_counts(view: dict[str, Any]) -> dict[str, int]:
    counts = view["counts"]
    keys = (
        "observed_threads",
        "observed_communities",
        "observed_accounts",
        "reports",
        "firsthand_reports",
        "exact_copies",
    )
    return {key: int(counts[key]) for key in keys}


def history_shape(view: dict[str, Any]) -> list[tuple[str, str, str, str | None]]:
    return [
        (
            event["source_thread_id"],
            event["action"],
            event["issue_id"],
            event["previous_issue_id"],
        )
        for event in view["membership_history"]
    ]


# Journey: one `watch run` per recorded phase, inspected after each.


@dataclass(frozen=True)
class PhaseRun:
    exit_code: int
    payload: dict[str, Any]
    digest_json: dict[str, Any] | None
    digest_markdown: str | None
    issue_list: dict[str, Any]
    inspections: dict[str, dict[str, Any]]

    @property
    def digest(self) -> dict[str, Any]:
        digest: dict[str, Any] = self.payload["digest"]
        return digest

    def presented_json(self) -> dict[str, Any]:
        assert self.digest_json is not None
        return self.digest_json

    def presented_markdown(self) -> str:
        assert self.digest_markdown is not None
        return self.digest_markdown


def run_journey_phase(monkeypatch: pytest.MonkeyPatch, phase: ReplayPhase) -> PhaseRun:
    install_transport(monkeypatch, phase)
    exit_code, payload = run_cli("watch", "run", "--id", WATCH_ID)
    digest = payload["digest"]
    digest_json: dict[str, Any] | None = None
    digest_markdown: str | None = None
    if digest["status"] == "presented":
        digest_json = json.loads(Path(digest["json_path"]).read_text(encoding="utf-8"))
        digest_markdown = Path(digest["markdown_path"]).read_text(encoding="utf-8")
    list_code, issue_list = run_cli("issues", "inspect", "--watch", WATCH_ID)
    assert list_code == 0, issue_list
    inspections = {
        str(issue["issue_id"]): inspect_issue(str(issue["issue_id"]))
        for issue in issue_list["issues"]
    }
    return PhaseRun(
        exit_code=exit_code,
        payload=payload,
        digest_json=digest_json,
        digest_markdown=digest_markdown,
        issue_list=issue_list,
        inspections=inspections,
    )


@pytest.fixture(scope="module")
def journey(tmp_path_factory: pytest.TempPathFactory) -> dict[str, PhaseRun]:
    with pytest.MonkeyPatch.context() as monkeypatch:
        isolate_environment(monkeypatch, tmp_path_factory.mktemp("journey"))
        create_watch()
        return {
            phase.name: run_journey_phase(monkeypatch, phase)
            for phase in load_replay_phases(PHASES_PATH)
        }


def finding_refs(digest_json: dict[str, Any], thread_id: str) -> list[dict[str, Any]]:
    return [
        ref
        for ref in digest_json["issues"]["entry_refs"]
        if ref["source_thread_id"] == thread_id and ref["kind"] == "finding"
    ]


def test_watch_run_issue_journey_presents_schema_three_digests_with_issue_view(
    journey: dict[str, PhaseRun],
) -> None:
    for name in PRESENTING_PHASES:
        step = journey[name]
        digest_json = step.presented_json()
        entry_ids = [entry["entry_id"] for entry in digest_json["entries"]]
        ranked_ids = [item["entry_id"] for item in digest_json["ranking"]["order"]]

        assert step.exit_code == 0, name
        assert step.digest["status"] == "presented", name
        assert digest_json["schema_version"] == 3, name
        assert sorted(ranked_ids) == sorted(entry_ids), name
        assert digest_json["issues"]["semantic_grouping"] == "not_eligible", name
        assert step.digest["issues"]["policy_sha256"] == DEFAULT_POLICY.sha256(), name
    initial = journey["initial"].digest["issues"]
    assert (initial["threads_created"], initial["comparisons"]) == (5, 6)
    unchanged = journey["unchanged"]
    assert unchanged.exit_code == 0
    assert (unchanged.digest["status"], unchanged.digest["digest_id"]) == ("no_entries", None)
    assert unchanged.digest_json is None
    assert unchanged.digest["issues"]["entries"] == 0


def test_watch_run_repost_digest_cites_exact_copy_in_origin_issue(
    journey: dict[str, PhaseRun],
) -> None:
    digest_json = journey["repost"].presented_json()
    refs = finding_refs(digest_json, THREAD_C)

    assert refs
    assert all((ref["issue_id"], ref["home_issue_id"]) == (ISSUE_A, ISSUE_C) for ref in refs)
    copies = [report for ref in refs for report in ref["reports"]]
    [copy] = [report for report in copies if report["comment_id"] == "reddit:ic1"]
    assert (copy["lineage"], copy["copy_of_report_id"]) == ("exact_copy", ORIGIN_REPORT)
    [summary] = [issue for issue in digest_json["issues"]["issues"] if issue["issue_id"] == ISSUE_A]
    assert summary["member_thread_ids"] == sorted([THREAD_A, THREAD_C])
    assert (summary["counts"]["firsthand_reports"], summary["counts"]["exact_copies"]) == (2, 1)
    assert f"- Exact copy: reddit:ic1 copies {ORIGIN_REPORT}" in (
        journey["repost"].presented_markdown()
    )


def test_watch_run_updates_digest_entry_refs_carry_each_update_kind(
    journey: dict[str, PhaseRun],
) -> None:
    digest_json = journey["updates"].presented_json()
    updates = [update for ref in finding_refs(digest_json, THREAD_A) for update in ref["updates"]]

    assert {update["kind"]: update["comment_id"] for update in updates} == UPDATE_COMMENTS
    assert len(updates) == len(UPDATE_COMMENTS)
    assert all(ref["issue_id"] == ISSUE_A for ref in digest_json["issues"]["entry_refs"])
    [summary] = digest_json["issues"]["issues"]
    assert summary["update_counts"] == dict.fromkeys(UPDATE_COMMENTS, 1)


def test_watch_run_digest_markdown_states_unverified_independence_and_unresolved_issue(
    journey: dict[str, PhaseRun],
) -> None:
    for name in PRESENTING_PHASES:
        markdown = journey[name].presented_markdown()

        assert "\n## Issues\n" in markdown, name
        assert "independence: not verified" in markdown, name
        assert "(unverified; issue not resolved)" in markdown, name
        assert f"- Issue: {ISSUE_A}" in markdown, name
    assert "- Update: workaround in reddit:ia5 (cue " in journey["updates"].presented_markdown()


def test_issues_inspect_initial_list_reports_one_active_issue_per_thread(
    journey: dict[str, PhaseRun],
) -> None:
    issue_list = journey["initial"].issue_list

    assert (issue_list["artifact_type"], issue_list["status"]) == ("issue_list", "ok")
    assert issue_list["watch_id"] == WATCH_ID
    assert sorted(
        (issue["issue_id"], issue["state"], issue["member_thread_ids"])
        for issue in issue_list["issues"]
    ) == sorted((issue_id_for(WATCH_ID, t), "active", [t]) for t in INITIAL_THREADS)
    by_id = {issue["issue_id"]: issue for issue in issue_list["issues"]}
    assert by_id[ISSUE_A]["unconfirmed_suggestions"] >= 1
    assert by_id[ISSUE_F]["unconfirmed_suggestions"] == 0


def test_issues_inspect_initial_suggested_pair_stays_unconfirmed_with_separate_counts(
    journey: dict[str, PhaseRun],
) -> None:
    view_a = journey["initial"].inspections[ISSUE_A]
    view_b = journey["initial"].inspections[ISSUE_B]

    assert (view_a["artifact_type"], view_a["status"], view_a["issue_status"]) == (
        "issue_inspection",
        "ok",
        "open",
    )
    [pair] = suggestions_between(view_a, THREAD_A, THREAD_B)
    assert (pair["status"], pair["refusal_reason"], pair["currently_grouped"]) == (
        "unconfirmed",
        None,
        False,
    )
    assert pair["signals"]["themes"] == ["reliability"]
    single_thread = {
        "observed_threads": 1,
        "observed_communities": 1,
        "observed_accounts": 2,
        "reports": 2,
        "firsthand_reports": 2,
        "exact_copies": 0,
    }
    assert observation_counts(view_a) == observation_counts(view_b) == single_thread
    for view in (view_a, view_b):
        independence = view["counts"]["independence"]
        assert independence["status"] == "not_verified"
        assert independence["verified_independent_reports"] is None


def test_issues_inspect_initial_shared_link_pair_refused_and_distant_thread_unsuggested(
    journey: dict[str, PhaseRun],
) -> None:
    inspections = journey["initial"].inspections

    [refusal] = suggestions_between(inspections[ISSUE_D], THREAD_D, THREAD_E)
    assert (refusal["status"], refusal["refusal_reason"]) == ("refused", "shared_link_only")
    assert inspections[ISSUE_F]["suggestions"] == []


def test_issues_inspect_updates_issue_counts_observations_without_verifying_independence(
    journey: dict[str, PhaseRun],
) -> None:
    view_a = journey["updates"].inspections[ISSUE_A]

    assert observation_counts(view_a) == {
        "observed_threads": 2,
        "observed_communities": 2,
        "observed_accounts": 5,
        "reports": 7,
        "firsthand_reports": 6,
        "exact_copies": 1,
    }
    assert view_a["counts"]["independence"]["verified_independent_reports"] is None
    assert view_a["resolution"]["resolved"] is False
    assert view_a["resolution"]["resolution_claims"] == 1
    assert journey["updates"].inspections[ISSUE_C]["state"] == "inactive"


# Operator link and unlink over a copied post-repost workspace.


@dataclass(frozen=True)
class SeededWorkspace:
    root: Path
    state_relpath: Path


@pytest.fixture(scope="module")
def repost_workspace(tmp_path_factory: pytest.TempPathFactory) -> SeededWorkspace:
    root = tmp_path_factory.mktemp("repost")
    with pytest.MonkeyPatch.context() as monkeypatch:
        isolate_environment(monkeypatch, root)
        state_path = create_watch()
        create_watch(write_other_profile(root))
        watch_run(monkeypatch, "initial")
        watch_run(monkeypatch, "repost")
    return SeededWorkspace(root=root, state_relpath=state_path.relative_to(root))


@pytest.fixture
def repost_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, repost_workspace: SeededWorkspace
) -> Path:
    root = tmp_path / "workspace"
    shutil.copytree(repost_workspace.root, root)
    state_path = root / repost_workspace.state_relpath
    isolate_environment(monkeypatch, root)
    return state_path


def test_issues_inspect_leaves_state_bytes_unchanged(repost_state: Path) -> None:
    before = repost_state.read_bytes()

    list_code, _ = run_cli("issues", "inspect", "--watch", WATCH_ID)
    inspect_issue(ISSUE_A)

    assert list_code == 0
    assert repost_state.read_bytes() == before


def test_issues_link_thread_into_issue_adds_firsthand_reports_and_keeps_copies(
    repost_state: Path,
) -> None:
    linked = membership("link", ISSUE_A, THREAD_B, "  same markdown export crash  ")

    assert (linked["artifact_type"], linked["status"]) == ("issue_membership_change", "linked")
    event = linked["event"]
    assert (event["action"], event["basis"], event["reason"]) == (
        "linked",
        "operator",
        "same markdown export crash",
    )
    assert (event["issue_id"], event["previous_issue_id"]) == (ISSUE_A, ISSUE_B)
    assert linked["membership"] == {
        "source_thread_id": THREAD_B,
        "issue_id": ISSUE_A,
        "home_issue_id": ISSUE_B,
    }
    view_a = inspect_issue(ISSUE_A)
    assert observation_counts(view_a) == {
        "observed_threads": 3,
        "observed_communities": 2,
        "observed_accounts": 5,
        "reports": 5,
        "firsthand_reports": 4,
        "exact_copies": 1,
    }
    [pair] = suggestions_between(view_a, THREAD_A, THREAD_B)
    assert (pair["status"], pair["currently_grouped"]) == ("unconfirmed", True)
    view_b = inspect_issue(ISSUE_B)
    assert (view_b["state"], view_b["members"]) == ("inactive", [])


def test_issues_unlink_thread_restores_counts_and_records_history_in_order(
    repost_state: Path,
) -> None:
    before_a = observation_counts(inspect_issue(ISSUE_A))
    before_b = observation_counts(inspect_issue(ISSUE_B))
    linked = membership("link", ISSUE_A, THREAD_B, "same crash")

    unlinked = membership("unlink", ISSUE_A, THREAD_B, "different trigger")

    assert unlinked["status"] == "unlinked"
    assert unlinked["membership"] == {
        "source_thread_id": THREAD_B,
        "issue_id": ISSUE_B,
        "home_issue_id": ISSUE_B,
    }
    view_a = inspect_issue(ISSUE_A)
    assert observation_counts(view_a) == before_a
    assert (before_a["firsthand_reports"], before_a["exact_copies"]) == (2, 1)
    assert observation_counts(inspect_issue(ISSUE_B)) == before_b
    history = view_a["membership_history"]
    assert history[-2:] == [linked["event"], unlinked["event"]]
    assert history_shape(view_a)[-2:] == [
        (THREAD_B, "linked", ISSUE_A, ISSUE_B),
        (THREAD_B, "unlinked", ISSUE_B, ISSUE_A),
    ]
    assert [event["event_id"] for event in history] == sorted(
        event["event_id"] for event in history
    )


def membership_args(command: str, watch_id: str, issue_id: str, thread_id: str) -> tuple[str, ...]:
    return (
        "issues",
        command,
        "--watch",
        watch_id,
        "--issue",
        issue_id,
        "--thread",
        thread_id,
        "--reason",
        "operator review",
    )


REFUSALS: tuple[tuple[str, tuple[str, ...], str], ...] = (
    ("unknown_issue", membership_args("link", WATCH_ID, MISSING_ISSUE, THREAD_B), "unknown_issue"),
    (
        "cross_watch_issue",
        membership_args("link", OTHER_WATCH_ID, ISSUE_A, THREAD_B),
        "unknown_issue",
    ),
    (
        "cross_watch_inspect",
        ("issues", "inspect", "--watch", OTHER_WATCH_ID, "--issue", ISSUE_A),
        "unknown_issue",
    ),
    (
        "malformed_issue_id",
        membership_args("link", WATCH_ID, "iss-NOT-AN-ID", THREAD_B),
        "invalid_issue_request",
    ),
    (
        "blank_reason",
        (*membership_args("link", WATCH_ID, ISSUE_A, THREAD_B)[:-1], "   "),
        "invalid_issue_request",
    ),
    ("already_member", membership_args("link", WATCH_ID, ISSUE_A, THREAD_C), "already_member"),
    ("inactive_issue", membership_args("link", WATCH_ID, ISSUE_C, THREAD_B), "inactive_issue"),
    (
        "issue_has_other_members",
        membership_args("link", WATCH_ID, ISSUE_B, THREAD_A),
        "issue_has_other_members",
    ),
    ("not_member", membership_args("unlink", WATCH_ID, ISSUE_A, THREAD_B), "not_member"),
    ("not_linked", membership_args("unlink", WATCH_ID, ISSUE_A, THREAD_A), "not_linked"),
    (
        "unknown_watch",
        membership_args("link", UNKNOWN_WATCH_ID, ISSUE_A, THREAD_B),
        "unknown_watch",
    ),
    (
        "unknown_watch_inspect",
        ("issues", "inspect", "--watch", UNKNOWN_WATCH_ID),
        "unknown_watch",
    ),
)


@pytest.mark.parametrize(
    ("argv", "reason"),
    [pytest.param(argv, reason, id=case) for case, argv, reason in REFUSALS],
)
def test_issues_command_refusal_exits_one_with_reason_and_no_membership_event(
    repost_state: Path, argv: tuple[str, ...], reason: str
) -> None:
    events_before = membership_event_count(repost_state)

    exit_code, payload = run_cli(*argv)

    assert exit_code == 1
    assert payload["status"] == "error"
    assert payload["error"]["details"]["reason"] == reason
    assert membership_event_count(repost_state) == events_before


def test_issues_link_unlink_and_unchanged_run_keep_prior_artifacts_byte_identical(
    repost_state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    watch_run(monkeypatch, "updates")
    monitoring_root = repost_state.parent
    before = artifact_hashes(monitoring_root)

    membership("link", ISSUE_A, THREAD_B, "same crash")
    unchanged = watch_run(monkeypatch, "unchanged")
    membership("unlink", ISSUE_A, THREAD_B, "different trigger")

    after = artifact_hashes(monitoring_root)
    assert sum("/digests/" in path for path in before) == 2 * len(PRESENTING_PHASES)
    assert before.items() <= after.items()
    assert (unchanged["digest"]["status"], unchanged["digest"]["digest_id"]) == (
        "no_entries",
        None,
    )
    assert sum("/digests/" in path for path in after) == 2 * len(PRESENTING_PHASES)


# Digest crash after artifact rendering, before completion.


@dataclass(frozen=True)
class CrashedDigest:
    state_path: Path
    digest_id: str
    json_path: Path
    markdown_path: Path
    json_bytes: bytes
    markdown_bytes: bytes


def crash_initial_digest(monkeypatch: pytest.MonkeyPatch, workspace: Path) -> CrashedDigest:
    isolate_environment(monkeypatch, workspace)
    state_path = create_watch()
    install_transport(monkeypatch, phases_by_name()["initial"])

    def die(*args: Any, **kwargs: Any) -> None:
        raise ProcessDeathError

    with monkeypatch.context() as patch:
        patch.setattr(MonitoringStore, "complete_digest", die)
        with pytest.raises(ProcessDeathError):
            run_cli("watch", "run", "--id", WATCH_ID)
    [(digest_id, status, json_rel, markdown_rel)] = query_rows(
        state_path, "SELECT digest_id, status, json_path, markdown_path FROM watch_digests"
    )
    assert status == "pending"
    json_path = state_path.parent / str(json_rel)
    markdown_path = state_path.parent / str(markdown_rel)
    return CrashedDigest(
        state_path=state_path,
        digest_id=str(digest_id),
        json_path=json_path,
        markdown_path=markdown_path,
        json_bytes=json_path.read_bytes(),
        markdown_bytes=markdown_path.read_bytes(),
    )


def expire_dead_claim(monkeypatch: pytest.MonkeyPatch, state_path: Path) -> None:
    """Run the CLI store on a clock past the dead digest run's lease."""
    [(expires,)] = query_rows(
        state_path, "SELECT MAX(lease_expires_at_utc) FROM watch_runs WHERE status = 'running'"
    )
    assert expires is not None
    offset = float(expires) + 1.0 - time.time()
    monkeypatch.setattr(
        "threadsense.cli.MonitoringStore",
        partial(MonitoringStore, clock=lambda: time.time() + offset),
    )


def resume_digest(crashed: CrashedDigest) -> dict[str, Any]:
    exit_code, payload = run_cli("watch", "digest", "--id", WATCH_ID)
    assert exit_code == 0, payload
    assert (payload["status"], payload["resumed"]) == ("presented", True)
    assert payload["digest_id"] == crashed.digest_id
    [(status,)] = query_rows(crashed.state_path, "SELECT status FROM watch_digests")
    assert status == "written"
    return payload


def test_watch_digest_resume_after_crash_renders_identical_issue_view_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    crashed = crash_initial_digest(monkeypatch, tmp_path)
    expire_dead_claim(monkeypatch, crashed.state_path)

    resume_digest(crashed)

    assert crashed.json_path.read_bytes() == crashed.json_bytes
    assert crashed.markdown_path.read_bytes() == crashed.markdown_bytes
    resumed = json.loads(crashed.json_path.read_text(encoding="utf-8"))
    assert resumed["schema_version"] == 3
    assert sorted(item["entry_id"] for item in resumed["ranking"]["order"]) == sorted(
        entry["entry_id"] for entry in resumed["entries"]
    )
    assert {ref["issue_id"] for ref in resumed["issues"]["entry_refs"]} == {
        issue_id_for(WATCH_ID, thread) for thread in INITIAL_THREADS
    }


def test_watch_digest_resume_pre_schema_five_pending_digest_renders_schema_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    crashed = crash_initial_digest(monkeypatch, tmp_path)
    # A digest left pending by a pre-schema-5 build never wrote schema 2 artifacts.
    crashed.json_path.unlink()
    crashed.markdown_path.unlink()
    with closing(sqlite3.connect(crashed.state_path)) as conn:
        # Remove later capture triggers before dropping their source/control tables.
        for schema_version in range(PRE_VIEW_SCHEMA_VERSION + 1, STATE_SCHEMA_VERSION + 1):
            for statement in _SCHEMA_STATEMENTS[schema_version]:
                created_trigger = re.match(r"\s*CREATE TRIGGER (\w+)", statement)
                if created_trigger is not None:
                    conn.execute(f"DROP TRIGGER IF EXISTS {created_trigger.group(1)}")
        for schema_version in range(PRE_VIEW_SCHEMA_VERSION + 1, STATE_SCHEMA_VERSION + 1):
            for statement in _SCHEMA_STATEMENTS[schema_version]:
                created = re.match(r"\s*CREATE TABLE (\w+)", statement)
                if created is not None:
                    conn.execute(f"DROP TABLE {created.group(1)}")
        conn.execute(f"PRAGMA user_version = {PRE_VIEW_SCHEMA_VERSION}")
        conn.commit()
    expire_dead_claim(monkeypatch, crashed.state_path)

    resume_digest(crashed)

    resumed = json.loads(crashed.json_path.read_text(encoding="utf-8"))
    assert resumed["schema_version"] == 1
    assert "issues" not in resumed
    markdown = crashed.markdown_path.read_text(encoding="utf-8")
    assert "## Issues" not in markdown
    assert "- Issue: " not in markdown
    assert query_rows(crashed.state_path, "PRAGMA user_version") == [(STATE_SCHEMA_VERSION,)]
    assert query_rows(
        crashed.state_path,
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'watch_digest_issue_views'",
    ) == [("watch_digest_issue_views",)]


# `[issues]` bounds from the environment.


def run_initial_with_issue_env(
    monkeypatch: pytest.MonkeyPatch, workspace: Path, policy: IssuePolicy
) -> tuple[dict[str, Any], dict[str, Any]]:
    isolate_environment(monkeypatch, workspace)
    monkeypatch.setenv(
        "THREADSENSE_ISSUES_CANDIDATE_WINDOW_DAYS", str(policy.candidate_window_days)
    )
    monkeypatch.setenv(
        "THREADSENSE_ISSUES_MAX_CANDIDATE_COMPARISONS", str(policy.max_candidate_comparisons)
    )
    monkeypatch.setenv(
        "THREADSENSE_ISSUES_MAX_SUGGESTIONS_PER_THREAD", str(policy.max_suggestions_per_thread)
    )
    create_watch()
    payload = watch_run(monkeypatch, "initial")
    return payload["digest"]["issues"], inspect_issue(ISSUE_A)["grouping_policy"]


def assert_grouping_policy(grouping_policy: dict[str, Any], policy: IssuePolicy) -> None:
    assert grouping_policy["sha256"] == policy.sha256()
    candidates = grouping_policy["candidates"]
    assert (
        candidates["window_days"],
        candidates["max_comparisons_per_thread"],
        candidates["max_suggestions_per_thread"],
    ) == (
        policy.candidate_window_days,
        policy.max_candidate_comparisons,
        policy.max_suggestions_per_thread,
    )


def test_watch_run_issue_env_comparison_bound_limits_ingestion_comparisons(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    policy = IssuePolicy(30, 1, 1)

    ingestion, grouping_policy = run_initial_with_issue_env(monkeypatch, tmp_path, policy)

    assert ingestion["policy_sha256"] == policy.sha256()
    # One comparison for each of B, D and E; A has no earlier thread and F none in window.
    assert (ingestion["threads_created"], ingestion["comparisons"]) == (5, 3)
    assert_grouping_policy(grouping_policy, policy)


def test_watch_run_issue_env_suggestion_cap_reports_truncated_suggestions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    policy = IssuePolicy(365, 25, 1)

    ingestion, grouping_policy = run_initial_with_issue_env(monkeypatch, tmp_path, policy)

    assert ingestion["policy_sha256"] == policy.sha256()
    assert ingestion["comparisons"] == 10
    assert ingestion["suggestions_truncated"] >= 1
    assert_grouping_policy(grouping_policy, policy)
