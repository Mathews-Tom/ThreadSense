"""Monitoring smoke scenarios exercised through the real `threadsense` CLI.

Usage:
    uv run python scripts/smoke_monitoring.py --scenario profiles --work-dir DIR
    uv run python scripts/smoke_monitoring.py --scenario incremental --work-dir DIR
    uv run python scripts/smoke_monitoring.py --scenario issues --work-dir DIR
    uv run python scripts/smoke_monitoring.py --scenario feedback --work-dir DIR
    uv run python scripts/smoke_monitoring.py --scenario annotations --work-dir DIR --endpoint URL
    uv run python scripts/smoke_monitoring.py --scenario evaluation --work-dir DIR --endpoint URL

DIR must not exist or be empty. Every CLI call runs in a new interpreter process
against isolated storage under DIR; the profiles, incremental, issues and feedback scenarios
use no network or model access.

The profiles scenario makes run claims through `threadsense.monitoring` in separate
competing processes and observes them through `threadsense watch inspect`.

The incremental scenario drives `threadsense watch run|digest|inspect|schedule`
over the recorded phases in tests/fixtures/watch/incremental/. Each replaying
child installs the in-process recorded Reddit transport from
tests/integration/recorded_reddit.py before calling `threadsense.cli.main`; the
payloads are synthesized Reddit JSON, not live acquisition.

The issues scenario (scripts/smoke_issues.py) drives `threadsense watch run` and
`threadsense issues inspect|link|unlink` over the recorded phases in
tests/fixtures/watch/issues/, served in-process the same way from synthesized Reddit JSON.

The feedback scenario (scripts/smoke_feedback.py) drives `threadsense feedback record|list` and
`threadsense watch run|digest|explain|policy` over the recorded phases in
tests/fixtures/watch/incremental/ against a control and a treated storage, served in-process
the same way from synthesized Reddit JSON.

The annotations scenario (scripts/smoke_annotations.py) drives `threadsense decisions
annotate` against a locally running `laya.serve` given by --endpoint, which is required for
annotations and evaluation scenarios and refused for the others. It uses a committed fixture,
not a live source.

The evaluation scenario (scripts/smoke_evaluation.py) annotates the committed engineering
evaluation fixture against the same kind of --endpoint (also required), then runs `threadsense
decisions evaluate` with the source runtime configuration, conditionally acquiring a local
generative capture. Its second evaluation replays that capture without listing or inference,
or disables acquisition for typed/heuristic-only replay if no capture was produced. Policy
checks run offline. The real result is `not_eligible`; a disclosed constructed test-only fixture
exercises the eligible path, which can never permit activation. It never starts, stops or
configures a service.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import plistlib
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
import time
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from smoke_annotations import (
    AnnotationsScenario,
    SmokeFailure,
    check,
    child_base_env,
    decode_payload,
    file_sha256,
    redact_local_paths,
)
from smoke_evaluation import EvaluationScenario
from smoke_feedback import FeedbackScenario
from smoke_issues import IssuesScenario

from threadsense.models.analysis import load_analysis_artifact_file
from threadsense.monitoring import STATE_SCHEMA_VERSION

WATCH_ID = "agent-tooling"

PROFILE = """\
profile_version = 1
id = "agent-tooling"
name = "Coding agent friction"
domain = "developer_tools"
objective = "friction_analysis"
abstraction_level = "operational"
queries = ["mcp server", "coding agent"]
subreddits = ["r/LocalLLaMA", "ClaudeAI"]
refresh_interval_minutes = 360
include_terms = ["latency"]
exclude_terms = ["giveaway"]

[limits]
discovered_candidates = 50
refreshed_threads = 25
analyzed_revisions = 25
presented_findings = 10
"""

# Same content: different key order, list order, spacing and comments.
REFORMATTED_PROFILE = """\
# reformatted, semantically identical
[limits]
presented_findings = 10
analyzed_revisions = 25
refreshed_threads = 25
discovered_candidates = 50
"""

INVALID_PROFILES = {
    "zero_limit": ("discovered_candidates = 50", "discovered_candidates = 0"),
    "advanced_query": ('"mcp server", "coding agent"', '"mcp server", "title:agent"'),
    "path_traversal_id": ('id = "agent-tooling"', 'id = "../../escaped"'),
    "toml_date_id": ('id = "agent-tooling"', "id = 1979-05-27"),
    "non_finite_limit": ("discovered_candidates = 50", "discovered_candidates = nan"),
}

CLAIM_PROCESS = """
import json, sys, time
from pathlib import Path
from threadsense.errors import RunClaimError
from threadsense.monitoring import MonitoringStore

action, state_path, watch_id, argument, start_at = sys.argv[1:6]
while time.time() < float(start_at):
    time.sleep(0.001)
store = MonitoringStore(Path(state_path))
try:
    if action == "claim":
        claim = store.claim_run(watch_id, lease_seconds=int(argument))
        print(json.dumps({"outcome": "claimed", "run_id": claim.run_id,
                          "owner_token": claim.owner_token,
                          "recovered_run_id": claim.recovered_run_id}))
    else:
        run_id, owner_token = argument.split(":", 1)
        store.finalize_run(run_id, owner_token, "succeeded")
        print(json.dumps({"outcome": "finalized", "run_id": run_id}))
except RunClaimError as error:
    print(json.dumps({"outcome": "refused", "reason": error.details["reason"]}))
"""

# Runs in a separate interpreter: spills a large uncommitted update to the database
# file and exits without rolling back, leaving a genuine hot rollback journal.
CRASH_PROCESS = """
import os, sqlite3, sys
conn = sqlite3.connect(sys.argv[1], isolation_level=None)
conn.execute("PRAGMA cache_size = 5")
conn.execute("BEGIN IMMEDIATE")
conn.execute(sys.argv[2], ("x" * 1000000,))
os._exit(0)
"""
CRASH_UPDATE = "UPDATE watch_revisions SET profile_json = ?"
# Same crashed writer, but it first sets an uncommitted supported user_version, so SQLite
# writes the committed page 1 into the genuine rollback journal before the spill.
CRASH_UNCOMMITTED_VERSION_PROCESS = """
import os, sqlite3, sys
conn = sqlite3.connect(sys.argv[1], isolation_level=None)
conn.execute("PRAGMA cache_size = 5")
conn.execute("BEGIN IMMEDIATE")
conn.execute("PRAGMA user_version = 1")
conn.execute(sys.argv[2], ("x" * 1000000,))
os._exit(0)
"""
# Offset of the big-endian user_version field in the SQLite database header.
USER_VERSION_OFFSET = 60
NEWER_SCHEMA_VERSION = STATE_SCHEMA_VERSION + 1

REPO_ROOT = Path(__file__).resolve().parents[1]
INTEGRATION_DIR = REPO_ROOT / "tests" / "integration"
INCREMENTAL_FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "watch" / "incremental"
INCREMENTAL_PROFILE_PATH = INCREMENTAL_FIXTURE_DIR / "profile.toml"
INCREMENTAL_PHASES_PATH = INCREMENTAL_FIXTURE_DIR / "phases.json"
INCREMENTAL_WATCH_ID = "sync-conflicts"
FIXTURE_PROVENANCE = "synthesized recorded Reddit JSON served in-process; not live acquisition"
THREAD_A = "sc1aaa"
THREAD_B = "sc1bbb"
# profile.toml: limits.presented_findings = 3 and refresh_interval_minutes = 60.
PRESENTATION_LIMIT = 3
REFRESH_INTERVAL_SECONDS = 3600
UNREPRESENTABLE_CRON_MINUTES = 45
# Short leases so crashed claims expire quickly, yet long enough for the child to reach
# its crash boundary (for the digest crash: after the sweep half of `watch run`) while it
# still owns the run.
SWEEP_CRASH_LEASE_SECONDS = 2
DIGEST_CRASH_LEASE_SECONDS = 5
HELD_CLAIM_LEASE_SECONDS = 3600
CRASH_EXIT_CODE = 75
NO_REPLAY = "none"
NO_CRASH = "none"
HOSTILE_WORKSPACE_NAME = "work space 'single' \"double\" 100%"
INCREMENTAL_TABLES = (
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
# Sweep outcome and `watch run` exit code per recorded phase, in phases.json order.
PHASE_OUTCOMES: dict[str, tuple[str, int]] = {
    "initial": ("changes", 0),
    "unchanged": ("no_changes", 0),
    "score_only": ("no_changes", 0),
    "search_omits_known": ("changes", 0),
    "edit": ("changes", 0),
    "partial": ("partial", 1),
    "reappear": ("changes", 0),
    "backlog": ("no_changes", 0),
}
# Revisions each phase creates, keyed by (thread, sequence), with their novel comments.
PHASE_REVISIONS: dict[str, dict[tuple[str, int], list[tuple[str, str]]]] = {
    "initial": {
        (THREAD_A, 1): [(f"reddit:ka{index}", "initial_observation") for index in (1, 2, 3)],
        (THREAD_B, 1): [(f"reddit:kb{index}", "initial_observation") for index in (1, 2)],
    },
    "search_omits_known": {(THREAD_A, 2): [("reddit:ka4", "new_source_event")]},
    "edit": {(THREAD_B, 2): [("reddit:kb2", "edited")]},
    "reappear": {(THREAD_A, 3): [("reddit:ka5", "newly_observed_historical")]},
}

# Runs in a fresh interpreter: serves one recorded phase in-process through the CLI's
# connector seam, optionally dies at a durability boundary, then runs the real CLI.
REPLAY_CHILD = """
import os, sys
from pathlib import Path

integration_dir, phases_path, phase_name, crash, *argv = sys.argv[1:]
sys.path.insert(0, integration_dir)

import threadsense.cli
from recorded_reddit import PhaseReplayTransport, load_replay_phases, replay_connector_factory
from threadsense.monitoring import MonitoringStore


def refuse_reddit(config):
    raise RuntimeError("this command must not request Reddit")


def die(*args, **kwargs):
    sys.stdout.flush()
    os._exit(75)


if phase_name == "none":
    threadsense.cli.build_reddit_connector = refuse_reddit
else:
    phases = {phase.name: phase for phase in load_replay_phases(Path(phases_path))}
    transport = PhaseReplayTransport(phases[phase_name])
    threadsense.cli.build_reddit_connector = replay_connector_factory(transport)
if crash in ("record_acquisition", "complete_digest"):
    setattr(MonitoringStore, crash, die)
elif crash != "none":
    raise SystemExit(f"unknown crash point {crash}")
sys.exit(threadsense.cli.main(argv))
"""


def directory_digest(directory: Path) -> dict[str, str]:
    return {entry.name: file_sha256(entry) for entry in sorted(directory.iterdir())}


class ProfilesScenario:
    def __init__(self, work_dir: Path) -> None:
        self.work_dir = work_dir
        self.store_root = work_dir / "store"
        self.state_path = self.store_root.resolve() / "monitoring" / "state.sqlite3"
        self.evidence: list[dict[str, Any]] = []
        self.base_env = child_base_env()

    def record(self, step: str, **observed: Any) -> None:
        self.evidence.append({"step": step, "observed": observed})
        print(f"step={step} ok")

    def write_profile(self, name: str, text: str) -> Path:
        path = self.work_dir / "profiles" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return path

    def cli(self, *args: str, store_root: Path | None = None) -> tuple[int, dict[str, Any]]:
        env = {**self.base_env, "THREADSENSE_STORAGE_ROOT": str(store_root or self.store_root)}
        completed = subprocess.run(
            [sys.executable, "-m", "threadsense", "--output-format", "json", *args],
            capture_output=True,
            text=True,
            check=False,
            cwd=self.work_dir,
            env=env,
            timeout=60,
        )
        return completed.returncode, decode_payload(
            completed.stdout, completed.stderr, " ".join(args)
        )

    def inspect(self) -> dict[str, Any]:
        code, payload = self.cli("watch", "inspect", "--id", WATCH_ID)
        check(code == 0, f"watch inspect failed: {payload}")
        return payload

    def state_processes(self, *argvs: list[str]) -> list[dict[str, Any]]:
        start_at = time.time() + 2.0
        processes = [
            subprocess.Popen(
                [sys.executable, "-c", CLAIM_PROCESS, *argv, str(start_at)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=self.base_env,
            )
            for argv in argvs
        ]
        results: list[dict[str, Any]] = []
        for process in processes:
            stdout, stderr = process.communicate(timeout=60)
            check(process.returncode == 0, f"claim process failed: {stderr[-500:]}")
            results.append(json.loads(stdout))
        return results

    def run(self) -> None:
        self.persist_and_restart()
        self.identical_content_is_unchanged()
        self.edit_creates_revision()
        self.invalid_profiles_fail_before_writes()
        winner = self.competing_claims_admit_one_owner()
        current = self.expired_claim_is_recovered(winner)
        self.stale_owner_is_refused(winner, current)
        self.newer_schema_is_refused_unmodified()
        self.crash_journal_is_recovered("list")
        self.crash_journal_is_recovered("create")
        self.unsupported_crash_journal_is_refused_unmodified()
        self.empty_state_file_reads_as_empty()

    def persist_and_restart(self) -> None:
        code, created = self.cli(
            "watch", "create", "--profile", str(self.write_profile("v1.toml", PROFILE))
        )
        check(code == 0 and created["status"] == "created", f"create failed: {created}")
        check(created["revision"] == 1, "first save must be revision 1")
        check(Path(created["state_path"]) == self.state_path, "state path is not under storage")
        code, listed = self.cli("watch", "list")
        check(code == 0, f"watch list failed: {listed}")
        check([w["watch_id"] for w in listed["watches"]] == [WATCH_ID], "listed watches differ")
        profile = self.inspect()["profile"]
        check(profile["queries"] == ["coding agent", "mcp server"], "queries not persisted")
        check(profile["subreddits"] == ["ClaudeAI", "LocalLLaMA"], "subreddits not persisted")
        check(profile["limits"]["presented_findings"] == 10, "limits not persisted")
        self.record(
            "persist_and_restart",
            revision=created["revision"],
            content_sha256=created["content_sha256"],
            reloaded_profile=profile,
        )

    def identical_content_is_unchanged(self) -> None:
        before = file_sha256(self.state_path)
        body = PROFILE.split("[limits]", 1)[0]
        reordered_body = "\n".join(reversed(body.strip().splitlines())).replace(
            '["mcp server", "coding agent"]', '[ "coding agent" , "mcp server" ]'
        )
        path = self.write_profile("reformatted.toml", reordered_body + "\n" + REFORMATTED_PROFILE)
        code, result = self.cli("watch", "create", "--profile", str(path))
        check(code == 0 and result["status"] == "unchanged", f"identical content: {result}")
        check(result["revision"] == 1, "identical content changed the revision")
        check(file_sha256(self.state_path) == before, "identical content modified the state file")
        check(len(self.inspect()["revisions"]) == 1, "identical content added a revision")
        self.record("identical_content_unchanged", status=result["status"], state_sha256=before)

    def edit_creates_revision(self) -> None:
        edited = PROFILE.replace("refresh_interval_minutes = 360", "refresh_interval_minutes = 720")
        code, result = self.cli(
            "watch", "create", "--profile", str(self.write_profile("edit.toml", edited))
        )
        check(code == 0 and result["status"] == "revised", f"edit: {result}")
        check(result["revision"] == 2, "edit must create revision 2")
        inspected = self.inspect()
        hashes = [item["content_sha256"] for item in inspected["revisions"]]
        check(len(set(hashes)) == 2, "revisions must have distinct content hashes")
        check(inspected["profile"]["refresh_interval_minutes"] == 720, "edit not current")
        self.record("edit_revised", revision=result["revision"], revision_hashes=hashes)

    def invalid_profiles_fail_before_writes(self) -> None:
        before = file_sha256(self.state_path)
        refused: dict[str, Any] = {}
        for name, (original, replacement) in INVALID_PROFILES.items():
            path = self.write_profile(
                f"invalid-{name}.toml", PROFILE.replace(original, replacement)
            )
            fresh_root = self.work_dir / f"fresh-{name}"
            payloads = [
                self.cli("watch", "create", "--profile", str(path), store_root=root)
                for root in (self.store_root, fresh_root)
            ]
            for code, payload in payloads:
                check(code == 1, f"{name} was accepted")
                check(payload["error"]["code"] == "monitoring_input_error", f"{name}: {payload}")
            check(not fresh_root.exists(), f"{name} created storage before failing")
            check(not (self.work_dir / "escaped").exists(), f"{name} escaped the work dir")
            refused[name] = payloads[0][1]["error"]["details"]
        check(file_sha256(self.state_path) == before, "invalid profile modified the state file")
        self.record("invalid_profiles_refused", refused=refused)

    def competing_claims_admit_one_owner(self) -> dict[str, Any]:
        path = str(self.state_path)
        outcomes = self.state_processes(*[["claim", path, WATCH_ID, "5"] for _ in range(5)])
        winners = [item for item in outcomes if item["outcome"] == "claimed"]
        reasons = sorted(item.get("reason", "") for item in outcomes if item not in winners)
        check(len(winners) == 1, f"expected one owner, got {outcomes}")
        check(reasons == ["claim_held"] * 4, f"competitors were not refused: {outcomes}")
        active = self.inspect()["active_run"]
        check(active is not None and active["run_id"] == winners[0]["run_id"], "wrong owner")
        self.record("competing_claims", claimed=1, refused=reasons, owner=active["run_id"])
        return {**winners[0], "lease_expires_at_utc": active["lease_expires_at_utc"]}

    def expired_claim_is_recovered(self, stale: dict[str, Any]) -> dict[str, Any]:
        time.sleep(max(0.0, stale["lease_expires_at_utc"] - time.time()) + 0.2)
        (current,) = self.state_processes(["claim", str(self.state_path), WATCH_ID, "600"])
        check(current["outcome"] == "claimed", f"recovery claim failed: {current}")
        check(current["recovered_run_id"] == stale["run_id"], "expired run was not recovered")
        self.record("expired_claim_recovered", recovered=stale["run_id"], owner=current["run_id"])
        return current

    def stale_owner_is_refused(self, stale: dict[str, Any], current: dict[str, Any]) -> None:
        path = str(self.state_path)
        (refused,) = self.state_processes(
            ["finalize", path, WATCH_ID, f"{stale['run_id']}:{stale['owner_token']}"]
        )
        check(refused == {"outcome": "refused", "reason": "stale_owner"}, f"stale: {refused}")
        runs = {run["run_id"]: run for run in self.inspect()["recent_runs"]}
        check(runs[stale["run_id"]]["status"] == "abandoned", "stale run not abandoned")
        check(
            runs[stale["run_id"]]["recovered_by_run_id"] == current["run_id"],
            "abandoned run does not name its recovering run",
        )
        (finalized,) = self.state_processes(
            ["finalize", path, WATCH_ID, f"{current['run_id']}:{current['owner_token']}"]
        )
        check(finalized["outcome"] == "finalized", f"current owner finalize: {finalized}")
        inspected = self.inspect()
        statuses = {run["run_id"]: run["status"] for run in inspected["recent_runs"]}
        check(inspected["active_run"] is None, "finalized run still active")
        check(statuses[current["run_id"]] == "succeeded", "current run not succeeded")
        self.record("stale_owner_refused", refusal=refused["reason"], run_statuses=statuses)

    def newer_schema_is_refused_unmodified(self) -> None:
        newer_root = self.work_dir / "newer-store"
        newer_state = newer_root / "monitoring" / "state.sqlite3"
        newer_state.parent.mkdir(parents=True)
        shutil.copyfile(self.state_path, newer_state)
        with closing(sqlite3.connect(newer_state)) as conn:
            conn.execute(f"PRAGMA user_version = {NEWER_SCHEMA_VERSION}")
            conn.commit()
        before = directory_digest(newer_state.parent)
        profile = str(self.work_dir / "profiles" / "v1.toml")
        reasons: list[str] = []
        for args in (
            ("watch", "create", "--profile", profile),
            ("watch", "list"),
            ("watch", "inspect", "--id", WATCH_ID),
        ):
            code, payload = self.cli(*args, store_root=newer_root)
            check(code == 1, f"newer schema accepted by {args[1]}")
            reasons.append(payload["error"]["details"]["reason"])
        check(set(reasons) == {"unsupported_schema_version"}, f"wrong refusal: {reasons}")
        check(directory_digest(newer_state.parent) == before, "newer schema state was modified")
        self.record("newer_schema_refused", reasons=reasons, files=before)

    def store_copy(self, name: str) -> tuple[Path, Path]:
        root = self.work_dir / name
        state = root / "monitoring" / "state.sqlite3"
        state.parent.mkdir(parents=True)
        shutil.copyfile(self.state_path, state)
        return root, state

    def crash_writer(self, state: Path, statement: str, process: str = CRASH_PROCESS) -> int:
        subprocess.run(
            [sys.executable, "-c", process, str(state), statement],
            check=True,
            env=self.base_env,
            timeout=60,
        )
        journal = Path(f"{state}-journal")
        check(journal.is_file() and journal.stat().st_size > 512, "crash left no hot journal")
        return journal.stat().st_size

    def crash_journal_is_recovered(self, first_command: str) -> None:
        expected = self.inspect()
        root, state = self.store_copy(f"crash-{first_command}-store")
        journal_size = self.crash_writer(state, CRASH_UPDATE)
        profile = str(self.work_dir / "profiles" / "edit.toml")
        commands = {
            "list": ("watch", "list"),
            "create": ("watch", "create", "--profile", profile),
            "inspect": ("watch", "inspect", "--id", WATCH_ID),
        }
        order = [first_command, *(name for name in commands if name != first_command)]
        payloads: dict[str, dict[str, Any]] = {}
        for name in order:
            code, payloads[name] = self.cli(*commands[name], store_root=root)
            check(code == 0, f"watch {name} after crash failed: {payloads[name]}")
        check(not Path(f"{state}-journal").exists(), "hot journal was not recovered")
        listed = [watch["watch_id"] for watch in payloads["list"]["watches"]]
        check(listed == [WATCH_ID], f"recovered list differs: {listed}")
        check(payloads["create"]["status"] == "unchanged", "recovered profile was not intact")
        recovered = payloads["inspect"]
        for key in ("current_revision", "revisions", "profile", "recent_runs", "run_count"):
            check(recovered[key] == expected[key], f"recovered {key} differs from committed")
        self.record(
            f"crash_recovered_via_{first_command}",
            journal_bytes=journal_size,
            order=order,
            current_revision=recovered["current_revision"],
            run_count=recovered["run_count"],
        )

    def unsupported_crash_journal_is_refused_unmodified(self) -> None:
        newer_root, newer_state = self.store_copy("crash-newer-store")
        with closing(sqlite3.connect(newer_state)) as conn:
            conn.execute(f"PRAGMA user_version = {NEWER_SCHEMA_VERSION}")
            conn.commit()
        foreign_root = self.work_dir / "crash-foreign-store"
        foreign_state = foreign_root / "monitoring" / "state.sqlite3"
        foreign_state.parent.mkdir(parents=True)
        with closing(sqlite3.connect(foreign_state)) as conn:
            conn.execute("CREATE TABLE notes (body TEXT)")
            conn.execute("INSERT INTO notes VALUES ('kept')")
            conn.commit()
        cases = [
            (newer_root, newer_state, CRASH_UPDATE, "unsupported_schema_version"),
            (foreign_root, foreign_state, "UPDATE notes SET body = ?", "foreign_database"),
        ]
        profile = str(self.work_dir / "profiles" / "v1.toml")
        observed: dict[str, list[str]] = {}
        for root, state, statement, expected in cases:
            self.crash_writer(state, statement)
            before = directory_digest(state.parent)
            reasons: list[str] = []
            for args in (
                ("watch", "list"),
                ("watch", "inspect", "--id", WATCH_ID),
                ("watch", "create", "--profile", profile),
            ):
                code, payload = self.cli(*args, store_root=root)
                check(code == 1, f"{expected} with hot journal accepted by {args[1]}")
                reasons.append(payload["error"]["details"]["reason"])
            check(set(reasons) == {expected}, f"wrong hot-journal refusal: {reasons}")
            check(directory_digest(state.parent) == before, f"{expected} files were modified")
            observed[expected] = reasons
        header_fault = self.misleading_header_crash_is_refused(profile)
        self.record("unsupported_crash_journal_refused", reasons=observed, **header_fault)

    def misleading_header_crash_is_refused(self, profile: str) -> dict[str, Any]:
        """Refuse a committed-newer crash whose raw page-1 header claims version 1."""
        root, state = self.store_copy("crash-header-fault-store")
        with closing(sqlite3.connect(state)) as conn:
            conn.execute(f"PRAGMA user_version = {NEWER_SCHEMA_VERSION}")
            conn.commit()
        self.crash_writer(state, CRASH_UPDATE, CRASH_UNCOMMITTED_VERSION_PROCESS)
        journal = Path(f"{state}-journal")
        crash_header = int.from_bytes(
            state.read_bytes()[USER_VERSION_OFFSET : USER_VERSION_OFFSET + 4], "big"
        )
        # Deterministic injected page fault standing in for a partial page-1 write; it is
        # not an observed SQLite spill. The journal is SQLite's own and is never edited.
        with state.open("r+b") as handle:
            handle.seek(USER_VERSION_OFFSET)
            handle.write((1).to_bytes(4, "big"))
        raw_header = int.from_bytes(
            state.read_bytes()[USER_VERSION_OFFSET : USER_VERSION_OFFSET + 4], "big"
        )
        check(raw_header == 1, f"injected header fault reads {raw_header}")
        recovery = self.work_dir / "crash-header-fault-recovery"
        recovery.mkdir()
        copy = recovery / state.name
        shutil.copyfile(state, copy)
        shutil.copyfile(journal, Path(f"{copy}-journal"))
        with closing(sqlite3.connect(copy)) as conn:
            # The first read through a write-capable handle rolls the copy's journal back.
            committed = int(conn.execute("PRAGMA user_version").fetchone()[0])
        check(not Path(f"{copy}-journal").exists(), "private copy journal was not rolled back")
        check(
            committed == NEWER_SCHEMA_VERSION,
            f"private recovery found committed version {committed}",
        )
        before = {"database": file_sha256(state), "journal": file_sha256(journal)}
        files_before = directory_digest(state.parent)
        reasons: list[str] = []
        for args in (
            ("watch", "list"),
            ("watch", "inspect", "--id", WATCH_ID),
            ("watch", "create", "--profile", profile),
        ):
            code, payload = self.cli(*args, store_root=root)
            check(code == 1, f"misleading header crash accepted by {args[1]}")
            details = payload["error"]["details"]
            check(
                details["schema_version"] == NEWER_SCHEMA_VERSION,
                f"refusal read version {details}",
            )
            reasons.append(details["reason"])
        check(set(reasons) == {"unsupported_schema_version"}, f"wrong refusal: {reasons}")
        after = {"database": file_sha256(state), "journal": file_sha256(journal)}
        check(after == before, "misleading header crash files were modified")
        check(directory_digest(state.parent) == files_before, "header fault directory changed")
        return {
            "fault_injection": (
                "deterministic four-byte page-1 user_version overwrite after genuine crash; "
                "not an observed SQLite spill"
            ),
            "crash_raw_header_user_version": crash_header,
            "raw_header_user_version": raw_header,
            "recovered_committed_user_version": committed,
            "header_fault_reasons": reasons,
            "header_fault_sha256_before": before,
            "header_fault_sha256_after": after,
        }

    def empty_state_file_reads_as_empty(self) -> None:
        root = self.work_dir / "empty-store"
        state = root / "monitoring" / "state.sqlite3"
        state.parent.mkdir(parents=True)
        state.touch()
        before = directory_digest(state.parent)
        code, listed = self.cli("watch", "list", store_root=root)
        check(code == 0 and listed["watches"] == [], f"empty state list: {listed}")
        code, inspected = self.cli("watch", "inspect", "--id", WATCH_ID, store_root=root)
        reason = inspected["error"]["details"]["reason"]
        check(code == 1 and reason == "unknown_watch", f"empty state inspect: {inspected}")
        check(directory_digest(state.parent) == before, "empty state file was modified")
        self.record("empty_state_reads_empty", watches=listed["watches"], inspect=reason)


@dataclass(frozen=True)
class IncrementalStore:
    """An isolated storage root and the directory its CLI children run in."""

    root: Path
    cwd: Path

    @property
    def state_path(self) -> Path:
        return self.root / "monitoring" / "state.sqlite3"

    @property
    def monitoring_root(self) -> Path:
        return self.state_path.parent


@dataclass(frozen=True)
class StoreState:
    """Durable state read back through a read-only connection plus artifact hashes."""

    tables: dict[str, list[dict[str, Any]]]
    artifacts: dict[str, str]

    def entries(self) -> dict[str, dict[str, Any]]:
        return {row["entry_id"]: row for row in self.tables["watch_digest_entries"]}

    def payloads(self) -> list[dict[str, Any]]:
        return [json.loads(row["payload_json"]) for row in self.tables["watch_digest_entries"]]

    def unpresented(self) -> int:
        return sum(1 for row in self.tables["watch_digest_entries"] if row["digest_id"] is None)

    def comment_text(self, comment_id: str) -> str:
        rows = [row for row in self.tables["watch_comments"] if row["comment_id"] == comment_id]
        return str(single(rows, f"comment {comment_id}")["current_text_sha256"])

    def written_digests(self) -> list[dict[str, Any]]:
        rows = [row for row in self.tables["watch_digests"] if row["status"] == "written"]
        return sorted(rows, key=lambda row: int(row["sequence"]))

    def presented_entry_ids(self) -> list[str]:
        return [
            str(entry)
            for row in self.written_digests()
            for entry in json.loads(row["entry_ids_json"])
        ]


@dataclass(frozen=True)
class PhaseRun:
    phase: str
    exit_code: int
    payload: dict[str, Any]
    inspection: dict[str, Any]
    previous: StoreState
    state: StoreState


def single(rows: list[dict[str, Any]], what: str) -> dict[str, Any]:
    check(len(rows) == 1, f"expected exactly one {what}, found {len(rows)}")
    return rows[0]


def stable_id(prefix: str, *parts: str) -> str:
    return prefix + hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()[:32]


def tree_hashes(root: Path) -> dict[str, str]:
    """Every directory and file under `root`, files by content hash."""
    return {
        path.relative_to(root).as_posix(): file_sha256(path) if path.is_file() else "<dir>"
        for path in sorted(root.rglob("*"))
    }


def watch_artifact_hashes(monitoring_root: Path) -> dict[str, str]:
    watches = monitoring_root / "watches"
    if not watches.is_dir():
        return {}
    return {
        path.relative_to(monitoring_root).as_posix(): file_sha256(path)
        for path in sorted(watches.rglob("*"))
        if path.is_file()
    }


def non_state_tree(monitoring_root: Path) -> dict[str, str]:
    """Monitoring-root entries other than the state database and its journal."""
    return {
        path: digest
        for path, digest in tree_hashes(monitoring_root).items()
        if not path.startswith("state.sqlite3")
    }


def read_state(store: IncrementalStore) -> StoreState:
    uri = f"{store.state_path.as_uri()}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        tables = {
            table: [dict(row) for row in conn.execute(f"SELECT * FROM {table}")]
            for table in INCREMENTAL_TABLES
        }
    return StoreState(tables=tables, artifacts=watch_artifact_hashes(store.monitoring_root))


def cited_comment_ids(payload: dict[str, Any]) -> set[str]:
    """Comment ids an entry presents: all novel comments, or a finding's novel evidence."""
    key = "novel_comments" if payload["kind"] == "thread_update" else "novel_evidence"
    return {str(comment["comment_id"]) for comment in payload[key]}


def wait_for_lease_expiry(lease_expires_at_utc: float) -> None:
    time.sleep(max(0.0, lease_expires_at_utc - time.time()) + 0.2)


class IncrementalScenario:
    """Drives `threadsense watch` over the recorded incremental phases in fresh children."""

    def __init__(self, work_dir: Path) -> None:
        self.work_dir = work_dir
        self.payload_dir = work_dir / "payloads"
        self.evidence: list[dict[str, Any]] = []
        self.base_env = child_base_env()
        self.call_count = 0
        self.journey = self.new_store("store")
        self.journey_artifacts: dict[str, str] = {}
        document = json.loads(INCREMENTAL_PHASES_PATH.read_text(encoding="utf-8"))
        self.phase_names: list[str] = [str(phase["name"]) for phase in document["phases"]]

    def record(self, step: str, **observed: Any) -> None:
        self.evidence.append({"step": step, "observed": observed})
        print(f"step={step} ok")

    def new_store(self, name: str) -> IncrementalStore:
        return IncrementalStore(root=self.work_dir / name, cwd=self.work_dir)

    def spawn(
        self, store: IncrementalStore, args: tuple[str, ...], *, phase: str, crash_point: str
    ) -> subprocess.CompletedProcess[str]:
        env = {
            **self.base_env,
            "THREADSENSE_STORAGE_ROOT": str(store.root),
            "THREADSENSE_REDDIT_MAX_RETRIES": "0",
            "THREADSENSE_REDDIT_REQUEST_DELAY": "0",
        }
        command = [
            sys.executable,
            "-c",
            REPLAY_CHILD,
            str(INTEGRATION_DIR),
            str(INCREMENTAL_PHASES_PATH),
            phase,
            crash_point,
            "--output-format",
            "json",
            *args,
        ]
        return subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            cwd=store.cwd,
            env=env,
            timeout=120,
        )

    def persist(self, label: str, document: dict[str, Any]) -> None:
        self.call_count += 1
        self.payload_dir.mkdir(exist_ok=True)
        path = self.payload_dir / f"{self.call_count:03d}-{label}.json"
        path.write_text(json.dumps(document, indent=2, sort_keys=True), encoding="utf-8")

    def cli(
        self, store: IncrementalStore, *args: str, label: str, phase: str = NO_REPLAY
    ) -> tuple[int, dict[str, Any]]:
        completed = self.spawn(store, args, phase=phase, crash_point=NO_CRASH)
        payload = decode_payload(completed.stdout, completed.stderr, " ".join(args))
        self.persist(
            label,
            {
                "argv": list(args),
                "phase": phase,
                "exit_code": completed.returncode,
                "payload": payload,
            },
        )
        return completed.returncode, payload

    def crash(
        self, store: IncrementalStore, *args: str, label: str, phase: str, crash_point: str
    ) -> None:
        completed = self.spawn(store, args, phase=phase, crash_point=crash_point)
        self.persist(
            label,
            {
                "argv": list(args),
                "phase": phase,
                "crash_point": crash_point,
                "exit_code": completed.returncode,
                "stdout": completed.stdout,
                "stderr_tail": completed.stderr[-2000:],
            },
        )
        check(
            completed.returncode == CRASH_EXIT_CODE,
            f"{label}: child did not die at {crash_point}: exit {completed.returncode} "
            f"{completed.stderr[-500:]}",
        )
        check(completed.stdout.strip() == "", f"{label}: crashed child printed a payload")

    def watch(
        self, store: IncrementalStore, command: str, *extra: str, label: str, phase: str = NO_REPLAY
    ) -> tuple[int, dict[str, Any]]:
        """Run `threadsense watch <command> --id <watch> [extra...]` in a fresh child."""
        return self.cli(
            store, "watch", command, "--id", INCREMENTAL_WATCH_ID, *extra, label=label, phase=phase
        )

    def inspect(self, store: IncrementalStore, label: str) -> dict[str, Any]:
        code, payload = self.watch(store, "inspect", label=label)
        check(code == 0 and payload["artifact_type"] == "watch_inspection", f"{label}: {payload}")
        return payload

    def create_watch(
        self, store: IncrementalStore, label: str, profile_path: Path = INCREMENTAL_PROFILE_PATH
    ) -> dict[str, Any]:
        code, created = self.cli(
            store, "watch", "create", "--profile", str(profile_path), label=label
        )
        check(code == 0, f"{label}: watch create failed: {created}")
        check(
            Path(created["state_path"]) == store.state_path,
            f"{label}: state path {created['state_path']} is not under {store.root}",
        )
        return created

    def run(self) -> None:
        self.journey_over_recorded_phases()
        self.sweep_crash_is_recovered_once()
        self.digest_crash_resumes_same_digest()
        self.live_claim_refuses_overlapping_run()
        self.schedule_renders_without_writes()

    # Journey: one `watch run` per recorded phase against one store.

    def journey_over_recorded_phases(self) -> None:
        created = self.create_watch(self.journey, "journey-create")
        check(
            self.phase_names == list(PHASE_OUTCOMES),
            f"recorded phases differ from the scenario: {self.phase_names}",
        )
        self.record("create", revision=created["revision"], state_path=created["state_path"])
        state = read_state(self.journey)
        for phase in self.phase_names:
            state = self.journey_step(phase, state)
        self.check_journey_totals(state)

    def journey_step(self, phase: str, previous: StoreState) -> StoreState:
        code, payload = self.watch(self.journey, "run", label=f"run-{phase}", phase=phase)
        check(payload.get("artifact_type") == "watch_run", f"{phase}: watch run failed: {payload}")
        inspection = self.inspect(self.journey, f"inspect-{phase}")
        run = PhaseRun(phase, code, payload, inspection, previous, read_state(self.journey))
        outcome, exit_code = PHASE_OUTCOMES[phase]
        observed = (code, payload["status"], payload["sweep"]["outcome"])
        check(observed == (exit_code, outcome, outcome), f"{phase}: exit/outcome {observed}")
        self.check_digest_follows_sweep(run)
        new_entries = self.check_new_revisions(run)
        self.check_evidence_links(run.state, new_entries)
        self.check_presented_digest(run)
        self.check_digest_status(run)
        self.check_journey_artifacts_unchanged(phase, run.state)
        details = self.check_phase_behavior(run)
        digest = payload["digest"]
        self.record(
            phase,
            exit_code=code,
            outcome=outcome,
            new_entry_ids=sorted(new_entries),
            digest_status=digest["status"],
            digest_id=digest["digest_id"],
            presented_entry_ids=digest["entry_ids"],
            unpresented_entries_after=run.state.unpresented(),
            **details,
        )
        return run.state

    def check_digest_follows_sweep(self, run: PhaseRun) -> None:
        recent = run.inspection["recent_runs"]
        digest, sweep = run.payload["digest"], run.payload["sweep"]
        check(
            [item["run_id"] for item in recent[:2]] == [digest["run_id"], sweep["run_id"]],
            f"{run.phase}: digest run did not follow the sweep run: {recent[:2]}",
        )
        check(recent[0]["status"] == "succeeded", f"{run.phase}: digest run {recent[0]}")
        check(run.inspection["active_run"] is None, f"{run.phase}: a run is still active")

    def check_new_revisions(self, run: PhaseRun) -> dict[str, dict[str, Any]]:
        expected = PHASE_REVISIONS.get(run.phase, {})
        known = {row["revision_id"] for row in run.previous.tables["watch_thread_revisions"]}
        created = {
            (row["source_thread_id"], row["sequence"]): row["revision_id"]
            for row in run.state.tables["watch_thread_revisions"]
            if row["revision_id"] not in known
        }
        check(set(created) == set(expected), f"{run.phase}: new revisions {sorted(created)}")
        earlier = run.previous.entries()
        new_rows = {key: row for key, row in run.state.entries().items() if key not in earlier}
        new_entries = {key: json.loads(row["payload_json"]) for key, row in new_rows.items()}
        digest = run.payload["digest"]
        check(
            {row["created_run_id"] for row in new_rows.values()} <= {digest["run_id"]}
            and digest["materialized_entries"] == len(new_entries),
            f"{run.phase}: entries not materialized by this run's digest",
        )
        check(
            {payload["revision_id"] for payload in new_entries.values()} <= set(created.values()),
            f"{run.phase}: new entries cite revisions this run did not create",
        )
        for key, revision_id in created.items():
            update_id = stable_id("ent-", INCREMENTAL_WATCH_ID, revision_id, "thread_update")
            check(update_id in new_entries, f"{run.phase}: no thread_update entry for {key}")
            novelty = [
                (comment["comment_id"], comment["kind"])
                for comment in new_entries[update_id]["novel_comments"]
            ]
            check(novelty == expected[key], f"{run.phase}: novelty of {key} is {novelty}")
        return new_entries

    def check_evidence_links(self, state: StoreState, new_entries: dict[str, Any]) -> None:
        root = self.journey.monitoring_root
        revisions = {row["revision_id"]: row for row in state.tables["watch_thread_revisions"]}
        analyses = {row["revision_id"]: row for row in state.tables["watch_revision_analyses"]}
        for entry_id, payload in new_entries.items():
            check(payload["revision_id"] in analyses, f"{entry_id}: revision was not analyzed")
            revision, analysis = revisions[payload["revision_id"]], analyses[payload["revision_id"]]
            links = (
                payload["snapshot_path"],
                payload["snapshot_sha256"],
                payload["analysis_path"],
                payload["analysis_sha256"],
            )
            recorded = (
                revision["snapshot_path"],
                revision["snapshot_sha256"],
                analysis["analysis_path"],
                analysis["analysis_sha256"],
            )
            check(links == recorded, f"{entry_id}: evidence links {links} != {recorded}")
            check(
                file_sha256(root / payload["snapshot_path"]) == payload["snapshot_sha256"]
                and file_sha256(root / payload["analysis_path"]) == payload["analysis_sha256"],
                f"{entry_id}: linked snapshot or analysis bytes do not match",
            )
        for payload in new_entries.values():
            if payload["kind"] == "thread_update":
                self.check_finding_entries(payload, new_entries)

    def check_finding_entries(self, update: dict[str, Any], new_entries: dict[str, Any]) -> None:
        novel_ids = {comment["comment_id"] for comment in update["novel_comments"]}
        analysis_path = self.journey.monitoring_root / update["analysis_path"]
        analysis = load_analysis_artifact_file(analysis_path)
        expected = {
            stable_id(
                "ent-", INCREMENTAL_WATCH_ID, update["revision_id"], "finding", finding.theme_key
            ): sorted(novel_ids & set(finding.evidence_comment_ids))
            for finding in analysis.findings
            if novel_ids & set(finding.evidence_comment_ids)
        }
        findings = [
            payload
            for payload in new_entries.values()
            if payload["kind"] == "finding" and payload["revision_id"] == update["revision_id"]
        ]
        actual = {
            payload["entry_id"]: payload["novel_evidence_comment_ids"] for payload in findings
        }
        check(actual == expected, f"{update['revision_id']}: finding entries {actual}")
        for payload in findings:
            cited = [comment["comment_id"] for comment in payload["novel_evidence"]]
            check(cited == payload["novel_evidence_comment_ids"], f"{payload['entry_id']}: {cited}")

    def check_presented_digest(self, run: PhaseRun) -> None:
        digest, state = run.payload["digest"], run.state
        if digest["status"] == "no_entries":
            check(
                (digest["digest_id"], digest["entry_ids"], state.unpresented()) == (None, [], 0),
                f"{run.phase}: no digest although entries are unpresented",
            )
            return
        check(
            (digest["status"], digest["resumed"]) == ("presented", False),
            f"{run.phase}: digest {digest['status']} resumed={digest['resumed']}",
        )
        rows = state.tables["watch_digests"]
        row = single(
            [item for item in rows if item["digest_id"] == digest["digest_id"]],
            f"{run.phase} digest row",
        )
        entry_ids = json.loads(row["entry_ids_json"])
        check(
            (row["status"], row["sequence"], entry_ids)
            == ("written", digest["sequence"], digest["entry_ids"]),
            f"{run.phase}: payload does not match the written digest row",
        )
        check(1 <= len(entry_ids) <= PRESENTATION_LIMIT, f"{run.phase}: {len(entry_ids)} entries")
        check(
            row["digest_id"]
            == stable_id("dig-", INCREMENTAL_WATCH_ID, str(row["sequence"]), *entry_ids),
            f"{run.phase}: digest id is not derived from its sequence and entries",
        )
        self.check_digest_files(self.journey, row, state)
        expected_backlog = {
            "presentation_limit": PRESENTATION_LIMIT,
            "presented_in_digest": len(entry_ids),
            "unpresented_entries_after": state.unpresented(),
            "refresh_pending": run.payload["sweep"]["summary"]["refresh"]["pending"],
            "latest_sweep_outcome": run.payload["sweep"]["outcome"],
            "search_completeness": "unknown",
        }
        backlog = digest["backlog"]
        check(backlog == json.loads(row["backlog_json"]), f"{run.phase}: backlog differs")
        observed = {key: backlog.get(key) for key in expected_backlog}
        check(observed == expected_backlog, f"{run.phase}: backlog {observed}")
        check(
            len(entry_ids) == PRESENTATION_LIMIT or state.unpresented() == 0,
            f"{run.phase}: digest left entries unpresented below its limit",
        )
        linked = {
            key for key, item in state.entries().items() if item["digest_id"] == row["digest_id"]
        }
        check(linked == set(entry_ids), f"{run.phase}: linked entries {sorted(linked)}")

    def check_digest_files(
        self, store: IncrementalStore, row: dict[str, Any], state: StoreState
    ) -> None:
        root = store.monitoring_root
        json_path, markdown_path = root / row["json_path"], root / row["markdown_path"]
        stem = f"{row['sequence']:06d}-{row['digest_id']}"
        check(
            json_path.parent == root / "watches" / INCREMENTAL_WATCH_ID / "digests"
            and (json_path.name, markdown_path.name) == (f"{stem}.json", f"{stem}.md"),
            f"{row['digest_id']}: unexpected artifact paths {row['json_path']}",
        )
        check(
            (file_sha256(json_path), file_sha256(markdown_path))
            == (row["json_sha256"], row["markdown_sha256"]),
            f"{row['digest_id']}: artifact bytes do not match recorded hashes",
        )
        document = json.loads(json_path.read_text(encoding="utf-8"))
        entries = state.entries()
        expected = [
            json.loads(entries[key]["payload_json"]) for key in json.loads(row["entry_ids_json"])
        ]
        check(
            (document["artifact_type"], document["digest_id"], document["sequence"])
            == ("watch_digest", row["digest_id"], row["sequence"])
            and document["entries"] == expected
            and document["backlog"] == json.loads(row["backlog_json"]),
            f"{row['digest_id']}: JSON artifact does not match durable state",
        )

    def check_digest_status(self, run: PhaseRun) -> None:
        status, state = run.inspection["digest"], run.state
        observed = (
            status["entry_count"],
            status["unpresented_entries"],
            status["pending_digest_id"],
            status["digest_count"],
        )
        expected = (
            len(state.tables["watch_digest_entries"]),
            state.unpresented(),
            None,
            len(state.written_digests()),
        )
        check(observed == expected, f"{run.phase}: inspect digest {observed} != {expected}")

    def check_journey_artifacts_unchanged(self, phase: str, state: StoreState) -> None:
        changed = sorted(
            path
            for path, digest in self.journey_artifacts.items()
            if state.artifacts.get(path) != digest
        )
        check(not changed, f"{phase}: earlier artifacts changed or vanished: {changed}")
        self.journey_artifacts.update(state.artifacts)

    def thread_update(self, state: StoreState, thread_id: str, sequence: int) -> dict[str, Any]:
        rows = [
            row
            for row in state.tables["watch_thread_revisions"]
            if row["source_thread_id"] == thread_id and row["sequence"] == sequence
        ]
        revision_id = single(rows, f"revision {thread_id}#{sequence}")["revision_id"]
        row = state.entries().get(
            stable_id("ent-", INCREMENTAL_WATCH_ID, revision_id, "thread_update")
        )
        if row is None:
            raise SmokeFailure(f"no thread_update entry for {thread_id}#{sequence}")
        payload: dict[str, Any] = json.loads(row["payload_json"])
        return payload

    def check_phase_behavior(self, run: PhaseRun) -> dict[str, Any]:
        if run.phase == "search_omits_known":
            return self.check_known_thread_refresh(run)
        if run.phase == "edit":
            return self.check_edit(run)
        if run.phase == "partial":
            return self.check_partial(run)
        if run.phase == "reappear":
            return self.check_reappear(run)
        if run.phase == "backlog":
            return self.check_backlog(run)
        return {}

    def check_known_thread_refresh(self, run: PhaseRun) -> dict[str, Any]:
        sweep_run = run.payload["sweep"]["run_id"]
        discovered = sorted(
            row["source_thread_id"]
            for row in run.state.tables["watch_discoveries"]
            if row["run_id"] == sweep_run
        )
        check(THREAD_A not in discovered, f"search_omits_known: search returned {discovered}")
        update = self.thread_update(run.state, THREAD_A, 2)
        comment = single(update["novel_comments"], "new comment on the known thread")
        check(
            isinstance(comment["first_observed_at_utc"], float),
            "search_omits_known: new comment has no first observation time",
        )
        return {"discovered_threads": discovered, "novel_comment": comment["comment_id"]}

    def check_edit(self, run: PhaseRun) -> dict[str, Any]:
        change = single(self.thread_update(run.state, THREAD_B, 2)["novel_comments"], "edit")
        before = run.previous.comment_text("reddit:kb2")
        after = run.state.comment_text("reddit:kb2")
        check(
            "source_edited_at_utc" in change and change["source_edited_at_utc"] is None,
            f"edit: source edit time must be recorded as null: {change}",
        )
        check(
            change["previous_text_sha256"] == before and change["text_sha256"] == after != before,
            "edit: change does not link the previous and current text",
        )
        check(isinstance(change["observed_at_utc"], float), "edit: no edit observation time")
        return {
            "edited_comment": change["comment_id"],
            "source_edited_at_utc": change["source_edited_at_utc"],
            "previous_text_sha256": before,
            "text_sha256": after,
        }

    def check_partial(self, run: PhaseRun) -> dict[str, Any]:
        before = run.previous.comment_text("reddit:ka2")
        check(
            run.state.comment_text("reddit:ka2") == before,
            "partial: comment absent from a truncated acquisition lost its text",
        )
        initial_a = single(
            [
                row
                for row in run.state.tables["watch_thread_revisions"]
                if row["source_thread_id"] == THREAD_A and row["sequence"] == 1
            ],
            "first revision of thread A",
        )["revision_id"]
        citing = sorted(
            {
                payload["revision_id"]
                for payload in run.state.payloads()
                if "reddit:ka2" in cited_comment_ids(payload)
            }
        )
        check(citing == [initial_a], f"partial: absent comment cited by revisions {citing}")
        failures = [
            [item["stage"], item["target"]] for item in run.payload["sweep"]["summary"]["failures"]
        ]
        check(failures == [["fetch", THREAD_B]], f"partial: failures {failures}")
        return {"absent_comment_text_sha256": before, "failures": failures}

    def check_reappear(self, run: PhaseRun) -> dict[str, Any]:
        earlier = run.previous.entries()
        cited = sorted(
            {
                comment_id
                for key, row in run.state.entries().items()
                if key not in earlier
                for comment_id in cited_comment_ids(json.loads(row["payload_json"]))
            }
        )
        check(cited == ["reddit:ka5"], f"reappear: new entries cite {cited}")
        novelty = {
            row["comment_id"]: row["novelty"]
            for row in run.state.tables["watch_comments"]
            if row["comment_id"] in ("reddit:ka2", "reddit:ka5")
        }
        check(
            novelty
            == {"reddit:ka2": "initial_observation", "reddit:ka5": "newly_observed_historical"},
            f"reappear: comment novelty {novelty}",
        )
        return {"cited_comments": cited, "comment_novelty": novelty}

    def check_backlog(self, run: PhaseRun) -> dict[str, Any]:
        refresh = run.payload["sweep"]["summary"]["refresh"]
        counts = [refresh["tracked_threads"], refresh["attempted"], refresh["pending"]]
        check(counts == [4, 2, 2], f"backlog: tracked/attempted/pending {counts}")
        never = run.inspection["sweep"]["refresh_due_never_refreshed"]
        check(never == 2, f"backlog: {never} tracked threads never refreshed")
        digest = run.payload["digest"]
        if digest["status"] == "presented":
            pending = digest["backlog"]["refresh_pending"]
            check(pending == 2, f"backlog: digest reports refresh_pending {pending}")
        return {"tracked_attempted_pending": counts, "never_refreshed": never}

    def check_journey_totals(self, state: StoreState) -> None:
        updates = sorted(
            row["revision_id"]
            for row in state.tables["watch_digest_entries"]
            if row["kind"] == "thread_update"
        )
        analyzed = sorted(row["revision_id"] for row in state.tables["watch_revision_analyses"])
        expected_count = sum(len(revisions) for revisions in PHASE_REVISIONS.values())
        check(
            updates == analyzed and len(updates) == expected_count,
            f"journey: {len(updates)} thread updates for {len(analyzed)} analyzed revisions",
        )
        digests = state.written_digests()
        presented = state.presented_entry_ids()
        check(
            [row["sequence"] for row in digests] == list(range(1, len(digests) + 1)),
            "journey: digest sequences are not contiguous",
        )
        check(len(presented) == len(set(presented)), "journey: an entry was presented twice")
        check(
            all(row["status"] == "written" for row in state.tables["watch_digests"]),
            "journey: a digest was left pending",
        )
        self.record(
            "journey_totals",
            revisions=len(analyzed),
            entries=len(state.tables["watch_digest_entries"]),
            digests=len(digests),
            presented_entries=len(presented),
            unpresented_entries=state.unpresented(),
        )

    # Crash and overlap boundaries, each against its own store.

    def sweep_crash_is_recovered_once(self) -> None:
        store = self.new_store("store-sweep-crash")
        self.create_watch(store, "sweep-crash-create")
        self.crash(
            store,
            *("watch", "run", "--id", INCREMENTAL_WATCH_ID),
            *("--lease-seconds", str(SWEEP_CRASH_LEASE_SECONDS)),
            label="sweep-crash",
            phase="initial",
            crash_point="record_acquisition",
        )
        crashed = read_state(store)
        dead = single(crashed.tables["watch_runs"], "crashed sweep run")
        check(dead["status"] == "running", f"sweep crash: dead run is {dead['status']}")
        check(
            crashed.tables["watch_acquisitions"] == []
            and crashed.tables["watch_thread_revisions"] == [],
            "sweep crash: acquisition state was committed before the crash",
        )
        check(bool(crashed.artifacts), "sweep crash: no artifact was published before the crash")
        wait_for_lease_expiry(float(dead["lease_expires_at_utc"]))
        code, resumed = self.watch(store, "run", label="sweep-resume", phase="initial")
        sweep = resumed.get("sweep", {})
        check(
            (code, sweep.get("outcome"), sweep.get("recovered_run_id"))
            == (0, "changes", dead["run_id"]),
            f"sweep crash: resume did not recover the expired claim: {resumed}",
        )
        after_resume = read_state(store)
        orphaned = [
            path
            for path, digest in crashed.artifacts.items()
            if after_resume.artifacts.get(path) != digest
        ]
        check(not orphaned, f"sweep crash: files from the dead run changed: {orphaned}")
        code, rerun = self.watch(store, "run", label="sweep-rerun", phase="initial")
        check(
            code == 0 and rerun["sweep"]["outcome"] == "no_changes",
            f"sweep crash: rerun is not a no-op: {rerun}",
        )
        inspection = self.inspect(store, "sweep-crash-inspect")
        check(inspection["active_run"] is None, "sweep crash: a run is still active")
        final = read_state(store)
        revisions = self.check_each_revision_once(final)
        self.record(
            "sweep_crash_recovery",
            dead_run_id=dead["run_id"],
            orphan_artifacts=sorted(crashed.artifacts),
            recovered_by=sweep["run_id"],
            revisions=revisions,
            rerun_outcome=rerun["sweep"]["outcome"],
        )

    def check_each_revision_once(self, state: StoreState) -> list[str]:
        revisions = [row["revision_id"] for row in state.tables["watch_thread_revisions"]]
        updates = [
            row["revision_id"]
            for row in state.tables["watch_digest_entries"]
            if row["kind"] == "thread_update"
        ]
        expected_count = len(PHASE_REVISIONS["initial"])
        check(
            len(revisions) == len(set(revisions)) == expected_count,
            f"sweep crash: revisions {revisions}",
        )
        check(sorted(updates) == sorted(revisions), f"sweep crash: thread updates {updates}")
        presented = state.presented_entry_ids()
        check(len(presented) == len(set(presented)), "sweep crash: an entry was presented twice")
        return sorted(revisions)

    def digest_crash_resumes_same_digest(self) -> None:
        store = self.new_store("store-digest-crash")
        self.create_watch(store, "digest-crash-create")
        self.crash(
            store,
            *("watch", "run", "--id", INCREMENTAL_WATCH_ID),
            *("--lease-seconds", str(DIGEST_CRASH_LEASE_SECONDS)),
            label="digest-crash",
            phase="initial",
            crash_point="complete_digest",
        )
        crashed = read_state(store)
        pending = single(crashed.tables["watch_digests"], "pending digest")
        check(
            (pending["status"], pending["json_sha256"], pending["markdown_sha256"])
            == ("pending", None, None),
            f"digest crash: digest row {pending}",
        )
        check(
            crashed.unpresented() == len(crashed.tables["watch_digest_entries"]) > 0,
            "digest crash: entries were linked to a digest that never completed",
        )
        files = self.digest_file_hashes(store, pending)
        inspection = self.inspect(store, "digest-crash-inspect")
        check(
            inspection["digest"]["pending_digest_id"] == pending["digest_id"],
            "digest crash: inspect does not report the pending digest",
        )
        holder = single(
            [row for row in crashed.tables["watch_runs"] if row["status"] == "running"],
            "dead digest run",
        )
        wait_for_lease_expiry(float(holder["lease_expires_at_utc"]))
        self.check_digest_resume(store, pending, files)
        statuses = self.drain_digests(store, len(crashed.tables["watch_digest_entries"]))
        final = read_state(store)
        entry_ids = [row["entry_id"] for row in final.tables["watch_digest_entries"]]
        presented = final.presented_entry_ids()
        pending_ids = json.loads(pending["entry_ids_json"])
        check(len(entry_ids) == len(set(entry_ids)), "digest crash: duplicate entry ids")
        check(
            sorted(presented) == sorted(entry_ids) and len(presented) == len(set(presented)),
            "digest crash: entries were not presented exactly once",
        )
        check(presented[: len(pending_ids)] == pending_ids, "digest crash: resume reordered")
        self.record(
            "digest_crash_resume",
            digest_id=pending["digest_id"],
            sequence=pending["sequence"],
            entry_ids=pending_ids,
            file_sha256=list(files),
            later_statuses=statuses,
            presented_entries=len(presented),
        )

    def digest_file_hashes(self, store: IncrementalStore, row: dict[str, Any]) -> tuple[str, str]:
        root = store.monitoring_root
        paths = (root / row["json_path"], root / row["markdown_path"])
        check(all(path.is_file() for path in paths), f"{row['digest_id']}: artifact missing")
        return file_sha256(paths[0]), file_sha256(paths[1])

    def check_digest_resume(
        self, store: IncrementalStore, pending: dict[str, Any], files: tuple[str, str]
    ) -> None:
        code, payload = self.watch(store, "digest", label="digest-resume")
        observed = (
            code,
            payload.get("artifact_type"),
            payload.get("status"),
            payload.get("resumed"),
            payload.get("digest_id"),
            payload.get("sequence"),
            payload.get("entry_ids"),
        )
        expected = (
            0,
            "watch_digest_result",
            "presented",
            True,
            pending["digest_id"],
            pending["sequence"],
            json.loads(pending["entry_ids_json"]),
        )
        check(observed == expected, f"digest crash: resume {observed} != {expected}")
        resumed = read_state(store)
        rows = resumed.tables["watch_digests"]
        row = single(
            [item for item in rows if item["digest_id"] == pending["digest_id"]], "resumed digest"
        )
        check(
            row["status"] == "written" and (row["json_sha256"], row["markdown_sha256"]) == files,
            f"digest crash: resumed row {row}",
        )
        check(
            self.digest_file_hashes(store, row) == files,
            "digest crash: resumed artifacts are not byte-identical",
        )
        self.check_digest_files(store, row, resumed)

    def drain_digests(self, store: IncrementalStore, entry_count: int) -> list[str]:
        statuses: list[str] = []
        for index in range(entry_count + 1):
            code, payload = self.watch(store, "digest", label=f"digest-drain-{index}")
            check(code == 0, f"digest drain failed: {payload}")
            statuses.append(str(payload["status"]))
            if payload["status"] == "no_entries":
                return statuses
        raise SmokeFailure(f"digest presentation did not drain: {statuses}")

    def live_claim_refuses_overlapping_run(self) -> None:
        store = self.new_store("store-overlap")
        self.create_watch(store, "overlap-create")
        self.crash(
            store,
            *("watch", "run", "--id", INCREMENTAL_WATCH_ID),
            *("--lease-seconds", str(HELD_CLAIM_LEASE_SECONDS)),
            label="overlap-holder-crash",
            phase="initial",
            crash_point="record_acquisition",
        )
        before = read_state(store)
        holder = single(before.tables["watch_runs"], "held claim")
        check(
            holder["status"] == "running" and holder["lease_expires_at_utc"] > time.time() + 60,
            f"overlap: claim is not live: {holder}",
        )
        tree_before = non_state_tree(store.monitoring_root)
        code, payload = self.watch(store, "run", label="overlap-refused")
        error = payload.get("error", {})
        observed = (code, error.get("code"), error.get("details", {}).get("reason"))
        check(
            observed == (1, "run_claim_error", "claim_held"),
            f"overlap: watch run was not refused: {payload}",
        )
        check(read_state(store).tables == before.tables, "overlap: refused run changed state")
        check(non_state_tree(store.monitoring_root) == tree_before, "overlap: refused run wrote")
        inspection = self.inspect(store, "overlap-inspect")
        active = inspection["active_run"]
        check(
            active is not None and active["run_id"] == holder["run_id"],
            f"overlap: inspect does not report the held claim: {active}",
        )
        self.record(
            "overlap_refused",
            holder_run_id=holder["run_id"],
            exit_code=code,
            error_code=error.get("code"),
            reason=error.get("details", {}).get("reason"),
        )

    # Render-only scheduling from a workspace whose path needs quoting and escaping.

    def schedule_renders_without_writes(self) -> None:
        workspace = self.work_dir / HOSTILE_WORKSPACE_NAME
        workspace.mkdir()
        store = IncrementalStore(root=workspace / ".threadsense", cwd=workspace)
        self.create_watch(store, "schedule-create")
        before = tree_hashes(workspace)
        plist = self.check_launchd(store, self.render_schedule(store, "launchd"))
        tokens = self.check_cron(store, self.render_schedule(store, "cron"))
        check(tree_hashes(workspace) == before, "schedule: rendering wrote files")
        reason = self.check_unrepresentable_interval(store)
        self.record(
            "schedule",
            workspace=str(workspace),
            launchd_program_arguments=plist["ProgramArguments"],
            launchd_start_interval=plist["StartInterval"],
            cron_tokens=tokens,
            unrepresentable_interval_reason=reason,
            installed=False,
            enabled=False,
        )

    def render_schedule(self, store: IncrementalStore, schedule_format: str) -> dict[str, Any]:
        code, payload = self.cli(
            store,
            *("watch", "schedule", "--id", INCREMENTAL_WATCH_ID),
            *("--format", schedule_format, "--workspace", str(store.cwd)),
            label=f"schedule-{schedule_format}",
        )
        check(code == 0 and payload.get("artifact_type") == "watch_schedule", f"{payload}")
        flags = (payload["status"], payload["format"], payload["installed"], payload["enabled"])
        check(flags == ("rendered", schedule_format, False, False), f"schedule: {flags}")
        expected_argv = [
            str(Path(sys.executable).absolute()),
            *("-m", "threadsense", "--output-format", "json", "watch", "run"),
            *("--id", INCREMENTAL_WATCH_ID, "--lease-seconds", str(REFRESH_INTERVAL_SECONDS)),
        ]
        log_path = store.monitoring_root / "watches" / INCREMENTAL_WATCH_ID / "schedule"
        observed = (
            payload["argv"],
            payload["workspace"],
            payload["storage_root"],
            payload["log_path"],
            payload["lease_seconds"],
            payload["overlap_policy"],
        )
        expected = (
            expected_argv,
            str(store.cwd),
            str(store.root),
            str(log_path / "watch-run.log"),
            REFRESH_INTERVAL_SECONDS,
            "run_claim",
        )
        check(observed == expected, f"schedule: {observed} != {expected}")
        check(not log_path.exists(), "schedule: rendering created the log directory")
        return payload

    def check_launchd(self, store: IncrementalStore, payload: dict[str, Any]) -> dict[str, Any]:
        plist: dict[str, Any] = plistlib.loads(payload["content"].encode("utf-8"))
        observed = (
            plist["Label"],
            plist["ProgramArguments"],
            plist["WorkingDirectory"],
            plist["StartInterval"],
            plist["EnvironmentVariables"],
            plist["StandardOutPath"],
            plist["StandardErrorPath"],
            plist["RunAtLoad"],
        )
        expected = (
            f"com.threadsense.watch.{INCREMENTAL_WATCH_ID}",
            payload["argv"],
            str(store.cwd),
            REFRESH_INTERVAL_SECONDS,
            {"THREADSENSE_STORAGE_ROOT": str(store.root)},
            payload["log_path"],
            payload["log_path"],
            False,
        )
        check(observed == expected, f"launchd: {observed} != {expected}")
        check(
            payload["interval_seconds"] == REFRESH_INTERVAL_SECONDS
            and Path(plist["ProgramArguments"][0]).is_absolute(),
            "launchd: interval or executable is wrong",
        )
        return plist

    def check_cron(self, store: IncrementalStore, payload: dict[str, Any]) -> list[str]:
        lines = payload["content"].splitlines()
        check(len(lines) == 1, f"cron: expected one line, got {lines}")
        line = lines[0]
        check(re.search(r"(?<!\\)%", line) is None, f"cron: unescaped % in {line}")
        tokens = shlex.split(line.replace("\\%", "%"))
        expected = [
            *("0", "*/1", "*", "*", "*", "cd", str(store.cwd), "&&"),
            f"THREADSENSE_STORAGE_ROOT={store.root}",
            "exec",
            *payload["argv"],
            *(">>", payload["log_path"], "2>&1"),
        ]
        check(tokens == expected, f"cron: {tokens} != {expected}")
        return tokens

    def check_unrepresentable_interval(self, store: IncrementalStore) -> str:
        text = INCREMENTAL_PROFILE_PATH.read_text(encoding="utf-8")
        check("refresh_interval_minutes = 60" in text, "fixture profile interval changed")
        profile = self.work_dir / "profiles" / "unrepresentable-interval.toml"
        profile.parent.mkdir(parents=True, exist_ok=True)
        profile.write_text(
            text.replace(
                "refresh_interval_minutes = 60",
                f"refresh_interval_minutes = {UNREPRESENTABLE_CRON_MINUTES}",
            ),
            encoding="utf-8",
        )
        created = self.create_watch(store, "schedule-interval-revision", profile)
        check(created["revision"] == 2, f"schedule: interval revision {created['revision']}")
        before = tree_hashes(store.cwd)
        code, refused = self.cli(
            store,
            *("watch", "schedule", "--id", INCREMENTAL_WATCH_ID),
            *("--format", "cron", "--workspace", str(store.cwd)),
            label="schedule-cron-refused",
        )
        error = refused.get("error", {})
        details = error.get("details", {})
        observed = (code, error.get("code"), details.get("reason"), details.get("interval_minutes"))
        expected = (
            1,
            "monitoring_input_error",
            "cron_interval_unrepresentable",
            UNREPRESENTABLE_CRON_MINUTES,
        )
        check(observed == expected, f"schedule: cron refusal {observed}")
        check(tree_hashes(store.cwd) == before, "schedule: refused rendering wrote files")
        return str(details["reason"])


Scenario = (
    ProfilesScenario | IncrementalScenario | IssuesScenario | FeedbackScenario | AnnotationsScenario
)


def evidence_document(scenario: Scenario, status: str) -> dict[str, Any]:
    document: dict[str, Any] = {"status": status}
    if isinstance(scenario, IncrementalScenario | IssuesScenario | FeedbackScenario):
        document["fixture_provenance"] = FIXTURE_PROVENANCE
    if isinstance(scenario, AnnotationsScenario):
        document["observations"] = scenario.observations
    document["steps"] = scenario.evidence
    return document


def prepare_work_dir(work_dir: Path) -> Path:
    if work_dir.exists() and any(work_dir.iterdir()):
        raise SystemExit(f"work dir must be empty or absent: {work_dir}")
    work_dir.mkdir(parents=True, exist_ok=True)
    return work_dir.resolve()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run monitoring smoke scenarios through the threadsense CLI."
    )
    parser.add_argument(
        "--scenario",
        choices=["profiles", "incremental", "issues", "feedback", "annotations", "evaluation"],
        required=True,
    )
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument(
        "--endpoint", help="Local laya.serve URL; required for annotations and evaluation only."
    )
    args = parser.parse_args()
    endpoint_scenarios = ("annotations", "evaluation")
    if args.scenario in endpoint_scenarios and args.endpoint is None:
        parser.error(f"--scenario {args.scenario} requires --endpoint")
    if args.scenario not in endpoint_scenarios and args.endpoint is not None:
        parser.error(
            "--endpoint is only valid for --scenario annotations or evaluation, "
            f"not {args.scenario}"
        )
    work_dir = prepare_work_dir(args.work_dir)
    scenario: Scenario
    if args.scenario == "evaluation":
        scenario = EvaluationScenario(work_dir, args.endpoint)
    elif args.scenario == "annotations":
        scenario = AnnotationsScenario(work_dir, args.endpoint)
    elif args.scenario == "incremental":
        scenario = IncrementalScenario(work_dir)
    elif args.scenario == "issues":
        scenario = IssuesScenario(work_dir)
    elif args.scenario == "feedback":
        scenario = FeedbackScenario(work_dir)
    else:
        scenario = ProfilesScenario(work_dir)
    evidence_path = work_dir / f"{args.scenario}-evidence.json"
    try:
        scenario.run()
    except SmokeFailure as failure:
        # Failure text can quote CLI payloads; evidence never records local absolute paths.
        reason = redact_local_paths(str(failure))
        failed = {**evidence_document(scenario, "failed"), "error": reason}
        evidence_path.write_text(json.dumps(failed), encoding="utf-8")
        print(f"scenario={args.scenario} status=failed reason={reason}")
        return 1
    evidence_path.write_text(
        json.dumps(evidence_document(scenario, "passed"), indent=2), encoding="utf-8"
    )
    print(f"scenario={args.scenario} status=passed evidence={evidence_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
