"""Monitoring smoke scenarios exercised through the real `threadsense` CLI.

Usage:
    uv run python scripts/smoke_monitoring.py --scenario profiles --work-dir DIR

DIR must not exist or be empty. Every CLI call runs in a new interpreter process
against isolated storage under DIR; no network or model access is used. Run
claims are made through `threadsense.monitoring` in separate competing processes
and observed through `threadsense watch inspect`, because no CLI starts runs yet.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from contextlib import closing
from pathlib import Path
from typing import Any

import threadsense

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


class SmokeFailure(Exception):
    pass


def check(condition: bool, message: str) -> None:
    if not condition:
        raise SmokeFailure(message)


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def directory_digest(directory: Path) -> dict[str, str]:
    return {entry.name: file_sha256(entry) for entry in sorted(directory.iterdir())}


class ProfilesScenario:
    def __init__(self, work_dir: Path) -> None:
        self.work_dir = work_dir
        self.store_root = work_dir / "store"
        self.state_path = self.store_root.resolve() / "monitoring" / "state.sqlite3"
        self.evidence: list[dict[str, Any]] = []
        source_root = str(Path(threadsense.__file__).resolve().parents[1])
        self.base_env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("THREADSENSE_") and key != "PYTHONPATH"
        }
        # Child interpreters import the same sources as this script.
        self.base_env["PYTHONPATH"] = source_root
        self.base_env["THREADSENSE_RUNTIME_ENABLED"] = "false"

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
        try:
            payload: dict[str, Any] = json.loads(completed.stdout)
        except json.JSONDecodeError as error:
            raise SmokeFailure(
                f"threadsense {' '.join(args)} printed no JSON: {completed.stderr[-500:]}"
            ) from error
        return completed.returncode, payload

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
            conn.execute("PRAGMA user_version = 2")
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


def prepare_work_dir(work_dir: Path) -> Path:
    if work_dir.exists() and any(work_dir.iterdir()):
        raise SystemExit(f"work dir must be empty or absent: {work_dir}")
    work_dir.mkdir(parents=True, exist_ok=True)
    return work_dir.resolve()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run monitoring smoke scenarios through the threadsense CLI."
    )
    parser.add_argument("--scenario", choices=["profiles"], required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    args = parser.parse_args()
    scenario = ProfilesScenario(prepare_work_dir(args.work_dir))
    evidence_path = scenario.work_dir / f"{args.scenario}-evidence.json"
    try:
        scenario.run()
    except SmokeFailure as failure:
        evidence_path.write_text(
            json.dumps({"status": "failed", "steps": scenario.evidence, "error": str(failure)}),
            encoding="utf-8",
        )
        print(f"scenario={args.scenario} status=failed reason={failure}")
        return 1
    evidence_path.write_text(
        json.dumps({"status": "passed", "steps": scenario.evidence}, indent=2), encoding="utf-8"
    )
    print(f"scenario={args.scenario} status=passed evidence={evidence_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
