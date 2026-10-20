"""One saved-watch operator acceptance journey; only annotation uses the supplied model.

The recorded source is engineering-authored, as are the frozen evaluation labels.
This is workflow acceptance, never an independent semantic-quality measurement.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import plistlib
import shlex
import sqlite3
import subprocess
import sys
import time
from contextlib import closing
from pathlib import Path
from typing import Any

from smoke_annotations import SmokeFailure, check, child_base_env, decode_payload, file_sha256
from smoke_evaluation import activation_policy, evidence_bindings

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "watch" / "issues"
WATCH = "export-issues"
ENDPOINT = "http://127.0.0.1:62635"
CHECKPOINT = "7b928d828b7b0e022f929d9bd2e44165aa270148"
LEASE = 5
REQUIRED_STAGES = (
    "profile",
    "baseline",
    "freeze",
    "annotations",
    "evaluation",
    "policy",
    "policy_refusals",
    "new_community",
    "feedback",
    "publication_recovery",
    "issue_history",
    "replay",
    "edit_observation",
    "coverage",
    "cohort",
    "scheduler",
    "integrity",
)
POLICY_TOLERANCES: dict[str, float | int] = {
    "max_error_rate": 0.0,
    "min_decision_rate": 1.0,
    "min_labeled_decided": 1000000,
}

# Inference output is never patched. The only injection seams are the recorded source
# connector and a deliberate process death after real artifact publication, before commit.
CHILD = """
import dataclasses, json, os, sys
from pathlib import Path
integration, phases, phase, crash_receipt, *argv = sys.argv[1:]
sys.path.insert(0, integration)
import threadsense.cli
from recorded_reddit import PhaseReplayTransport, load_replay_phases, replay_connector_factory
from threadsense.monitoring import MonitoringStore

def refuse(config):
    raise RuntimeError('journey forbids unrecorded source access')
threadsense.cli.build_reddit_connector = refuse
if phase != 'none':
    selected = {p.name: p for p in load_replay_phases(Path(phases))}[phase]
    threadsense.cli.build_reddit_connector = replay_connector_factory(
        PhaseReplayTransport(selected))
if crash_receipt != 'none':
    def crash(self, claim, digest_id, json_sha256, markdown_sha256):
        Path(crash_receipt).write_text(json.dumps({
            'claim': dataclasses.asdict(claim), 'digest_id': digest_id,
            'json_sha256': json_sha256, 'markdown_sha256': markdown_sha256}))
        os._exit(75)
    MonitoringStore.complete_digest = crash
sys.exit(threadsense.cli.main(argv))
"""
FENCE_CHILD = """
import json, sys
from pathlib import Path
from threadsense.monitoring import MonitoringStore
from threadsense.errors import RunClaimError
receipt = json.loads(Path(sys.argv[2]).read_text())['claim']
try:
    MonitoringStore(Path(sys.argv[1])).finalize_run(
        receipt['run_id'], receipt['owner_token'], 'succeeded')
except RunClaimError as error:
    print(json.dumps({'reason': error.details['reason']}))
else:
    raise SystemExit('expired owner unexpectedly finalized')
"""


def load(path: Path) -> dict[str, Any]:
    value: Any = json.loads(path.read_text(encoding="utf-8"))
    check(isinstance(value, dict), f"not a JSON object: {path}")
    result: dict[str, Any] = value
    return result


def save(path: Path, document: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def hashes(root: Path) -> dict[str, str]:
    return {
        str(path): file_sha256(path)
        for path in sorted(root.rglob("*"))
        if path.is_file()
        and path.suffix not in (".sqlite3", "-journal", "-wal", "-shm")
        and not path.name.startswith("state.sqlite3")
    }


def assert_preserved(before: dict[str, str]) -> None:
    for raw, digest in before.items():
        path = Path(raw)
        check(path.is_file() and file_sha256(path) == digest, f"historical artifact changed: {raw}")


def prepare_phases(work_dir: Path) -> Path:
    """Extend one recorded cohort with explicit bounded-source failure/coverage cases."""
    document = load(FIXTURE / "phases.json")
    for phase in document["phases"]:
        for route in phase["routes"]:
            if "response" in route:
                route["response"] = str(FIXTURE / route["response"])
    unchanged = next(phase for phase in document["phases"] if phase["name"] == "unchanged")
    edit = copy.deepcopy(unchanged)
    edit["name"] = "evidence_edit"
    edit["fresh_comment_ids"] = []
    edit["fresh_post_ids"] = []
    edit_route = next(route for route in edit["routes"] if "/comments/ix1aaa/" in route["url"])
    edited_source: Any = json.loads(Path(edit_route["response"]).read_text(encoding="utf-8"))
    for node in edited_source[1]["data"]["children"]:
        if node["kind"] == "t1" and node["data"]["id"] == "ia2":
            node["data"]["body"] += " The same export crash also loses the attachment index."
            node["data"]["edited"] = 1760000700.0
    edit_route["response"] = str(save(work_dir / "recorded" / "edited.json", edited_source))
    document["phases"].append(edit)
    for name in ("coverage_partial", "coverage_truncated"):
        phase = copy.deepcopy(unchanged)
        phase["name"] = name
        phase["fresh_comment_ids"] = []
        phase["fresh_post_ids"] = []
        route = next(route for route in phase["routes"] if "/comments/ix1aaa/" in route["url"])
        if name == "coverage_partial":
            route.pop("response")
            route["status"] = 503
        else:
            response: Any = json.loads(Path(route["response"]).read_text(encoding="utf-8"))
            response[1]["data"]["children"].append(
                {"kind": "more", "data": {"id": "unexpanded", "children": ["unseen"], "count": 1}}
            )
            response_path = save(work_dir / "recorded" / "truncated.json", response)
            route["response"] = str(response_path)
            expansion = save(
                work_dir / "recorded" / "unexpanded.json", {"json": {"data": {"things": []}}}
            )
            phase["routes"].append(
                {
                    "url": "https://www.reddit.com/api/morechildren.json",
                    "params_match": {"link_id": "t3_ix1aaa", "children": "unseen"},
                    "response": str(expansion),
                }
            )
        document["phases"].append(phase)
    return save(work_dir / "recorded" / "phases.json", document)


class JourneyScenario:
    """No independent subscenarios or secondary watch/database are created."""

    def __init__(self, work_dir: Path, endpoint: str) -> None:
        check(endpoint == ENDPOINT, "all requires the explicitly owned checkpoint endpoint")
        self.work_dir = work_dir
        self.endpoint = endpoint
        self.store_root = work_dir / "store"
        self.monitoring = self.store_root / "monitoring"
        self.state_path = self.monitoring / "state.sqlite3"
        self.evidence: list[dict[str, Any]] = []
        self.observations: dict[str, Any] = {
            "watch_id": WATCH,
            "storage_root": str(self.store_root),
            "state_path": str(self.state_path),
            "endpoint": endpoint,
            "fixture_provenance": (
                "engineering-authored recorded Reddit served in-process; not live"
            ),
            "independent_quality_claim": False,
            "semantic_activation_performed": False,
            "generative_comparator": {"status": "unavailable", "reason": "explicitly_disabled"},
            "required_stages": list(REQUIRED_STAGES),
        }
        self.base_env = {
            key: value
            for key, value in child_base_env().items()
            if not key.startswith(("GIT_", "UV_", "THREADSENSE_"))
            and key not in {"PYTHONHOME", "PYTHONOPTIMIZE", "VIRTUAL_ENV", "CONDA_PREFIX"}
        }
        self.base_env.update(
            {
                "THREADSENSE_STORAGE_ROOT": str(self.store_root),
                "THREADSENSE_RUNTIME_ENABLED": "false",
                "THREADSENSE_REDDIT_MAX_RETRIES": "0",
                "THREADSENSE_REDDIT_REQUEST_DELAY": "0",
            }
        )
        self.phases = prepare_phases(work_dir)
        self.profile_path = work_dir / "profile.toml"
        self.profile_path.write_bytes((FIXTURE / "profile.toml").read_bytes())
        self.calls = 0
        self.preserved: dict[str, str] = {}
        self.annotation_paths: list[Path] = []
        self.manifest = work_dir / "evaluation" / "manifest.json"
        self.frozen: dict[str, str] = {}
        self.issue_id = ""
        self.evaluation_path = Path()
        self.policy_path = Path()

    def record(self, step: str, **observed: Any) -> None:
        check(step not in {item["step"] for item in self.evidence}, f"duplicate stage {step}")
        self.evidence.append({"step": step, "observed_at_utc": time.time(), "observed": observed})
        print(f"step={step} ok")

    def rows(self, table: str) -> list[dict[str, Any]]:
        with closing(sqlite3.connect(f"{self.state_path.as_uri()}?mode=ro", uri=True)) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(row) for row in conn.execute(f'SELECT * FROM "{table}" ORDER BY rowid')]

    def process(
        self,
        args: tuple[str, ...],
        *,
        label: str,
        phase: str = "none",
        crash: Path | None = None,
        program: str = CHILD,
    ) -> subprocess.CompletedProcess[str]:
        self.calls += 1
        prefix = self.work_dir / "payloads" / f"{self.calls:03d}-{label}"
        prefix.parent.mkdir(exist_ok=True)
        if program == CHILD:
            argv = [
                sys.executable,
                "-c",
                program,
                str(ROOT / "tests" / "integration"),
                str(self.phases),
                phase,
                str(crash) if crash else "none",
                "--output-format",
                "json",
                *args,
            ]
        else:
            argv = [sys.executable, "-c", program, *args]
        started = time.time()
        try:
            completed = subprocess.run(
                argv,
                cwd=self.work_dir,
                env=self.base_env,
                capture_output=True,
                text=True,
                timeout=900 if "annotate" in args else 120,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            save(
                prefix.with_suffix(".json"),
                {
                    "argv": list(args),
                    "phase": phase,
                    "started_at_utc": started,
                    "error_type": type(error).__name__,
                },
            )
            raise SmokeFailure(f"{label}: process failed: {type(error).__name__}") from error
        stdout = prefix.with_suffix(".stdout.log")
        stderr = prefix.with_suffix(".stderr.log")
        stdout.write_text(completed.stdout, encoding="utf-8")
        stderr.write_text(completed.stderr, encoding="utf-8")
        save(
            prefix.with_suffix(".json"),
            {
                "argv": list(args),
                "executable": sys.executable,
                "phase": phase,
                "started_at_utc": started,
                "finished_at_utc": time.time(),
                "exit_code": completed.returncode,
                "stdout_path": str(stdout),
                "stdout_sha256": file_sha256(stdout),
                "stderr_path": str(stderr),
                "stderr_sha256": file_sha256(stderr),
                "fresh_interpreter": True,
            },
        )
        return completed

    def cli(self, *args: str, label: str, phase: str = "none") -> tuple[int, dict[str, Any]]:
        completed = self.process(args, label=label, phase=phase)
        return completed.returncode, decode_payload(completed.stdout, completed.stderr, label)

    def ok(self, *args: str, label: str, phase: str = "none") -> dict[str, Any]:
        code, payload = self.cli(*args, label=label, phase=phase)
        check(code == 0, f"{label}: exit={code}: {payload}")
        return payload

    def watch(self, command: str, *extra: str, label: str, phase: str = "none") -> dict[str, Any]:
        return self.ok("watch", command, "--id", WATCH, *extra, label=label, phase=phase)

    def artifact(self, raw: str) -> Path:
        path = Path(raw)
        if not path.is_absolute():
            path = self.work_dir / path
        check(
            path.resolve().is_relative_to(self.work_dir), f"artifact escapes work directory: {raw}"
        )
        check(path.is_file(), f"artifact absent: {raw}")
        return path

    def alert(self, result: dict[str, Any]) -> dict[str, Any]:
        check(result["artifact_type"] == "watch_alert_result", f"missing alert result: {result}")
        path = self.artifact(result["json_path"])
        report = load(path)
        check(report["report_id"] == result["report_id"], "alert report identity differs")
        self.artifact(result["markdown_path"])
        check(
            report["coverage"]["rate_trend_eligible"] is False,
            "bounded recorded source was used for a rate/trend claim",
        )
        denominators = report["bounded_denominators"]
        check(
            denominators["population_denominator"] is None
            and denominators["independent_people_denominator"] is None,
            "bounded candidate/comment counts became population/independence denominators",
        )
        for event in result["alerts"]:
            check(
                event["claims_are_verified"] is False and event["evidence"],
                "uncited/verified claim",
            )
            for citation in event["evidence"]:
                check(citation["text_sha256"] and citation["permalink"], "untraceable citation")
                check(citation["first_observed_at_utc"] is not None, "no observation timestamp")
                snapshot = self.monitoring / citation["snapshot_path"]
                check(
                    file_sha256(snapshot) == citation["snapshot_sha256"],
                    "citation snapshot mismatch",
                )
        return report

    def run(self) -> None:
        stages = (
            self.profile,
            self.baseline,
            self.freeze,
            self.annotations,
            self.evaluate,
            self.policy,
            self.policy_refusals,
            self.new_community,
            self.feedback,
            self.publication_recovery,
            self.issue_history,
            self.replay,
            self.edit_observation,
            self.coverage,
            self.cohort,
            self.scheduler,
            self.integrity,
        )
        try:
            for stage in stages:
                stage()
        except (OSError, ValueError, KeyError, TypeError, sqlite3.Error) as error:
            raise SmokeFailure(
                f"journey artifact/contract failure: {type(error).__name__}: {error}"
            ) from error
        check(
            tuple(item["step"] for item in self.evidence) == REQUIRED_STAGES, "incomplete journey"
        )

    def profile(self) -> None:
        created = self.ok("watch", "create", "--profile", str(self.profile_path), label="create")
        listed = self.ok("watch", "list", label="list")
        check([row["watch_id"] for row in listed["watches"]] == [WATCH], "not exactly one watch")
        inspected = self.watch("inspect", label="show")
        check(Path(created["state_path"]) == self.state_path, "unexpected database")
        self.record(
            "profile",
            created=created,
            listing=listed,
            inspection=inspected,
            profile_sha256=file_sha256(self.profile_path),
        )

    def baseline(self) -> None:
        payload = self.watch("run", label="initial", phase="initial")
        result = payload["alerts"]
        self.alert(result)
        check(
            result["status"] == "baseline" and not result["new_alert_ids"],
            "initial demand invented",
        )
        check(result["refusal_reasons"], "bounded unknown coverage permitted trends")
        self.preserved.update(hashes(self.monitoring))
        self.record(
            "baseline", sweep=payload, bounded_unknown_coverage_refused=result["refusal_reasons"]
        )

    def freeze(self) -> None:
        # The cohort, held-out thread and labels are fixed before inference, not selected
        # by model outcomes. One explicit export-crash label is sufficient for checker
        # mechanics, never sufficient for independent promotion.
        template = load(ROOT / "tests" / "fixtures" / "decisions" / "evaluation" / "manifest.json")
        revisions = {row["source_thread_id"]: row for row in self.rows("watch_thread_revisions")}
        threads = []
        for source_id, partition in (
            ("ix1aaa", "evaluation"),
            ("ix1bbb", "development"),
            ("ix1fff", "calibration"),
        ):
            row = revisions[source_id]
            path = self.monitoring / row["snapshot_path"]
            check(file_sha256(path) == row["snapshot_sha256"], "saved snapshot hash differs")
            snapshot = load(path)
            threads.append(
                {
                    "thread_id": snapshot["thread"]["thread_id"],
                    "path": os.path.relpath(path, self.manifest.parent),
                    "sha256": row["snapshot_sha256"],
                    "partition": partition,
                    "source_language": "en",
                    "comment_languages": {},
                }
            )
        labels = {
            "artifact_kind": "typed_decision_evaluation_labels",
            "schema_version": 1,
            "origin": {
                "kind": "engineering_authored",
                "authored_by": "AI coding agent",
                "method": "Fixed export-crash expectation before any journey inference",
                "disclosure": "Engineering-only; not independent adjudication or quality evidence",
            },
            "labels": [
                {
                    "thread_id": "reddit:ix1aaa",
                    "comment_id": "reddit:ia1",
                    "question_id": "theme",
                    "expected": "reliability",
                    "case": "repetition",
                    "rationale": "The recorded report describes an export crash.",
                }
            ],
        }
        label_path = save(self.manifest.parent / "labels.json", labels)
        template.update(
            {
                "dataset_id": "monitoring-journey-engineering-v1",
                "description": "Frozen saved-watch snapshots; workflow acceptance only",
                "threads": threads,
                "issue_groups": [],
                "sampling": {
                    "method": "predeclared_saved_watch_snapshots",
                    "selection": "all comments of fixed A/B/F initial revisions",
                    "max_threads": 3,
                    "max_comments_per_thread": 100,
                },
                "labels": {"path": "labels.json", "sha256": file_sha256(label_path)},
            }
        )
        save(self.manifest, template)
        self.frozen = {
            str(self.manifest): file_sha256(self.manifest),
            str(label_path): file_sha256(label_path),
        }
        scope = save(
            self.manifest.parent / "scope.json",
            {
                "question_id": "theme",
                "consumer": "semantic_reranking",
                "scope": "runtime",
                "tolerances": POLICY_TOLERANCES,
                "independent_quality_claim": False,
                "generative_runtime_enabled": False,
                "policy_installation_allowed": False,
            },
        )
        self.frozen[str(scope)] = file_sha256(scope)
        for thread in threads:
            path = (self.manifest.parent / thread["path"]).resolve()
            self.frozen[str(path)] = thread["sha256"]
        self.record(
            "freeze",
            manifest_path=str(self.manifest),
            frozen_sha256=self.frozen,
            label_origin=labels["origin"],
            partitions=threads,
        )

    def annotations(self) -> None:
        manifest = load(self.manifest)
        records = []
        for thread in manifest["threads"]:
            assert_preserved(self.frozen)
            snapshot = (self.manifest.parent / thread["path"]).resolve()
            payload = self.ok(
                "decisions",
                "annotate",
                "--input",
                str(snapshot),
                "--endpoint",
                self.endpoint,
                "--requirement",
                "required",
                "--output-dir",
                str(self.work_dir / "annotations"),
                "--domain",
                "developer_tools",
                "--objective",
                "friction_analysis",
                label=f"annotate-{thread['thread_id'].split(':')[-1]}",
            )
            check(
                payload["status"] == "complete" and payload["satisfied"] is True,
                f"required inference incomplete: {payload}",
            )
            path = self.artifact(payload["decision_path"])
            check(file_sha256(path) == payload["decision_sha256"], "annotation hash differs")
            decision = load(path)
            check(decision["input"]["sha256"] == thread["sha256"], "inference used unrelated input")
            verification = decision["backend"]["checkpoint_verification"]
            check(
                verification is not None and verification["basis"] == "client_file_sha256",
                "no actual checkpoint file verification",
            )
            check(
                decision["backend"]["manifest"]["checkpoint"]["revision"] == CHECKPOINT,
                "unexpected checkpoint revision",
            )
            exchanges = decision["exchanges"]
            check(
                bool(exchanges)
                and all(
                    item["outcome"] == "response" and item["status_code"] == 200
                    for item in exchanges
                ),
                "no successful real exchanges",
            )
            for item in exchanges:
                check(
                    hashlib.sha256(item["response_body"].encode()).hexdigest()
                    == item["response_sha256"],
                    "response receipt hash differs",
                )
            source_ids = {comment["comment_id"] for comment in load(snapshot)["thread"]["comments"]}
            check(
                {item["comment_id"] for item in decision["items"]} == source_ids,
                "annotation omitted source comments",
            )
            self.annotation_paths.append(path)
            self.artifact(payload["run_path"])
            records.append(
                {
                    "thread_id": thread["thread_id"],
                    "snapshot": str(snapshot),
                    "snapshot_sha256": thread["sha256"],
                    "annotation": payload,
                    "checkpoint_verification": verification,
                    "exchange_response_sha256": [item["response_sha256"] for item in exchanges],
                }
            )
        self.preserved.update(hashes(self.work_dir / "annotations"))
        self.record("annotations", required=True, inputs=records, independent_quality_claim=False)

    def evaluate(self) -> None:
        assert_preserved(self.frozen)
        args = ["decisions", "evaluate", "--manifest", str(self.manifest)]
        for path in self.annotation_paths:
            args.extend(("--annotation", str(path)))
        payload = self.ok(*args, label="evaluate")
        self.evaluation_path = self.artifact(payload["evaluation_path"])
        check(
            file_sha256(self.evaluation_path) == payload["evaluation_sha256"],
            "evaluation hash differs",
        )
        document = load(self.evaluation_path)
        check(
            document["comparators"]["generative"]["comparison"] == "not_performed",
            "generative runtime was not disabled",
        )
        check(
            payload["independent_adjudication"] is False, "engineering labels claimed independent"
        )
        self.preserved.update(hashes(self.store_root))
        self.record(
            "evaluation",
            result=payload,
            generative_runtime_enabled=False,
            claims=document["claims"],
            no_independent_quality_claim=True,
        )

    def check_policy(self, evaluation: Path, policy: Path, label: str) -> dict[str, Any]:
        payload = self.ok(
            "decisions",
            "policy",
            "check",
            "--evaluation",
            str(evaluation),
            "--policy",
            str(policy),
            label=label,
        )
        check(
            payload["eligibility"] == "not_eligible" and payload["activation_permitted"] is False,
            f"unexpected eligibility/activation: {payload}",
        )
        path = self.artifact(payload["policy_check_path"])
        check(file_sha256(path) == payload["policy_check_sha256"], "policy check hash differs")
        check(load(path)["activation"]["performed"] is False, "policy installed/activated")
        return payload

    def policy(self) -> None:
        # Constant tolerances are intentionally ineligible for runtime use with these
        # engineering labels; never loosened based on observed decisions.
        policy = activation_policy(
            "runtime",
            "theme",
            evidence_bindings(load(self.evaluation_path)),
            POLICY_TOLERANCES,
        )
        self.policy_path = save(self.work_dir / "policies" / "policy.json", policy)
        result = self.check_policy(self.evaluation_path, self.policy_path, "policy")
        self.record(
            "policy",
            result=result,
            installed=False,
            activation_performed=False,
            supported_mode="deterministic monitoring with typed shadow annotations",
        )

    def policy_refusals(self) -> None:
        original = hashes(self.store_root)
        missing = self.check_policy(
            self.work_dir / "absent" / ("0" * 64 + ".json"), self.policy_path, "policy-missing"
        )
        check("evidence_missing" in missing["reasons"], f"missing evidence accepted: {missing}")
        stale_policy = load(self.policy_path)
        stale_policy["bindings"]["dataset_manifest_sha256"] = "0" * 64
        path = save(self.work_dir / "policies" / "stale.json", stale_policy)
        stale = self.check_policy(self.evaluation_path, path, "policy-stale")
        check(
            any("mismatch" in reason or "stale" in reason for reason in stale["reasons"]),
            f"stale binding not refused: {stale}",
        )
        assert_preserved(original)
        self.record(
            "policy_refusals", missing=missing, stale=stale, original_evidence_preserved=True
        )

    def new_community(self) -> None:
        payload = self.watch("run", label="repost", phase="repost")
        result = payload["alerts"]
        self.alert(result)
        check(result["new_alert_ids"], "new community/evidence failed to alert")
        listing = self.ok("issues", "inspect", "--watch", WATCH, label="issues-list")
        # Listing shape is public CLI, not an independent database invented for the smoke.
        homes = [item for item in listing["issues"] if item["home_thread_id"] == "ix1aaa"]
        check(len(homes) == 1, "missing original issue")
        self.issue_id = homes[0]["issue_id"]
        issue = self.inspect_issue("copy-history")
        reports = {row["comment_id"]: row for row in issue["reports"]}
        check(
            reports["reddit:ic1"]["lineage"] == "exact_copy"
            and reports["reddit:ic1"]["copy_of_report_id"] == reports["reddit:ia1"]["report_id"],
            "copy counted as independent report",
        )
        check(
            issue["counts"]["observed_communities"] == 2 and issue["counts"]["exact_copies"] == 1,
            "community/copy counts differ",
        )
        self.preserved.update(hashes(self.monitoring))
        self.record("new_community", sweep=payload, issue=issue, independent_people_verified=False)

    def inspect_issue(self, label: str) -> dict[str, Any]:
        payload = self.ok(
            "issues", "inspect", "--watch", WATCH, "--issue", self.issue_id, label=label
        )
        return payload

    def feedback(self) -> None:
        entries_before = self.rows("watch_digest_entries")
        findings = [
            row
            for row in entries_before
            if json.loads(row["payload_json"])["kind"] == "finding" and row["digest_id"] is not None
        ]
        check(bool(findings), "no presented finding for feedback")
        initial_digest = self.rows("watch_digests")[0]
        ranking = load(self.monitoring / initial_digest["json_path"])["ranking"]["order"]
        finding_ids = {row["entry_id"] for row in findings}
        target = next(item["entry_id"] for item in ranking if item["entry_id"] in finding_ids)
        historical = self.watch("explain", "--entry", target, label="historical-explain")
        issue_before = self.inspect_issue("before-feedback")
        pref = self.ok(
            "feedback",
            "record",
            "--watch",
            WATCH,
            "--target",
            target,
            "--target-kind",
            "finding",
            "--kind",
            "preference",
            "--value",
            "irrelevant",
            "--key",
            "journey-preference",
            label="preference",
        )
        correction = self.ok(
            "feedback",
            "record",
            "--watch",
            WATCH,
            "--target",
            target,
            "--target-kind",
            "finding",
            "--kind",
            "correction",
            "--value",
            "misclassified",
            "--key",
            "journey-correction",
            "--note",
            "Engineering operator assertion; not a source fact",
            label="correction",
        )
        current = self.watch("explain", "--entry", target, "--current", label="current-explain")
        replay = self.watch("explain", "--entry", target, label="historical-explain-restart")
        check(
            replay["ranking"] == historical["ranking"] and replay["basis"] == historical["basis"],
            "feedback rewrote historical ranking",
        )
        before = historical["ranking"]
        after = current["current"]
        pre = next(item for item in before["components"] if item["component"] == "preference")
        post = next(item for item in after["components"] if item["component"] == "preference")
        check(
            post["points"] != pre["points"]
            and after["score"] - before["score"] == post["points"] - pre["points"],
            "preference did not affect ranking score",
        )
        check(after["rank"] > before["rank"], "preference did not change presented ranking order")
        check(
            [item for item in before["components"] if item["component"] != "preference"]
            == [item for item in after["components"] if item["component"] != "preference"],
            "preference altered non-preference ranking factors",
        )
        listed = self.ok("feedback", "list", "--watch", WATCH, label="feedback-list")
        check(
            listed["correction_view"][0]["effect"] == "assertion_only",
            "correction promoted to evidence",
        )
        check(
            self.rows("watch_digest_entries") == entries_before, "feedback rewrote evidence entries"
        )
        check(
            self.inspect_issue("after-feedback") == issue_before, "feedback changed issue evidence"
        )
        assert_preserved(self.preserved)
        self.record(
            "feedback",
            preference=pref,
            correction=correction,
            listing=listed,
            historical=historical,
            current=current,
            evidence_unchanged=True,
        )

    def publication_recovery(self) -> None:
        receipt_path = self.work_dir / "publication-crash.json"
        completed = self.process(
            ("watch", "run", "--id", WATCH, "--lease-seconds", str(LEASE)),
            label="publication-crash",
            phase="updates",
            crash=receipt_path,
        )
        check(
            completed.returncode == 75 and receipt_path.is_file(),
            "did not reach real publication crash",
        )
        receipt = load(receipt_path)
        pending = [row for row in self.rows("watch_digests") if row["status"] == "pending"]
        check(
            len(pending) == 1 and pending[0]["digest_id"] == receipt["digest_id"],
            "missing pending publication",
        )
        row = pending[0]
        paths = [self.monitoring / row[key] for key in ("json_path", "markdown_path")]
        artifacts = {str(path): file_sha256(path) for path in paths}
        check(
            list(artifacts.values()) == [receipt["json_sha256"], receipt["markdown_sha256"]],
            "crash receipt does not bind actual files",
        )
        code, refused = self.cli("watch", "digest", "--id", WATCH, label="held-claim-refusal")
        check(
            code == 1 and refused["error"]["details"]["reason"] == "claim_held",
            "live overlap not fenced",
        )
        active = next(
            item for item in self.rows("watch_runs") if item["run_id"] == receipt["claim"]["run_id"]
        )
        time.sleep(max(0.0, float(active["lease_expires_at_utc"]) - time.time()) + 0.1)
        recovered = self.watch("digest", label="publication-recovery")
        check(
            recovered["resumed"] is True and recovered["digest_id"] == receipt["digest_id"],
            "fresh interpreter failed to resume same publication",
        )
        assert_preserved(artifacts)
        stale = self.process(
            (str(self.state_path), str(receipt_path)), label="stale-owner", program=FENCE_CHILD
        )
        check(
            stale.returncode == 0 and json.loads(stale.stdout)["reason"] == "stale_owner",
            "stale owner was not fenced",
        )
        old = next(item for item in self.rows("watch_runs") if item["run_id"] == active["run_id"])
        check(
            old["status"] == "abandoned" and old["recovered_by_run_id"],
            "missing durable recovery lineage",
        )
        # Digest recovery presents alerts too; replay must preserve, not redeliver, them.
        alerts = recovered["alerts"]
        self.alert(alerts)
        check(bool(alerts["new_alert_ids"]), "new update evidence missing after recovery")
        replayed = self.watch("alerts", label="post-recovery-alerts")
        check(
            replayed["alert_ids"] == alerts["alert_ids"] and not replayed["new_alert_ids"],
            "post-recovery presentation changed or redelivered alerts",
        )
        self.record(
            "publication_recovery",
            pending=row,
            artifact_sha256=artifacts,
            held_refusal=refused,
            recovered=recovered,
            abandoned_run=old,
            stale_owner_refused=True,
            alerts=alerts,
        )

    def issue_history(self) -> None:
        issue = self.inspect_issue("updated-history")
        expected = {"reproduction", "contradiction", "workaround", "resolution_claim"}
        check(
            {item["kind"] for item in issue["updates"]} == expected,
            "missing issue update semantics",
        )
        check(
            all(issue["update_counts"][kind] == 1 for kind in expected), "duplicate/missing updates"
        )
        check(
            issue["resolution"]["resolved"] is False and issue["issue_status"] == "open",
            "resolution claim became verified resolution",
        )
        check(
            issue["counts"]["firsthand_reports"] == 6 and issue["counts"]["exact_copies"] == 1,
            "copy corrupted report counts",
        )
        alerts = self.watch("alerts", label="alerts-restart")
        self.alert(alerts)
        check(
            alerts["resumed"] is True and not alerts["new_alert_ids"], "restart redelivered alerts"
        )
        citations = [citation for event in alerts["alerts"] for citation in event["evidence"]]
        check(bool(citations), "updates have no citations")
        check(
            any(item["source_created_utc"] != item["first_observed_at_utc"] for item in citations),
            "source and observation times collapsed",
        )
        check(
            all(item.get("source_thread_created_utc") is not None for item in citations),
            "thread creation time unavailable in this recorded source",
        )
        self.preserved.update(hashes(self.store_root))
        self.record(
            "issue_history",
            issue=issue,
            alerts=alerts,
            citations=citations,
            reproduction_verified=False,
            resolution_verified=False,
            thread_creation_basis="Reddit post created_utc, never earliest-comment proxy",
        )

    def stable_history(self) -> dict[str, list[dict[str, Any]]]:
        tables = (
            "watch_digest_entries",
            "watch_issue_reports",
            "watch_issue_updates",
            "watch_feedback_keys",
            "watch_preference_events",
            "watch_correction_events",
            "watch_alert_events",
        )
        return {table: self.rows(table) for table in tables}

    def replay(self) -> None:
        before = self.stable_history()
        feedback_before = self.ok(
            "feedback", "list", "--watch", WATCH, label="feedback-before-replay"
        )
        issue_before = self.inspect_issue("history-before-replay")
        result = self.watch("run", label="unchanged-replay", phase="unchanged")
        alerts = result["alerts"]
        self.alert(alerts)
        check(not alerts["new_alert_ids"], "unchanged replay emitted alert")
        check(
            self.stable_history() == before, "unchanged replay duplicated evidence/history/feedback"
        )
        check(
            self.inspect_issue("history-after-replay") == issue_before, "unchanged history differs"
        )
        check(
            self.ok("feedback", "list", "--watch", WATCH, label="feedback-after-replay")
            == feedback_before,
            "unchanged replay changed feedback IDs",
        )
        restarted = self.watch("alerts", label="unchanged-alerts-restart")
        check(
            restarted["alert_ids"] == alerts["alert_ids"] and not restarted["new_alert_ids"],
            "fresh interpreter changed alerts",
        )
        assert_preserved(self.preserved)
        self.record(
            "replay",
            sweep=result,
            stable_ids={table: rows for table, rows in before.items()},
            repeated_alerts=False,
            old_artifacts_unchanged=True,
        )

    def edit_observation(self) -> None:
        payload = self.watch("run", label="evidence-edit", phase="evidence_edit")
        result = payload["alerts"]
        self.alert(result)
        citations = [
            citation
            for event in result["alerts"]
            for citation in event["evidence"]
            if citation["comment_id"] == "reddit:ia2"
        ]
        check(bool(result["new_alert_ids"]) and bool(citations), "edited source evidence not cited")
        check(
            all(
                item["edit_observed_at_utc"] is not None
                and item["edit_observed_at_utc"] > item["first_observed_at_utc"]
                for item in citations
            ),
            "edit observation collapsed into first observation",
        )
        self.record(
            "edit_observation",
            result=result,
            citations=citations,
            source_edit_time_is_observation_time=False,
        )

    def coverage(self) -> None:
        cases = []
        for phase in ("coverage_truncated", "coverage_partial"):
            code, payload = self.cli("watch", "run", "--id", WATCH, label=phase, phase=phase)
            check(
                code == (1 if phase == "coverage_partial" else 0),
                f"coverage phase failed: {payload}",
            )
            alerts = payload["alerts"]
            report = self.alert(alerts)
            check(alerts["refusal_reasons"], "unsupported coverage permitted rate/trend inference")
            expected = (
                "partial_sweep" if phase == "coverage_partial" else "comment_coverage_truncated"
            )
            check(expected in alerts["refusal_reasons"], f"missing coverage refusal: {expected}")
            cases.append(
                {
                    "phase": phase,
                    "exit_code": code,
                    "result": alerts,
                    "coverage": report["coverage"],
                }
            )
        acquisitions = self.rows("watch_acquisitions")
        check(
            {row["completeness"] for row in acquisitions} >= {"unknown", "truncated"},
            "did not exercise unknown and truncated acquisition",
        )
        check(
            any(row["outcome"] == "partial" for row in self.rows("watch_sweeps")),
            "did not persist partial sweep",
        )
        self.record(
            "coverage", cases=cases, unknown_baseline_exercised=True, rate_or_trend_claim=False
        )

    def cohort(self) -> None:
        previous = self.watch("alerts", label="cohort-before")
        text = self.profile_path.read_text(encoding="utf-8")
        check("presented_findings = 20" in text, "unexpected original limit")
        self.profile_path.write_text(
            text.replace("presented_findings = 20", "presented_findings = 21"), encoding="utf-8"
        )
        revised = self.ok(
            "watch", "create", "--profile", str(self.profile_path), label="revise-same-profile"
        )
        result = self.watch("run", label="changed-cohort", phase="unchanged")
        alerts = result["alerts"]
        self.alert(alerts)
        check(
            revised["revision"] == 2 and alerts["cohort_id"] != previous["cohort_id"],
            "profile change failed to reset cohort",
        )
        check(
            alerts["status"] == "baseline" and not alerts["new_alert_ids"],
            "cohort change became demand",
        )
        check(len(self.rows("watches")) == 1, "cohort created a second watch")
        self.record("cohort", revision=revised, result=alerts, change_is_demand=False)

    def scheduler(self) -> None:
        before = hashes(self.store_root)
        rendered = {}
        for kind in ("cron", "launchd"):
            payload = self.watch(
                "schedule",
                "--format",
                kind,
                "--workspace",
                str(self.work_dir),
                label=f"schedule-{kind}",
            )
            check(
                payload["installed"] is False and payload["enabled"] is False,
                "scheduler installed/enabled job",
            )
            argv = payload["argv"]
            check(
                Path(argv[0]).is_absolute() and "--id" in argv and WATCH in argv,
                "scheduler argv not absolute/watch-bound",
            )
            check(Path(payload["log_path"]).is_absolute(), "scheduler log path not absolute")
            if kind == "launchd":
                plist = plistlib.loads(payload["content"].encode())
                check(
                    plist["ProgramArguments"] == argv and plist["RunAtLoad"] is False,
                    "launchd argv/activation mismatch",
                )
            else:
                tokens = shlex.split(payload["content"].replace("\\%", "%"))
                check(
                    "exec" in tokens and all(arg in tokens for arg in argv),
                    "cron argv quoting changed",
                )
            rendered[kind] = payload
        assert_preserved(before)
        self.record(
            "scheduler",
            renders=rendered,
            installed=False,
            enabled=False,
            overlap_prevention=(
                "watch run exclusive durable claim; exercised claim_held and stale_owner"
            ),
        )

    def integrity(self) -> None:
        assert_preserved(self.frozen)
        assert_preserved(self.preserved)
        self.observations["artifact_sha256"] = hashes(self.work_dir)
        self.observations["database_sha256"] = file_sha256(self.state_path)
        self.observations["completed_stages"] = [item["step"] for item in self.evidence] + [
            "integrity"
        ]
        self.record(
            "integrity",
            frozen_sha256=self.frozen,
            historical_artifact_count=len(self.preserved),
            database_sha256=self.observations["database_sha256"],
            one_watch=True,
            semantic_activation_performed=False,
            independent_quality_claim=False,
        )
