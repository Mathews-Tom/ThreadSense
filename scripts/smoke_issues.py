"""Behavioral scenario `issues` (V5): cross-thread issue history through the real CLI.

Run through scripts/smoke_monitoring.py:
    uv run python scripts/smoke_monitoring.py --scenario issues --work-dir DIR

Drives `threadsense watch create|run` and `threadsense issues inspect|link|unlink` over the
recorded phases in tests/fixtures/watch/issues/. Every CLI call is a fresh interpreter with
isolated storage under DIR. Each child installs the in-process recorded Reddit transport from
tests/integration/recorded_reddit.py before calling `threadsense.cli.main`; the payloads are
synthesized Reddit JSON, not live acquisition. No network and no model are used.
"""

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path
from typing import Any

from smoke_annotations import SmokeFailure, check, child_base_env, decode_payload, file_sha256

from threadsense.monitoring.issue_records import issue_id_for

REPO_ROOT = Path(__file__).resolve().parents[1]
INTEGRATION_DIR = REPO_ROOT / "tests" / "integration"
ISSUES_FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "watch" / "issues"
ISSUES_PROFILE_PATH = ISSUES_FIXTURE_DIR / "profile.toml"
ISSUES_PHASES_PATH = ISSUES_FIXTURE_DIR / "phases.json"
WATCH_ID = "export-issues"
OTHER_WATCH_ID = "export-issues-other"
UNKNOWN_WATCH_ID = "no-such-watch"
UNKNOWN_ISSUE_ID = "iss-" + "0" * 32
MALFORMED_ISSUE_ID = "iss-xyz"
STATE_FILE = "state.sqlite3"
NO_REPLAY = "none"
CLI_TIMEOUT_SECONDS = 120
PHASES = ["initial", "repost", "updates", "unchanged"]
THREAD_A = "ix1aaa"
THREAD_B = "ix1bbb"
THREAD_C = "ix1ccc"
THREAD_D = "ix1ddd"
THREAD_E = "ix1eee"
THREAD_F = "ix1fff"
INITIAL_THREADS = (THREAD_A, THREAD_B, THREAD_D, THREAD_E, THREAD_F)
ORIGIN_COMMENT = "reddit:ia1"
COPY_COMMENT = "reddit:ic1"
SHARED_LINK = "https://status.example.com/incidents/77"
SHARED_LINK_REFUSAL = "shared_link_only"
OPERATOR_REASON = "smoke: operator membership decision"
REPOST_COUNTS = {
    "observed_threads": 2,
    "observed_communities": 2,
    "observed_accounts": 3,
    "firsthand_reports": 2,
    "exact_copies": 1,
}
# One update per kind in phase `updates`; cues from UPDATE_CUES matched in thread_a_updates.json.
EXPECTED_CUES = {
    "reproduction": "steps to reproduce",
    "contradiction": "cannot reproduce",
    "workaround": "workaround",
    "resolution_claim": "fixed in",
}
DIGEST_MARKDOWN_PHRASES = ("## Issues", "- Update:", "not resolved")

# Runs in a fresh interpreter: serves one recorded phase in-process through the CLI's
# connector seam (or refuses Reddit for phase "none"), then runs the real CLI.
REPLAY_CHILD = """
import sys
from pathlib import Path

integration_dir, phases_path, phase_name, *argv = sys.argv[1:]
sys.path.insert(0, integration_dir)

import threadsense.cli
from recorded_reddit import PhaseReplayTransport, load_replay_phases, replay_connector_factory


def refuse_reddit(config):
    raise RuntimeError("this command must not request Reddit")


if phase_name == "none":
    threadsense.cli.build_reddit_connector = refuse_reddit
else:
    phases = {phase.name: phase for phase in load_replay_phases(Path(phases_path))}
    transport = PhaseReplayTransport(phases[phase_name])
    threadsense.cli.build_reddit_connector = replay_connector_factory(transport)
sys.exit(threadsense.cli.main(argv))
"""


def member_ids(issue: dict[str, Any]) -> list[str]:
    return sorted(str(member["source_thread_id"]) for member in issue["members"])


def pair_suggestions(issue: dict[str, Any], first: str, second: str) -> list[dict[str, Any]]:
    """Suggestions between two threads, in either comparison direction."""
    return [
        suggestion
        for suggestion in issue["suggestions"]
        if {suggestion["source_thread_id"], suggestion["candidate_thread_id"]} == {first, second}
    ]


def report_for(issue: dict[str, Any], comment_id: str) -> dict[str, Any]:
    rows = [report for report in issue["reports"] if report["comment_id"] == comment_id]
    check(len(rows) == 1, f"{issue['issue_id']}: {len(rows)} reports for {comment_id}")
    report: dict[str, Any] = rows[0]
    return report


def count_values(issue: dict[str, Any], *keys: str) -> tuple[Any, ...]:
    return tuple(issue["counts"][key] for key in keys)


def membership_argv(command: str, watch_id: str, issue_id: str, thread_id: str) -> list[str]:
    """`issues link|unlink` arguments without `--reason`."""
    return [
        *("issues", command, "--watch", watch_id),
        *("--issue", issue_id, "--thread", thread_id),
    ]


def monitoring_file_hashes(monitoring_root: Path) -> dict[str, str]:
    """Every file under the monitoring root except the state database and its journals."""
    return {
        path.relative_to(monitoring_root).as_posix(): file_sha256(path)
        for path in sorted(monitoring_root.rglob("*"))
        if path.is_file() and not path.name.startswith(STATE_FILE)
    }


class IssuesScenario:
    """Drives `threadsense watch` and `threadsense issues` over the recorded issue phases."""

    def __init__(self, work_dir: Path) -> None:
        self.work_dir = work_dir
        self.store_root = work_dir / "store"
        self.payload_dir = work_dir / "payloads"
        self.evidence: list[dict[str, Any]] = []
        self.base_env = child_base_env()
        self.call_count = 0
        self.issue_ids: dict[str, str] = {}
        document = json.loads(ISSUES_PHASES_PATH.read_text(encoding="utf-8"))
        self.phase_names: list[str] = [str(phase["name"]) for phase in document["phases"]]

    @property
    def monitoring_root(self) -> Path:
        return self.store_root / "monitoring"

    @property
    def state_path(self) -> Path:
        return self.monitoring_root / STATE_FILE

    def record(self, step: str, **observed: Any) -> None:
        self.evidence.append({"step": step, "observed": observed})
        print(f"step={step} ok")

    def persist(self, label: str, document: dict[str, Any]) -> None:
        self.call_count += 1
        self.payload_dir.mkdir(exist_ok=True)
        path = self.payload_dir / f"{self.call_count:03d}-{label}.json"
        path.write_text(json.dumps(document, indent=2, sort_keys=True), encoding="utf-8")

    def cli(self, *args: str, label: str, phase: str = NO_REPLAY) -> tuple[int, dict[str, Any]]:
        env = {
            **self.base_env,
            "THREADSENSE_STORAGE_ROOT": str(self.store_root),
            "THREADSENSE_REDDIT_MAX_RETRIES": "0",
            "THREADSENSE_REDDIT_REQUEST_DELAY": "0",
        }
        command = [
            *(sys.executable, "-c", REPLAY_CHILD, str(INTEGRATION_DIR)),
            *(str(ISSUES_PHASES_PATH), phase, "--output-format", "json", *args),
        ]
        try:
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=False,
                cwd=self.work_dir,
                env=env,
                timeout=CLI_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as error:
            raise SmokeFailure(f"{label}: threadsense {' '.join(args)} timed out") from error
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

    def create_watch(self, profile_path: Path, label: str) -> dict[str, Any]:
        code, created = self.cli("watch", "create", "--profile", str(profile_path), label=label)
        check(code == 0, f"{label}: watch create failed: {created}")
        check(
            Path(created["state_path"]) == self.state_path,
            f"{label}: state path {created['state_path']} is not under the work dir store",
        )
        return created

    def watch_run(self, phase: str) -> dict[str, Any]:
        argv = ("watch", "run", "--id", WATCH_ID)
        code, payload = self.cli(*argv, label=f"run-{phase}", phase=phase)
        check(
            code == 0 and payload.get("artifact_type") == "watch_run",
            f"{phase}: watch run failed: exit {code} {payload}",
        )
        digest: dict[str, Any] = payload["digest"]
        return digest

    def presented_digest(self, phase: str) -> tuple[dict[str, Any], dict[str, Any], str]:
        """Run one phase; return its digest result, JSON artifact and Markdown artifact."""
        digest = self.watch_run(phase)
        check(
            (digest["status"], digest["resumed"]) == ("presented", False),
            f"{phase}: digest {digest['status']} resumed={digest['resumed']}",
        )
        document: dict[str, Any] = json.loads(Path(digest["json_path"]).read_text("utf-8"))
        markdown = Path(digest["markdown_path"]).read_text(encoding="utf-8")
        check(
            (document["digest_id"], document["schema_version"]) == (digest["digest_id"], 2),
            f"{phase}: digest artifact {document['digest_id']} schema {document['schema_version']}",
        )
        grouping = document.get("issues", {}).get("semantic_grouping")
        check(grouping == "not_eligible", f"{phase}: digest semantic grouping {grouping}")
        return digest, document, markdown

    def list_issues(self, label: str) -> list[dict[str, Any]]:
        code, payload = self.cli("issues", "inspect", "--watch", WATCH_ID, label=label)
        check(
            code == 0 and payload.get("artifact_type") == "issue_list",
            f"{label}: issue list failed: exit {code} {payload}",
        )
        issues: list[dict[str, Any]] = payload["issues"]
        return issues

    def issue_states(self, label: str) -> dict[str, str]:
        """Issue state keyed by home thread."""
        listing = self.list_issues(label)
        return {str(item["home_thread_id"]): str(item["state"]) for item in listing}

    def inspect(self, thread_id: str, label: str) -> dict[str, Any]:
        """Inspect the home issue of `thread_id`."""
        issue_id = self.issue_ids[thread_id]
        code, payload = self.cli(
            "issues", "inspect", "--watch", WATCH_ID, "--issue", issue_id, label=label
        )
        check(
            code == 0
            and payload.get("artifact_type") == "issue_inspection"
            and payload.get("issue_id") == issue_id,
            f"{label}: issue inspection failed: exit {code} {payload}",
        )
        return payload

    def change(self, command: str, thread_id: str, into: str) -> dict[str, Any]:
        """`issues link|unlink` of `thread_id` against the home issue of `into`."""
        argv = membership_argv(command, WATCH_ID, self.issue_ids[into], thread_id)
        label = f"{command}-{thread_id}"
        code, payload = self.cli(*argv, "--reason", OPERATOR_REASON, label=label)
        status = "linked" if command == "link" else "unlinked"
        observed = (code, payload.get("artifact_type"), payload.get("status"))
        check(
            observed == (0, "issue_membership_change", status),
            f"{command} {thread_id}: {observed} {payload}",
        )
        event = payload["event"]
        check(
            (event["action"], event["basis"], event["source_thread_id"], event["reason"])
            == (status, "operator", thread_id, OPERATOR_REASON),
            f"{command} {thread_id}: membership event {event}",
        )
        return payload

    def membership_events(self) -> int:
        uri = f"{self.state_path.as_uri()}?mode=ro"
        with closing(sqlite3.connect(uri, uri=True)) as conn:
            row = conn.execute("SELECT COUNT(*) FROM watch_issue_memberships").fetchone()
        return int(row[0])

    def refuse(self, label: str, argv: list[str], reason: str) -> dict[str, Any]:
        """Run a command that must exit 1 with `reason` and append no membership event."""
        before = self.membership_events()
        code, payload = self.cli(*argv, label=label)
        error = payload.get("error", {})
        details = error.get("details", {})
        observed = (code, payload.get("status"), details.get("reason"))
        check(observed == (1, "error", reason), f"{label}: refusal {observed}: {payload}")
        after = self.membership_events()
        check(after == before, f"{label}: membership events {before} -> {after}")
        return {
            "exit_code": code,
            "code": error.get("code"),
            "reason": reason,
            "field": details.get("field"),
            "membership_events": after,
        }

    def run(self) -> None:
        check(self.phase_names == PHASES, f"recorded phases differ: {self.phase_names}")
        self.create_watches()
        self.initial_phase()
        self.repost_phase()
        hashes, reports = self.updates_phase()
        self.operator_link()
        self.refusals()
        self.operator_unlink()
        self.reversible_lineage()
        self.unchanged_phase(hashes, reports)

    def create_watches(self) -> None:
        created = self.create_watch(ISSUES_PROFILE_PATH, "create")
        text = ISSUES_PROFILE_PATH.read_text(encoding="utf-8")
        id_line = f'id = "{WATCH_ID}"\n'
        check(text.count(id_line) == 1, "profile: watch id line is not present exactly once")
        other_path = self.work_dir / "profiles" / f"{OTHER_WATCH_ID}.toml"
        other_path.parent.mkdir()
        other_path.write_text(text.replace(id_line, f'id = "{OTHER_WATCH_ID}"\n'), encoding="utf-8")
        other = self.create_watch(other_path, "create-other")
        self.record(
            "create",
            watch_revision=created["revision"],
            other_watch_id=OTHER_WATCH_ID,
            other_watch_revision=other["revision"],
        )

    def initial_phase(self) -> None:
        digest, _, _ = self.presented_digest("initial")
        listing = self.list_issues("list-initial")
        homes = sorted((str(item["home_thread_id"]), str(item["state"])) for item in listing)
        expected_homes = sorted((thread, "active") for thread in INITIAL_THREADS)
        check(homes == expected_homes, f"initial: issues {homes}")
        self.issue_ids = {str(item["home_thread_id"]): str(item["issue_id"]) for item in listing}
        for thread_id, issue_id in self.issue_ids.items():
            check(issue_id == issue_id_for(WATCH_ID, thread_id), f"initial: {thread_id} issue id")
        issues = {
            thread: self.inspect(thread, f"inspect-initial-{thread}") for thread in INITIAL_THREADS
        }
        separate = {thread: member_ids(issue) for thread, issue in issues.items()}
        check(
            separate == {thread: [thread] for thread in INITIAL_THREADS},
            f"initial: members {separate}",
        )
        firsthand = (
            count_values(issues[THREAD_A], "firsthand_reports"),
            count_values(issues[THREAD_B], "firsthand_reports"),
        )
        check(firsthand == ((2,), (2,)), f"initial: A/B firsthand reports {firsthand}")
        a_b = pair_suggestions(issues[THREAD_A], THREAD_A, THREAD_B)
        check(
            bool(a_b)
            and all(
                (item["status"], item["currently_grouped"]) == ("unconfirmed", False)
                for item in a_b
            ),
            f"initial: A-B suggestions {a_b}",
        )
        d_e = pair_suggestions(issues[THREAD_D], THREAD_D, THREAD_E)
        check(
            bool(d_e)
            and all(
                (item["status"], item["refusal_reason"], item["currently_grouped"])
                == ("refused", SHARED_LINK_REFUSAL, False)
                and SHARED_LINK in item["signals"]["links"]
                for item in d_e
            ),
            f"initial: D-E suggestions {d_e}",
        )
        check(issues[THREAD_F]["suggestions"] == [], "initial: F has suggestions")
        self.record(
            "initial",
            digest_id=digest["digest_id"],
            digest_schema_version=2,
            semantic_grouping="not_eligible",
            issue_ids=dict(self.issue_ids),
            a_b_suggestions=[(item["status"], item["signals"]) for item in a_b],
            d_e_refusals=[(item["refusal_reason"], item["signals"]["links"]) for item in d_e],
            ingestion=digest["issues"],
        )

    def repost_phase(self) -> None:
        digest, document, _ = self.presented_digest("repost")
        self.issue_ids[THREAD_C] = issue_id_for(WATCH_ID, THREAD_C)
        states = self.issue_states("list-repost")
        check(
            (states.get(THREAD_A), states.get(THREAD_C)) == ("active", "inactive"),
            f"repost: issue states {states}",
        )
        issue = self.inspect(THREAD_A, "inspect-repost-a")
        check(member_ids(issue) == [THREAD_A, THREAD_C], f"repost: members {member_ids(issue)}")
        basis = {str(member["source_thread_id"]): member["basis"] for member in issue["members"]}
        check(basis[THREAD_C] == "exact_content_lineage", f"repost: C basis {basis}")
        origin = report_for(issue, ORIGIN_COMMENT)
        copy = report_for(issue, COPY_COMMENT)
        check(
            (copy["source_thread_id"], copy["lineage"], copy["copy_of_report_id"])
            == (THREAD_C, "exact_copy", origin["report_id"]),
            f"repost: copy report {copy}",
        )
        counts = {key: issue["counts"][key] for key in REPOST_COUNTS}
        check(counts == REPOST_COUNTS, f"repost: counts {counts}")
        independence = issue["counts"]["independence"]
        check(
            (independence["status"], independence["verified_independent_reports"])
            == ("not_verified", None),
            f"repost: independence {independence}",
        )
        foreign = [r["comment_id"] for r in issue["reports"] if r["source_thread_id"] == THREAD_B]
        check(not foreign, f"repost: B reports in A's issue {foreign}")
        refs = self.copy_references(document, str(origin["report_id"]))
        self.record(
            "repost",
            digest_id=digest["digest_id"],
            members=member_ids(issue),
            counts=counts,
            copy_report_id=copy["report_id"],
            copy_of_report_id=origin["report_id"],
            entry_refs=refs,
            ingestion=digest["issues"],
        )

    def copy_references(self, document: dict[str, Any], origin_report_id: str) -> list[str]:
        """C's finding entries cite A's issue and the exact-copy report; return entry ids."""
        refs = [
            ref
            for ref in document["issues"]["entry_refs"]
            if (ref["source_thread_id"], ref["kind"]) == (THREAD_C, "finding")
        ]
        check(bool(refs), "repost: digest cites no finding entry of C")
        for ref in refs:
            check(
                (ref["issue_id"], ref["home_issue_id"])
                == (self.issue_ids[THREAD_A], self.issue_ids[THREAD_C]),
                f"repost: entry ref {ref['entry_id']} issue {ref['issue_id']}",
            )
        copies = [
            (report["comment_id"], report["copy_of_report_id"])
            for ref in refs
            for report in ref["reports"]
            if report["lineage"] == "exact_copy"
        ]
        check(copies == [(COPY_COMMENT, origin_report_id)], f"repost: cited copies {copies}")
        cited = [issue["issue_id"] for issue in document["issues"]["issues"]]
        check(self.issue_ids[THREAD_A] in cited, f"repost: digest issues {cited}")
        return [str(ref["entry_id"]) for ref in refs]

    def updates_phase(self) -> tuple[dict[str, str], list[dict[str, Any]]]:
        digest, _, markdown = self.presented_digest("updates")
        issue = self.inspect(THREAD_A, "inspect-updates-a")
        update_counts = issue["update_counts"]
        check(update_counts == dict.fromkeys(EXPECTED_CUES, 1), f"updates: {update_counts}")
        cues = {str(update["kind"]): update["cue"] for update in issue["updates"]}
        check(
            len(issue["updates"]) == len(EXPECTED_CUES) and cues == EXPECTED_CUES,
            f"updates: cues {cues}",
        )
        resolution = issue["resolution"]
        observed = (
            issue["status"],
            issue["issue_status"],
            resolution["resolved"],
            resolution["resolution_claims"],
        )
        check(observed == ("ok", "open", False, 1), f"updates: resolution {observed}")
        counts = count_values(issue, "firsthand_reports", "exact_copies")
        check(counts == (6, 1), f"updates: firsthand/exact copies {counts}")
        missing = [phrase for phrase in DIGEST_MARKDOWN_PHRASES if phrase not in markdown]
        check(not missing, f"updates: digest Markdown lacks {missing}")
        self.record(
            "updates",
            digest_id=digest["digest_id"],
            update_cues=cues,
            resolution=resolution,
            firsthand_reports=counts[0],
            markdown_phrases=list(DIGEST_MARKDOWN_PHRASES),
            ingestion=digest["issues"],
        )
        hashes = monitoring_file_hashes(self.monitoring_root)
        reports: list[dict[str, Any]] = issue["reports"]
        self.record("snapshot", files=len(hashes), a_reports=len(reports))
        return hashes, reports

    def operator_link(self) -> None:
        linked = self.change("link", THREAD_B, THREAD_A)
        issue = self.inspect(THREAD_A, "inspect-linked-b")
        members = member_ids(issue)
        check(members == [THREAD_A, THREAD_B, THREAD_C], f"link: members {members}")
        counts = count_values(issue, "observed_threads", "firsthand_reports", "exact_copies")
        check(counts == (3, 8, 1), f"link: threads/firsthand/exact copies {counts}")
        states = self.issue_states("list-linked-b")
        check(states.get(THREAD_B) == "inactive", f"link: B home issue {states.get(THREAD_B)}")
        self.record(
            "link",
            event_id=linked["event"]["event_id"],
            members=members,
            observed_threads=counts[0],
            firsthand_reports=counts[1],
            exact_copies=counts[2],
            b_home_state=states[THREAD_B],
        )

    def refusals(self) -> None:
        issue_a = self.issue_ids[THREAD_A]
        reason = ["--reason", OPERATOR_REASON]
        cases: list[tuple[str, list[str], str]] = [
            (
                "refuse-unknown-issue",
                [*membership_argv("link", WATCH_ID, UNKNOWN_ISSUE_ID, THREAD_D), *reason],
                "unknown_issue",
            ),
            (
                "refuse-cross-watch",
                [*membership_argv("link", OTHER_WATCH_ID, issue_a, THREAD_D), *reason],
                "unknown_issue",
            ),
            (
                "refuse-malformed-issue",
                [*membership_argv("link", WATCH_ID, MALFORMED_ISSUE_ID, THREAD_D), *reason],
                "invalid_issue_request",
            ),
            (
                "refuse-blank-reason",
                [*membership_argv("link", WATCH_ID, issue_a, THREAD_D), "--reason", "   "],
                "invalid_issue_request",
            ),
            (
                "refuse-already-member",
                [*membership_argv("link", WATCH_ID, issue_a, THREAD_C), *reason],
                "already_member",
            ),
            (
                "refuse-unknown-watch",
                ["issues", "inspect", "--watch", UNKNOWN_WATCH_ID],
                "unknown_watch",
            ),
        ]
        observed = {label: self.refuse(label, argv, expected) for label, argv, expected in cases}
        fields = (
            observed["refuse-malformed-issue"]["field"],
            observed["refuse-blank-reason"]["field"],
        )
        check(fields == ("issue_id", "reason"), f"refusals: invalid request fields {fields}")
        self.record("refusals", **observed)

    def operator_unlink(self) -> None:
        unlinked = self.change("unlink", THREAD_B, THREAD_A)
        check(
            unlinked["membership"]["issue_id"] == self.issue_ids[THREAD_B],
            f"unlink: B moved to {unlinked['membership']}",
        )
        issue = self.inspect(THREAD_A, "inspect-unlinked-b")
        members = member_ids(issue)
        counts = count_values(issue, "observed_threads", "firsthand_reports")
        check(
            (members, counts) == ([THREAD_A, THREAD_C], (2, 6)),
            f"unlink: members {members} threads/firsthand {counts}",
        )
        states = self.issue_states("list-unlinked-b")
        check(states.get(THREAD_B) == "active", f"unlink: B home issue {states.get(THREAD_B)}")
        history = [
            (event["action"], event["basis"], event["issue_id"], event["previous_issue_id"])
            for event in issue["membership_history"]
            if event["source_thread_id"] == THREAD_B
        ]
        issue_a, issue_b = self.issue_ids[THREAD_A], self.issue_ids[THREAD_B]
        expected = [
            ("linked", "operator", issue_a, issue_b),
            ("unlinked", "operator", issue_b, issue_a),
        ]
        check(history == expected, f"unlink: A's history for B {history}")
        repeat = self.refuse(
            "refuse-repeat-unlink",
            [
                *membership_argv("unlink", WATCH_ID, issue_a, THREAD_B),
                *("--reason", OPERATOR_REASON),
            ],
            "not_member",
        )
        self.record(
            "unlink",
            event_id=unlinked["event"]["event_id"],
            members=members,
            observed_threads=counts[0],
            firsthand_reports=counts[1],
            b_home_state=states[THREAD_B],
            b_history=history,
            repeat_unlink=repeat,
        )

    def reversible_lineage(self) -> None:
        origin = report_for(self.inspect(THREAD_A, "inspect-before-unlink-c"), ORIGIN_COMMENT)
        unlinked = self.change("unlink", THREAD_C, THREAD_A)
        check(
            unlinked["membership"]["issue_id"] == self.issue_ids[THREAD_C],
            f"lineage: C moved to {unlinked['membership']}",
        )
        home = self.inspect(THREAD_C, "inspect-unlinked-c")
        counts = count_values(home, "firsthand_reports", "exact_copies")
        check(
            (home["state"], member_ids(home), counts) == ("active", [THREAD_C], (0, 1)),
            f"lineage: C home {home['state']} {member_ids(home)} {counts}",
        )
        copy = report_for(home, COPY_COMMENT)
        check(
            (copy["lineage"], copy["copy_of_report_id"]) == ("exact_copy", origin["report_id"]),
            f"lineage: copy report changed {copy}",
        )
        alone = self.inspect(THREAD_A, "inspect-without-c")
        check(member_ids(alone) == [THREAD_A], f"lineage: A members {member_ids(alone)}")
        self.change("link", THREAD_C, THREAD_A)
        rejoined = self.inspect(THREAD_A, "inspect-relinked-c")
        check(
            member_ids(rejoined) == [THREAD_A, THREAD_C],
            f"lineage: A members after relink {member_ids(rejoined)}",
        )
        states = self.issue_states("list-relinked-c")
        check(states.get(THREAD_C) == "inactive", f"lineage: C home {states.get(THREAD_C)}")
        self.record(
            "reversible_lineage",
            c_home_counts={"firsthand_reports": counts[0], "exact_copies": counts[1]},
            c_copy_of_report_id=copy["copy_of_report_id"],
            a_members_after_relink=member_ids(rejoined),
        )

    def unchanged_phase(self, hashes: dict[str, str], reports: list[dict[str, Any]]) -> None:
        digest = self.watch_run("unchanged")
        check(
            (digest["status"], digest["digest_id"]) == ("no_entries", None),
            f"unchanged: digest {digest['status']} {digest['digest_id']}",
        )
        current = monitoring_file_hashes(self.monitoring_root)
        changed = sorted(path for path, sha in hashes.items() if current.get(path) != sha)
        check(not changed, f"unchanged: prior artifacts changed or vanished {changed}")
        issue = self.inspect(THREAD_A, "inspect-unchanged-a")
        check(issue["reports"] == reports, "unchanged: A's reports differ from the snapshot")
        check(
            member_ids(issue) == [THREAD_A, THREAD_C],
            f"unchanged: A members {member_ids(issue)}",
        )
        self.record(
            "unchanged",
            digest_status=digest["status"],
            preserved_files=len(hashes),
            new_files=sorted(set(current) - set(hashes)),
            a_reports=len(reports),
            ingestion=digest["issues"],
        )
