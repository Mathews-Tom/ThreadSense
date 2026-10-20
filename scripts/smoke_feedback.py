"""Behavioral scenario `feedback` (V6): operator feedback and digest ranking through the CLI.

Run through scripts/smoke_monitoring.py:
    uv run python scripts/smoke_monitoring.py --scenario feedback --work-dir DIR

Drives `threadsense watch create|run|digest|explain|policy` and `threadsense feedback
record|list` over the recorded phases in tests/fixtures/watch/incremental/. Two isolated
storages live under DIR: `control` receives no feedback and `treated` receives preferences
and a correction, so the effect of feedback on presentation order is measured against an
identical run. Every CLI call is a fresh interpreter. Each child installs the in-process
recorded Reddit transport from tests/integration/recorded_reddit.py before calling
`threadsense.cli.main` (or refuses Reddit entirely); the payloads are synthesized Reddit JSON,
not live acquisition. No network and no model are used.
"""

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tomllib
from collections.abc import Callable, Mapping
from contextlib import closing
from pathlib import Path
from typing import Any

from smoke_annotations import SmokeFailure, check, child_base_env, decode_payload, file_sha256

from threadsense.monitoring.ranking import DEFAULT_RANKING_POLICY, parse_ranking_policy

REPO_ROOT = Path(__file__).resolve().parents[1]
INTEGRATION_DIR = REPO_ROOT / "tests" / "integration"
FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "watch" / "incremental"
PROFILE_PATH = FIXTURE_DIR / "profile.toml"
PHASES_PATH = FIXTURE_DIR / "phases.json"
WATCH_ID = "sync-conflicts"
OTHER_WATCH_ID = "sync-conflicts-other"
UNKNOWN_ENTRY_ID = "ent-" + "0" * 32
STATE_FILE = "state.sqlite3"
NO_REPLAY = "none"
CLI_TIMEOUT_SECONDS = 120
PHASES = [
    "initial",
    "unchanged",
    "score_only",
    "search_omits_known",
    "edit",
    "partial",
    "reappear",
    "backlog",
]
THREAD_A = "sc1aaa"
THREAD_B = "sc1bbb"
DIGEST_SCHEMA_VERSION = 3
SEVERITY_ORDER = {"high": 3, "medium": 2, "low": 1}
PREFERENCE_KEY = "pref-irrelevant"
USEFUL_KEY = "pref-useful"
CORRECTION_KEY = "corr-misclassified"
CORRECTION_NOTE = "smoke: theme assignment disputed by the operator"
CUSTOM_POLICY_NAME = "zero-preference"
ENTRY_VIEW_FIELDS = (
    "entry_id",
    "kind",
    "source_thread_id",
    "revision_id",
    "theme_key",
    "severity",
    "finding_comment_count",
    "novel_evidence_comment_ids",
    "counts",
    "thread_text_changed",
)
COMMENT_VIEW_FIELDS = ("comment_id", "kind", "text_sha256", "previous_text_sha256", "excerpt")

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


def bounded_preference_points(value: str) -> int:
    """Points the default policy gives one preference value, after its bound."""
    bound = DEFAULT_RANKING_POLICY.preference_bound
    return max(-bound, min(bound, DEFAULT_RANKING_POLICY.preference_points[value]))


def entry_view(entry: Mapping[str, Any]) -> dict[str, Any]:
    """Evidence fields of an entry payload without storage- or time-specific values."""
    key = "novel_evidence" if entry["kind"] == "finding" else "novel_comments"
    view = {field: entry.get(field) for field in ENTRY_VIEW_FIELDS}
    view["comments"] = [
        {field: comment.get(field) for field in COMMENT_VIEW_FIELDS} for comment in entry[key]
    ]
    return view


def component(item: Mapping[str, Any], name: str) -> dict[str, Any]:
    matches = [entry for entry in item["components"] if entry["component"] == name]
    check(len(matches) == 1, f"{item['entry_id']}: {len(matches)} {name} components")
    found: dict[str, Any] = matches[0]
    return found


def order_ids(ranking: Mapping[str, Any]) -> list[str]:
    return [str(item["entry_id"]) for item in ranking["order"]]


def ranked_item(ranking: Mapping[str, Any], entry_id: str) -> dict[str, Any]:
    items = [item for item in ranking["order"] if item["entry_id"] == entry_id]
    check(len(items) == 1, f"ranking: {len(items)} items for {entry_id}")
    found: dict[str, Any] = items[0]
    return found


def check_ranking_invariants(ranking: Mapping[str, Any], label: str) -> None:
    """Semantic scoring is disclosed and inert; corrections are no input; scores add up."""
    check(
        ranking["semantic"]["status"] == "not_eligible",
        f"{label}: semantic status {ranking['semantic']}",
    )
    check(
        ranking["corrections"] == "not_a_ranking_input",
        f"{label}: corrections {ranking['corrections']}",
    )
    ranks = [int(item["rank"]) for item in ranking["order"]]
    check(ranks == list(range(1, len(ranks) + 1)), f"{label}: ranks {ranks}")
    for item in ranking["order"]:
        semantic = component(item, "semantic")
        check(
            (semantic["points"], semantic["status"]) == (None, "not_eligible"),
            f"{label}: {item['entry_id']} semantic component {semantic}",
        )
        points = [part["points"] for part in item["components"] if part["points"] is not None]
        total = sum(int(value) for value in points)
        check(total == item["score"], f"{label}: {item['entry_id']} {total} != {item['score']}")


def toml_scalar(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, int | str):
        raise SmokeFailure(f"policy value {value!r} has no TOML rendering in this scenario")
    return str(value) if isinstance(value, int) else json.dumps(value)


def render_toml(table: Mapping[str, Any], path: tuple[str, ...] = ()) -> list[str]:
    """Scalars first, then one `[a.b]` header per nested table."""
    lines = [f"[{'.'.join(path)}]"] if path else []
    lines.extend(
        f"{key} = {toml_scalar(value)}"
        for key, value in table.items()
        if not isinstance(value, Mapping)
    )
    for key, value in table.items():
        if isinstance(value, Mapping):
            lines.extend(["", *render_toml(value, (*path, key))])
    return lines


class PayloadLog:
    """Persists every CLI call's argv, phase, exit code and payload in call order."""

    def __init__(self, payload_dir: Path) -> None:
        self.payload_dir = payload_dir
        self.call_count = 0

    def persist(self, label: str, document: dict[str, Any]) -> None:
        self.call_count += 1
        self.payload_dir.mkdir(exist_ok=True)
        path = self.payload_dir / f"{self.call_count:03d}-{label}.json"
        path.write_text(json.dumps(document, indent=2, sort_keys=True), encoding="utf-8")


class Storage:
    """One isolated THREADSENSE_STORAGE_ROOT under the work dir, driven through the CLI."""

    def __init__(self, name: str, work_dir: Path, log: PayloadLog) -> None:
        self.name = name
        self.work_dir = work_dir
        self.root = work_dir / name
        self.log = log
        self.base_env = child_base_env()

    @property
    def monitoring_root(self) -> Path:
        return self.root / "monitoring"

    @property
    def state_path(self) -> Path:
        return self.monitoring_root / STATE_FILE

    def cli(self, *args: str, label: str, phase: str = NO_REPLAY) -> tuple[int, dict[str, Any]]:
        env = {
            **self.base_env,
            "THREADSENSE_STORAGE_ROOT": str(self.root),
            "THREADSENSE_REDDIT_MAX_RETRIES": "0",
            "THREADSENSE_REDDIT_REQUEST_DELAY": "0",
        }
        command = [
            *(sys.executable, "-c", REPLAY_CHILD, str(INTEGRATION_DIR)),
            *(str(PHASES_PATH), phase, "--output-format", "json", *args),
        ]
        named = f"{self.name}-{label}"
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
            raise SmokeFailure(f"{named}: threadsense {' '.join(args)} timed out") from error
        payload = decode_payload(completed.stdout, completed.stderr, " ".join(args))
        self.log.persist(
            named,
            {
                "argv": list(args),
                "phase": phase,
                "exit_code": completed.returncode,
                "payload": payload,
            },
        )
        return completed.returncode, payload

    def rows(self, sql: str, params: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        """Read-only query against the storage's state database."""
        uri = f"{self.state_path.as_uri()}?mode=ro"
        with closing(sqlite3.connect(uri, uri=True)) as conn:
            return [tuple(row) for row in conn.execute(sql, params).fetchall()]

    def entry_payloads(self, watch_id: str) -> dict[str, dict[str, Any]]:
        rows = self.rows(
            "SELECT entry_id, payload_json FROM watch_digest_entries WHERE watch_id = ?",
            (watch_id,),
        )
        return {str(entry_id): json.loads(str(payload)) for entry_id, payload in rows}

    def unpresented(self, watch_id: str) -> list[dict[str, Any]]:
        rows = self.rows(
            "SELECT payload_json FROM watch_digest_entries "
            "WHERE watch_id = ? AND digest_id IS NULL ORDER BY entry_id",
            (watch_id,),
        )
        return [json.loads(str(row[0])) for row in rows]

    def feedback_keys(self) -> int:
        rows = self.rows("SELECT COUNT(*) FROM watch_feedback_keys WHERE watch_id = ?", (WATCH_ID,))
        return int(rows[0][0])

    def file_hashes(self) -> dict[str, str]:
        """Every file under the monitoring root except the state database and its journals."""
        return {
            path.relative_to(self.monitoring_root).as_posix(): file_sha256(path)
            for path in sorted(self.monitoring_root.rglob("*"))
            if path.is_file() and not path.name.startswith(STATE_FILE)
        }

    def create_watch(self, profile_path: Path, label: str) -> dict[str, Any]:
        code, created = self.cli("watch", "create", "--profile", str(profile_path), label=label)
        check(code == 0, f"{self.name} {label}: watch create failed: {created}")
        check(
            Path(created["state_path"]) == self.state_path,
            f"{self.name} {label}: state path is not under the work dir storage",
        )
        return created

    def watch_run(self, watch_id: str, phase: str) -> dict[str, Any]:
        argv = ("watch", "run", "--id", watch_id)
        code, payload = self.cli(*argv, label=f"run-{watch_id}-{phase}", phase=phase)
        check(
            code == 0 and payload.get("artifact_type") == "watch_run",
            f"{self.name} {phase}: watch run failed: exit {code} {payload}",
        )
        digest: dict[str, Any] = payload["digest"]
        return digest

    def presented(self, digest: dict[str, Any], label: str) -> dict[str, Any]:
        """The digest's JSON artifact, checked against its result and the stored entries."""
        named = f"{self.name} {label}"
        check(
            (digest["status"], digest["resumed"]) == ("presented", False),
            f"{named}: digest {digest['status']} resumed={digest['resumed']}",
        )
        document: dict[str, Any] = json.loads(Path(digest["json_path"]).read_text("utf-8"))
        check(
            (document["digest_id"], document["schema_version"])
            == (digest["digest_id"], DIGEST_SCHEMA_VERSION),
            f"{named}: artifact {document['digest_id']} schema {document['schema_version']}",
        )
        ids = [str(entry["entry_id"]) for entry in document["entries"]]
        check(ids == list(digest["entry_ids"]), f"{named}: artifact entries {ids}")
        stored = self.entry_payloads(WATCH_ID)
        check(
            all(entry == stored[str(entry["entry_id"])] for entry in document["entries"]),
            f"{named}: artifact entries differ from the stored entry payloads",
        )
        ranking = document["ranking"]
        summary = digest["ranking"]
        check(
            summary["ranked_entry_ids"] == order_ids(ranking)
            and sorted(order_ids(ranking)) == sorted(ids),
            f"{named}: ranked ids {summary['ranked_entry_ids']} vs entries {ids}",
        )
        basis = {key: summary[key] for key in ranking["basis"]}
        check(basis == ranking["basis"], f"{named}: result basis {basis} != {ranking['basis']}")
        check_ranking_invariants(ranking, named)
        return document


class FeedbackScenario:
    """Drives feedback, ranking, explanation and policy selection over recorded phases."""

    def __init__(self, work_dir: Path) -> None:
        self.work_dir = work_dir
        self.evidence: list[dict[str, Any]] = []
        log = PayloadLog(work_dir / "payloads")
        self.control = Storage("control", work_dir, log)
        self.treated = Storage("treated", work_dir, log)
        self.written: dict[str, dict[str, str]] = {"control": {}, "treated": {}}
        document = json.loads(PHASES_PATH.read_text(encoding="utf-8"))
        self.phase_names: list[str] = [str(phase["name"]) for phase in document["phases"]]
        self.baseline_payloads: dict[str, dict[str, Any]] = {}
        self.preferenced = ""
        self.corrected = ""
        self.thread_update = ""
        self.preference_id = ""
        self.correction_id = ""
        self.ranked_digest_id = ""
        self.explained: dict[str, Any] = {}

    def record(self, step: str, **observed: Any) -> None:
        self.evidence.append({"step": step, "observed": observed})
        print(f"step={step} ok")

    def track(self, store: Storage) -> dict[str, str]:
        """Fail if any recorded file changed; record the hash of every new file once."""
        written = self.written[store.name]
        current = store.file_hashes()
        changed = sorted(path for path, sha in written.items() if current.get(path) != sha)
        check(not changed, f"{store.name}: written files changed or vanished {changed}")
        for path, sha in current.items():
            written.setdefault(path, sha)
        return current

    def run(self) -> None:
        check(self.phase_names == PHASES, f"recorded phases differ: {self.phase_names}")
        self.baseline()
        self.record_feedback()
        self.refusals()
        self.feedback_listing()
        self.compare_digests()
        self.current_preference()
        self.policy_switch()
        self.final_immutability()

    def baseline(self) -> None:
        observed: dict[str, Any] = {}
        first_digests: dict[str, list[str]] = {}
        views: dict[str, dict[str, Any]] = {}
        for store in (self.control, self.treated):
            created = store.create_watch(PROFILE_PATH, "create")
            digest = store.watch_run(WATCH_ID, "initial")
            document = store.presented(digest, "initial")
            threads = sorted({str(entry["source_thread_id"]) for entry in document["entries"]})
            check(threads == [THREAD_A], f"{store.name} initial: presented threads {threads}")
            pending = store.unpresented(WATCH_ID)
            kinds = sorted(str(entry["kind"]) for entry in pending)
            pending_threads = {str(entry["source_thread_id"]) for entry in pending}
            check(
                (pending_threads, kinds) == ({THREAD_B}, ["finding", "finding", "thread_update"]),
                f"{store.name} initial: unpresented {pending_threads} {kinds}",
            )
            payloads = store.entry_payloads(WATCH_ID)
            views[store.name] = {key: entry_view(value) for key, value in payloads.items()}
            first_digests[store.name] = list(digest["entry_ids"])
            files = self.track(store)
            observed[store.name] = {
                "watch_revision": created["revision"],
                "first_digest_entry_ids": first_digests[store.name],
                "first_ranking_basis": document["ranking"]["basis"],
                "unpresented_entry_ids": [str(entry["entry_id"]) for entry in pending],
                "stored_entries": len(payloads),
                "artifact_files": len(files),
            }
        check(
            first_digests["control"] == first_digests["treated"],
            f"initial: first digests differ {first_digests}",
        )
        check(views["control"] == views["treated"], "initial: stored entry evidence differs")
        self.baseline_payloads = self.treated.entry_payloads(WATCH_ID)
        self.choose_targets()
        self.record(
            "baseline",
            **observed,
            preferenced_finding=self.preferenced,
            corrected_finding=self.corrected,
            thread_update=self.thread_update,
        )

    def choose_targets(self) -> None:
        """Thread B's highest-severity unpresented finding receives the preference."""
        pending = self.treated.unpresented(WATCH_ID)
        findings = sorted(
            (entry for entry in pending if entry["kind"] == "finding"),
            key=lambda entry: -SEVERITY_ORDER[str(entry["severity"])],
        )
        severities = [str(entry["severity"]) for entry in findings]
        check(len(set(severities)) == 2, f"initial: B finding severities {severities}")
        self.preferenced = str(findings[0]["entry_id"])
        self.corrected = str(findings[1]["entry_id"])
        updates = [entry for entry in pending if entry["kind"] == "thread_update"]
        self.thread_update = str(updates[0]["entry_id"])

    def feedback(self, label: str, *options: str) -> tuple[int, dict[str, Any]]:
        argv = ("feedback", "record", "--watch", WATCH_ID, *options)
        return self.treated.cli(*argv, label=label)

    def preference_argv(self, target: str, value: str, key: str) -> list[str]:
        return [
            *("--target", target, "--target-kind", "finding", "--kind", "preference"),
            *("--value", value, "--key", key),
        ]

    def recorded(self, label: str, options: list[str], status: str) -> dict[str, Any]:
        code, payload = self.feedback(label, *options)
        observed = (code, payload.get("artifact_type"), payload.get("status"))
        check(observed == (0, "feedback_event", status), f"{label}: {observed} {payload}")
        event: dict[str, Any] = payload["feedback"]
        return event

    def record_feedback(self) -> None:
        options = self.preference_argv(self.preferenced, "irrelevant", PREFERENCE_KEY)
        preference = self.recorded("preference", options, "recorded")
        check(
            (preference["kind"], preference["sequence"], preference["scope_kind"])
            == ("preference", 1, "finding"),
            f"preference: event {preference}",
        )
        repeat = self.recorded("preference-repeat", options, "unchanged")
        check(
            repeat["feedback_id"] == preference["feedback_id"],
            f"preference repeat: {repeat['feedback_id']} != {preference['feedback_id']}",
        )
        correction = self.recorded(
            "correction",
            [
                *("--target", self.corrected, "--target-kind", "finding"),
                *("--kind", "correction", "--value", "misclassified"),
                *("--key", CORRECTION_KEY, "--note", CORRECTION_NOTE),
            ],
            "recorded",
        )
        check(
            (correction["kind"], correction["sequence"], correction["note"])
            == ("correction", 1, CORRECTION_NOTE),
            f"correction: event {correction}",
        )
        self.preference_id = str(preference["feedback_id"])
        self.correction_id = str(correction["feedback_id"])
        self.record(
            "feedback_recorded",
            preference_id=self.preference_id,
            preference_scope=[preference["scope_kind"], preference["scope_key"]],
            repeat_status="unchanged",
            repeat_feedback_id=repeat["feedback_id"],
            correction_id=self.correction_id,
            correction_target=correction["target_id"],
            feedback_keys=self.treated.feedback_keys(),
        )

    def other_watch_entry(self) -> str:
        """Create and run a second watch in the treated storage; return one of its entries."""
        text = PROFILE_PATH.read_text(encoding="utf-8")
        id_line = f'id = "{WATCH_ID}"\n'
        check(text.count(id_line) == 1, "profile: watch id line is not present exactly once")
        other_path = self.work_dir / "profiles" / f"{OTHER_WATCH_ID}.toml"
        other_path.parent.mkdir(exist_ok=True)
        other_path.write_text(text.replace(id_line, f'id = "{OTHER_WATCH_ID}"\n'), "utf-8")
        self.treated.create_watch(other_path, "create-other")
        self.treated.watch_run(OTHER_WATCH_ID, "initial")
        entries = sorted(self.treated.entry_payloads(OTHER_WATCH_ID))
        check(bool(entries), "other watch: no digest entries")
        return entries[0]

    def refuse(self, label: str, options: list[str], reason: str) -> dict[str, Any]:
        """A feedback record that must exit 1 with `reason` and write no feedback."""
        return self.expect_refusal(label, lambda: self.feedback(label, *options), reason)

    def refuse_explain(self, label: str, entry_id: str, reason: str) -> dict[str, Any]:
        """A `watch explain` that must exit 1 with `reason` and write no feedback."""
        argv = ("watch", "explain", "--id", WATCH_ID, "--entry", entry_id)
        return self.expect_refusal(label, lambda: self.treated.cli(*argv, label=label), reason)

    def expect_refusal(
        self, label: str, invoke: Callable[[], tuple[int, dict[str, Any]]], reason: str
    ) -> dict[str, Any]:
        before = self.treated.feedback_keys()
        code, payload = invoke()
        details = payload.get("error", {}).get("details", {})
        observed = (code, payload.get("status"), details.get("reason"))
        check(observed == (1, "error", reason), f"{label}: refusal {observed}: {payload}")
        after = self.treated.feedback_keys()
        check(after == before, f"{label}: feedback keys {before} -> {after}")
        return {"exit_code": code, "reason": reason, "feedback_keys": after, "details": details}

    def refusals(self) -> None:
        other_entry = self.other_watch_entry()
        cases = [
            (
                "refuse-idempotency-conflict",
                self.preference_argv(self.preferenced, "already_known", PREFERENCE_KEY),
                "idempotency_conflict",
            ),
            (
                "refuse-unknown-target",
                self.preference_argv(UNKNOWN_ENTRY_ID, "irrelevant", "pref-unknown"),
                "unknown_target",
            ),
            (
                "refuse-cross-watch",
                self.preference_argv(other_entry, "irrelevant", "pref-cross-watch"),
                "cross_watch_target",
            ),
            (
                "refuse-kind-mismatch",
                self.preference_argv(self.thread_update, "irrelevant", "pref-mismatch"),
                "target_kind_mismatch",
            ),
        ]
        observed = {label: self.refuse(label, argv, reason) for label, argv, reason in cases}
        explain_cases = [
            ("refuse-explain-unknown-entry", UNKNOWN_ENTRY_ID, "unknown_entry"),
            ("refuse-explain-cross-watch", other_entry, "cross_watch_target"),
        ]
        for label, entry_id, reason in explain_cases:
            observed[label] = self.refuse_explain(label, entry_id, reason)
        conflict = observed["refuse-idempotency-conflict"]["details"]
        check(
            conflict.get("feedback_id") == self.preference_id,
            f"idempotency conflict cites {conflict.get('feedback_id')}",
        )
        mismatch = observed["refuse-kind-mismatch"]["details"]
        check(
            mismatch.get("entry_kind") == "thread_update",
            f"kind mismatch: entry kind {mismatch.get('entry_kind')}",
        )
        self.track(self.treated)
        self.record("feedback_refusals", other_watch_entry=other_entry, **observed)

    def feedback_listing(self) -> None:
        argv = ("feedback", "list", "--watch", WATCH_ID)
        code, payload = self.treated.cli(*argv, label="feedback-list")
        check(
            (code, payload.get("artifact_type")) == (0, "feedback_history"),
            f"feedback list: exit {code} {payload}",
        )
        preferences = [str(event["feedback_id"]) for event in payload["preferences"]]
        corrections = [str(event["feedback_id"]) for event in payload["corrections"]]
        effective = [str(event["feedback_id"]) for event in payload["effective_preferences"]]
        view = [str(item["target_id"]) for item in payload["correction_view"]]
        observed = (preferences, corrections, effective, view)
        expected = (
            [self.preference_id],
            [self.correction_id],
            [self.preference_id],
            [self.corrected],
        )
        check(observed == expected, f"feedback list: {observed}")
        kinds = {str(event["kind"]) for event in payload["preferences"]}
        check(kinds == {"preference"}, f"feedback list: preference kinds {kinds}")
        self.record(
            "feedback_list",
            preferences=preferences,
            corrections=corrections,
            effective_preferences=effective,
            correction_view_targets=view,
        )

    def digest_both(self) -> dict[str, dict[str, Any]]:
        documents: dict[str, dict[str, Any]] = {}
        for store in (self.control, self.treated):
            code, payload = store.cli("watch", "digest", "--id", WATCH_ID, label="digest")
            check(
                (code, payload.get("artifact_type")) == (0, "watch_digest_result"),
                f"{store.name} digest: exit {code} {payload}",
            )
            documents[store.name] = store.presented(payload, "digest")
            self.track(store)
        return documents

    def compare_digests(self) -> None:
        documents = self.digest_both()
        control, treated = documents["control"], documents["treated"]
        control_ids = [str(entry["entry_id"]) for entry in control["entries"]]
        treated_ids = [str(entry["entry_id"]) for entry in treated["entries"]]
        check(control_ids == treated_ids, f"digest: presented {control_ids} vs {treated_ids}")
        check(self.preferenced in control_ids, "digest: the preferenced finding was not presented")
        control_views = [entry_view(entry) for entry in control["entries"]]
        treated_views = [entry_view(entry) for entry in treated["entries"]]
        check(control_views == treated_views, "digest: presented evidence differs")
        check(control["backlog"] == treated["backlog"], "digest: backlog differs")
        stored = self.treated.entry_payloads(WATCH_ID)
        check(stored == self.baseline_payloads, "digest: treated stored entry payloads changed")
        control_order = order_ids(control["ranking"])
        treated_order = order_ids(treated["ranking"])
        findings = [
            entry_id
            for entry_id in control_order
            if ranked_item(control["ranking"], entry_id)["kind"] == "finding"
        ]
        check(
            findings[0] == self.preferenced and control_order[-1] != self.preferenced,
            f"digest: control order {control_order} for {self.preferenced}",
        )
        expected = [entry for entry in control_order if entry != self.preferenced]
        check(
            treated_order == [*expected, self.preferenced],
            f"digest: treated order {treated_order} vs control {control_order}",
        )
        shift = self.check_score_effects(control["ranking"], treated["ranking"])
        self.ranked_digest_id = str(treated["digest_id"])
        self.record(
            "ranked_digest_comparison",
            presented_entry_ids=control_ids,
            control_order=control_order,
            treated_order=treated_order,
            control_basis=control["ranking"]["basis"],
            treated_basis=treated["ranking"]["basis"],
            backlog=treated["backlog"],
            preference_shift=shift,
            corrections=treated["ranking"]["corrections"],
            semantic=treated["ranking"]["semantic"]["status"],
        )
        self.explain_preferenced(treated["ranking"])

    def check_score_effects(
        self, control: Mapping[str, Any], treated: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Only the preferenced entry's preference component and score differ."""
        check(
            (control["basis"]["preference_sequence"], treated["basis"]["preference_sequence"])
            == (0, 1),
            f"digest: preference sequences {control['basis']} {treated['basis']}",
        )
        same = ("profile_revision", "policy_id", "objective", "policy_name")
        check(
            all(control["basis"][key] == treated["basis"][key] for key in same)
            and control["basis"]["policy_id"] == DEFAULT_RANKING_POLICY.policy_id,
            f"digest: bases {control['basis']} {treated['basis']}",
        )
        check(
            self.correction_id not in json.dumps(treated),
            "digest: the ranking references the correction",
        )
        for entry_id in order_ids(control):
            if entry_id == self.preferenced:
                continue
            before = {k: v for k, v in ranked_item(control, entry_id).items() if k != "rank"}
            after = {k: v for k, v in ranked_item(treated, entry_id).items() if k != "rank"}
            check(before == after, f"digest: {entry_id} ranking changed without feedback on it")
        before_item = ranked_item(control, self.preferenced)
        after_item = ranked_item(treated, self.preferenced)
        control_pref = component(before_item, "preference")
        treated_pref = component(after_item, "preference")
        points = bounded_preference_points("irrelevant")
        cited = [str(match["feedback_id"]) for match in treated_pref["matched"]]
        check(
            (control_pref["points"], control_pref["matched"]) == (0, []),
            f"digest: control preference component {control_pref}",
        )
        check(
            (treated_pref["points"], cited) == (points, [self.preference_id]),
            f"digest: treated preference component {treated_pref}",
        )
        check(
            after_item["score"] == before_item["score"] + points,
            f"digest: scores {before_item['score']} -> {after_item['score']}",
        )
        return {
            "entry_id": self.preferenced,
            "control_score": before_item["score"],
            "treated_score": after_item["score"],
            "preference_points": treated_pref["points"],
            "cited_feedback_ids": cited,
        }

    def explain(self, entry_id: str, label: str, *flags: str) -> dict[str, Any]:
        argv = ("watch", "explain", "--id", WATCH_ID, "--entry", entry_id, *flags)
        code, payload = self.treated.cli(*argv, label=label)
        observed = (code, payload.get("artifact_type"), payload.get("status"))
        check(observed == (0, "watch_explanation", "ranked"), f"{label}: {observed} {payload}")
        check(
            payload["replay"] == "matches_written_digest",
            f"{label}: replay {payload['replay']}",
        )
        return payload

    def explain_preferenced(self, ranking: Mapping[str, Any]) -> None:
        payload = self.explain(self.preferenced, "explain-preferenced")
        item = payload["ranking"]
        check(item == ranked_item(ranking, self.preferenced), "explain: ranking differs")
        check(
            (item["rank"], payload["basis"]["preference_sequence"]) == (len(ranking["order"]), 1),
            f"explain: rank {item['rank']} basis {payload['basis']}",
        )
        check(
            payload["digest"]["digest_id"] == self.ranked_digest_id,
            f"explain: digest {payload['digest']}",
        )
        self.explained = {"rank": item["rank"], "score": item["score"]}
        self.record(
            "explain_preferenced",
            entry_id=self.preferenced,
            digest_id=self.ranked_digest_id,
            replay=payload["replay"],
            rank=item["rank"],
            entries=len(ranking["order"]),
            score=item["score"],
            basis=payload["basis"],
        )

    def current_preference(self) -> None:
        options = self.preference_argv(self.preferenced, "useful", USEFUL_KEY)
        useful = self.recorded("preference-useful", options, "recorded")
        check(useful["sequence"] == 2, f"useful preference: sequence {useful['sequence']}")
        before = self.track(self.treated)
        payload = self.explain(self.preferenced, "explain-current", "--current")
        historical, current = payload["ranking"], payload["current"]
        check(
            {"rank": historical["rank"], "score": historical["score"]} == self.explained,
            f"explain current: historical {historical['rank']} {historical['score']}",
        )
        check(
            (current["recorded"], current["basis"]["preference_sequence"]) == (False, 2),
            f"explain current: {current['recorded']} basis {current['basis']}",
        )
        delta = bounded_preference_points("useful") - bounded_preference_points("irrelevant")
        cited = [str(match["feedback_id"]) for match in component(current, "preference")["matched"]]
        check(
            current["score"] > historical["score"]
            and current["score"] - historical["score"] == delta
            and cited == [str(useful["feedback_id"])],
            f"explain current: score {historical['score']} -> {current['score']} cites {cited}",
        )
        after = self.track(self.treated)
        check(after == before, "explain current: monitoring files changed")
        self.record(
            "current_preference",
            useful_feedback_id=useful["feedback_id"],
            historical_rank=historical["rank"],
            historical_score=historical["score"],
            current_rank=current["rank"],
            current_score=current["score"],
            current_cites=cited,
            unchanged_files=len(after),
        )

    def policy(self, label: str, *flags: str) -> dict[str, Any]:
        argv = ("watch", "policy", "--id", WATCH_ID, *flags)
        code, payload = self.treated.cli(*argv, label=label)
        check(code == 0, f"{label}: exit {code} {payload}")
        return payload

    def select_policy(self, label: str, flags: tuple[str, str], expected: tuple[str, ...]) -> None:
        payload = self.policy(label, *flags)
        observed = (
            payload["artifact_type"],
            payload["status"],
            payload["policy_id"],
            payload["previous_policy_id"],
        )
        check(observed == expected, f"{label}: {observed}")

    def write_custom_policy(self) -> tuple[Path, str]:
        document = DEFAULT_RANKING_POLICY.to_dict()
        document["name"] = CUSTOM_POLICY_NAME
        document["preference"] = {key: 0 for key in document["preference"]}
        text = "\n".join(render_toml(document)) + "\n"
        check(tomllib.loads(text) == document, "policy: rendered TOML does not round-trip")
        path = self.work_dir / "policies" / f"{CUSTOM_POLICY_NAME}.toml"
        path.parent.mkdir()
        path.write_text(text, encoding="utf-8")
        return path, parse_ranking_policy(document).policy_id

    def run_ranked(self, phase: str, policy_id: str) -> dict[str, Any]:
        document = self.treated.presented(self.treated.watch_run(WATCH_ID, phase), phase)
        basis = document["ranking"]["basis"]
        check(
            (basis["policy_id"], basis["preference_sequence"]) == (policy_id, 2),
            f"{phase}: ranking basis {basis}",
        )
        self.track(self.treated)
        return document

    def policy_switch(self) -> None:
        shown = self.policy("policy-show")
        default_id = DEFAULT_RANKING_POLICY.policy_id
        check(
            (shown["artifact_type"], shown["active_policy_id"], shown["default_policy_id"])
            == ("ranking_policy", default_id, default_id),
            f"policy show: {shown}",
        )
        path, custom_id = self.write_custom_policy()
        selected = ("ranking_policy_selection", "selected")
        self.select_policy("policy-set", ("--set", str(path)), (*selected, custom_id, default_id))
        self.select_policy(
            "policy-set-repeat",
            ("--set", str(path)),
            ("ranking_policy_selection", "unchanged", custom_id, custom_id),
        )
        custom = self.run_ranked("search_omits_known", custom_id)
        zero = [component(item, "preference")["points"] for item in custom["ranking"]["order"]]
        check(
            custom["ranking"]["basis"]["policy_name"] == CUSTOM_POLICY_NAME and set(zero) == {0},
            f"custom digest: {custom['ranking']['basis']} preference points {zero}",
        )
        self.select_policy(
            "policy-select-default", ("--select", default_id), (*selected, default_id, custom_id)
        )
        default = self.run_ranked("edit", default_id)
        historical = self.explain(self.preferenced, "explain-historical-default")
        check(
            historical["basis"]["policy_id"] == default_id
            and {key: historical["ranking"][key] for key in ("rank", "score")} == self.explained,
            f"explain historical: {historical['basis']} {historical['ranking']['rank']}",
        )
        custom_entry = order_ids(custom["ranking"])[0]
        replayed = self.explain(custom_entry, "explain-custom-entry")
        check(
            replayed["basis"]["policy_id"] == custom_id
            and replayed["ranking"] == ranked_item(custom["ranking"], custom_entry),
            f"explain custom entry: {replayed['basis']}",
        )
        self.record(
            "policy_switch",
            default_policy_id=default_id,
            custom_policy_id=custom_id,
            custom_digest_id=custom["digest_id"],
            custom_order=order_ids(custom["ranking"]),
            default_digest_id=default["digest_id"],
            default_order=order_ids(default["ranking"]),
            historical_explain_policy=historical["basis"]["policy_id"],
            historical_rank=historical["ranking"]["rank"],
            custom_entry=custom_entry,
            custom_entry_explain_policy=replayed["basis"]["policy_id"],
        )

    def final_immutability(self) -> None:
        observed: dict[str, Any] = {}
        for store in (self.control, self.treated):
            self.track(store)
            written = self.written[store.name]
            rows = store.rows(
                "SELECT digest_id, status, json_path, json_sha256, markdown_path, "
                "markdown_sha256 FROM watch_digests ORDER BY watch_id, sequence",
                (),
            )
            for digest_id, status, json_path, json_sha, md_path, md_sha in rows:
                check(status == "written", f"{store.name}: digest {digest_id} is {status}")
                for relative, sha in ((json_path, json_sha), (md_path, md_sha)):
                    actual = file_sha256(store.monitoring_root / str(relative))
                    check(
                        actual == sha == written.get(str(relative)),
                        f"{store.name}: {relative} hash differs from its digest row or write",
                    )
            observed[store.name] = {"digests": len(rows), "tracked_files": len(written)}
        self.record("final_immutability", **observed)
