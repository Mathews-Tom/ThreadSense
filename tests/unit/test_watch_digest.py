from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import tomllib
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from threadsense.errors import MonitoringStateError, RunClaimError
from threadsense.models.canonical import (
    AuthorRef,
    Comment,
    ProvenanceMetadata,
    SourceRef,
    Thread,
)
from threadsense.monitoring import MonitoringStore, parse_watch_profile
from threadsense.monitoring.digest import (
    DigestResult,
    build_digest_entries,
    present_watch_digest,
    rank_pending_digest,
    render_digest_json,
    render_digest_markdown,
)
from threadsense.monitoring.digest_records import PendingDigest
from threadsense.monitoring.issue_records import IssuePolicy, issue_id_for, report_id_for
from threadsense.monitoring.issues import ingest_issue_evidence
from threadsense.monitoring.state import RunClaim
from threadsense.monitoring.sweep_records import (
    AcquisitionRecord,
    ChangeKind,
    CommentChange,
    DiscoveredThread,
    RevisionRecord,
)
from threadsense.observability import TraceContext

FIXTURE_PROFILE = Path(__file__).resolve().parents[1] / "fixtures" / "watch" / "profile_v1.toml"
WATCH_ID = "agent-tooling"
THREAD_ID = "abc123"
THREAD_URL = f"https://www.reddit.com/r/ClaudeAI/comments/{THREAD_ID}/"
TITLE = "Agent friction"
THREAD_TEXT_SHA256 = hashlib.sha256(b"thread text").hexdigest()
ISSUE_POLICY = IssuePolicy(
    candidate_window_days=30, max_candidate_comparisons=25, max_suggestions_per_thread=3
)
ISSUE_ID = issue_id_for(WATCH_ID, THREAD_ID)
INDEPENDENCE_BASIS = (
    "distinct accounts and communities are observations, not proof of independent sources"
)
LOGGER = logging.getLogger("threadsense.tests.watch_digest")
TRACE = TraceContext.create(run_id="test-watch-digest", source_name="watch")
Findings = list[tuple[str, list[str]]]


class ManualClock:
    def __init__(self, start: float) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


def text_sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def bytes_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def profile_payload(presented_findings: int) -> dict[str, Any]:
    with FIXTURE_PROFILE.open("rb") as handle:
        payload = tomllib.load(handle)
    payload["limits"]["presented_findings"] = presented_findings
    return payload


def snapshot_bytes(bodies: dict[str, str], created: dict[str, float], observed_at: float) -> bytes:
    thread = Thread(
        thread_id=f"reddit:{THREAD_ID}",
        source=SourceRef(
            source_name="reddit",
            community="ClaudeAI",
            source_thread_id=THREAD_ID,
            thread_url=THREAD_URL,
        ),
        title=TITLE,
        body=None,
        permalink=THREAD_URL,
        author=AuthorRef(username="op", source_author_id=None),
        comments=[
            Comment(
                thread_id=f"reddit:{THREAD_ID}",
                comment_id=comment_id,
                parent_comment_id=None,
                author=AuthorRef(username="commenter", source_author_id=None),
                body=bodies[comment_id],
                score=1,
                created_utc=created[comment_id],
                depth=0,
                permalink=f"{THREAD_URL}{comment_id}/",
            )
            for comment_id in sorted(bodies)
        ],
        comment_count=len(bodies),
        provenance=ProvenanceMetadata(
            raw_artifact_path="raw.json",
            raw_sha256="0" * 64,
            retrieved_at_utc=observed_at,
            normalized_at_utc=observed_at,
            schema_version=2,
            normalization_version="canonical-thread-v2",
        ),
    )
    return json.dumps(thread.to_dict(), indent=2, ensure_ascii=False).encode("utf-8")


def analysis_bytes(findings: Findings) -> bytes:
    payload = {
        "artifact_kind": "thread_analysis",
        "schema_version": 1,
        "analysis_version": "hybrid-v2",
        "analysis": {
            "thread_id": f"reddit:{THREAD_ID}",
            "source_name": "reddit",
            "title": TITLE,
            "total_comments": 1,
            "filtered_comment_count": 0,
            "distinct_comment_count": 1,
            "duplicate_group_count": 0,
            "top_phrases": [],
            "findings": [
                {
                    "theme_key": theme_key,
                    "theme_label": f"Theme {theme_key}",
                    "severity": "high",
                    "comment_count": len(evidence),
                    "issue_marker_count": 0,
                    "request_marker_count": 0,
                    "key_phrases": [],
                    "evidence_comment_ids": evidence,
                    "quotes": [],
                }
                for theme_key, evidence in findings
            ],
            "duplicate_groups": [],
            "top_quotes": [],
            "alignment_check": None,
            "provenance": {
                "normalized_artifact_path": "snapshot.json",
                "normalized_sha256": "0" * 64,
                "source_thread_id": THREAD_ID,
                "analyzed_at_utc": 1.0,
                "schema_version": 2,
                "analysis_version": "hybrid-v2",
            },
        },
    }
    return json.dumps(payload, indent=2).encode("utf-8")


class Harness:
    """Drives real store writes and artifacts for one tracked thread of the watch."""

    def __init__(self, tmp_path: Path, *, presented_findings: int = 10, title: str = TITLE) -> None:
        self.clock = ManualClock(1_000.0)
        self.store = MonitoringStore(tmp_path / "monitoring" / "state.sqlite3", self.clock)
        self.store.save_profile(parse_watch_profile(profile_payload(presented_findings)))
        self.title = title
        self.bodies: dict[str, str] = {}
        self.created: dict[str, float] = {}
        self.revision_id: str | None = None
        self.observed_at: list[float] = []

    def sweep(self, bodies: dict[str, str], findings: Findings | None = None) -> str | None:
        """Record one acquisition; analyze the revision it creates when findings are given."""
        claim = self.claim()
        self.store.record_discoveries(claim, [self._discovery()])
        revision = self._acquire(claim, bodies)
        if revision is not None and findings is not None:
            path, sha256 = self.write(
                f"watches/{WATCH_ID}/threads/{THREAD_ID}/analyses/{revision.revision_id}/"
                f"{claim.run_id}.json",
                analysis_bytes(findings),
            )
            self.store.record_analysis(claim, revision.revision_id, path, sha256)
        self.store.complete_sweep(claim, "changes", {"refresh": {"pending": 3}})
        return None if revision is None else revision.revision_id

    def claim(self) -> RunClaim:
        self.clock.now += 10.0
        return self.store.claim_run(WATCH_ID, lease_seconds=600)

    def present(self) -> DigestResult:
        self.clock.now += 1.0
        return present_watch_digest(
            store=self.store,
            watch_id=WATCH_ID,
            lease_seconds=600,
            issue_policy=ISSUE_POLICY,
            logger=LOGGER,
            trace=TRACE,
        )

    def materialize(self, claim: RunClaim) -> int:
        return sum(
            self.store.materialize_digest_entries(claim, build_digest_entries(WATCH_ID, item))
            for item in self.store.unmaterialized_revisions(WATCH_ID)
        )

    def write(self, relative: str, data: bytes) -> tuple[Path, str]:
        path = self.store.monitoring_root() / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return path, bytes_sha256(data)

    def query(self, sql: str) -> list[tuple[Any, ...]]:
        uri = f"{self.store.path.resolve().as_uri()}?mode=ro"
        with closing(sqlite3.connect(uri, uri=True)) as conn:
            return conn.execute(sql).fetchall()

    def digest_files(self) -> dict[str, bytes]:
        directory = self.store.monitoring_root() / "watches" / WATCH_ID / "digests"
        return {path.name: path.read_bytes() for path in sorted(directory.iterdir())}

    def _discovery(self) -> DiscoveredThread:
        return DiscoveredThread(
            query="coding agent",
            source_thread_id=THREAD_ID,
            thread_url=THREAD_URL,
            subreddit="ClaudeAI",
            title=self.title,
            source_created_utc=500.0,
            rank=1,
            score=10,
            num_comments=len(self.bodies),
            selected=True,
            rejection_reason=None,
        )

    def _acquire(self, claim: RunClaim, bodies: dict[str, str]) -> RevisionRecord | None:
        first = not self.observed_at
        changes: list[CommentChange] = []
        for comment_id in sorted(bodies):
            sha256 = text_sha256(bodies[comment_id])
            if comment_id not in self.bodies:
                self.created[comment_id] = self.clock.now - 5.0
                kind: ChangeKind = "initial_observation" if first else "new_source_event"
                changes.append(
                    CommentChange(comment_id, kind, sha256, None, self.created[comment_id])
                )
            elif self.bodies[comment_id] != bodies[comment_id]:
                previous = text_sha256(self.bodies[comment_id])
                changes.append(
                    CommentChange(comment_id, "edited", sha256, previous, self.created[comment_id])
                )
        observed_at = self.store.now()
        revision = self._revision(bodies, changes, observed_at) if changes else None
        self.store.record_acquisition(claim, self._acquisition(bodies, observed_at), revision)
        self.bodies = dict(bodies)
        self.observed_at.append(observed_at)
        if revision is not None:
            self.revision_id = revision.revision_id
        return revision

    def _acquisition(self, bodies: dict[str, str], observed_at: float) -> AcquisitionRecord:
        raw = json.dumps({"observed_at": observed_at, "bodies": bodies}, sort_keys=True).encode()
        acquisition_id = "acq-" + bytes_sha256(raw)[:32]
        base = f"watches/{WATCH_ID}/threads/{THREAD_ID}/acquisitions/{acquisition_id}"
        raw_path, raw_sha256 = self.write(f"{base}.raw.json", raw)
        normalized_path, normalized_sha256 = self.write(
            f"{base}.normalized.json", b"normalized " + raw
        )
        return AcquisitionRecord(
            acquisition_id=acquisition_id,
            source_thread_id=THREAD_ID,
            observed_at_utc=observed_at,
            raw_path=raw_path,
            raw_sha256=raw_sha256,
            normalized_path=normalized_path,
            normalized_sha256=normalized_sha256,
            completeness="unknown",
            cache_status="disabled",
            parsed_comment_count=len(bodies),
            reported_comment_count=len(bodies),
            observed_comments=tuple(
                (comment_id, text_sha256(bodies[comment_id]), self.created[comment_id])
                for comment_id in sorted(bodies)
            ),
            absent_known_comment_count=0,
        )

    def _revision(
        self, bodies: dict[str, str], changes: list[CommentChange], observed_at: float
    ) -> RevisionRecord:
        snapshot = snapshot_bytes(bodies, self.created, observed_at)
        snapshot_sha256 = bytes_sha256(snapshot)
        revision_id = "rev-" + text_sha256(f"{self.revision_id or ''}\n{snapshot_sha256}")[:32]
        snapshot_path, _ = self.write(
            f"watches/{WATCH_ID}/threads/{THREAD_ID}/revisions/{revision_id}/"
            f"{snapshot_sha256}.json",
            snapshot,
        )
        return RevisionRecord(
            revision_id=revision_id,
            previous_revision_id=self.revision_id,
            substantive_sha256=snapshot_sha256,
            thread_text_sha256=THREAD_TEXT_SHA256,
            snapshot_path=snapshot_path,
            snapshot_sha256=snapshot_sha256,
            changes=tuple(changes),
            thread_text_changed=False,
        )


def expected_update_id(revision_id: str) -> str:
    return "ent-" + text_sha256(f"{WATCH_ID}\n{revision_id}\nthread_update")[:32]


def expected_finding_id(revision_id: str, theme_key: str) -> str:
    return "ent-" + text_sha256(f"{WATCH_ID}\n{revision_id}\nfinding\n{theme_key}")[:32]


def crash_complete_digest(monkeypatch: pytest.MonkeyPatch) -> None:
    def crash(*_args: object) -> None:
        raise RuntimeError("process died before the digest was marked written")

    monkeypatch.setattr(MonitoringStore, "complete_digest", crash)


def test_present_watch_digest_first_analysis_presents_deterministic_evidence_entries(
    tmp_path: Path,
) -> None:
    harness = Harness(tmp_path)
    revision_id = harness.sweep(
        {"c1": "latency is   bad\nhere", "c2": "unrelated"},
        findings=[("latency", ["c1"]), ("stale", ["c9"])],
    )
    assert revision_id is not None

    result = harness.present()

    artifact = json.loads(harness.digest_files()[f"000001-{result.digest_id}.json"])
    entries = {entry["kind"]: entry for entry in artifact["entries"]}
    assert result.status == "presented"
    assert (result.sequence, result.materialized_entries) == (1, 2)
    assert set(result.entry_ids) == {
        expected_update_id(revision_id),
        expected_finding_id(revision_id, "latency"),
    }
    assert [entry["entry_id"] for entry in artifact["entries"]] == list(result.entry_ids)
    assert entries["thread_update"]["novel_comments"][0] == {
        "comment_id": "c1",
        "kind": "initial_observation",
        "text_sha256": text_sha256("latency is   bad\nhere"),
        "previous_text_sha256": None,
        "source_created_utc": harness.created["c1"],
        "first_observed_at_utc": harness.observed_at[0],
        "permalink": f"{THREAD_URL}c1/",
        "excerpt": "latency is bad here",
    }
    assert entries["thread_update"]["counts"]["initial_observation"] == 2
    assert entries["finding"]["novel_evidence_comment_ids"] == ["c1"]
    assert harness.store.digest_status(WATCH_ID)["unpresented_entries"] == 0
    assert result.issues["policy_sha256"] == ISSUE_POLICY.sha256()
    assert (
        result.issues["entries"],
        result.issues["threads_created"],
        result.issues["reports_recorded"],
    ) == (1, 1, 1)
    assert artifact["schema_version"] == 3
    assert sorted(item["entry_id"] for item in artifact["ranking"]["order"]) == sorted(
        result.entry_ids
    )
    assert artifact["issues"] == expected_single_thread_view(
        list(result.entry_ids),
        finding_id=expected_finding_id(revision_id, "latency"),
        comment_text="latency is   bad\nhere",
    )


def expected_single_thread_view(
    entry_ids: list[str], *, finding_id: str, comment_text: str
) -> dict[str, Any]:
    """Issue view of one thread whose finding cites comment c1 firsthand and no update cue."""
    report = {
        "report_id": report_id_for(WATCH_ID, THREAD_ID, "c1"),
        "comment_id": "c1",
        "lineage": "firsthand",
        "copy_of_report_id": None,
        "text_sha256": text_sha256(comment_text),
    }
    return {
        "semantic_grouping": "not_eligible",
        "entry_refs": [
            {
                "entry_id": entry_id,
                "kind": "finding" if entry_id == finding_id else "thread_update",
                "source_thread_id": THREAD_ID,
                "issue_id": ISSUE_ID,
                "home_issue_id": ISSUE_ID,
                "reports": [report] if entry_id == finding_id else [],
                "updates": [],
            }
            for entry_id in entry_ids
        ],
        "issues": [
            {
                "issue_id": ISSUE_ID,
                "home_thread_id": THREAD_ID,
                "state": "active",
                "member_thread_ids": [THREAD_ID],
                "counts": {
                    "observed_threads": 1,
                    "observed_communities": 1,
                    "observed_accounts": 1,
                    "reports_without_account": 0,
                    "reports": 1,
                    "firsthand_reports": 1,
                    "exact_copies": 0,
                    "independence": {
                        "status": "not_verified",
                        "verified_independent_reports": None,
                        "basis": INDEPENDENCE_BASIS,
                    },
                },
                "update_counts": {
                    "reproduction": 0,
                    "contradiction": 0,
                    "workaround": 0,
                    "resolution_claim": 0,
                },
                "unconfirmed_suggestions": 0,
                "suggestions": [],
            }
        ],
    }


def test_materialize_digest_entries_existing_entry_ids_are_skipped(tmp_path: Path) -> None:
    harness = Harness(tmp_path)
    harness.sweep({"c1": "latency"}, findings=[("latency", ["c1"])])
    [item] = harness.store.unmaterialized_revisions(WATCH_ID)
    entries = build_digest_entries(WATCH_ID, item)
    claim = harness.claim()

    first = harness.store.materialize_digest_entries(claim, entries)
    again = harness.store.materialize_digest_entries(claim, build_digest_entries(WATCH_ID, item))

    assert build_digest_entries(WATCH_ID, item) == entries
    assert (first, again) == (2, 0)
    assert harness.store.unmaterialized_revisions(WATCH_ID) == []
    assert harness.query("SELECT COUNT(*) FROM watch_digest_entries") == [(2,)]


def test_unmaterialized_revisions_superseded_revision_folds_novelty_into_next_analysis(
    tmp_path: Path,
) -> None:
    harness = Harness(tmp_path)
    harness.sweep({"c1": "first"}, findings=[])
    harness.present()
    harness.sweep({"c1": "first", "c2": "second"})
    latest = harness.sweep({"c1": "first edited", "c2": "second edited"}, findings=[])

    [item] = harness.store.unmaterialized_revisions(WATCH_ID)

    assert item.revision.revision_id == latest
    assert item.revision.sequence == 3
    assert item.thread_text_changed is False
    assert list(item.novel_comments) == [
        {
            "comment_id": "c1",
            "kind": "edited",
            "text_sha256": text_sha256("first edited"),
            "previous_text_sha256": text_sha256("first"),
            "source_created_utc": harness.created["c1"],
            "source_edited_at_utc": None,
            "observed_at_utc": harness.observed_at[2],
        },
        {
            "comment_id": "c2",
            "kind": "new_source_event",
            "text_sha256": text_sha256("second edited"),
            "previous_text_sha256": None,
            "source_created_utc": harness.created["c2"],
            "first_observed_at_utc": harness.observed_at[1],
        },
    ]


def test_unmaterialized_revisions_repeated_edits_preserve_baseline_and_latest_observation(
    tmp_path: Path,
) -> None:
    harness = Harness(tmp_path)
    harness.sweep({"c1": "first"}, findings=[])
    harness.present()
    harness.sweep({"c1": "intermediate edit"})
    latest = harness.sweep({"c1": "latest edit"}, findings=[])
    assert latest is not None

    [item] = harness.store.unmaterialized_revisions(WATCH_ID)

    expected_change = {
        "comment_id": "c1",
        "kind": "edited",
        "text_sha256": text_sha256("latest edit"),
        "previous_text_sha256": text_sha256("first"),
        "source_created_utc": harness.created["c1"],
        "source_edited_at_utc": None,
        "observed_at_utc": harness.observed_at[2],
    }
    assert item.revision.revision_id == latest
    assert item.revision.sequence == 3
    assert list(item.novel_comments) == [expected_change]

    result = harness.present()

    artifact = json.loads(harness.digest_files()[f"000002-{result.digest_id}.json"])
    [entry] = artifact["entries"]
    assert result.status == "presented"
    assert (result.sequence, result.materialized_entries) == (2, 1)
    assert entry["entry_id"] == expected_update_id(latest)
    assert entry["kind"] == "thread_update"
    assert entry["revision_id"] == latest
    assert entry["novel_comments"] == [
        {
            **expected_change,
            "permalink": f"{THREAD_URL}c1/",
            "excerpt": "latest edit",
        }
    ]


def test_present_watch_digest_unchanged_sweep_creates_no_entries(tmp_path: Path) -> None:
    harness = Harness(tmp_path)
    harness.sweep({"c1": "latency"}, findings=[("latency", ["c1"])])
    harness.present()
    unchanged = harness.sweep({"c1": "latency"})

    result = harness.present()

    assert unchanged is None
    assert (result.status, result.materialized_entries, result.digest_id) == (
        "no_entries",
        0,
        None,
    )
    assert harness.store.digest_status(WATCH_ID)["entry_count"] == 2
    assert harness.store.inspect_watch(WATCH_ID)["recent_runs"][0]["status"] == "succeeded"


def test_present_watch_digest_crash_before_complete_resumes_same_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = Harness(tmp_path)
    harness.sweep({"c1": "latency"}, findings=[("latency", ["c1"])])
    crash_complete_digest(monkeypatch)
    with pytest.raises(RuntimeError):
        harness.present()
    pending = harness.store.digest_status(WATCH_ID)
    crashed_files = harness.digest_files()
    crashed_run = harness.store.inspect_watch(WATCH_ID)["recent_runs"][0]
    monkeypatch.undo()

    resumed = harness.present()
    after = harness.present()

    [json_name] = [name for name in crashed_files if name.endswith(".json")]
    crashed_ids = [entry["entry_id"] for entry in json.loads(crashed_files[json_name])["entries"]]
    assert crashed_run["status"] == "failed"
    assert (pending["unpresented_entries"], pending["digest_count"]) == (2, 0)
    assert resumed.resumed is True
    assert (resumed.digest_id, resumed.sequence) == (pending["pending_digest_id"], 1)
    assert list(resumed.entry_ids) == crashed_ids
    crashed_artifact = json.loads(crashed_files[json_name])
    assert crashed_artifact["schema_version"] == 3
    assert sorted(item["entry_id"] for item in crashed_artifact["ranking"]["order"]) == sorted(
        crashed_ids
    )
    assert [ref["entry_id"] for ref in crashed_artifact["issues"]["entry_refs"]] == crashed_ids
    assert harness.digest_files() == crashed_files
    assert harness.query(
        "SELECT digest_id, COUNT(*) FROM watch_digest_entries GROUP BY digest_id"
    ) == [(resumed.digest_id, 2)]
    assert harness.store.digest_status(WATCH_ID)["pending_digest_id"] is None
    assert after.status == "no_entries"


def test_present_watch_digest_differing_existing_artifact_is_artifact_conflict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = Harness(tmp_path)
    harness.sweep({"c1": "latency"}, findings=[("latency", ["c1"])])
    crash_complete_digest(monkeypatch)
    with pytest.raises(RuntimeError):
        harness.present()
    monkeypatch.undo()
    [json_name] = [name for name in harness.digest_files() if name.endswith(".json")]
    conflicting = harness.store.monitoring_root() / "watches" / WATCH_ID / "digests" / json_name
    conflicting.write_bytes(b"{}\n")

    with pytest.raises(MonitoringStateError) as raised:
        harness.present()

    status = harness.store.digest_status(WATCH_ID)
    assert raised.value.details["reason"] == "artifact_conflict"
    assert conflicting.read_bytes() == b"{}\n"
    assert status["pending_digest_id"] is not None
    assert (status["unpresented_entries"], status["digest_count"]) == (2, 0)


def write_pending(store: MonitoringStore, digest: PendingDigest) -> tuple[bytes, bytes]:
    """Write the artifacts the digest step would publish: ranked from the recorded basis."""
    digest = rank_pending_digest(store, WATCH_ID, digest)
    json_bytes = render_digest_json(WATCH_ID, digest)
    markdown_bytes = render_digest_markdown(WATCH_ID, digest)
    digest.json_path.parent.mkdir(parents=True, exist_ok=True)
    digest.json_path.write_bytes(json_bytes)
    digest.markdown_path.write_bytes(markdown_bytes)
    return json_bytes, markdown_bytes


def test_complete_digest_tampered_artifact_is_refused_and_presents_nothing(
    tmp_path: Path,
) -> None:
    harness = Harness(tmp_path)
    harness.sweep({"c1": "latency"}, findings=[("latency", ["c1"])])
    claim = harness.claim()
    harness.materialize(claim)
    digest = harness.store.begin_digest(claim, 10)
    assert digest is not None
    json_bytes, markdown_bytes = write_pending(harness.store, digest)
    digest.markdown_path.write_bytes(markdown_bytes + b"tampered\n")

    with pytest.raises(MonitoringStateError) as raised:
        harness.store.complete_digest(
            claim, digest.digest_id, bytes_sha256(json_bytes), bytes_sha256(markdown_bytes)
        )

    status = harness.store.digest_status(WATCH_ID)
    assert raised.value.details["reason"] == "artifact_hash_mismatch"
    assert status["pending_digest_id"] == digest.digest_id
    assert (status["unpresented_entries"], status["digest_count"]) == (2, 0)


def test_complete_digest_stale_claim_is_fenced_and_next_run_resumes(tmp_path: Path) -> None:
    harness = Harness(tmp_path)
    harness.sweep({"c1": "latency"}, findings=[("latency", ["c1"])])
    stale = harness.claim()
    harness.materialize(stale)
    digest = harness.store.begin_digest(stale, 10)
    assert digest is not None
    json_bytes, markdown_bytes = write_pending(harness.store, digest)
    harness.clock.now = stale.lease_expires_at_utc + 1.0
    current = harness.store.claim_run(WATCH_ID, lease_seconds=600)

    with pytest.raises(RunClaimError) as raised:
        harness.store.complete_digest(
            stale, digest.digest_id, bytes_sha256(json_bytes), bytes_sha256(markdown_bytes)
        )
    harness.store.finalize_run(current.run_id, current.owner_token, "succeeded")
    resumed = harness.present()

    assert raised.value.details["reason"] == "stale_owner"
    assert (resumed.digest_id, resumed.resumed) == (digest.digest_id, True)
    # The digest began before any ingestion; the resume ingests the thread's issue history
    # but keeps rendering the recorded, reference-free view.
    assert digest.issue_view == {
        "semantic_grouping": "not_eligible",
        "entry_refs": [],
        "issues": [],
    }
    assert resumed.issues["threads_created"] == 1
    assert [issue["issue_id"] for issue in harness.store.list_issues(WATCH_ID)] == [ISSUE_ID]
    assert digest.json_path.read_bytes() == json_bytes
    assert digest.markdown_path.read_bytes() == markdown_bytes
    assert harness.store.digest_status(WATCH_ID)["unpresented_entries"] == 0


@pytest.mark.parametrize(
    ("renderer_name", "replace_claim"),
    [
        pytest.param("render_digest_json", False, id="json-expiry"),
        pytest.param("render_digest_json", True, id="json-replacement"),
        pytest.param("render_digest_markdown", False, id="markdown-expiry"),
        pytest.param("render_digest_markdown", True, id="markdown-replacement"),
    ],
)
def test_present_watch_digest_lost_claim_during_render_prevents_publication_and_resumes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    renderer_name: str,
    replace_claim: bool,
) -> None:
    harness = Harness(tmp_path)
    revision_id = harness.sweep({"c1": "latency"}, findings=[("latency", ["c1"])])
    assert revision_id is not None
    expected_ids = sorted(
        [expected_update_id(revision_id), expected_finding_id(revision_id, "latency")]
    )
    real_renderer = (
        render_digest_json if renderer_name == "render_digest_json" else render_digest_markdown
    )
    rendered_digests: list[PendingDigest] = []
    published_json: list[bytes] = []
    replacement_claims: list[RunClaim] = []

    def lose_claim_after_render(watch_id: str, digest: PendingDigest) -> bytes:
        rendered = real_renderer(watch_id, digest)
        rendered_digests.append(digest)
        if renderer_name == "render_digest_markdown":
            json_bytes = digest.json_path.read_bytes()
            assert json_bytes == render_digest_json(watch_id, digest)
            artifact = json.loads(json_bytes)
            assert artifact["digest_id"] == digest.digest_id
            assert sorted(entry["entry_id"] for entry in artifact["entries"]) == expected_ids
            published_json.append(json_bytes)
        harness.clock.now += 2.0
        if replace_claim:
            replacement_claims.append(harness.store.claim_run(WATCH_ID, lease_seconds=600))
        return rendered

    monkeypatch.setattr(f"threadsense.monitoring.digest.{renderer_name}", lose_claim_after_render)
    with pytest.raises(RunClaimError) as raised:
        present_watch_digest(
            store=harness.store,
            watch_id=WATCH_ID,
            lease_seconds=1,
            issue_policy=ISSUE_POLICY,
            logger=LOGGER,
            trace=TRACE,
        )

    [digest] = rendered_digests
    assert raised.value.details["reason"] == ("stale_owner" if replace_claim else "lease_expired")
    if renderer_name == "render_digest_json":
        assert not digest.json_path.exists()
    else:
        assert digest.json_path.read_bytes() == published_json[0]
    assert not digest.markdown_path.exists()
    pending = harness.store.digest_status(WATCH_ID)
    assert pending["pending_digest_id"] == digest.digest_id
    assert (pending["unpresented_entries"], pending["digest_count"]) == (2, 0)
    assert harness.query("SELECT digest_id, status FROM watch_digests") == [
        (digest.digest_id, "pending")
    ]
    assert harness.query(
        "SELECT digest_id, presented_at_utc, COUNT(*) "
        "FROM watch_digest_entries GROUP BY digest_id, presented_at_utc"
    ) == [(None, None, 2)]
    monkeypatch.setattr(f"threadsense.monitoring.digest.{renderer_name}", real_renderer)
    for claim in replacement_claims:
        harness.store.finalize_run(claim.run_id, claim.owner_token, "succeeded")

    resumed = harness.present()
    after = harness.present()

    assert (resumed.status, resumed.digest_id, resumed.sequence, resumed.resumed) == (
        "presented",
        digest.digest_id,
        digest.sequence,
        True,
    )
    assert sorted(resumed.entry_ids) == expected_ids
    assert resumed.materialized_entries == 0
    json_bytes = digest.json_path.read_bytes()
    artifact = json.loads(json_bytes)
    assert artifact["digest_id"] == digest.digest_id
    assert artifact["sequence"] == digest.sequence
    assert sorted(entry["entry_id"] for entry in artifact["entries"]) == expected_ids
    assert digest.markdown_path.read_bytes() == render_digest_markdown(WATCH_ID, digest)
    if published_json:
        assert json_bytes == published_json[0]
    assert harness.query(
        "SELECT digest_id, COUNT(*) FROM watch_digest_entries GROUP BY digest_id"
    ) == [(digest.digest_id, 2)]
    assert harness.query("SELECT digest_id, status FROM watch_digests") == [
        (digest.digest_id, "written")
    ]
    status = harness.store.digest_status(WATCH_ID)
    assert status["pending_digest_id"] is None
    assert (status["unpresented_entries"], status["digest_count"]) == (0, 1)
    assert (after.status, after.digest_id, after.materialized_entries) == ("no_entries", None, 0)


def test_present_watch_digest_limit_presents_oldest_and_reports_backlog(tmp_path: Path) -> None:
    harness = Harness(tmp_path, presented_findings=2)
    harness.sweep(
        {"c1": "one", "c2": "two", "c3": "three"},
        findings=[("alpha", ["c1"]), ("beta", ["c2"]), ("gamma", ["c3"])],
    )

    first = harness.present()
    second = harness.present()
    third = harness.present()

    all_ids = sorted(first.entry_ids + second.entry_ids)
    assert (first.sequence, second.sequence) == (1, 2)
    assert list(first.entry_ids) == all_ids[:2]
    assert list(second.entry_ids) == all_ids[2:]
    assert first.backlog == {
        "presentation_limit": 2,
        "presented_in_digest": 2,
        "unpresented_entries_after": 2,
        "pending_analyses": 0,
        "tracked_threads": 1,
        "refresh_pending": 3,
        "latest_sweep_outcome": "changes",
        "search_completeness": "unknown",
    }
    assert second.backlog is not None
    assert second.backlog["unpresented_entries_after"] == 0
    assert (third.status, third.backlog) == ("no_entries", None)


def test_render_digest_markdown_hostile_source_text_is_escaped(tmp_path: Path) -> None:
    harness = Harness(tmp_path, title="# Pwn <img src=x>")
    hostile = "<script>alert(1)</script>\n**bold** [link](http://x) `code` # h | p \\ _u_"
    harness.sweep({"c1": hostile}, findings=[("latency", ["c1"])])

    result = harness.present()

    markdown = harness.digest_files()[f"000001-{result.digest_id}.md"].decode("utf-8")
    escaped = (
        r"> \<script\>alert(1)\</script\> \*\*bold\*\* \[link\](http://x) "
        r"\`code\` \# h \| p \\ \_u\_"
    )
    assert markdown.splitlines().count(escaped) == 2
    assert r"Thread update: \# Pwn \<img src=x\>" in markdown
    assert "<script>" not in markdown
    assert "<img src=x>" not in markdown
    assert "## Issues" in markdown.splitlines()
    assert markdown.splitlines().count(f"- Issue: {ISSUE_ID}") == 2


def begin_ingested_digest(harness: Harness) -> tuple[RunClaim, PendingDigest]:
    """Materialize, ingest issue evidence and begin a digest under one held claim, ranked."""
    claim = harness.claim()
    harness.materialize(claim)
    ingest_issue_evidence(harness.store, claim, ISSUE_POLICY)
    digest = harness.store.begin_digest(claim, 10)
    assert digest is not None
    return claim, rank_pending_digest(harness.store, WATCH_ID, digest)


def pre_issue(digest: PendingDigest) -> PendingDigest:
    """The same digest as recorded before issue views (and so rankings) existed."""
    return replace(digest, issue_view=None, ranking_basis=None, ranking=None)


def pre_ranking(digest: PendingDigest) -> PendingDigest:
    """The same digest as recorded with an issue view but before rankings existed."""
    return replace(digest, ranking_basis=None, ranking=None)


def test_begin_digest_new_digest_records_issue_view_rendered_as_schema_three(
    tmp_path: Path,
) -> None:
    harness = Harness(tmp_path)
    revision_id = harness.sweep({"c1": "latency"}, findings=[("latency", ["c1"])])
    assert revision_id is not None

    _, digest = begin_ingested_digest(harness)

    artifact = json.loads(render_digest_json(WATCH_ID, digest))
    expected = expected_single_thread_view(
        [str(entry["entry_id"]) for entry in digest.entries],
        finding_id=expected_finding_id(revision_id, "latency"),
        comment_text="latency",
    )
    assert digest.resumed is False
    assert digest.issue_view == expected
    assert (artifact["schema_version"], artifact["issues"]) == (3, expected)
    assert artifact["ranking"] == digest.ranking
    assert sorted(item["entry_id"] for item in artifact["ranking"]["order"]) == sorted(
        str(entry["entry_id"]) for entry in digest.entries
    )
    [(stored_digest_id, view_json)] = harness.query(
        "SELECT digest_id, view_json FROM watch_digest_issue_views"
    )
    assert (stored_digest_id, json.loads(view_json)) == (digest.digest_id, expected)
    assert digest.ranking_basis is not None
    assert harness.query(
        "SELECT digest_id, profile_revision, policy_id, preference_sequence "
        "FROM watch_digest_rankings"
    ) == [
        (
            digest.digest_id,
            digest.ranking_basis.profile_revision,
            digest.ranking_basis.policy_id,
            digest.ranking_basis.preference_sequence,
        )
    ]


def test_render_digest_json_without_issue_view_renders_schema_one_bytes(
    tmp_path: Path,
) -> None:
    harness = Harness(tmp_path)
    harness.sweep({"c1": "latency"}, findings=[("latency", ["c1"])])
    _, digest = begin_ingested_digest(harness)
    schema_one = {
        "artifact_type": "watch_digest",
        "schema_version": 1,
        "watch_id": WATCH_ID,
        "digest_id": digest.digest_id,
        "sequence": digest.sequence,
        "created_at_utc": digest.created_at_utc,
        "entries": list(digest.entries),
        "backlog": digest.backlog,
    }

    rendered = render_digest_json(WATCH_ID, pre_issue(digest))

    text = json.dumps(schema_one, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False)
    assert rendered == (text + "\n").encode("utf-8")
    assert "issues" not in json.loads(rendered)


def test_render_digest_markdown_without_issue_view_omits_only_issue_lines(
    tmp_path: Path,
) -> None:
    harness = Harness(tmp_path)
    harness.sweep({"c1": "steps to reproduce: export twice"}, findings=[("latency", ["c1"])])
    _, digest = begin_ingested_digest(harness)

    with_issues = render_digest_markdown(WATCH_ID, pre_ranking(digest)).decode("utf-8").splitlines()
    without = render_digest_markdown(WATCH_ID, pre_issue(digest))

    start = with_issues.index("## Issues")
    first_entry = next(index for index, line in enumerate(with_issues) if line.startswith("## 1. "))
    stripped = [
        line
        for line in with_issues[: start - 1] + with_issues[first_entry - 1 :]
        if not line.startswith(("- Issue: ", "- Update: ", "- Exact copy: "))
    ]
    assert without.decode("utf-8").splitlines() == stripped
    assert len(stripped) < len(with_issues)


def test_render_digest_markdown_issue_view_lists_issue_and_update_lines(tmp_path: Path) -> None:
    harness = Harness(tmp_path)
    harness.sweep({"c1": "steps to reproduce: export twice"}, findings=[("latency", ["c1"])])

    result = harness.present()

    lines = harness.digest_files()[f"000001-{result.digest_id}.md"].decode("utf-8").splitlines()
    start = lines.index("## Issues")
    assert lines[start : start + 11] == [
        "## Issues",
        "",
        "- Semantic grouping: not eligible; suggestions are unconfirmed and never merge "
        "memberships or counts.",
        "",
        f"### Issue {ISSUE_ID} (active, open)",
        "",
        f"- Member threads: {THREAD_ID}",
        "- Firsthand reports: 1; exact copies: 0",
        "- Observed threads: 1; accounts: 1; communities: 1; independence: not verified",
        "- Updates: reproduction 1, contradiction 0, workaround 0, resolution claims 0 "
        "(unverified; issue not resolved)",
        "",
    ]
    revision_lines = [index for index, line in enumerate(lines) if line.startswith("- Revision: ")]
    assert len(revision_lines) == 2
    assert all(lines[index + 1] == f"- Issue: {ISSUE_ID}" for index in revision_lines)
    assert lines.count('- Update: reproduction in c1 (cue "steps to reproduce", unverified)') == 1
    assert not [line for line in lines if line.startswith("- Exact copy: ")]
    assert not [
        line
        for line in lines
        if line.startswith(("- Unconfirmed suggestion: ", "- Refused grouping: "))
    ]


HOSTILE_ISSUE_TEXT = "<b>x</b>\n*y* [z](u) #h |p| _q_ `c` \\"
ESCAPED_ISSUE_TEXT = r"\<b\>x\</b\> \*y\* \[z\](u) \#h \|p\| \_q\_ \`c\` \\"


def hostile_suggestion(status: str, *, currently_grouped: bool, source: str) -> dict[str, Any]:
    return {
        "source_thread_id": source,
        "candidate_thread_id": HOSTILE_ISSUE_TEXT,
        "status": status,
        "refusal_reason": "shared_link_only" if status == "refused" else None,
        "currently_grouped": currently_grouped,
        "signals": {
            "themes": [HOSTILE_ISSUE_TEXT],
            "key_phrases": [HOSTILE_ISSUE_TEXT, "plain"],
        },
    }


def hostile_issue_view(recorded: dict[str, Any], finding_id: str) -> dict[str, Any]:
    """The recorded view with source-derived fields replaced by Markdown-hostile text."""
    issue = {
        **recorded["issues"][0],
        "issue_id": HOSTILE_ISSUE_TEXT,
        "member_thread_ids": [HOSTILE_ISSUE_TEXT],
        "suggestions": [
            hostile_suggestion("unconfirmed", currently_grouped=False, source=HOSTILE_ISSUE_TEXT),
            hostile_suggestion("refused", currently_grouped=False, source=HOSTILE_ISSUE_TEXT),
            hostile_suggestion("unconfirmed", currently_grouped=True, source="grouped-source"),
        ],
    }
    copy_report = {
        "report_id": "rpt-copy",
        "comment_id": HOSTILE_ISSUE_TEXT,
        "lineage": "exact_copy",
        "copy_of_report_id": HOSTILE_ISSUE_TEXT,
        "text_sha256": text_sha256("copied"),
    }
    update = {
        "update_id": "upd-hostile",
        "kind": "reproduction",
        "cue": HOSTILE_ISSUE_TEXT,
        "report_id": "rpt-copy",
        "comment_id": HOSTILE_ISSUE_TEXT,
    }
    refs = [
        {**ref, "issue_id": HOSTILE_ISSUE_TEXT, "reports": [copy_report], "updates": [update]}
        for ref in recorded["entry_refs"]
        if ref["entry_id"] == finding_id
    ]
    return {"semantic_grouping": "not_eligible", "entry_refs": refs, "issues": [issue]}


def test_render_digest_markdown_hostile_issue_view_text_is_escaped(tmp_path: Path) -> None:
    harness = Harness(tmp_path)
    revision_id = harness.sweep({"c1": "latency"}, findings=[("latency", ["c1"])])
    assert revision_id is not None
    _, digest = begin_ingested_digest(harness)
    assert digest.issue_view is not None
    view = hostile_issue_view(digest.issue_view, expected_finding_id(revision_id, "latency"))

    markdown = render_digest_markdown(WATCH_ID, replace(digest, issue_view=view)).decode("utf-8")

    lines = markdown.splitlines()
    text = ESCAPED_ISSUE_TEXT
    assert f"### Issue {text} (active, open)" in lines
    assert f"- Member threads: {text}" in lines
    unconfirmed = (
        f"- Unconfirmed suggestion: {text} and {text} (themes: {text}; key phrases: {text}, plain)"
    )
    assert lines.count(unconfirmed) == 1
    assert lines.count(f"- Refused grouping: {text} and {text} (shared link only)") == 1
    assert lines.count(f"- Issue: {text}") == 1
    assert lines.count(f'- Update: reproduction in {text} (cue "{text}", unverified)') == 1
    assert lines.count(f"- Exact copy: {text} copies {text}") == 1
    assert "grouped-source" not in markdown
    assert "<b>" not in markdown
    assert "_q_" not in markdown


def test_begin_digest_resumed_digest_renders_recorded_issue_view_bytes(tmp_path: Path) -> None:
    harness = Harness(tmp_path)
    harness.sweep({"c1": "latency"}, findings=[("latency", ["c1"])])
    claim, digest = begin_ingested_digest(harness)
    json_bytes = render_digest_json(WATCH_ID, digest)
    markdown_bytes = render_digest_markdown(WATCH_ID, digest)
    harness.store.finalize_run(claim.run_id, claim.owner_token, "failed")
    harness.sweep(
        {"c1": "latency", "c2": "steps to reproduce: export twice"},
        findings=[("latency", ["c2"])],
    )

    _, resumed = begin_ingested_digest(harness)

    [issue] = harness.store.list_issues(WATCH_ID)
    assert (issue["counts"]["firsthand_reports"], issue["update_counts"]["reproduction"]) == (2, 1)
    assert (resumed.digest_id, resumed.resumed) == (digest.digest_id, True)
    assert resumed.issue_view == digest.issue_view
    assert render_digest_json(WATCH_ID, resumed) == json_bytes
    assert render_digest_markdown(WATCH_ID, resumed) == markdown_bytes


def test_render_digest_recorded_basis_without_materialized_ranking_is_refused(
    tmp_path: Path,
) -> None:
    harness = Harness(tmp_path)
    harness.sweep({"c1": "latency"}, findings=[("latency", ["c1"])])
    claim = harness.claim()
    harness.materialize(claim)
    digest = harness.store.begin_digest(claim, 10)
    assert digest is not None
    assert digest.ranking_basis is not None
    assert digest.ranking is None

    for renderer in (render_digest_json, render_digest_markdown):
        with pytest.raises(MonitoringStateError) as raised:
            renderer(WATCH_ID, digest)
        assert raised.value.details["reason"] == "ranking_not_materialized"
        assert raised.value.details["digest_id"] == digest.digest_id
    assert not digest.json_path.exists()
    assert not digest.markdown_path.exists()


def test_render_digest_pre_ranking_digest_renders_schema_two_in_selection_order(
    tmp_path: Path,
) -> None:
    harness = Harness(tmp_path)
    harness.sweep({"c1": "one", "c2": "two"}, findings=[("alpha", ["c1"]), ("beta", ["c2"])])
    _, digest = begin_ingested_digest(harness)
    selection = [str(entry["entry_id"]) for entry in digest.entries]
    legacy = pre_ranking(digest)

    artifact = json.loads(render_digest_json(WATCH_ID, legacy))
    lines = render_digest_markdown(WATCH_ID, legacy).decode("utf-8").splitlines()

    assert artifact["schema_version"] == 2
    assert "ranking" not in artifact
    assert artifact["issues"] == digest.issue_view
    assert [entry["entry_id"] for entry in artifact["entries"]] == selection
    assert "## Ranking" not in lines
    assert not [line for line in lines if line.startswith("- Rank ")]
    assert [
        line.removeprefix("- Entry: ") for line in lines if line.startswith("- Entry: ")
    ] == selection
    ranked_lines = render_digest_markdown(WATCH_ID, digest).decode("utf-8").splitlines()
    assert "## Ranking" in ranked_lines
