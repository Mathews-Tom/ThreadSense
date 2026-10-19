from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import tomllib
from contextlib import closing
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
    render_digest_json,
    render_digest_markdown,
)
from threadsense.monitoring.digest_records import PendingDigest
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
            store=self.store, watch_id=WATCH_ID, lease_seconds=600, logger=LOGGER, trace=TRACE
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


def write_pending(digest: PendingDigest) -> tuple[bytes, bytes]:
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
    json_bytes, markdown_bytes = write_pending(digest)
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
    json_bytes, markdown_bytes = write_pending(digest)
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
    assert digest.json_path.read_bytes() == json_bytes
    assert harness.store.digest_status(WATCH_ID)["unpresented_entries"] == 0


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
