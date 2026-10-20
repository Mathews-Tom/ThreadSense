from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import shutil
import sqlite3
import tempfile
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from threadsense.config import StorageConfig
from threadsense.errors import MonitoringInputError, MonitoringStateError, RunClaimError
from threadsense.monitoring.digest_records import (
    _DIGEST_SCHEMA_VERSION,
    _UNMATERIALIZED_REVISIONS,
    DIGEST_SCHEMA_STATEMENTS,
    DigestEntryRecord,
    MaterializationInput,
    PendingDigest,
    _backlog_snapshot,
    _digest_relative_paths,
    _digest_row_by_id,
    _digest_status,
    _entry_payloads,
    _folded_novelty,
    _insert_entry,
    _next_sequence,
    _pending_digest_row,
    _require_analyzed_revision,
    _unpresented_entry_ids,
    _validate_entry,
    digest_id_for,
)
from threadsense.monitoring.issue_lineage import (
    _empty_issue_ingestion,
    _pending_issue_entries,
    _record_issue_evidence,
    _validate_issue_batch,
)
from threadsense.monitoring.issue_records import (
    _ISSUE_SCHEMA_VERSION,
    ISSUE_SCHEMA_STATEMENTS,
    IssueEntryInput,
    IssueIngestion,
    IssuePolicy,
    PendingIssueEntry,
)
from threadsense.monitoring.issue_views import (
    _change_issue_membership,
    _inspect_issue,
    _issue_request,
    _list_issues,
    _require_issue_ids,
    _unknown_issue,
)
from threadsense.monitoring.profiles import (
    WATCH_PROFILE_VERSION,
    WatchProfile,
    parse_watch_profile,
    validate_watch_id,
)
from threadsense.monitoring.sweep_records import (
    _FAILURE_STAGES,
    _PENDING_ANALYSES_FROM,
    _REVISION_REF_COLUMNS,
    _SUCCEEDED_SWEEP_OUTCOMES,
    _SWEEP_OUTCOMES,
    _SWEEP_SCHEMA_VERSION,
    _TRACKED_THREADS_WHERE,
    AcquisitionRecord,
    DiscoveredThread,
    FailureStage,
    KnownComment,
    RevisionRecord,
    RevisionRef,
    SweepOutcome,
    ThreadHistory,
    TrackedThread,
    _AcquisitionWrite,
    _apply_comment_writes,
    _canonical_json,
    _comment_writes,
    _count,
    _insert_revision,
    _known_comment_hashes,
    _novelty_cutoff,
    _pending_analysis_count,
    _stored_novelty,
    _sweep_status,
    _tracked_thread,
    _upsert_watch_thread,
    _validate_acquisition,
    _validate_discovery,
    _validate_limit,
    _validate_observed_at,
)

STATE_SCHEMA_VERSION = 4
STATE_APPLICATION_ID = 0x54534D4E
STATE_FILENAME = "state.sqlite3"
BUSY_TIMEOUT_SECONDS = 5.0
LEASE_SECONDS_RANGE = (1, 86400)
RECENT_RUN_LIMIT = 20

RunOutcome = Literal["succeeded", "failed"]
ProfileWriteStatus = Literal["created", "revised", "unchanged"]


_SCHEMA_V1 = (
    """
    CREATE TABLE watches (
        watch_id TEXT PRIMARY KEY,
        current_revision INTEGER NOT NULL CHECK (current_revision >= 1),
        created_at_utc REAL NOT NULL,
        updated_at_utc REAL NOT NULL
    )
    """,
    """
    CREATE TABLE watch_revisions (
        watch_id TEXT NOT NULL REFERENCES watches (watch_id),
        revision INTEGER NOT NULL CHECK (revision >= 1),
        profile_version INTEGER NOT NULL,
        content_sha256 TEXT NOT NULL,
        profile_json TEXT NOT NULL,
        created_at_utc REAL NOT NULL,
        PRIMARY KEY (watch_id, revision)
    )
    """,
    """
    CREATE TABLE watch_runs (
        run_id TEXT PRIMARY KEY,
        watch_id TEXT NOT NULL,
        profile_revision INTEGER NOT NULL,
        owner_token_sha256 TEXT NOT NULL,
        status TEXT NOT NULL
            CHECK (status IN ('running', 'succeeded', 'failed', 'abandoned')),
        claimed_at_utc REAL NOT NULL,
        lease_expires_at_utc REAL NOT NULL,
        finished_at_utc REAL,
        recovered_by_run_id TEXT,
        FOREIGN KEY (watch_id, profile_revision)
            REFERENCES watch_revisions (watch_id, revision)
    )
    """,
    """
    CREATE UNIQUE INDEX watch_runs_single_running
        ON watch_runs (watch_id) WHERE status = 'running'
    """,
)

_SCHEMA_V2 = (
    """
    CREATE TABLE watch_threads (
        watch_id TEXT NOT NULL REFERENCES watches (watch_id),
        source_thread_id TEXT NOT NULL,
        thread_url TEXT NOT NULL,
        subreddit TEXT NOT NULL,
        title TEXT NOT NULL,
        source_created_utc REAL,
        selected INTEGER NOT NULL CHECK (selected IN (0, 1)),
        rejection_reason TEXT,
        first_discovered_at_utc REAL NOT NULL,
        first_discovered_run_id TEXT NOT NULL,
        last_discovered_at_utc REAL NOT NULL,
        last_attempted_at_utc REAL,
        last_refreshed_at_utc REAL,
        last_acquisition_id TEXT,
        current_revision_id TEXT,
        PRIMARY KEY (watch_id, source_thread_id)
    )
    """,
    """
    CREATE TABLE watch_discoveries (
        run_id TEXT NOT NULL REFERENCES watch_runs (run_id),
        watch_id TEXT NOT NULL,
        query TEXT NOT NULL,
        source_thread_id TEXT NOT NULL,
        rank INTEGER NOT NULL,
        score INTEGER NOT NULL,
        num_comments INTEGER NOT NULL,
        selected INTEGER NOT NULL CHECK (selected IN (0, 1)),
        rejection_reason TEXT,
        observed_at_utc REAL NOT NULL,
        PRIMARY KEY (run_id, query, source_thread_id)
    )
    """,
    """
    CREATE TABLE watch_acquisitions (
        acquisition_id TEXT PRIMARY KEY,
        watch_id TEXT NOT NULL,
        source_thread_id TEXT NOT NULL,
        run_id TEXT NOT NULL REFERENCES watch_runs (run_id),
        observed_at_utc REAL NOT NULL,
        raw_path TEXT NOT NULL,
        raw_sha256 TEXT NOT NULL,
        normalized_path TEXT NOT NULL,
        normalized_sha256 TEXT NOT NULL,
        completeness TEXT NOT NULL CHECK (completeness IN ('truncated', 'unknown')),
        cache_status TEXT NOT NULL,
        parsed_comment_count INTEGER NOT NULL,
        reported_comment_count INTEGER NOT NULL,
        absent_known_comment_count INTEGER NOT NULL,
        revision_id TEXT
    )
    """,
    """
    CREATE TABLE watch_fetch_failures (
        failure_id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT NOT NULL REFERENCES watch_runs (run_id),
        watch_id TEXT NOT NULL,
        stage TEXT NOT NULL CHECK (stage IN ('search', 'fetch', 'analyze')),
        target TEXT NOT NULL,
        error_json TEXT NOT NULL,
        observed_at_utc REAL NOT NULL
    )
    """,
    """
    CREATE TABLE watch_comments (
        watch_id TEXT NOT NULL,
        source_thread_id TEXT NOT NULL,
        comment_id TEXT NOT NULL,
        current_text_sha256 TEXT NOT NULL,
        source_created_utc REAL NOT NULL,
        novelty TEXT NOT NULL CHECK (
            novelty IN ('initial_observation', 'new_source_event', 'newly_observed_historical')
        ),
        first_observed_at_utc REAL NOT NULL,
        first_observed_run_id TEXT NOT NULL,
        first_acquisition_id TEXT NOT NULL,
        last_seen_at_utc REAL NOT NULL,
        last_seen_acquisition_id TEXT NOT NULL,
        PRIMARY KEY (watch_id, source_thread_id, comment_id)
    )
    """,
    """
    CREATE TABLE watch_comment_texts (
        watch_id TEXT NOT NULL,
        source_thread_id TEXT NOT NULL,
        comment_id TEXT NOT NULL,
        acquisition_id TEXT NOT NULL,
        text_sha256 TEXT NOT NULL,
        change TEXT NOT NULL CHECK (change IN ('first_observed', 'edited')),
        observed_at_utc REAL NOT NULL,
        PRIMARY KEY (watch_id, source_thread_id, comment_id, acquisition_id)
    )
    """,
    """
    CREATE TABLE watch_thread_revisions (
        revision_id TEXT PRIMARY KEY,
        watch_id TEXT NOT NULL,
        source_thread_id TEXT NOT NULL,
        sequence INTEGER NOT NULL CHECK (sequence >= 1),
        previous_revision_id TEXT,
        substantive_sha256 TEXT NOT NULL,
        thread_text_sha256 TEXT NOT NULL,
        snapshot_path TEXT NOT NULL,
        snapshot_sha256 TEXT NOT NULL,
        acquisition_id TEXT NOT NULL REFERENCES watch_acquisitions (acquisition_id),
        run_id TEXT NOT NULL REFERENCES watch_runs (run_id),
        created_at_utc REAL NOT NULL,
        changes_json TEXT NOT NULL,
        UNIQUE (watch_id, source_thread_id, sequence)
    )
    """,
    """
    CREATE TABLE watch_revision_analyses (
        revision_id TEXT PRIMARY KEY REFERENCES watch_thread_revisions (revision_id),
        watch_id TEXT NOT NULL,
        analysis_path TEXT NOT NULL,
        analysis_sha256 TEXT NOT NULL,
        run_id TEXT NOT NULL REFERENCES watch_runs (run_id),
        analyzed_at_utc REAL NOT NULL
    )
    """,
    """
    CREATE TABLE watch_sweeps (
        run_id TEXT PRIMARY KEY REFERENCES watch_runs (run_id),
        watch_id TEXT NOT NULL,
        outcome TEXT NOT NULL CHECK (outcome IN ('changes', 'no_changes', 'partial', 'failed')),
        summary_json TEXT NOT NULL,
        finished_at_utc REAL NOT NULL
    )
    """,
)

# Each version lists only its additive statements; a later version only appends.
_SCHEMA_STATEMENTS: dict[int, tuple[str, ...]] = {
    1: _SCHEMA_V1,
    2: _SCHEMA_V2,
    3: DIGEST_SCHEMA_STATEMENTS,
    4: ISSUE_SCHEMA_STATEMENTS,
}


@dataclass(frozen=True)
class ProfileWriteResult:
    status: ProfileWriteStatus
    watch_id: str
    revision: int
    content_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "watch_id": self.watch_id,
            "revision": self.revision,
            "content_sha256": self.content_sha256,
        }


@dataclass(frozen=True)
class RunClaim:
    """Exclusive right to run one watch until the lease expires.

    `owner_token` is returned only to the claimant; the store keeps its digest.
    """

    run_id: str
    watch_id: str
    profile_revision: int
    owner_token: str
    claimed_at_utc: float
    lease_expires_at_utc: float
    recovered_run_id: str | None


def monitoring_state_path(storage: StorageConfig) -> Path:
    """Resolve the state database path, refusing locations outside the storage root."""
    relative = Path(storage.monitoring_dirname)
    if (
        relative.is_absolute()
        or not relative.parts
        or any(part in ("", ".", "..") for part in relative.parts)
    ):
        raise MonitoringInputError(
            "monitoring directory must be a relative path inside the storage root",
            details={"monitoring_dirname": storage.monitoring_dirname},
        )
    root = storage.root_dir.resolve()
    path = (root / relative / STATE_FILENAME).resolve()
    if not path.is_relative_to(root):
        raise MonitoringInputError(
            "monitoring state path escapes the storage root",
            details={"storage_root": str(root), "state_path": str(path)},
        )
    return path


class MonitoringStore:
    def __init__(self, path: Path, clock: Callable[[], float] = time.time) -> None:
        self.path = path
        self._clock = clock

    def save_profile(self, profile: WatchProfile) -> ProfileWriteResult:
        """Persist a profile revision; identical content leaves the store untouched."""
        content_sha256 = profile.content_sha256()
        with self._connect_writable(create=True) as conn, _immediate(conn):
            row = conn.execute(
                "SELECT w.current_revision, r.content_sha256 FROM watches AS w "
                "JOIN watch_revisions AS r "
                "ON r.watch_id = w.watch_id AND r.revision = w.current_revision "
                "WHERE w.watch_id = ?",
                (profile.watch_id,),
            ).fetchone()
            if row is not None and row[1] == content_sha256:
                return ProfileWriteResult(
                    "unchanged", profile.watch_id, int(row[0]), content_sha256
                )
            now = self._clock()
            revision = 1 if row is None else int(row[0]) + 1
            if row is None:
                conn.execute(
                    "INSERT INTO watches "
                    "(watch_id, current_revision, created_at_utc, updated_at_utc) "
                    "VALUES (?, ?, ?, ?)",
                    (profile.watch_id, revision, now, now),
                )
            else:
                conn.execute(
                    "UPDATE watches SET current_revision = ?, updated_at_utc = ? "
                    "WHERE watch_id = ?",
                    (revision, now, profile.watch_id),
                )
            conn.execute(
                "INSERT INTO watch_revisions (watch_id, revision, profile_version, "
                "content_sha256, profile_json, created_at_utc) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    profile.watch_id,
                    revision,
                    WATCH_PROFILE_VERSION,
                    content_sha256,
                    profile.canonical_json(),
                    now,
                ),
            )
        status: ProfileWriteStatus = "created" if revision == 1 else "revised"
        return ProfileWriteResult(status, profile.watch_id, revision, content_sha256)

    def list_watches(self) -> list[dict[str, Any]]:
        with self._connect_readonly() as handle:
            if handle is None:
                return []
            conn, _version = handle
            rows = conn.execute(
                "SELECT w.watch_id, w.current_revision, w.created_at_utc, w.updated_at_utc, "
                "r.content_sha256, r.profile_json FROM watches AS w "
                "JOIN watch_revisions AS r "
                "ON r.watch_id = w.watch_id AND r.revision = w.current_revision "
                "ORDER BY w.watch_id"
            ).fetchall()
        watches: list[dict[str, Any]] = []
        for watch_id, revision, created_at, updated_at, content_sha256, profile_json in rows:
            profile = _load_stored_profile(watch_id, revision, content_sha256, profile_json)
            watches.append(
                {
                    "watch_id": watch_id,
                    "name": profile.name,
                    "current_revision": revision,
                    "content_sha256": content_sha256,
                    "created_at_utc": created_at,
                    "updated_at_utc": updated_at,
                }
            )
        return watches

    def inspect_watch(self, watch_id: str) -> dict[str, Any]:
        validate_watch_id(watch_id)
        with self._connect_readonly() as handle:
            if handle is None:
                raise _unknown_watch(watch_id)
            conn, version = handle
            watch = conn.execute(
                "SELECT current_revision, created_at_utc, updated_at_utc FROM watches "
                "WHERE watch_id = ?",
                (watch_id,),
            ).fetchone()
            if watch is None:
                raise _unknown_watch(watch_id)
            revisions = conn.execute(
                "SELECT revision, content_sha256, profile_json, created_at_utc "
                "FROM watch_revisions WHERE watch_id = ? ORDER BY revision",
                (watch_id,),
            ).fetchall()
            run_count = conn.execute(
                "SELECT COUNT(*) FROM watch_runs WHERE watch_id = ?", (watch_id,)
            ).fetchone()[0]
            runs = conn.execute(
                "SELECT run_id, profile_revision, status, claimed_at_utc, "
                "lease_expires_at_utc, finished_at_utc, recovered_by_run_id "
                "FROM watch_runs WHERE watch_id = ? "
                "ORDER BY claimed_at_utc DESC, rowid DESC LIMIT ?",
                (watch_id, RECENT_RUN_LIMIT),
            ).fetchall()
            sweep = _sweep_status(conn, version, watch_id)
            digest = _digest_status(conn, version, watch_id, self.monitoring_root())
        current_revision, created_at, updated_at = watch
        current = [row for row in revisions if row[0] == current_revision]
        if len(current) != 1:
            raise MonitoringStateError(
                "watch current revision is missing",
                details={
                    "reason": "revision_missing",
                    "watch_id": watch_id,
                    "revision": current_revision,
                },
            )
        profile = _load_stored_profile(watch_id, current_revision, current[0][1], current[0][2])
        recent_runs = [_run_to_dict(row) for row in runs]
        return {
            "watch_id": watch_id,
            "current_revision": current_revision,
            "created_at_utc": created_at,
            "updated_at_utc": updated_at,
            "profile": profile.to_dict(),
            "revisions": [
                {"revision": revision, "content_sha256": sha256, "created_at_utc": created}
                for revision, sha256, _profile_json, created in revisions
            ],
            "active_run": next((run for run in recent_runs if run["status"] == "running"), None),
            "run_count": run_count,
            "recent_runs": recent_runs,
            "sweep": sweep,
            "digest": digest,
        }

    def claim_run(self, watch_id: str, *, lease_seconds: int) -> RunClaim:
        """Atomically claim the single active run slot for a watch.

        A live claim refuses the request. An expired claim is marked abandoned and
        superseded, which fences its owner out of any later finalization.
        """
        validate_watch_id(watch_id)
        low, high = LEASE_SECONDS_RANGE
        if type(lease_seconds) is not int or not low <= lease_seconds <= high:
            raise MonitoringInputError(
                f"run lease must be an integer between {low} and {high} seconds",
                details={"lease_seconds": lease_seconds},
            )
        run_id = f"run-{secrets.token_hex(8)}"
        owner_token = secrets.token_urlsafe(32)
        with self._connect_writable(create=False) as conn, _immediate(conn):
            watch = conn.execute(
                "SELECT current_revision FROM watches WHERE watch_id = ?", (watch_id,)
            ).fetchone()
            if watch is None:
                raise _unknown_watch(watch_id)
            now = self._clock()
            active = conn.execute(
                "SELECT run_id, lease_expires_at_utc FROM watch_runs "
                "WHERE watch_id = ? AND status = 'running'",
                (watch_id,),
            ).fetchone()
            recovered_run_id: str | None = None
            if active is not None:
                if active[1] > now:
                    raise RunClaimError(
                        "watch already has an active run",
                        details={
                            "reason": "claim_held",
                            "watch_id": watch_id,
                            "active_run_id": active[0],
                            "lease_expires_at_utc": active[1],
                        },
                    )
                recovered_run_id = str(active[0])
                conn.execute(
                    "UPDATE watch_runs SET status = 'abandoned', finished_at_utc = ?, "
                    "recovered_by_run_id = ? WHERE run_id = ? AND status = 'running'",
                    (now, run_id, recovered_run_id),
                )
            lease_expires = now + lease_seconds
            conn.execute(
                "INSERT INTO watch_runs (run_id, watch_id, profile_revision, "
                "owner_token_sha256, status, claimed_at_utc, lease_expires_at_utc) "
                "VALUES (?, ?, ?, ?, 'running', ?, ?)",
                (run_id, watch_id, watch[0], _token_digest(owner_token), now, lease_expires),
            )
        return RunClaim(
            run_id=run_id,
            watch_id=watch_id,
            profile_revision=int(watch[0]),
            owner_token=owner_token,
            claimed_at_utc=now,
            lease_expires_at_utc=lease_expires,
            recovered_run_id=recovered_run_id,
        )

    @contextmanager
    def fenced_artifact_write(self, claim: RunClaim) -> Iterator[None]:
        """Hold the live claim fence through one artifact publication."""
        with self._connect_writable(create=False) as conn, _immediate(conn):
            self._fence_claim(conn, claim)
            yield

    def finalize_run(self, run_id: str, owner_token: str, outcome: RunOutcome) -> None:
        """Record a run outcome; only the current owner of a live lease may do so."""
        if outcome not in ("succeeded", "failed"):
            raise MonitoringInputError(
                "run outcome must be succeeded or failed", details={"outcome": outcome}
            )
        with self._connect_writable(create=False) as conn, _immediate(conn):
            _run_watch_id, now = self._fence_run(conn, run_id, owner_token)
            conn.execute(
                "UPDATE watch_runs SET status = ?, finished_at_utc = ? "
                "WHERE run_id = ? AND status = 'running'",
                (outcome, now, run_id),
            )

    def monitoring_root(self) -> Path:
        return self.path.parent

    def now(self) -> float:
        """Read the store clock, the single time source for observation ordering."""
        return self._clock()

    def load_profile_revision(self, watch_id: str, revision: int) -> WatchProfile:
        """Load and validate one stored profile revision of a watch."""
        validate_watch_id(watch_id)
        if type(revision) is not int or revision < 1:
            raise MonitoringInputError(
                "profile revision must be a positive integer", details={"revision": revision}
            )
        with self._connect_readonly() as handle:
            if handle is None:
                raise _unknown_watch(watch_id)
            conn, _version = handle
            watch = conn.execute("SELECT 1 FROM watches WHERE watch_id = ?", (watch_id,)).fetchone()
            if watch is None:
                raise _unknown_watch(watch_id)
            row = conn.execute(
                "SELECT content_sha256, profile_json FROM watch_revisions "
                "WHERE watch_id = ? AND revision = ?",
                (watch_id, revision),
            ).fetchone()
        if row is None:
            raise MonitoringStateError(
                "watch profile revision does not exist",
                details={"reason": "revision_missing", "watch_id": watch_id, "revision": revision},
            )
        return _load_stored_profile(watch_id, revision, row[0], row[1])

    def record_discoveries(self, claim: RunClaim, discoveries: Sequence[DiscoveredThread]) -> int:
        """Record a run's discovery list; return the number of newly tracked threads.

        A thread once selected stays tracked, whatever later discoveries decide.
        """
        for discovery in discoveries:
            _validate_discovery(discovery)
        newly_tracked = 0
        with self._connect_writable(create=False) as conn, _immediate(conn):
            now = self._fence_claim(conn, claim)
            for discovery in discoveries:
                conn.execute(
                    "INSERT INTO watch_discoveries (run_id, watch_id, query, source_thread_id, "
                    "rank, score, num_comments, selected, rejection_reason, observed_at_utc) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        claim.run_id,
                        claim.watch_id,
                        discovery.query,
                        discovery.source_thread_id,
                        discovery.rank,
                        discovery.score,
                        discovery.num_comments,
                        int(discovery.selected),
                        discovery.rejection_reason,
                        now,
                    ),
                )
                if _upsert_watch_thread(conn, claim, discovery, now):
                    newly_tracked += 1
        return newly_tracked

    def refresh_queue(self, watch_id: str, limit: int) -> list[TrackedThread]:
        """Tracked threads in FIFO order, at most `limit` of them.

        Ordered by COALESCE(last_attempted_at_utc, first_discovered_at_utc) ascending, then
        source_thread_id, so every tracked thread, including a newly discovered one, waits at
        most ceil(tracked / limit) sweeps.
        """
        validate_watch_id(watch_id)
        _validate_limit(limit)
        with self._connect_sweep_readonly() as conn:
            if conn is None:
                return []
            rows = conn.execute(
                "SELECT source_thread_id, thread_url, subreddit, title, last_refreshed_at_utc, "
                "current_revision_id FROM watch_threads WHERE watch_id = ? AND selected = 1 "
                "ORDER BY COALESCE(last_attempted_at_utc, first_discovered_at_utc) ASC, "
                "source_thread_id ASC LIMIT ?",
                (watch_id, limit),
            ).fetchall()
        return [TrackedThread(*row) for row in rows]

    def tracked_thread_count(self, watch_id: str) -> int:
        validate_watch_id(watch_id)
        with self._connect_sweep_readonly() as conn:
            if conn is None:
                return 0
            return _count(conn, _TRACKED_THREADS_WHERE, watch_id)

    def thread_history(self, watch_id: str, source_thread_id: str) -> ThreadHistory:
        """Durable comment history of a thread; an untracked thread has an empty history."""
        validate_watch_id(watch_id)
        empty = ThreadHistory(source_thread_id, {}, None, None)
        with self._connect_sweep_readonly() as conn:
            if conn is None:
                return empty
            thread = conn.execute(
                "SELECT current_revision_id FROM watch_threads "
                "WHERE watch_id = ? AND source_thread_id = ?",
                (watch_id, source_thread_id),
            ).fetchone()
            if thread is None:
                return empty
            comments = conn.execute(
                "SELECT comment_id, current_text_sha256, source_created_utc, novelty, "
                "first_observed_at_utc FROM watch_comments "
                "WHERE watch_id = ? AND source_thread_id = ? ORDER BY comment_id",
                (watch_id, source_thread_id),
            ).fetchall()
            current_revision_id = thread[0]
            prior_observed_at = _novelty_cutoff(conn, watch_id, source_thread_id)
            current = (
                None
                if current_revision_id is None
                else self._revision_ref(conn, watch_id, current_revision_id)
            )
        known = {
            comment_id: KnownComment(
                comment_id, text_sha256, created_utc, _stored_novelty(novelty), first_observed
            )
            for comment_id, text_sha256, created_utc, novelty, first_observed in comments
        }
        return ThreadHistory(source_thread_id, known, current, prior_observed_at)

    def record_acquisition(
        self,
        claim: RunClaim,
        acquisition: AcquisitionRecord,
        revision: RevisionRecord | None,
    ) -> None:
        """Record one successful acquisition, its comment history and optional revision."""
        _validate_acquisition(acquisition)
        with self._connect_writable(create=False) as conn, _immediate(conn):
            now = self._fence_claim(conn, claim)
            thread = _tracked_thread(conn, claim.watch_id, acquisition.source_thread_id)
            current_revision_id, last_acquisition_id = thread
            _validate_observed_at(conn, claim.watch_id, acquisition, now)
            duplicate = conn.execute(
                "SELECT 1 FROM watch_acquisitions WHERE acquisition_id = ?",
                (acquisition.acquisition_id,),
            ).fetchone()
            if duplicate is not None:
                raise MonitoringStateError(
                    "acquisition was already recorded",
                    details={
                        "reason": "duplicate_acquisition",
                        "acquisition_id": acquisition.acquisition_id,
                    },
                )
            if revision is not None and revision.previous_revision_id != current_revision_id:
                raise MonitoringStateError(
                    "revision does not extend the thread's current revision",
                    details={
                        "reason": "revision_conflict",
                        "source_thread_id": acquisition.source_thread_id,
                        "current_revision_id": current_revision_id,
                        "previous_revision_id": revision.previous_revision_id,
                    },
                )
            raw_path = self._verified_artifact(acquisition.raw_path, acquisition.raw_sha256)
            normalized_path = self._verified_artifact(
                acquisition.normalized_path, acquisition.normalized_sha256
            )
            snapshot_path = (
                None
                if revision is None
                else self._verified_artifact(revision.snapshot_path, revision.snapshot_sha256)
            )
            known = _known_comment_hashes(conn, claim.watch_id, acquisition.source_thread_id)
            writes = _comment_writes(
                acquisition, revision, known, first_acquisition=last_acquisition_id is None
            )
            conn.execute(
                "INSERT INTO watch_acquisitions (acquisition_id, watch_id, source_thread_id, "
                "run_id, observed_at_utc, raw_path, raw_sha256, normalized_path, "
                "normalized_sha256, completeness, cache_status, parsed_comment_count, "
                "reported_comment_count, absent_known_comment_count, revision_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    acquisition.acquisition_id,
                    claim.watch_id,
                    acquisition.source_thread_id,
                    claim.run_id,
                    acquisition.observed_at_utc,
                    raw_path,
                    acquisition.raw_sha256,
                    normalized_path,
                    acquisition.normalized_sha256,
                    acquisition.completeness,
                    acquisition.cache_status,
                    acquisition.parsed_comment_count,
                    acquisition.reported_comment_count,
                    acquisition.absent_known_comment_count,
                    None if revision is None else revision.revision_id,
                ),
            )
            write = _AcquisitionWrite(claim, acquisition, now)
            if revision is not None and snapshot_path is not None:
                _insert_revision(conn, write, revision, snapshot_path)
                current_revision_id = revision.revision_id
            _apply_comment_writes(conn, write, writes)
            conn.execute(
                "UPDATE watch_threads SET last_attempted_at_utc = ?, last_refreshed_at_utc = ?, "
                "last_acquisition_id = ?, current_revision_id = ? "
                "WHERE watch_id = ? AND source_thread_id = ?",
                (
                    now,
                    acquisition.observed_at_utc,
                    acquisition.acquisition_id,
                    current_revision_id,
                    claim.watch_id,
                    acquisition.source_thread_id,
                ),
            )

    def record_failure(
        self,
        claim: RunClaim,
        *,
        stage: FailureStage,
        target: str,
        error: dict[str, Any],
    ) -> None:
        """Record a failed sweep step; a failed fetch still counts as an attempt."""
        if stage not in _FAILURE_STAGES:
            raise MonitoringInputError(
                "failure stage must be search, fetch or analyze",
                details={"reason": "invalid_stage", "stage": stage},
            )
        if not isinstance(target, str) or not target:
            raise MonitoringInputError(
                "failure target must be a non-empty string",
                details={"reason": "invalid_target", "target": target},
            )
        error_json = _canonical_json(error, "error")
        with self._connect_writable(create=False) as conn, _immediate(conn):
            now = self._fence_claim(conn, claim)
            conn.execute(
                "INSERT INTO watch_fetch_failures (run_id, watch_id, stage, target, error_json, "
                "observed_at_utc) VALUES (?, ?, ?, ?, ?, ?)",
                (claim.run_id, claim.watch_id, stage, target, error_json, now),
            )
            if stage == "fetch":
                _tracked_thread(conn, claim.watch_id, target)
                conn.execute(
                    "UPDATE watch_threads SET last_attempted_at_utc = ? "
                    "WHERE watch_id = ? AND source_thread_id = ?",
                    (now, claim.watch_id, target),
                )

    def pending_analyses(self, watch_id: str, limit: int) -> list[RevisionRef]:
        """Unanalyzed current revisions, oldest first; superseded revisions are skipped."""
        validate_watch_id(watch_id)
        _validate_limit(limit)
        with self._connect_sweep_readonly() as conn:
            if conn is None:
                return []
            rows = conn.execute(
                f"SELECT {_REVISION_REF_COLUMNS} {_PENDING_ANALYSES_FROM} "
                "ORDER BY r.created_at_utc ASC, r.revision_id ASC LIMIT ?",
                (watch_id, limit),
            ).fetchall()
        return [self._revision_ref_from_row(row) for row in rows]

    def pending_analysis_count(self, watch_id: str) -> int:
        validate_watch_id(watch_id)
        with self._connect_sweep_readonly() as conn:
            if conn is None:
                return 0
            return _pending_analysis_count(conn, watch_id)

    def record_analysis(
        self, claim: RunClaim, revision_id: str, analysis_path: Path, analysis_sha256: str
    ) -> None:
        """Record the analysis of a revision; an already analyzed revision is left untouched."""
        with self._connect_writable(create=False) as conn, _immediate(conn):
            now = self._fence_claim(conn, claim)
            known_revision = conn.execute(
                "SELECT 1 FROM watch_thread_revisions WHERE revision_id = ? AND watch_id = ?",
                (revision_id, claim.watch_id),
            ).fetchone()
            if known_revision is None:
                raise MonitoringStateError(
                    "revision does not exist for this watch",
                    details={
                        "reason": "unknown_revision",
                        "watch_id": claim.watch_id,
                        "revision_id": revision_id,
                    },
                )
            stored_path = self._verified_artifact(analysis_path, analysis_sha256)
            analyzed = conn.execute(
                "SELECT 1 FROM watch_revision_analyses WHERE revision_id = ?", (revision_id,)
            ).fetchone()
            if analyzed is not None:
                # A retry after a committed analysis never overwrites the first record.
                return
            conn.execute(
                "INSERT INTO watch_revision_analyses (revision_id, watch_id, analysis_path, "
                "analysis_sha256, run_id, analyzed_at_utc) VALUES (?, ?, ?, ?, ?, ?)",
                (revision_id, claim.watch_id, stored_path, analysis_sha256, claim.run_id, now),
            )

    def complete_sweep(
        self, claim: RunClaim, outcome: SweepOutcome, summary: Mapping[str, Any]
    ) -> None:
        """Record the sweep outcome and finish its run in the same transaction."""
        if outcome not in _SWEEP_OUTCOMES:
            raise MonitoringInputError(
                "sweep outcome must be changes, no_changes, partial or failed",
                details={"reason": "invalid_outcome", "outcome": outcome},
            )
        summary_json = _canonical_json(dict(summary), "summary")
        run_status: RunOutcome = "succeeded" if outcome in _SUCCEEDED_SWEEP_OUTCOMES else "failed"
        with self._connect_writable(create=False) as conn, _immediate(conn):
            now = self._fence_claim(conn, claim)
            conn.execute(
                "INSERT INTO watch_sweeps (run_id, watch_id, outcome, summary_json, "
                "finished_at_utc) VALUES (?, ?, ?, ?, ?)",
                (claim.run_id, claim.watch_id, outcome, summary_json, now),
            )
            conn.execute(
                "UPDATE watch_runs SET status = ?, finished_at_utc = ? "
                "WHERE run_id = ? AND status = 'running'",
                (run_status, now, claim.run_id),
            )

    def sweep_status(self, watch_id: str) -> dict[str, Any]:
        validate_watch_id(watch_id)
        with self._connect_readonly() as handle:
            if handle is None:
                return _sweep_status(None, 0, watch_id)
            conn, version = handle
            return _sweep_status(conn, version, watch_id)

    def unmaterialized_revisions(self, watch_id: str) -> list[MaterializationInput]:
        """Analyzed revisions without digest entries, oldest first, with folded novelty."""
        validate_watch_id(watch_id)
        root = self.monitoring_root()
        inputs: list[MaterializationInput] = []
        with self._connect_digest_readonly() as conn:
            if conn is None:
                return []
            rows = conn.execute(
                f"SELECT {_REVISION_REF_COLUMNS}, a.analysis_path, a.analysis_sha256, "
                f"t.thread_url, t.subreddit, t.title {_UNMATERIALIZED_REVISIONS}",
                (watch_id,),
            ).fetchall()
            for row in rows:
                revision = self._revision_ref_from_row(tuple(row[:7]))
                novel, thread_text_changed = _folded_novelty(
                    conn, watch_id, revision.source_thread_id, revision.sequence
                )
                inputs.append(
                    MaterializationInput(
                        revision=revision,
                        analysis_path=root / row[7],
                        analysis_sha256=row[8],
                        thread_url=row[9],
                        subreddit=row[10],
                        title=row[11],
                        novel_comments=novel,
                        thread_text_changed=thread_text_changed,
                        monitoring_root=root,
                    )
                )
        return inputs

    def materialize_digest_entries(
        self, claim: RunClaim, entries: Sequence[DigestEntryRecord]
    ) -> int:
        """Record digest entries; return how many were new, skipping recorded entry ids."""
        for entry in entries:
            _validate_entry(entry)
        inserted = 0
        with self._connect_writable(create=False) as conn, _immediate(conn):
            now = self._fence_claim(conn, claim)
            for entry in entries:
                _require_analyzed_revision(conn, claim.watch_id, entry)
                inserted += int(_insert_entry(conn, claim, entry, now))
        return inserted

    def begin_digest(self, claim: RunClaim, limit: int) -> PendingDigest | None:
        """Resume the watch's pending digest, else record one over the oldest unpresented entries.

        Entries stay unpresented until `complete_digest` has verified both digest artifacts.
        """
        if type(limit) is not int or limit < 1:
            raise MonitoringInputError(
                "digest limit must be a positive integer",
                details={"reason": "invalid_limit", "limit": limit},
            )
        root = self.monitoring_root().absolute()
        with self._connect_writable(create=False) as conn, _immediate(conn):
            now = self._fence_claim(conn, claim)
            pending = _pending_digest_row(conn, claim.watch_id)
            if pending is not None:
                return PendingDigest(
                    digest_id=pending.digest_id,
                    sequence=pending.sequence,
                    json_path=root / pending.json_path,
                    markdown_path=root / pending.markdown_path,
                    created_at_utc=pending.created_at_utc,
                    entries=_entry_payloads(conn, claim.watch_id, pending.entry_ids),
                    backlog=pending.backlog,
                    resumed=True,
                )
            entry_ids = tuple(_unpresented_entry_ids(conn, claim.watch_id, limit))
            if not entry_ids:
                return None
            sequence = _next_sequence(conn, claim.watch_id)
            digest_id = digest_id_for(claim.watch_id, sequence, entry_ids)
            json_path, markdown_path = _digest_relative_paths(claim.watch_id, sequence, digest_id)
            backlog = _backlog_snapshot(conn, claim.watch_id, limit, len(entry_ids))
            entries = _entry_payloads(conn, claim.watch_id, entry_ids)
            conn.execute(
                "INSERT INTO watch_digests (digest_id, watch_id, sequence, run_id, status, "
                "entry_ids_json, backlog_json, json_path, markdown_path, created_at_utc) "
                "VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?)",
                (
                    digest_id,
                    claim.watch_id,
                    sequence,
                    claim.run_id,
                    _canonical_json(list(entry_ids), "entry_ids"),
                    _canonical_json(backlog, "backlog"),
                    json_path,
                    markdown_path,
                    now,
                ),
            )
        return PendingDigest(
            digest_id=digest_id,
            sequence=sequence,
            json_path=root / json_path,
            markdown_path=root / markdown_path,
            created_at_utc=now,
            entries=entries,
            backlog=backlog,
            resumed=False,
        )

    def complete_digest(
        self, claim: RunClaim, digest_id: str, json_sha256: str, markdown_sha256: str
    ) -> None:
        """Mark a pending digest written once both artifacts verify, presenting its entries."""
        root = self.monitoring_root()
        with self._connect_writable(create=False) as conn, _immediate(conn):
            now = self._fence_claim(conn, claim)
            digest = _digest_row_by_id(conn, claim.watch_id, digest_id)
            if digest.status != "pending":
                raise MonitoringStateError(
                    "digest is not pending",
                    details={
                        "reason": "digest_not_pending",
                        "digest_id": digest_id,
                        "status": digest.status,
                    },
                )
            artifacts = ((digest.json_path, json_sha256), (digest.markdown_path, markdown_sha256))
            for relative, sha256 in artifacts:
                if self._verified_artifact(root / relative, sha256) != relative:
                    raise MonitoringStateError(
                        "digest artifact does not resolve to its recorded path",
                        details={"reason": "artifact_outside_state", "path": relative},
                    )
            conn.execute(
                "UPDATE watch_digests SET status = 'written', json_sha256 = ?, "
                "markdown_sha256 = ?, written_at_utc = ? "
                "WHERE digest_id = ? AND status = 'pending'",
                (json_sha256, markdown_sha256, now, digest_id),
            )
            for entry_id in digest.entry_ids:
                linked = conn.execute(
                    "UPDATE watch_digest_entries SET digest_id = ?, presented_at_utc = ? "
                    "WHERE entry_id = ? AND watch_id = ? AND digest_id IS NULL",
                    (digest_id, now, entry_id, claim.watch_id),
                ).rowcount
                if linked != 1:
                    raise MonitoringStateError(
                        "digest entry was already presented or is missing",
                        details={"reason": "entry_already_presented", "entry_id": entry_id},
                    )

    def digest_status(self, watch_id: str) -> dict[str, Any]:
        validate_watch_id(watch_id)
        root = self.monitoring_root()
        with self._connect_readonly() as handle:
            if handle is None:
                return _digest_status(None, 0, watch_id, root)
            conn, version = handle
            return _digest_status(conn, version, watch_id, root)

    def pending_issue_entries(self, watch_id: str) -> list[PendingIssueEntry]:
        """Finding entries not yet in issue history, with their recorded artifacts."""
        validate_watch_id(watch_id)
        root = self.monitoring_root()
        with self._connect_issue_readonly() as conn:
            if conn is None:
                return []
            return _pending_issue_entries(conn, watch_id, root)

    def record_issue_evidence(
        self, claim: RunClaim, entries: Sequence[IssueEntryInput], policy: IssuePolicy
    ) -> IssueIngestion:
        """Record finding evidence as issue history in one fenced, append-only transaction."""
        batch = _validate_issue_batch(entries, policy)
        with self._connect_writable(create=False) as conn, _immediate(conn):
            # Fence first: a stale owner is refused even when it has nothing to record.
            now = self._fence_claim(conn, claim)
            if not batch:
                return _empty_issue_ingestion(claim.run_id)
            return _record_issue_evidence(conn, claim, batch, policy=policy, now=now)

    def link_issue_thread(
        self, watch_id: str, issue_id: str, source_thread_id: str, reason: str
    ) -> dict[str, Any]:
        """Move a thread into an active issue with one operator membership event."""
        request = _issue_request(watch_id, issue_id, source_thread_id, reason)
        with self._connect_writable(create=False) as conn, _immediate(conn):
            self._require_watch(conn, watch_id)
            return _change_issue_membership(conn, request, "linked", self.now())

    def unlink_issue_thread(
        self, watch_id: str, issue_id: str, source_thread_id: str, reason: str
    ) -> dict[str, Any]:
        """Return a linked thread to its home issue with one operator membership event."""
        request = _issue_request(watch_id, issue_id, source_thread_id, reason)
        with self._connect_writable(create=False) as conn, _immediate(conn):
            self._require_watch(conn, watch_id)
            return _change_issue_membership(conn, request, "unlinked", self.now())

    def inspect_issue(self, watch_id: str, issue_id: str) -> dict[str, Any]:
        _require_issue_ids(watch_id, issue_id)
        with self._connect_known_watch_readonly(watch_id) as (conn, version):
            if version < _ISSUE_SCHEMA_VERSION:
                raise _unknown_issue(watch_id, issue_id)
            return _inspect_issue(conn, watch_id, issue_id)

    def list_issues(self, watch_id: str) -> list[dict[str, Any]]:
        validate_watch_id(watch_id)
        with self._connect_known_watch_readonly(watch_id) as (conn, version):
            if version < _ISSUE_SCHEMA_VERSION:
                return []
            return _list_issues(conn, watch_id)

    @staticmethod
    def _require_watch(conn: sqlite3.Connection, watch_id: str) -> None:
        if conn.execute("SELECT 1 FROM watches WHERE watch_id = ?", (watch_id,)).fetchone() is None:
            raise _unknown_watch(watch_id)

    def _verified_artifact(self, path: Path, sha256: str) -> str:
        """Verify an artifact inside the monitoring root; return its stored relative path."""
        root = self.monitoring_root().resolve()
        resolved = path.resolve()
        details: dict[str, Any] = {"path": str(path), "monitoring_root": str(root)}
        if not resolved.is_relative_to(root):
            raise MonitoringStateError(
                "artifact is outside the monitoring state directory",
                details={"reason": "artifact_outside_state", **details},
            )
        try:
            actual = _file_sha256(resolved)
        except (FileNotFoundError, IsADirectoryError, NotADirectoryError) as error:
            raise MonitoringStateError(
                "artifact does not exist", details={"reason": "artifact_missing", **details}
            ) from error
        if not hmac.compare_digest(actual, sha256):
            raise MonitoringStateError(
                "artifact content does not match its recorded hash",
                details={
                    "reason": "artifact_hash_mismatch",
                    "expected_sha256": sha256,
                    "actual_sha256": actual,
                    **details,
                },
            )
        return resolved.relative_to(root).as_posix()

    def _revision_ref(
        self, conn: sqlite3.Connection, watch_id: str, revision_id: str
    ) -> RevisionRef:
        row = conn.execute(
            f"SELECT {_REVISION_REF_COLUMNS} FROM watch_thread_revisions AS r "
            "WHERE r.revision_id = ? AND r.watch_id = ?",
            (revision_id, watch_id),
        ).fetchone()
        if row is None:
            raise MonitoringStateError(
                "thread current revision is missing",
                details={
                    "reason": "revision_missing",
                    "watch_id": watch_id,
                    "revision_id": revision_id,
                },
            )
        return self._revision_ref_from_row(row)

    def _revision_ref_from_row(self, row: tuple[Any, ...]) -> RevisionRef:
        revision_id, thread_id, sequence, snapshot, snapshot_sha256, text_sha256, created = row
        return RevisionRef(
            revision_id=revision_id,
            source_thread_id=thread_id,
            sequence=int(sequence),
            snapshot_path=self.monitoring_root() / snapshot,
            snapshot_sha256=snapshot_sha256,
            thread_text_sha256=text_sha256,
            created_at_utc=created,
        )

    def _fence_run(
        self, conn: sqlite3.Connection, run_id: str, owner_token: str
    ) -> tuple[str, float]:
        """Refuse unless the caller owns the live lease of the run; return its watch and now."""
        row = conn.execute(
            "SELECT watch_id, owner_token_sha256, status, lease_expires_at_utc, "
            "recovered_by_run_id FROM watch_runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            raise RunClaimError(
                "run does not exist", details={"reason": "unknown_run", "run_id": run_id}
            )
        run_watch_id, token_sha256, status, lease_expires, recovered_by = row
        if not hmac.compare_digest(token_sha256, _token_digest(owner_token)):
            raise RunClaimError(
                "owner token does not match the run claim",
                details={"reason": "owner_mismatch", "run_id": run_id},
            )
        if status != "running":
            raise RunClaimError(
                "run is no longer owned by this claimant",
                details={
                    "reason": "stale_owner",
                    "run_id": run_id,
                    "status": status,
                    "recovered_by_run_id": recovered_by,
                },
            )
        now = self._clock()
        if lease_expires <= now:
            raise RunClaimError(
                "run lease expired before finalization",
                details={
                    "reason": "lease_expired",
                    "run_id": run_id,
                    "lease_expires_at_utc": lease_expires,
                },
            )
        return str(run_watch_id), now

    def _fence_claim(self, conn: sqlite3.Connection, claim: RunClaim) -> float:
        """Fence a claim and verify it belongs to the watch its run was claimed for."""
        run_watch_id, now = self._fence_run(conn, claim.run_id, claim.owner_token)
        if run_watch_id != claim.watch_id:
            raise RunClaimError(
                "claim watch does not match the run claim",
                details={
                    "reason": "owner_mismatch",
                    "run_id": claim.run_id,
                    "watch_id": claim.watch_id,
                    "run_watch_id": run_watch_id,
                },
            )
        return now

    @contextmanager
    def _connect_readonly(self) -> Iterator[tuple[sqlite3.Connection, int] | None]:
        if not self.path.exists():
            yield None
            return
        conn, version = self._open_probe()
        with closing(conn):
            if version == 0:
                # An uninitialized file reads as an empty store; nothing is written.
                yield None
                return
            try:
                yield conn, version
            except sqlite3.Error as error:
                raise _sqlite_failure(error, self.path, "read_failed") from error

    @contextmanager
    def _connect_sweep_readonly(self) -> Iterator[sqlite3.Connection | None]:
        """Read sweep tables; a missing state or a pre-sweep schema reads as empty."""
        with self._connect_readonly() as handle:
            if handle is None or handle[1] < _SWEEP_SCHEMA_VERSION:
                yield None
                return
            yield handle[0]

    @contextmanager
    def _connect_digest_readonly(self) -> Iterator[sqlite3.Connection | None]:
        """Read digest tables; a missing state or a pre-digest schema reads as empty."""
        with self._connect_readonly() as handle:
            if handle is None or handle[1] < _DIGEST_SCHEMA_VERSION:
                yield None
                return
            yield handle[0]

    @contextmanager
    def _connect_known_watch_readonly(
        self, watch_id: str
    ) -> Iterator[tuple[sqlite3.Connection, int]]:
        """Read state for a watch that must exist; a missing state or watch is refused."""
        with self._connect_readonly() as handle:
            if handle is None:
                raise _unknown_watch(watch_id)
            self._require_watch(handle[0], watch_id)
            yield handle

    @contextmanager
    def _connect_issue_readonly(self) -> Iterator[sqlite3.Connection | None]:
        """Read issue tables; a missing state or a pre-issue schema reads as empty."""
        with self._connect_readonly() as handle:
            if handle is None or handle[1] < _ISSUE_SCHEMA_VERSION:
                yield None
                return
            yield handle[0]

    @contextmanager
    def _connect_writable(self, *, create: bool) -> Iterator[sqlite3.Connection]:
        if self.path.exists():
            # Refuse unknown or foreign databases through a read-only handle first,
            # so a refused file is never opened for writing. A hot journal is rolled
            # back only after a private copy proves the recovered state is supported.
            probe, version = self._open_probe()
            probe.close()
            if version == 0 and not create:
                raise MonitoringStateError(
                    "monitoring state is not initialized; create a watch first",
                    details={"reason": "state_missing", "path": str(self.path)},
                )
        elif create:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        else:
            raise MonitoringStateError(
                "monitoring state does not exist; create a watch first",
                details={"reason": "state_missing", "path": str(self.path)},
            )
        mode = "rwc" if create else "rw"
        with closing(self._open(f"{self.path.resolve().as_uri()}?mode={mode}")) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            with _immediate(conn):
                version = _require_supported_schema(conn, self.path, allow_uninitialized=True)
                if version < STATE_SCHEMA_VERSION:
                    # Additive evolution: apply every later version's statements in order.
                    for schema_version in range(version + 1, STATE_SCHEMA_VERSION + 1):
                        for statement in _SCHEMA_STATEMENTS[schema_version]:
                            conn.execute(statement)
                    if version == 0:
                        conn.execute(f"PRAGMA application_id = {STATE_APPLICATION_ID}")
                    conn.execute(f"PRAGMA user_version = {STATE_SCHEMA_VERSION}")
            yield conn

    def _open_probe(self) -> tuple[sqlite3.Connection, int]:
        """Open and validate the existing file read-only, recovering a hot journal once."""
        try:
            return self._open_validated_readonly()
        except _HotJournal:
            self._recover_hot_journal()
        try:
            return self._open_validated_readonly()
        except _HotJournal as error:
            raise _recovery_failed(self.path, error) from error

    def _open_validated_readonly(self) -> tuple[sqlite3.Connection, int]:
        conn = self._open(f"{self.path.resolve().as_uri()}?mode=ro")
        try:
            return conn, _require_supported_schema(conn, self.path, allow_uninitialized=True)
        except BaseException:
            conn.close()
            raise

    def _recover_hot_journal(self) -> None:
        """Let SQLite roll back a hot journal once a private copy proves it is supported.

        A writer that crashed after spilling its cache leaves partially written pages
        in the database and the originals in the rollback journal, which read-only
        handles cannot restore. The pair is copied and rolled back privately first;
        the original is opened for writing only when the recovered copy is a
        supported ThreadSense state and the pair did not change meanwhile.
        """
        journal = _journal_path(self.path)
        try:
            fingerprint = _fingerprint(self.path, journal)
            with tempfile.TemporaryDirectory() as scratch:
                recovered_sha256 = self._recover_copy(Path(scratch), fingerprint)
            if _fingerprint(self.path, journal) != fingerprint:
                raise _recovery_conflict(self.path)
            with closing(self._open(f"{self.path.resolve().as_uri()}?mode=rw")) as conn:
                # The first read performs SQLite's rollback, then holds a SHARED lock
                # so the restored bytes cannot change before they are compared.
                conn.execute("BEGIN")
                conn.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
                if _file_sha256(self.path) != recovered_sha256:
                    raise _recovery_conflict(self.path)
                _require_supported_schema(conn, self.path, allow_uninitialized=True)
                conn.execute("COMMIT")
        except sqlite3.Error as error:
            failure = _sqlite_failure(error, self.path, "recovery_failed")
            if failure.details["reason"] == "state_locked":
                raise failure from error
            raise _recovery_failed(self.path, error) from error
        except FileNotFoundError as error:
            if error.filename is None or Path(error.filename) not in (self.path, journal):
                raise _recovery_failed(self.path, error) from error
            # A file of the pair vanished: another process recovered or replaced it first.
            raise _recovery_conflict(self.path) from error
        except (OSError, _HotJournal) as error:
            raise _recovery_failed(self.path, error) from error
        except MonitoringStateError as error:
            if error.details["reason"] != "open_failed":
                raise
            raise _recovery_failed(self.path, error.details["error"]) from error

    def _recover_copy(self, scratch: Path, fingerprint: tuple[str, str]) -> str:
        """Roll back a private copy of the pair and return the recovered database hash."""
        copy = scratch / STATE_FILENAME
        shutil.copyfile(self.path, copy)
        shutil.copyfile(_journal_path(self.path), _journal_path(copy))
        if _fingerprint(copy, _journal_path(copy)) != fingerprint:
            raise _recovery_conflict(self.path)
        with closing(self._open(f"{copy.resolve().as_uri()}?mode=rw")) as conn:
            try:
                # The first read rolls the copy back; its failures are recovery failures.
                conn.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
            except sqlite3.DatabaseError as error:
                if getattr(error, "sqlite_errorcode", None) != sqlite3.SQLITE_NOTADB:
                    raise
            # Refusals report the original path; only the copy has been touched.
            _require_supported_schema(conn, self.path, allow_uninitialized=True)
            check = conn.execute("PRAGMA quick_check").fetchone()[0]
        if check != "ok":
            raise _recovery_failed(self.path, f"quick_check: {check}")
        return _file_sha256(copy)

    def _open(self, uri: str) -> sqlite3.Connection:
        try:
            return sqlite3.connect(
                uri, uri=True, timeout=BUSY_TIMEOUT_SECONDS, isolation_level=None
            )
        except sqlite3.Error as error:
            raise MonitoringStateError(
                "monitoring state could not be opened",
                details={"reason": "open_failed", "path": str(self.path), "error": str(error)},
            ) from error


@contextmanager
def _immediate(conn: sqlite3.Connection) -> Iterator[None]:
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.Error as error:
        raise _sqlite_failure(error, None, "transaction_failed") from error
    try:
        yield
        conn.execute("COMMIT")
    except BaseException as error:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        if isinstance(error, sqlite3.Error):
            raise _sqlite_failure(error, None, "transaction_failed") from error
        raise


class _HotJournal(Exception):
    """A read-only handle met a hot rollback journal that only a writer can roll back."""


def _journal_path(path: Path) -> Path:
    return Path(f"{path}-journal")


def _file_sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _fingerprint(path: Path, journal: Path) -> tuple[str, str]:
    return _file_sha256(path), _file_sha256(journal)


def _recovery_failed(path: Path, error: object) -> MonitoringStateError:
    return MonitoringStateError(
        "monitoring state crash recovery failed",
        details={
            "reason": "recovery_failed",
            "path": str(path),
            "journal_path": str(_journal_path(path)),
            "error": str(error),
        },
    )


def _recovery_conflict(path: Path) -> MonitoringStateError:
    return MonitoringStateError(
        "monitoring state changed during crash recovery",
        details={
            "reason": "recovery_conflict",
            "path": str(path),
            "journal_path": str(_journal_path(path)),
        },
    )


def _sqlite_failure(error: sqlite3.Error, path: Path | None, reason: str) -> MonitoringStateError:
    """Map a SQLite error to a state error, reporting lock contention distinctly."""
    details: dict[str, Any] = {"error": str(error)}
    if path is not None:
        details["path"] = str(path)
    if getattr(error, "sqlite_errorcode", None) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
        return MonitoringStateError(
            "monitoring state is locked by another process",
            details={"reason": "state_locked", **details},
        )
    return MonitoringStateError(
        "monitoring state operation failed", details={"reason": reason, **details}
    )


def _require_supported_schema(
    conn: sqlite3.Connection, path: Path, *, allow_uninitialized: bool
) -> int:
    """Return the schema version, refusing newer, foreign or uninitialized files."""
    try:
        application_id = conn.execute("PRAGMA application_id").fetchone()[0]
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        table_count = conn.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()[0]
    except sqlite3.DatabaseError as error:
        if getattr(error, "sqlite_errorcode", None) == sqlite3.SQLITE_READONLY_ROLLBACK:
            raise _HotJournal(str(error)) from error
        failure = _sqlite_failure(error, path, "not_a_database")
        if failure.details["reason"] == "not_a_database":
            failure = MonitoringStateError(
                "monitoring state is not a readable SQLite database", details=failure.details
            )
        raise failure from error
    if application_id == STATE_APPLICATION_ID and version > STATE_SCHEMA_VERSION:
        raise MonitoringStateError(
            "monitoring state schema is newer than this version of ThreadSense supports",
            details={
                "reason": "unsupported_schema_version",
                "path": str(path),
                "schema_version": version,
                "supported_schema_version": STATE_SCHEMA_VERSION,
            },
        )
    if application_id == STATE_APPLICATION_ID and 1 <= version <= STATE_SCHEMA_VERSION:
        return int(version)
    if application_id == 0 and version == 0 and table_count == 0 and allow_uninitialized:
        return 0
    raise MonitoringStateError(
        "file is not a ThreadSense monitoring state database",
        details={
            "reason": "foreign_database",
            "path": str(path),
            "application_id": application_id,
            "schema_version": version,
        },
    )


def _load_stored_profile(
    watch_id: str, revision: int, content_sha256: str, profile_json: str
) -> WatchProfile:
    stored_digest = hashlib.sha256(profile_json.encode("utf-8")).hexdigest()
    if stored_digest != content_sha256:
        raise MonitoringStateError(
            "stored watch profile does not match its content hash",
            details={"reason": "profile_hash_mismatch", "watch_id": watch_id, "revision": revision},
        )
    try:
        payload = json.loads(profile_json)
    except json.JSONDecodeError as error:
        raise MonitoringStateError(
            "stored watch profile is not valid JSON",
            details={"reason": "profile_unreadable", "watch_id": watch_id, "revision": revision},
        ) from error
    if not isinstance(payload, dict):
        raise MonitoringStateError(
            "stored watch profile is not a JSON object",
            details={"reason": "profile_unreadable", "watch_id": watch_id, "revision": revision},
        )
    try:
        return parse_watch_profile(payload)
    except MonitoringInputError as error:
        raise MonitoringStateError(
            "stored watch profile is not valid under the current profile schema",
            details={"reason": "profile_unreadable", "watch_id": watch_id, "revision": revision},
        ) from error


def _run_to_dict(row: tuple[Any, ...]) -> dict[str, Any]:
    run_id, revision, status, claimed_at, lease_expires, finished_at, recovered_by = row
    return {
        "run_id": run_id,
        "profile_revision": revision,
        "status": status,
        "claimed_at_utc": claimed_at,
        "lease_expires_at_utc": lease_expires,
        "finished_at_utc": finished_at,
        "recovered_by_run_id": recovered_by,
    }


def _token_digest(owner_token: str) -> str:
    return hashlib.sha256(owner_token.encode("utf-8")).hexdigest()


def _unknown_watch(watch_id: str) -> MonitoringStateError:
    return MonitoringStateError(
        "watch does not exist", details={"reason": "unknown_watch", "watch_id": watch_id}
    )


# Only a thread's current revision is due: its snapshot contains every superseded one.
