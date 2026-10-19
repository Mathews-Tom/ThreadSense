from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import shutil
import sqlite3
import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from threadsense.config import StorageConfig
from threadsense.errors import MonitoringInputError, MonitoringStateError, RunClaimError
from threadsense.monitoring.profiles import (
    WATCH_PROFILE_VERSION,
    WatchProfile,
    parse_watch_profile,
    validate_watch_id,
)

STATE_SCHEMA_VERSION = 1
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
        with self._connect_readonly() as conn:
            if conn is None:
                return []
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
        with self._connect_readonly() as conn:
            if conn is None:
                raise _unknown_watch(watch_id)
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

    def finalize_run(self, run_id: str, owner_token: str, outcome: RunOutcome) -> None:
        """Record a run outcome; only the current owner of a live lease may do so."""
        if outcome not in ("succeeded", "failed"):
            raise MonitoringInputError(
                "run outcome must be succeeded or failed", details={"outcome": outcome}
            )
        with self._connect_writable(create=False) as conn, _immediate(conn):
            row = conn.execute(
                "SELECT owner_token_sha256, status, lease_expires_at_utc, recovered_by_run_id "
                "FROM watch_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if row is None:
                raise RunClaimError(
                    "run does not exist", details={"reason": "unknown_run", "run_id": run_id}
                )
            token_sha256, status, lease_expires, recovered_by = row
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
            conn.execute(
                "UPDATE watch_runs SET status = ?, finished_at_utc = ? "
                "WHERE run_id = ? AND status = 'running'",
                (outcome, now, run_id),
            )

    @contextmanager
    def _connect_readonly(self) -> Iterator[sqlite3.Connection | None]:
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
                yield conn
            except sqlite3.Error as error:
                raise _sqlite_failure(error, self.path, "read_failed") from error

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
                if _require_supported_schema(conn, self.path, allow_uninitialized=True) == 0:
                    for statement in _SCHEMA_V1:
                        conn.execute(statement)
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
    if application_id == STATE_APPLICATION_ID and version == STATE_SCHEMA_VERSION:
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
