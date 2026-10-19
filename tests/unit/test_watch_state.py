from __future__ import annotations

import dataclasses
import hashlib
import json
import shutil
import sqlite3
import subprocess
import sys
import tomllib
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest

from threadsense.config import StorageConfig
from threadsense.errors import MonitoringInputError, MonitoringStateError, RunClaimError
from threadsense.monitoring import (
    MonitoringStore,
    load_watch_profile_file,
    monitoring_state_path,
    parse_watch_profile,
)
from threadsense.monitoring.state import (
    STATE_APPLICATION_ID,
    STATE_SCHEMA_VERSION,
    RunClaim,
)
from threadsense.monitoring.sweep_records import (
    AcquisitionRecord,
    CommentChange,
    Completeness,
    DiscoveredThread,
    RevisionRecord,
)

FIXTURE_PROFILE = Path(__file__).resolve().parents[1] / "fixtures" / "watch" / "profile_v1.toml"


class ManualClock:
    def __init__(self, start: float) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


def fixture_payload() -> dict[str, Any]:
    with FIXTURE_PROFILE.open("rb") as handle:
        return tomllib.load(handle)


def edited(**changes: Any) -> dict[str, Any]:
    payload = fixture_payload()
    payload.update(changes)
    return payload


def store_at(tmp_path: Path, clock: ManualClock | None = None) -> MonitoringStore:
    path = tmp_path / "monitoring" / "state.sqlite3"
    return MonitoringStore(path, clock or ManualClock(1_000.0))


def directory_snapshot(directory: Path) -> dict[str, bytes]:
    return {entry.name: entry.read_bytes() for entry in sorted(directory.iterdir())}


def test_load_watch_profile_v1_fixture_returns_canonical_sets() -> None:
    profile = load_watch_profile_file(FIXTURE_PROFILE)

    assert profile.watch_id == "agent-tooling"
    assert profile.queries == ("coding agent", "mcp server")
    assert profile.subreddits == ("ClaudeAI", "LocalLLaMA")
    assert profile.include_terms == ("context window", "latency")
    assert profile.to_dict()["profile_version"] == 1
    assert profile.limits.presented_findings == 10


def test_profile_hash_reordered_sets_and_keys_is_identical() -> None:
    original = parse_watch_profile(fixture_payload())
    payload = fixture_payload()
    reordered = {key: payload[key] for key in reversed(list(payload))}
    reordered["queries"] = list(reversed(payload["queries"]))
    reordered["subreddits"] = ["LocalLLaMA", "r/ClaudeAI"]
    reordered["include_terms"] = list(reversed(payload["include_terms"]))

    assert parse_watch_profile(reordered).content_sha256() == original.content_sha256()


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        (
            {"limits": {**fixture_payload()["limits"], "discovered_candidates": 0}},
            {"field": "limits.discovered_candidates"},
        ),
        (
            {"limits": {**fixture_payload()["limits"], "presented_findings": 101}},
            {"field": "limits.presented_findings"},
        ),
        (
            {"limits": {**fixture_payload()["limits"], "refreshed_threads": True}},
            {"field": "limits.refreshed_threads"},
        ),
        ({"limits": {"discovered_candidates": 5}}, {"field": "limits.refreshed_threads"}),
        ({"refresh_interval_minutes": 5}, {"field": "refresh_interval_minutes"}),
        ({"queries": ['title:"agent"']}, {"field": "queries"}),
        ({"queries": []}, {"field": "queries"}),
        ({"queries": ["agent", "Agent"]}, {"field": "queries"}),
        ({"subreddits": ["../etc"]}, {"field": "subreddits"}),
        ({"id": "../escape"}, {"field": "id"}),
        ({"id": "watches/agent"}, {"field": "id"}),
        ({"id": "Agent"}, {"field": "id"}),
        ({"profile_version": 2}, {"field": "profile_version"}),
        ({"include_terms": ["Latency"], "exclude_terms": ["latency"]}, {"terms": ["latency"]}),
        ({"output_dir": "/tmp"}, {"fields": ["output_dir"]}),
    ],
)
def test_parse_watch_profile_invalid_input_raises_input_error(
    changes: dict[str, Any], expected: dict[str, Any]
) -> None:
    with pytest.raises(MonitoringInputError) as raised:
        parse_watch_profile(edited(**changes))

    assert expected.items() <= raised.value.details.items()


def test_save_profile_identical_content_is_unchanged_without_modifying_state(
    tmp_path: Path,
) -> None:
    clock = ManualClock(1_000.0)
    store = store_at(tmp_path, clock)
    created = store.save_profile(parse_watch_profile(fixture_payload()))
    before = directory_snapshot(store.path.parent)
    clock.now = 2_000.0

    repeated = store.save_profile(parse_watch_profile(fixture_payload()))

    assert created.status == "created"
    assert repeated.status == "unchanged"
    assert repeated.revision == created.revision == 1
    assert directory_snapshot(store.path.parent) == before


def test_save_profile_edits_append_revisions_including_reverts(tmp_path: Path) -> None:
    store = store_at(tmp_path)
    original = parse_watch_profile(fixture_payload())
    store.save_profile(original)

    revised = store.save_profile(parse_watch_profile(edited(refresh_interval_minutes=720)))
    reverted = store.save_profile(original)
    inspection = store.inspect_watch("agent-tooling")

    assert (revised.status, revised.revision) == ("revised", 2)
    assert (reverted.status, reverted.revision) == ("revised", 3)
    assert [item["revision"] for item in inspection["revisions"]] == [1, 2, 3]
    assert inspection["revisions"][0]["content_sha256"] == reverted.content_sha256
    assert inspection["profile"] == original.to_dict()


def test_save_profile_revision_hash_does_not_depend_on_save_time(tmp_path: Path) -> None:
    early = store_at(tmp_path / "early", ManualClock(1_000.0))
    late = store_at(tmp_path / "late", ManualClock(9_999_999.0))

    early_result = early.save_profile(parse_watch_profile(fixture_payload()))
    late_result = late.save_profile(parse_watch_profile(fixture_payload()))

    assert early_result.content_sha256 == late_result.content_sha256


def test_claim_run_live_claim_refuses_competing_claim(tmp_path: Path) -> None:
    clock = ManualClock(1_000.0)
    store = store_at(tmp_path, clock)
    store.save_profile(parse_watch_profile(fixture_payload()))
    first = store.claim_run("agent-tooling", lease_seconds=60)
    clock.now = 1_059.0

    with pytest.raises(RunClaimError) as raised:
        store.claim_run("agent-tooling", lease_seconds=60)

    assert raised.value.details["reason"] == "claim_held"
    assert raised.value.details["active_run_id"] == first.run_id
    assert store.inspect_watch("agent-tooling")["run_count"] == 1


def test_claim_run_after_lease_expiry_recovers_and_fences_stale_owner(tmp_path: Path) -> None:
    clock = ManualClock(1_000.0)
    store = store_at(tmp_path, clock)
    store.save_profile(parse_watch_profile(fixture_payload()))
    stale = store.claim_run("agent-tooling", lease_seconds=60)
    clock.now = 1_061.0

    current = store.claim_run("agent-tooling", lease_seconds=60)
    with pytest.raises(RunClaimError) as raised:
        store.finalize_run(stale.run_id, stale.owner_token, "succeeded")
    store.finalize_run(current.run_id, current.owner_token, "succeeded")
    runs = {run["run_id"]: run for run in store.inspect_watch("agent-tooling")["recent_runs"]}

    assert current.recovered_run_id == stale.run_id
    assert raised.value.details["reason"] == "stale_owner"
    assert raised.value.details["recovered_by_run_id"] == current.run_id
    assert runs[stale.run_id]["status"] == "abandoned"
    assert runs[current.run_id]["status"] == "succeeded"


def test_finalize_run_expired_lease_without_recovery_is_refused(tmp_path: Path) -> None:
    clock = ManualClock(1_000.0)
    store = store_at(tmp_path, clock)
    store.save_profile(parse_watch_profile(fixture_payload()))
    claim = store.claim_run("agent-tooling", lease_seconds=60)
    clock.now = 1_060.0

    with pytest.raises(RunClaimError) as raised:
        store.finalize_run(claim.run_id, claim.owner_token, "failed")

    assert raised.value.details["reason"] == "lease_expired"
    assert store.inspect_watch("agent-tooling")["active_run"]["status"] == "running"


def test_finalize_run_wrong_owner_token_is_refused(tmp_path: Path) -> None:
    store = store_at(tmp_path)
    store.save_profile(parse_watch_profile(fixture_payload()))
    claim = store.claim_run("agent-tooling", lease_seconds=60)

    with pytest.raises(RunClaimError) as raised:
        store.finalize_run(claim.run_id, "not-the-owner", "succeeded")

    assert raised.value.details["reason"] == "owner_mismatch"
    assert store.inspect_watch("agent-tooling")["active_run"]["run_id"] == claim.run_id


def test_claim_run_without_state_refuses_and_creates_nothing(tmp_path: Path) -> None:
    store = store_at(tmp_path)

    with pytest.raises(MonitoringStateError) as raised:
        store.claim_run("agent-tooling", lease_seconds=60)

    assert raised.value.details["reason"] == "state_missing"
    assert list(tmp_path.iterdir()) == []


def test_list_watches_without_state_returns_empty_and_creates_nothing(tmp_path: Path) -> None:
    store = store_at(tmp_path)

    watches = store.list_watches()

    assert watches == []
    assert list(tmp_path.iterdir()) == []


def mark_schema_newer(store: MonitoringStore) -> None:
    with closing(sqlite3.connect(store.path)) as conn:
        conn.execute(f"PRAGMA user_version = {STATE_SCHEMA_VERSION + 1}")
        conn.commit()


def make_foreign_database(store: MonitoringStore) -> None:
    store.path.parent.mkdir(parents=True)
    with closing(sqlite3.connect(store.path)) as conn:
        conn.execute("CREATE TABLE notes (body TEXT)")
        conn.commit()


def make_non_sqlite_file(store: MonitoringStore) -> None:
    store.path.parent.mkdir(parents=True)
    store.path.write_bytes(b"not a database, just bytes that are long enough to have a header")


@pytest.mark.parametrize(
    ("prepare", "reason"),
    [
        (mark_schema_newer, "unsupported_schema_version"),
        (make_foreign_database, "foreign_database"),
        (make_non_sqlite_file, "not_a_database"),
    ],
)
@pytest.mark.parametrize(
    "operation",
    [
        lambda store: store.save_profile(parse_watch_profile(fixture_payload())),
        lambda store: store.list_watches(),
        lambda store: store.inspect_watch("agent-tooling"),
        lambda store: store.claim_run("agent-tooling", lease_seconds=60),
        lambda store: store.finalize_run("run-0000000000000000", "token", "succeeded"),
    ],
    ids=["save", "list", "inspect", "claim", "finalize"],
)
def test_store_unsupported_database_is_refused_without_modification(
    tmp_path: Path,
    prepare: Callable[[MonitoringStore], None],
    reason: str,
    operation: Callable[[MonitoringStore], object],
) -> None:
    store = store_at(tmp_path)
    if prepare is mark_schema_newer:
        store.save_profile(parse_watch_profile(fixture_payload()))
    prepare(store)
    before = directory_snapshot(store.path.parent)

    with pytest.raises(MonitoringStateError) as raised:
        operation(store)

    assert raised.value.details["reason"] == reason
    assert directory_snapshot(store.path.parent) == before


def store_hash_consistent_profile_json(store: MonitoringStore, profile_json: str) -> None:
    digest = hashlib.sha256(profile_json.encode("utf-8")).hexdigest()
    with closing(sqlite3.connect(store.path)) as conn:
        conn.execute(
            "UPDATE watch_revisions SET profile_json = ?, content_sha256 = ?",
            (profile_json, digest),
        )
        conn.commit()


@pytest.mark.parametrize(
    "profile_json",
    [
        json.dumps(edited(profile_version=999), sort_keys=True),
        "[]",
        "null",
        '"x"',
    ],
    ids=["invalid-version", "array", "null", "string"],
)
@pytest.mark.parametrize(
    "operation",
    [
        lambda store: store.list_watches(),
        lambda store: store.inspect_watch("agent-tooling"),
    ],
    ids=["list", "inspect"],
)
def test_store_invalid_stored_profile_raises_state_error_without_modification(
    tmp_path: Path, operation: Callable[[MonitoringStore], object], profile_json: str
) -> None:
    store = store_at(tmp_path)
    store.save_profile(parse_watch_profile(fixture_payload()))
    store_hash_consistent_profile_json(store, profile_json)
    before = directory_snapshot(store.path.parent)

    with pytest.raises(MonitoringStateError) as raised:
        operation(store)

    assert raised.value.code == "monitoring_state_error"
    assert raised.value.details == {
        "reason": "profile_unreadable",
        "watch_id": "agent-tooling",
        "revision": 1,
    }
    assert directory_snapshot(store.path.parent) == before


@pytest.mark.parametrize("dirname", ["..", "../outside", "/tmp/monitoring", "a/../../b", "."])
def test_monitoring_state_path_escaping_storage_root_is_refused(
    tmp_path: Path, dirname: str
) -> None:
    storage = StorageConfig(root_dir=tmp_path / "store", monitoring_dirname=dirname)

    with pytest.raises(MonitoringInputError):
        monitoring_state_path(storage)


def test_monitoring_state_path_symlink_outside_root_is_refused(tmp_path: Path) -> None:
    root = tmp_path / "store"
    root.mkdir()
    (root / "monitoring").symlink_to(tmp_path / "elsewhere", target_is_directory=True)
    storage = StorageConfig(root_dir=root)

    with pytest.raises(MonitoringInputError) as raised:
        monitoring_state_path(storage)

    assert "escapes" in raised.value.message


def test_monitoring_state_path_default_is_inside_storage_root(tmp_path: Path) -> None:
    storage = StorageConfig(root_dir=tmp_path / "store")

    path = monitoring_state_path(storage)

    assert path == (tmp_path / "store").resolve() / "monitoring" / "state.sqlite3"


@pytest.mark.parametrize(
    "operation",
    [
        lambda store: store.save_profile(parse_watch_profile(fixture_payload())),
        lambda store: store.list_watches(),
        lambda store: store.inspect_watch("agent-tooling"),
        lambda store: store.claim_run("agent-tooling", lease_seconds=60),
    ],
    ids=["save", "list", "inspect", "claim"],
)
def test_store_held_exclusive_lock_reports_state_locked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: Callable[[MonitoringStore], object],
) -> None:
    store = store_at(tmp_path)
    store.save_profile(parse_watch_profile(fixture_payload()))
    monkeypatch.setattr("threadsense.monitoring.state.BUSY_TIMEOUT_SECONDS", 0.05)

    with closing(sqlite3.connect(store.path, isolation_level=None)) as holder:
        holder.execute("BEGIN EXCLUSIVE")
        with pytest.raises(MonitoringStateError) as raised:
            operation(store)
        holder.execute("ROLLBACK")

    assert raised.value.details["reason"] == "state_locked"


def test_claim_run_on_uninitialized_file_refuses_without_writing(tmp_path: Path) -> None:
    store = store_at(tmp_path)
    store.path.parent.mkdir(parents=True)
    store.path.write_bytes(b"")

    with pytest.raises(MonitoringStateError) as raised:
        store.claim_run("agent-tooling", lease_seconds=60)

    assert raised.value.details["reason"] == "state_missing"
    assert store.path.read_bytes() == b""


# Runs in a child process: spills a large uncommitted update to the database file,
# then dies without committing or rolling back, leaving a hot rollback journal.
CRASH_CODE = """
import os
import sqlite3
import sys

conn = sqlite3.connect(sys.argv[1], isolation_level=None)
conn.execute("PRAGMA cache_size = 5")
conn.execute("BEGIN IMMEDIATE")
conn.execute(sys.argv[2], ("x" * 1_000_000,))
os._exit(0)
"""
REVISION_UPDATE = "UPDATE watch_revisions SET profile_json = ?"
# Same crashed writer, but it first sets an uncommitted supported user_version, so SQLite
# writes the committed page 1 into the rollback journal before the spill.
CRASH_WITH_UNCOMMITTED_VERSION_CODE = f"""
import os
import sqlite3
import sys

conn = sqlite3.connect(sys.argv[1], isolation_level=None)
conn.execute("PRAGMA cache_size = 5")
conn.execute("BEGIN IMMEDIATE")
conn.execute("PRAGMA user_version = {STATE_SCHEMA_VERSION}")
conn.execute(sys.argv[2], ("x" * 1_000_000,))
os._exit(0)
"""
# Offset of the big-endian user_version field in the SQLite database header.
SQLITE_USER_VERSION_OFFSET = 60


def journal_path(store: MonitoringStore) -> Path:
    return Path(f"{store.path}-journal")


def crash_mid_transaction(
    store: MonitoringStore, update_sql: str, *, crash_code: str = CRASH_CODE
) -> None:
    subprocess.run(
        [sys.executable, "-c", crash_code, str(store.path), update_sql], check=True, timeout=30
    )
    journal = journal_path(store)
    assert journal.exists()
    assert journal.stat().st_size > 512


def prepare_newer_schema(store: MonitoringStore) -> None:
    store.save_profile(parse_watch_profile(fixture_payload()))
    mark_schema_newer(store)


def make_foreign_database_with_note(store: MonitoringStore) -> None:
    make_foreign_database(store)
    with closing(sqlite3.connect(store.path)) as conn:
        conn.execute("INSERT INTO notes (body) VALUES ('kept')")
        conn.commit()


@pytest.mark.parametrize(
    "first_access",
    [
        lambda store: store.list_watches(),
        lambda store: store.inspect_watch("agent-tooling"),
        lambda store: store.save_profile(parse_watch_profile(fixture_payload())),
    ],
    ids=["list", "inspect", "save"],
)
def test_store_hot_journal_on_supported_state_recovers_committed_state(
    tmp_path: Path, first_access: Callable[[MonitoringStore], object]
) -> None:
    store = store_at(tmp_path)
    profile = parse_watch_profile(fixture_payload())
    store.save_profile(profile)
    claim = store.claim_run("agent-tooling", lease_seconds=60)
    crash_mid_transaction(store, REVISION_UPDATE)

    first_access(store)

    journal_left = journal_path(store).exists()
    watches = store.list_watches()
    inspected = store.inspect_watch("agent-tooling")
    saved = store.save_profile(profile)
    store.finalize_run(claim.run_id, claim.owner_token, "succeeded")
    follow_up = store.claim_run("agent-tooling", lease_seconds=60)

    assert journal_left is False
    assert [(watch["watch_id"], watch["content_sha256"]) for watch in watches] == [
        ("agent-tooling", profile.content_sha256())
    ]
    assert inspected["profile"] == profile.to_dict()
    assert inspected["active_run"]["run_id"] == claim.run_id
    assert saved.status == "unchanged"
    assert follow_up.recovered_run_id is None


@pytest.mark.parametrize(
    ("prepare", "update_sql", "reason"),
    [
        (prepare_newer_schema, REVISION_UPDATE, "unsupported_schema_version"),
        (make_foreign_database_with_note, "UPDATE notes SET body = ?", "foreign_database"),
    ],
    ids=["newer", "foreign"],
)
@pytest.mark.parametrize(
    "operation",
    [
        lambda store: store.save_profile(parse_watch_profile(fixture_payload())),
        lambda store: store.list_watches(),
        lambda store: store.inspect_watch("agent-tooling"),
        lambda store: store.claim_run("agent-tooling", lease_seconds=60),
    ],
    ids=["save", "list", "inspect", "claim"],
)
def test_store_hot_journal_on_unsupported_state_is_refused_without_modification(
    tmp_path: Path,
    prepare: Callable[[MonitoringStore], None],
    update_sql: str,
    reason: str,
    operation: Callable[[MonitoringStore], object],
) -> None:
    store = store_at(tmp_path)
    prepare(store)
    crash_mid_transaction(store, update_sql)
    before = directory_snapshot(store.path.parent)

    with pytest.raises(MonitoringStateError) as raised:
        operation(store)

    assert raised.value.details["reason"] == reason
    assert directory_snapshot(store.path.parent) == before


def inject_header_user_version_fault(path: Path, version: int) -> None:
    """Overwrite only the four user_version header bytes of the database file.

    This is a deterministic injected page fault representing a partial page-1 write.
    It is not an observed automatic SQLite header spill. The rollback journal is the
    genuine one written by SQLite and is never fabricated or edited.
    """
    with path.open("r+b") as handle:
        handle.seek(SQLITE_USER_VERSION_OFFSET)
        handle.write(version.to_bytes(4, "big"))


def raw_header_user_version(path: Path) -> int:
    header = path.read_bytes()[SQLITE_USER_VERSION_OFFSET : SQLITE_USER_VERSION_OFFSET + 4]
    return int.from_bytes(header, "big")


def recover_private_copy_user_version(store: MonitoringStore, scratch: Path) -> int:
    """Roll back a private copy of the database and journal; return its committed version."""
    scratch.mkdir()
    copy = scratch / store.path.name
    shutil.copyfile(store.path, copy)
    shutil.copyfile(journal_path(store), Path(f"{copy}-journal"))
    with closing(sqlite3.connect(copy)) as conn:
        # The first read through a write-capable handle rolls the hot journal back.
        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
    assert not Path(f"{copy}-journal").exists()
    return version


@pytest.mark.parametrize(
    "operation",
    [
        lambda store: store.list_watches(),
        lambda store: store.inspect_watch("agent-tooling"),
        lambda store: store.save_profile(parse_watch_profile(fixture_payload())),
    ],
    ids=["list", "inspect", "save"],
)
def test_store_hot_journal_with_misleading_raw_header_is_refused_by_committed_version(
    tmp_path: Path, operation: Callable[[MonitoringStore], object]
) -> None:
    """Trust the committed identity in the genuine journal, not the raw page-1 header.

    The crashed writer and its rollback journal are genuine SQLite output. The raw
    header is then rewritten to the supported version by a deterministic injected page
    fault (a stand-in for a partial page-1 write, not an observed SQLite spill).
    """
    newer_version = STATE_SCHEMA_VERSION + 1
    store = store_at(tmp_path)
    prepare_newer_schema(store)
    crash_mid_transaction(store, REVISION_UPDATE, crash_code=CRASH_WITH_UNCOMMITTED_VERSION_CODE)
    inject_header_user_version_fault(store.path, STATE_SCHEMA_VERSION)
    assert raw_header_user_version(store.path) == STATE_SCHEMA_VERSION
    recovered_version = recover_private_copy_user_version(store, tmp_path / "private-recovery")
    assert recovered_version == newer_version
    database_before = store.path.read_bytes()
    journal_before = journal_path(store).read_bytes()
    before = directory_snapshot(store.path.parent)

    with pytest.raises(MonitoringStateError) as raised:
        operation(store)

    assert raised.value.details["reason"] == "unsupported_schema_version"
    assert raised.value.details["schema_version"] == newer_version
    assert store.path.read_bytes() == database_before
    assert journal_path(store).read_bytes() == journal_before
    assert directory_snapshot(store.path.parent) == before


def test_list_watches_hot_journal_on_write_protected_state_fails_without_modification(
    tmp_path: Path,
) -> None:
    store = store_at(tmp_path)
    store.save_profile(parse_watch_profile(fixture_payload()))
    crash_mid_transaction(store, REVISION_UPDATE)
    before = directory_snapshot(store.path.parent)
    store.path.chmod(0o444)

    try:
        with pytest.raises(MonitoringStateError) as raised:
            store.list_watches()
    finally:
        store.path.chmod(0o644)

    assert raised.value.details["reason"] == "recovery_failed"
    assert raised.value.details["journal_path"] == str(journal_path(store))
    assert directory_snapshot(store.path.parent) == before


def test_store_zero_byte_state_reads_empty_and_initializes_on_save(tmp_path: Path) -> None:
    store = store_at(tmp_path)
    store.path.parent.mkdir(parents=True)
    store.path.write_bytes(b"")
    before = directory_snapshot(store.path.parent)

    watches = store.list_watches()
    with pytest.raises(MonitoringStateError) as inspected:
        store.inspect_watch("agent-tooling")
    after_reads = directory_snapshot(store.path.parent)
    with pytest.raises(MonitoringStateError) as claimed:
        store.claim_run("agent-tooling", lease_seconds=60)
    saved = store.save_profile(parse_watch_profile(fixture_payload()))

    assert watches == []
    assert inspected.value.details["reason"] == "unknown_watch"
    assert after_reads == before
    assert claimed.value.details["reason"] == "state_missing"
    assert saved.status == "created"


# The schema shipped as version 1, kept literally so upgrades are tested against it.
V1_SCHEMA_DDL = (
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
V1_HISTORY_RUN_ID = "run-00000000000000v1"
WATCH_ID = "agent-tooling"
THREAD_ID = "abc123"
THREAD_TEXT_SHA256 = hashlib.sha256(b'["title",null]').hexdigest()
EMPTY_SWEEP_STATUS = {
    "tracked_threads": 0,
    "refresh_due_never_refreshed": 0,
    "pending_analyses": 0,
    "revision_count": 0,
    "comment_count": 0,
    "latest_sweep": None,
}


def text_sha256(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


FIRST_COMMENT = ("reddit:c1", text_sha256("first"), 600.0)
SECOND_COMMENT = ("reddit:c2", text_sha256("second"), 700.0)
LATER_COMMENT = ("reddit:c3", text_sha256("third"), 1_050.0)


def create_v1_state(store: MonitoringStore) -> None:
    """Write a version 1 database with one profile revision and one finished run."""
    profile = parse_watch_profile(fixture_payload())
    store.path.parent.mkdir(parents=True)
    with closing(sqlite3.connect(store.path)) as conn:
        for statement in V1_SCHEMA_DDL:
            conn.execute(statement)
        conn.execute(f"PRAGMA application_id = {STATE_APPLICATION_ID}")
        conn.execute("PRAGMA user_version = 1")
        conn.execute("INSERT INTO watches VALUES (?, 1, 900.0, 900.0)", (profile.watch_id,))
        conn.execute(
            "INSERT INTO watch_revisions VALUES (?, 1, ?, ?, ?, 900.0)",
            (
                profile.watch_id,
                profile.to_dict()["profile_version"],
                profile.content_sha256(),
                profile.canonical_json(),
            ),
        )
        conn.execute(
            "INSERT INTO watch_runs (run_id, watch_id, profile_revision, owner_token_sha256, "
            "status, claimed_at_utc, lease_expires_at_utc, finished_at_utc) "
            "VALUES (?, ?, 1, ?, 'succeeded', 900.0, 960.0, 950.0)",
            (V1_HISTORY_RUN_ID, profile.watch_id, "0" * 64),
        )
        conn.commit()


def schema_identity(store: MonitoringStore) -> tuple[int, int]:
    with closing(sqlite3.connect(f"{store.path.resolve().as_uri()}?mode=ro", uri=True)) as conn:
        application_id = int(conn.execute("PRAGMA application_id").fetchone()[0])
        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
    return application_id, version


def query_state(store: MonitoringStore, sql: str) -> list[tuple[Any, ...]]:
    with closing(sqlite3.connect(f"{store.path.resolve().as_uri()}?mode=ro", uri=True)) as conn:
        return conn.execute(sql).fetchall()


def discovered(
    thread_id: str, *, selected: bool = True, rank: int = 1, title: str = "title"
) -> DiscoveredThread:
    return DiscoveredThread(
        query="coding agent",
        source_thread_id=thread_id,
        thread_url=f"https://www.reddit.com/r/ClaudeAI/comments/{thread_id}/",
        subreddit="ClaudeAI",
        title=title,
        source_created_utc=500.0,
        rank=rank,
        score=10,
        num_comments=2,
        selected=selected,
        rejection_reason=None if selected else "query_mismatch",
    )


def write_artifact(store: MonitoringStore, relative: str, data: bytes) -> tuple[Path, str]:
    path = store.monitoring_root() / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path, hashlib.sha256(data).hexdigest()


def acquisition_of(
    store: MonitoringStore,
    tag: str,
    observed: list[tuple[str, str, float]],
    *,
    absent: int = 0,
    observed_at: float | None = None,
    completeness: Completeness = "unknown",
) -> AcquisitionRecord:
    """Write real raw and normalized artifacts for one acquisition of THREAD_ID.

    Like the sweep, the observation time defaults to the store clock sampled before recording.
    """
    raw_bytes = json.dumps({"tag": tag, "observed": observed}).encode("utf-8")
    acquisition_id = "acq-" + hashlib.sha256(raw_bytes).hexdigest()[:32]
    base = f"watches/{WATCH_ID}/threads/{THREAD_ID}/acquisitions/{acquisition_id}"
    raw_path, raw_sha256 = write_artifact(store, f"{base}.raw.json", raw_bytes)
    normalized_path, normalized_sha256 = write_artifact(
        store, f"{base}.normalized.json", b"normalized " + raw_bytes
    )
    return AcquisitionRecord(
        acquisition_id=acquisition_id,
        source_thread_id=THREAD_ID,
        observed_at_utc=store.now() if observed_at is None else observed_at,
        raw_path=raw_path,
        raw_sha256=raw_sha256,
        normalized_path=normalized_path,
        normalized_sha256=normalized_sha256,
        completeness=completeness,
        cache_status="disabled",
        parsed_comment_count=len(observed),
        reported_comment_count=len(observed),
        observed_comments=tuple(observed),
        absent_known_comment_count=absent,
    )


def revision_of(
    store: MonitoringStore, previous_revision_id: str | None, changes: tuple[CommentChange, ...]
) -> RevisionRecord:
    """Write a real snapshot artifact for a revision of THREAD_ID."""
    substantive = json.dumps([[change.to_dict() for change in changes]], sort_keys=True)
    substantive_sha256 = text_sha256(substantive)
    revision_id = (
        "rev-"
        + text_sha256(
            f"{WATCH_ID}\n{THREAD_ID}\n{previous_revision_id or ''}\n{substantive_sha256}"
        )[:32]
    )
    snapshot_bytes = json.dumps({"revision_id": revision_id}).encode("utf-8")
    snapshot_sha256 = hashlib.sha256(snapshot_bytes).hexdigest()
    snapshot_path, _ = write_artifact(
        store,
        f"watches/{WATCH_ID}/threads/{THREAD_ID}/revisions/{revision_id}/{snapshot_sha256}.json",
        snapshot_bytes,
    )
    return RevisionRecord(
        revision_id=revision_id,
        previous_revision_id=previous_revision_id,
        substantive_sha256=substantive_sha256,
        thread_text_sha256=THREAD_TEXT_SHA256,
        snapshot_path=snapshot_path,
        snapshot_sha256=snapshot_sha256,
        changes=changes,
        thread_text_changed=False,
    )


def new_comment_changes(kind: Any, *comments: tuple[str, str, float]) -> tuple[CommentChange, ...]:
    return tuple(
        CommentChange(comment_id, kind, sha256, None, created)
        for comment_id, sha256, created in comments
    )


def tracked_store(tmp_path: Path) -> tuple[MonitoringStore, ManualClock, RunClaim]:
    clock = ManualClock(1_000.0)
    store = store_at(tmp_path, clock)
    store.save_profile(parse_watch_profile(fixture_payload()))
    claim = store.claim_run(WATCH_ID, lease_seconds=3_600)
    store.record_discoveries(claim, [discovered(THREAD_ID)])
    return store, clock, claim


def record_initial(
    store: MonitoringStore, claim: RunClaim, *comments: tuple[str, str, float]
) -> RevisionRecord:
    revision = revision_of(store, None, new_comment_changes("initial_observation", *comments))
    store.record_acquisition(claim, acquisition_of(store, "initial", list(comments)), revision)
    return revision


def comment_text_history(store: MonitoringStore) -> list[tuple[Any, ...]]:
    return query_state(
        store,
        "SELECT comment_id, change, text_sha256 FROM watch_comment_texts "
        "ORDER BY comment_id, observed_at_utc",
    )


def test_store_v1_state_writable_call_upgrades_and_preserves_history(tmp_path: Path) -> None:
    store = store_at(tmp_path, ManualClock(1_000.0))
    create_v1_state(store)
    profile = parse_watch_profile(fixture_payload())

    claim = store.claim_run(WATCH_ID, lease_seconds=600)
    newly_tracked = store.record_discoveries(claim, [discovered(THREAD_ID)])
    inspected = store.inspect_watch(WATCH_ID)

    assert schema_identity(store) == (STATE_APPLICATION_ID, STATE_SCHEMA_VERSION)
    assert [revision["content_sha256"] for revision in inspected["revisions"]] == [
        profile.content_sha256()
    ]
    assert {run["run_id"]: run["status"] for run in inspected["recent_runs"]} == {
        V1_HISTORY_RUN_ID: "succeeded",
        claim.run_id: "running",
    }
    assert newly_tracked == 1
    assert inspected["sweep"]["tracked_threads"] == 1


def test_store_v1_state_read_only_access_returns_empty_sweep_state_without_modification(
    tmp_path: Path,
) -> None:
    store = store_at(tmp_path)
    create_v1_state(store)
    before = directory_snapshot(store.path.parent)

    watches = store.list_watches()
    inspected = store.inspect_watch(WATCH_ID)
    queue = store.refresh_queue(WATCH_ID, 10)
    pending = store.pending_analyses(WATCH_ID, 10)
    history = store.thread_history(WATCH_ID, THREAD_ID)
    counts = (store.tracked_thread_count(WATCH_ID), store.pending_analysis_count(WATCH_ID))
    status = store.sweep_status(WATCH_ID)

    assert directory_snapshot(store.path.parent) == before
    assert [watch["watch_id"] for watch in watches] == [WATCH_ID]
    assert inspected["run_count"] == 1
    assert inspected["sweep"] == EMPTY_SWEEP_STATUS
    assert status == EMPTY_SWEEP_STATUS
    assert (queue, pending, counts) == ([], [], (0, 0))
    assert (history.known_comments, history.current_revision) == ({}, None)
    assert history.prior_observed_at_utc is None


def test_list_watches_hot_journal_on_v1_state_recovers_without_upgrade(tmp_path: Path) -> None:
    store = store_at(tmp_path)
    create_v1_state(store)
    crash_mid_transaction(store, REVISION_UPDATE)

    watches = store.list_watches()

    assert [watch["watch_id"] for watch in watches] == [WATCH_ID]
    assert not journal_path(store).exists()
    assert schema_identity(store) == (STATE_APPLICATION_ID, 1)


@pytest.mark.parametrize(
    "operation",
    [
        lambda store: store.refresh_queue(WATCH_ID, 10),
        lambda store: store.sweep_status(WATCH_ID),
        lambda store: store.thread_history(WATCH_ID, THREAD_ID),
        lambda store: store.claim_run(WATCH_ID, lease_seconds=60),
    ],
    ids=["refresh_queue", "sweep_status", "thread_history", "claim"],
)
def test_store_version_3_state_is_refused_without_modification(
    tmp_path: Path, operation: Callable[[MonitoringStore], object]
) -> None:
    store = store_at(tmp_path)
    store.save_profile(parse_watch_profile(fixture_payload()))
    with closing(sqlite3.connect(store.path)) as conn:
        conn.execute("PRAGMA user_version = 3")
        conn.commit()
    before = directory_snapshot(store.path.parent)

    with pytest.raises(MonitoringStateError) as raised:
        operation(store)

    assert raised.value.details["reason"] == "unsupported_schema_version"
    assert raised.value.details["schema_version"] == 3
    assert directory_snapshot(store.path.parent) == before


@pytest.mark.parametrize(
    "operation",
    [
        lambda store, claim: store.record_discoveries(claim, [discovered("def456")]),
        lambda store, claim: store.record_acquisition(
            claim,
            acquisition_of(store, "stale", [FIRST_COMMENT]),
            revision_of(store, None, new_comment_changes("initial_observation", FIRST_COMMENT)),
        ),
        lambda store, claim: store.complete_sweep(claim, "changes", {"stale": True}),
    ],
    ids=["record_discoveries", "record_acquisition", "complete_sweep"],
)
def test_sweep_writes_stale_owner_after_recovery_are_refused(
    tmp_path: Path, operation: Callable[[MonitoringStore, RunClaim], object]
) -> None:
    clock = ManualClock(1_000.0)
    store = store_at(tmp_path, clock)
    store.save_profile(parse_watch_profile(fixture_payload()))
    stale = store.claim_run(WATCH_ID, lease_seconds=60)
    store.record_discoveries(stale, [discovered(THREAD_ID)])
    clock.now = 1_061.0
    current = store.claim_run(WATCH_ID, lease_seconds=60)
    before = store.sweep_status(WATCH_ID)

    with pytest.raises(RunClaimError) as raised:
        operation(store, stale)

    runs = {run["run_id"]: run["status"] for run in store.inspect_watch(WATCH_ID)["recent_runs"]}
    assert raised.value.details["reason"] == "stale_owner"
    assert raised.value.details["recovered_by_run_id"] == current.run_id
    assert store.sweep_status(WATCH_ID) == before
    assert store.thread_history(WATCH_ID, THREAD_ID).known_comments == {}
    assert runs == {stale.run_id: "abandoned", current.run_id: "running"}


def test_record_acquisition_expired_lease_without_recovery_is_refused(tmp_path: Path) -> None:
    store, clock, claim = tracked_store(tmp_path)
    acquisition = acquisition_of(store, "late", [FIRST_COMMENT])
    revision = revision_of(store, None, new_comment_changes("initial_observation", FIRST_COMMENT))
    clock.now = claim.lease_expires_at_utc

    with pytest.raises(RunClaimError) as raised:
        store.record_acquisition(claim, acquisition, revision)

    assert raised.value.details["reason"] == "lease_expired"
    assert store.sweep_status(WATCH_ID)["revision_count"] == 0


def remove_raw(store: MonitoringStore, acquisition: AcquisitionRecord) -> AcquisitionRecord:
    acquisition.raw_path.unlink()
    return acquisition


def tamper_normalized(store: MonitoringStore, acquisition: AcquisitionRecord) -> AcquisitionRecord:
    acquisition.normalized_path.write_bytes(b"tampered")
    return acquisition


def move_raw_outside(store: MonitoringStore, acquisition: AcquisitionRecord) -> AcquisitionRecord:
    outside = store.monitoring_root().parent / "outside.raw.json"
    outside.write_bytes(acquisition.raw_path.read_bytes())
    return dataclasses.replace(acquisition, raw_path=outside)


@pytest.mark.parametrize(
    ("corrupt", "reason"),
    [
        (remove_raw, "artifact_missing"),
        (tamper_normalized, "artifact_hash_mismatch"),
        (move_raw_outside, "artifact_outside_state"),
    ],
    ids=["missing", "hash_mismatch", "outside_root"],
)
def test_record_acquisition_unverifiable_artifact_is_refused_and_writes_nothing(
    tmp_path: Path,
    corrupt: Callable[[MonitoringStore, AcquisitionRecord], AcquisitionRecord],
    reason: str,
) -> None:
    store, _clock, claim = tracked_store(tmp_path)
    acquisition = corrupt(store, acquisition_of(store, "initial", [FIRST_COMMENT]))
    revision = revision_of(store, None, new_comment_changes("initial_observation", FIRST_COMMENT))

    with pytest.raises(MonitoringStateError) as raised:
        store.record_acquisition(claim, acquisition, revision)

    queue = store.refresh_queue(WATCH_ID, 10)
    assert raised.value.details["reason"] == reason
    assert query_state(store, "SELECT COUNT(*) FROM watch_acquisitions") == [(0,)]
    assert store.sweep_status(WATCH_ID)["revision_count"] == 0
    assert store.sweep_status(WATCH_ID)["comment_count"] == 0
    assert [thread.last_refreshed_at_utc for thread in queue] == [None]


def test_record_acquisition_absent_then_unchanged_comment_keeps_text_history(
    tmp_path: Path,
) -> None:
    store, clock, claim = tracked_store(tmp_path)
    initial = record_initial(store, claim, FIRST_COMMENT, SECOND_COMMENT)
    clock.now = 1_100.0
    absent = acquisition_of(store, "absent", [FIRST_COMMENT], absent=1)
    store.record_acquisition(claim, absent, None)
    clock.now = 1_200.0

    store.record_acquisition(
        claim, acquisition_of(store, "back", [FIRST_COMMENT, SECOND_COMMENT]), None
    )

    history = store.thread_history(WATCH_ID, THREAD_ID)
    assert history.known_comments["reddit:c2"].text_sha256 == SECOND_COMMENT[1]
    assert history.current_revision is not None
    assert history.current_revision.revision_id == initial.revision_id
    assert history.prior_observed_at_utc == 1_200.0
    assert comment_text_history(store) == [
        ("reddit:c1", "first_observed", FIRST_COMMENT[1]),
        ("reddit:c2", "first_observed", SECOND_COMMENT[1]),
    ]


def test_record_acquisition_edited_comment_appends_text_row_and_updates_hash(
    tmp_path: Path,
) -> None:
    store, clock, claim = tracked_store(tmp_path)
    initial = record_initial(store, claim, FIRST_COMMENT, SECOND_COMMENT)
    clock.now = 1_100.0
    edited_sha256 = text_sha256("first, edited")
    edit = CommentChange("reddit:c1", "edited", edited_sha256, FIRST_COMMENT[1], 600.0)
    revision = revision_of(store, initial.revision_id, (edit,))

    store.record_acquisition(
        claim,
        acquisition_of(store, "edit", [("reddit:c1", edited_sha256, 600.0), SECOND_COMMENT]),
        revision,
    )

    history = store.thread_history(WATCH_ID, THREAD_ID)
    edited_comment = history.known_comments["reddit:c1"]
    assert (edited_comment.text_sha256, edited_comment.novelty) == (
        edited_sha256,
        "initial_observation",
    )
    assert history.current_revision is not None
    assert (history.current_revision.revision_id, history.current_revision.sequence) == (
        revision.revision_id,
        2,
    )
    assert comment_text_history(store) == [
        ("reddit:c1", "first_observed", FIRST_COMMENT[1]),
        ("reddit:c1", "edited", edited_sha256),
        ("reddit:c2", "first_observed", SECOND_COMMENT[1]),
    ]


def new_comment_without_change(
    store: MonitoringStore, initial: RevisionRecord
) -> tuple[AcquisitionRecord, RevisionRecord | None]:
    observed = [FIRST_COMMENT, SECOND_COMMENT, LATER_COMMENT]
    return acquisition_of(store, "unexplained", observed), None


def change_for_unobserved_comment(
    store: MonitoringStore, initial: RevisionRecord
) -> tuple[AcquisitionRecord, RevisionRecord | None]:
    changes = new_comment_changes("new_source_event", LATER_COMMENT)
    acquisition = acquisition_of(store, "phantom", [FIRST_COMMENT, SECOND_COMMENT])
    return acquisition, revision_of(store, initial.revision_id, changes)


def edited_comment_without_change(
    store: MonitoringStore, initial: RevisionRecord
) -> tuple[AcquisitionRecord, RevisionRecord | None]:
    observed = [("reddit:c1", text_sha256("silently edited"), 600.0), SECOND_COMMENT]
    return acquisition_of(store, "silent-edit", observed), None


def unchanged_comment_with_change(
    store: MonitoringStore, initial: RevisionRecord
) -> tuple[AcquisitionRecord, RevisionRecord | None]:
    changes = (
        CommentChange("reddit:c1", "edited", FIRST_COMMENT[1], FIRST_COMMENT[1], 600.0),
        *new_comment_changes("new_source_event", LATER_COMMENT),
    )
    observed = [FIRST_COMMENT, SECOND_COMMENT, LATER_COMMENT]
    return acquisition_of(store, "spurious", observed), revision_of(
        store, initial.revision_id, changes
    )


@pytest.mark.parametrize(
    "build",
    [
        new_comment_without_change,
        change_for_unobserved_comment,
        edited_comment_without_change,
        unchanged_comment_with_change,
    ],
)
def test_record_acquisition_inconsistent_changes_are_rejected_and_write_nothing(
    tmp_path: Path,
    build: Callable[
        [MonitoringStore, RevisionRecord], tuple[AcquisitionRecord, RevisionRecord | None]
    ],
) -> None:
    store, clock, claim = tracked_store(tmp_path)
    initial = record_initial(store, claim, FIRST_COMMENT, SECOND_COMMENT)
    clock.now = 1_100.0
    acquisition, revision = build(store, initial)

    with pytest.raises(MonitoringStateError) as raised:
        store.record_acquisition(claim, acquisition, revision)

    history = store.thread_history(WATCH_ID, THREAD_ID)
    assert raised.value.details["reason"] == "inconsistent_changes"
    assert sorted(history.known_comments) == ["reddit:c1", "reddit:c2"]
    assert history.known_comments["reddit:c1"].text_sha256 == FIRST_COMMENT[1]
    assert history.prior_observed_at_utc == 1_000.0
    assert query_state(store, "SELECT COUNT(*) FROM watch_acquisitions") == [(1,)]


def test_record_acquisition_revision_not_extending_current_is_a_conflict(tmp_path: Path) -> None:
    store, clock, claim = tracked_store(tmp_path)
    initial = record_initial(store, claim, FIRST_COMMENT)
    clock.now = 1_100.0
    acquisition = acquisition_of(store, "fork", [FIRST_COMMENT, LATER_COMMENT])
    forked = revision_of(store, None, new_comment_changes("new_source_event", LATER_COMMENT))

    with pytest.raises(MonitoringStateError) as raised:
        store.record_acquisition(claim, acquisition, forked)

    history = store.thread_history(WATCH_ID, THREAD_ID)
    assert raised.value.details["reason"] == "revision_conflict"
    assert raised.value.details["current_revision_id"] == initial.revision_id
    assert sorted(history.known_comments) == ["reddit:c1"]


def test_record_acquisition_repeated_acquisition_id_is_refused(tmp_path: Path) -> None:
    store, clock, claim = tracked_store(tmp_path)
    record_initial(store, claim, FIRST_COMMENT)
    clock.now = 1_100.0
    acquisition = acquisition_of(store, "repeat", [FIRST_COMMENT])
    store.record_acquisition(claim, acquisition, None)

    with pytest.raises(MonitoringStateError) as raised:
        store.record_acquisition(claim, acquisition, None)

    assert raised.value.details["reason"] == "duplicate_acquisition"
    assert query_state(store, "SELECT COUNT(*) FROM watch_acquisitions") == [(2,)]


def test_record_acquisition_rejected_thread_is_unknown(tmp_path: Path) -> None:
    store, _clock, claim = tracked_store(tmp_path)
    store.record_discoveries(claim, [discovered("zzz999", selected=False, rank=2)])
    acquisition = dataclasses.replace(
        acquisition_of(store, "untracked", []), source_thread_id="zzz999"
    )

    with pytest.raises(MonitoringStateError) as raised:
        store.record_acquisition(claim, acquisition, revision_of(store, None, ()))

    assert raised.value.details["reason"] == "unknown_thread"
    assert store.thread_history(WATCH_ID, "zzz999").current_revision is None


def test_record_acquisition_observed_after_store_time_is_refused_and_writes_nothing(
    tmp_path: Path,
) -> None:
    store, _clock, claim = tracked_store(tmp_path)
    acquisition = acquisition_of(store, "future", [FIRST_COMMENT], observed_at=1_000.5)
    revision = revision_of(store, None, new_comment_changes("initial_observation", FIRST_COMMENT))

    with pytest.raises(MonitoringInputError) as raised:
        store.record_acquisition(claim, acquisition, revision)

    history = store.thread_history(WATCH_ID, THREAD_ID)
    assert raised.value.details["reason"] == "invalid_acquisition"
    assert history.known_comments == {}
    assert history.prior_observed_at_utc is None
    assert query_state(store, "SELECT COUNT(*) FROM watch_acquisitions") == [(0,)]


def test_record_acquisition_observed_before_latest_acquisition_is_refused(tmp_path: Path) -> None:
    store, clock, claim = tracked_store(tmp_path)
    record_initial(store, claim, FIRST_COMMENT)
    clock.now = 1_100.0
    acquisition = acquisition_of(store, "stale", [FIRST_COMMENT], observed_at=999.0)

    with pytest.raises(MonitoringInputError) as raised:
        store.record_acquisition(claim, acquisition, None)

    assert raised.value.details["reason"] == "invalid_acquisition"
    assert store.thread_history(WATCH_ID, THREAD_ID).prior_observed_at_utc == 1_000.0
    assert query_state(store, "SELECT COUNT(*) FROM watch_acquisitions") == [(1,)]


def test_thread_history_later_truncated_acquisition_keeps_complete_cutoff(tmp_path: Path) -> None:
    store, clock, claim = tracked_store(tmp_path)
    initial = record_initial(store, claim, FIRST_COMMENT)
    clock.now = 1_100.0
    truncated = acquisition_of(
        store,
        "truncated",
        [FIRST_COMMENT, LATER_COMMENT],
        observed_at=1_060.0,
        completeness="truncated",
    )
    changes = new_comment_changes("new_source_event", LATER_COMMENT)

    store.record_acquisition(claim, truncated, revision_of(store, initial.revision_id, changes))

    history = store.thread_history(WATCH_ID, THREAD_ID)
    assert history.prior_observed_at_utc == 1_000.0
    assert history.known_comments["reddit:c3"].first_observed_at_utc == 1_060.0


def test_thread_history_all_acquisitions_truncated_falls_back_to_first_acquisition(
    tmp_path: Path,
) -> None:
    store, clock, claim = tracked_store(tmp_path)
    revision = revision_of(store, None, new_comment_changes("initial_observation", FIRST_COMMENT))
    first = acquisition_of(store, "first-truncated", [FIRST_COMMENT], completeness="truncated")
    store.record_acquisition(claim, first, revision)
    clock.now = 1_100.0
    second = acquisition_of(store, "second-truncated", [FIRST_COMMENT], completeness="truncated")

    store.record_acquisition(claim, second, None)

    assert store.thread_history(WATCH_ID, THREAD_ID).prior_observed_at_utc == 1_000.0


@pytest.mark.parametrize("thread_id", ["ABC123", "abc_123", "", "a" * 17, "abc123\n"])
def test_record_discoveries_invalid_source_thread_id_is_refused_and_writes_nothing(
    tmp_path: Path, thread_id: str
) -> None:
    store, _clock, claim = tracked_store(tmp_path)
    before = store.sweep_status(WATCH_ID)

    with pytest.raises(MonitoringInputError) as raised:
        store.record_discoveries(claim, [discovered("def456"), discovered(thread_id, rank=2)])

    queue = store.refresh_queue(WATCH_ID, 10)
    assert raised.value.details["reason"] == "invalid_discovery"
    assert store.sweep_status(WATCH_ID) == before
    assert [thread.source_thread_id for thread in queue] == [THREAD_ID]
    assert query_state(store, "SELECT COUNT(*) FROM watch_discoveries") == [(1,)]


def test_refresh_queue_orders_by_attempt_then_discovery_and_applies_limit(tmp_path: Path) -> None:
    clock = ManualClock(1_000.0)
    store = store_at(tmp_path, clock)
    store.save_profile(parse_watch_profile(fixture_payload()))
    claim = store.claim_run(WATCH_ID, lease_seconds=3_600)
    first_tracked = store.record_discoveries(claim, [discovered("zzz111")])
    clock.now = 1_005.0
    later_tracked = store.record_discoveries(
        claim, [discovered("aaa111"), discovered("rej111", selected=False, rank=2)]
    )
    before = [thread.source_thread_id for thread in store.refresh_queue(WATCH_ID, 10)]
    clock.now = 1_010.0

    store.record_failure(claim, stage="fetch", target="zzz111", error={"code": "fetch_failed"})

    after = store.refresh_queue(WATCH_ID, 10)
    limited = store.refresh_queue(WATCH_ID, 1)
    assert (first_tracked, later_tracked) == (1, 1)
    assert before == ["zzz111", "aaa111"]
    assert [thread.source_thread_id for thread in after] == ["aaa111", "zzz111"]
    assert [thread.last_refreshed_at_utc for thread in after] == [None, None]
    assert [thread.source_thread_id for thread in limited] == ["aaa111"]
    assert store.tracked_thread_count(WATCH_ID) == 2


def test_record_discoveries_selected_thread_stays_tracked_when_later_rejected(
    tmp_path: Path,
) -> None:
    store, clock, claim = tracked_store(tmp_path)
    store.complete_sweep(claim, "no_changes", {})
    clock.now = 1_100.0
    later = store.claim_run(WATCH_ID, lease_seconds=3_600)

    newly_tracked = store.record_discoveries(
        later, [discovered(THREAD_ID, selected=False, title="renamed")]
    )

    queue = store.refresh_queue(WATCH_ID, 10)
    assert newly_tracked == 0
    assert [(thread.source_thread_id, thread.title) for thread in queue] == [(THREAD_ID, "renamed")]


def test_pending_analyses_superseded_revision_is_skipped(tmp_path: Path) -> None:
    store, clock, claim = tracked_store(tmp_path)
    initial = record_initial(store, claim, FIRST_COMMENT)
    clock.now = 1_100.0
    latest = revision_of(
        store, initial.revision_id, new_comment_changes("new_source_event", LATER_COMMENT)
    )
    store.record_acquisition(
        claim, acquisition_of(store, "later", [FIRST_COMMENT, LATER_COMMENT]), latest
    )

    pending = store.pending_analyses(WATCH_ID, 10)

    assert [(ref.revision_id, ref.sequence) for ref in pending] == [(latest.revision_id, 2)]
    assert pending[0].snapshot_path == latest.snapshot_path
    assert pending[0].snapshot_path.is_absolute()
    assert store.pending_analysis_count(WATCH_ID) == 1
    assert store.sweep_status(WATCH_ID)["revision_count"] == 2


@pytest.mark.parametrize(
    ("outcome", "run_status"),
    [
        ("changes", "succeeded"),
        ("no_changes", "succeeded"),
        ("partial", "failed"),
        ("failed", "failed"),
    ],
)
def test_complete_sweep_outcome_sets_run_status(
    tmp_path: Path, outcome: Any, run_status: str
) -> None:
    store, clock, claim = tracked_store(tmp_path)
    clock.now = 1_100.0
    summary = {"refresh": {"attempted": 0}, "outcome_label": outcome}

    store.complete_sweep(claim, outcome, summary)

    inspected = store.inspect_watch(WATCH_ID)
    run = inspected["recent_runs"][0]
    assert (run["run_id"], run["status"], run["finished_at_utc"]) == (
        claim.run_id,
        run_status,
        1_100.0,
    )
    assert inspected["active_run"] is None
    assert inspected["sweep"]["latest_sweep"] == {
        "run_id": claim.run_id,
        "outcome": outcome,
        "finished_at_utc": 1_100.0,
        "summary": summary,
    }


def test_complete_sweep_unknown_outcome_is_refused_and_run_stays_active(tmp_path: Path) -> None:
    store, _clock, claim = tracked_store(tmp_path)
    outcome: Any = "done"

    with pytest.raises(MonitoringInputError) as raised:
        store.complete_sweep(claim, outcome, {})

    assert raised.value.details["outcome"] == "done"
    assert store.inspect_watch(WATCH_ID)["active_run"]["run_id"] == claim.run_id
    assert store.sweep_status(WATCH_ID)["latest_sweep"] is None


def test_record_analysis_existing_record_is_left_untouched(tmp_path: Path) -> None:
    store, clock, claim = tracked_store(tmp_path)
    initial = record_initial(store, claim, FIRST_COMMENT)
    analyses = f"watches/{WATCH_ID}/threads/{THREAD_ID}/analyses/{initial.revision_id}"
    first_path, first_sha256 = write_artifact(store, f"{analyses}/first.json", b"first analysis")
    store.record_analysis(claim, initial.revision_id, first_path, first_sha256)
    clock.now = 1_100.0
    retry_path, retry_sha256 = write_artifact(store, f"{analyses}/retry.json", b"retry analysis")

    store.record_analysis(claim, initial.revision_id, retry_path, retry_sha256)

    rows = query_state(
        store,
        "SELECT analysis_path, analysis_sha256, analyzed_at_utc FROM watch_revision_analyses",
    )
    assert rows == [(f"{analyses}/first.json", first_sha256, 1_000.0)]
    assert store.pending_analyses(WATCH_ID, 10) == []


def test_record_analysis_unknown_revision_is_refused(tmp_path: Path) -> None:
    store, _clock, claim = tracked_store(tmp_path)
    path, sha256 = write_artifact(store, "watches/agent-tooling/orphan.json", b"analysis")

    with pytest.raises(MonitoringStateError) as raised:
        store.record_analysis(claim, "rev-missing", path, sha256)

    assert raised.value.details["reason"] == "unknown_revision"
    assert query_state(store, "SELECT COUNT(*) FROM watch_revision_analyses") == [(0,)]
