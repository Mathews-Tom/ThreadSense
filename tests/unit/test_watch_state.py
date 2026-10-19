from __future__ import annotations

import sqlite3
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
from threadsense.monitoring.state import STATE_SCHEMA_VERSION

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
