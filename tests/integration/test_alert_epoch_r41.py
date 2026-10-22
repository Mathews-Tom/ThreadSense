from __future__ import annotations

import hashlib
import os
import shutil
import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any

import pytest
from test_alert_resources import config_file, fresh_alerts
from test_watch_alerts import artifact_bytes, read_report, sweep
from test_watch_incremental import WATCH_ID, create_watch, isolate_environment, run_cli

from threadsense.errors import MonitoringStateError
from threadsense.monitoring import MonitoringStore, alert_artifacts, alert_sources
from threadsense.monitoring.alert_budget import AlertBudget, AlertSession
from threadsense.monitoring.alert_records import canonical_json
from threadsense.monitoring.alert_source_schema import SOURCE_SPECS


@pytest.fixture
def watch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    isolate_environment(monkeypatch, tmp_path)
    return create_watch()


def session_for(connection: sqlite3.Connection, root: Path) -> AlertSession:
    @contextmanager
    def fence() -> Iterator[None]:
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            yield

    return AlertSession(connection, root, WATCH_ID, fence, lambda: None)


def chronology(connection: sqlite3.Connection) -> tuple[Any, ...]:
    return tuple(
        tuple(connection.execute(f"SELECT * FROM {table} ORDER BY rowid"))
        for table in (
            "alert_source_epoch",
            "alert_source_bootstrap",
            "alert_legacy_reports",
            "alert_legacy_positions",
            "alert_legacy_heads",
            "alert_source_heads",
        )
    )


def mutate_cookie(connection: sqlite3.Connection) -> None:
    cookie = connection.execute("PRAGMA schema_version").fetchone()[0]
    connection.execute(f"PRAGMA schema_version={cookie + 1}")


def insert_raw_report(connection: sqlite3.Connection, report_id: str = "legacy-capture") -> None:
    connection.execute(
        "INSERT INTO watch_alert_reports VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            report_id,
            WATCH_ID,
            "sweep",
            "cohort",
            "window",
            "window",
            "baseline",
            "{}",
            hashlib.sha256(b"{}").hexdigest(),
            "legacy.json",
            "legacy.md",
            "run",
            1.0,
        ),
    )
    connection.commit()


def insert_pending(watch: Path, completed: dict[str, Any], report_id: str) -> dict[str, Any]:
    payload = {
        **read_report(completed),
        "schema_version": 1,
        "report_id": report_id,
        "window_id": report_id + "-window",
        "baseline_window_id": report_id + "-window",
    }
    text = canonical_json(payload)
    relative = f"{WATCH_ID}/alerts/{report_id}"
    with closing(sqlite3.connect(watch)) as connection:
        sweep_id, when = connection.execute(
            "SELECT run_id,finished_at_utc FROM watch_sweeps WHERE watch_id=? AND run_id=?",
            (WATCH_ID, payload["window"]["sweep_run_id"]),
        ).fetchone()
        connection.execute(
            "INSERT OR IGNORE INTO watch_alert_cohorts VALUES (?,?,?,?)",
            (payload["cohort_id"], WATCH_ID, "{}", when),
        )
        connection.execute(
            "INSERT INTO watch_alert_reports VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                report_id,
                WATCH_ID,
                sweep_id,
                payload["cohort_id"],
                payload["window_id"],
                payload["baseline_window_id"],
                payload["status"],
                text,
                hashlib.sha256(text.encode()).hexdigest(),
                relative + ".json",
                relative + ".md",
                sweep_id,
                when,
            ),
        )
        connection.commit()
    return payload


@pytest.mark.parametrize("phase", ["capture", "positioning"])
def test_unfinished_chronology_refuses_new_epoch_without_advancing(watch: Path, phase: str) -> None:
    with closing(sqlite3.connect(watch)) as connection:
        insert_raw_report(connection)
        session = session_for(connection, watch.parent)
        budget = AlertBudget(check=session.check)
        epoch = alert_sources._ensure_epoch(session, budget)
        if phase == "capture":
            alert_sources._freeze_family(session, budget, SOURCE_SPECS[0], epoch)
        else:
            for spec in SOURCE_SPECS[:2]:
                while not alert_sources._bootstrap_one(session, budget, spec, epoch):
                    pass
            assert connection.execute("SELECT position FROM alert_legacy_reports").fetchone() == (
                None,
            )
        before = chronology(connection)
        mutate_cookie(connection)
        connection.commit()
        resumed = session_for(connection, watch.parent)
        with pytest.raises(MonitoringStateError) as refused:
            alert_sources.bootstrap_legacy(resumed, AlertBudget(check=resumed.check))
        assert refused.value.details["reason"] == "source_epoch_changed"
        assert chronology(connection) == before


def test_seal_does_not_rebind_current_operation_and_fence_exit_rolls_back(watch: Path) -> None:
    with closing(sqlite3.connect(watch)) as connection:
        session = session_for(connection, watch.parent)
        budget = AlertBudget(check=session.check)
        alert_sources.bootstrap_legacy(session, budget)
        expected = session.expected_epoch
        audit = connection.execute("SELECT * FROM alert_source_epoch").fetchone()
        with pytest.raises(MonitoringStateError) as refused:
            with session.fence():
                connection.execute("CREATE TABLE r41_uncommitted(value INTEGER)")
        assert refused.value.details["reason"] == "source_epoch_changed"
        assert (
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE name='r41_uncommitted'"
            ).fetchone()
            is None
        )
        mutate_cookie(connection)
        connection.commit()
        with pytest.raises(MonitoringStateError):
            alert_sources.bootstrap_legacy(session, budget)
        assert session.expected_epoch == expected
        assert connection.execute("SELECT * FROM alert_source_epoch").fetchone() == audit


def test_seal_rechecks_concurrent_unpositioned_work(
    watch: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = alert_sources._order_legacy
    inserted = False

    def raced(session: AlertSession, budget: AlertBudget, epoch: tuple[int, int, int]) -> None:
        nonlocal inserted
        original(session, budget, epoch)
        if not inserted:
            inserted = True
            insert_raw_report(session.conn, "racing-seal")

    monkeypatch.setattr(alert_sources, "_order_legacy", raced)
    with closing(sqlite3.connect(watch)) as connection:
        session = session_for(connection, watch.parent)
        alert_sources.bootstrap_legacy(session, AlertBudget(check=session.check))
        assert inserted
        assert (
            connection.execute(
                "SELECT position FROM alert_legacy_reports WHERE report_id='racing-seal'"
            ).fetchone()[0]
            is not None
        )
        assert connection.execute("SELECT legacy_complete FROM alert_source_epoch").fetchone() == (
            1,
        )


def test_completed_copied_root_actual_cli_preserves_audit_and_historical_bytes(
    watch: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    completed = sweep(monkeypatch, "initial")
    historical = artifact_bytes(completed)
    insert_pending(watch, completed, "copied-legacy")
    code, recovered = run_cli("watch", "alerts", "--id", WATCH_ID)
    assert code == 0 and recovered["report_id"] == "copied-legacy", recovered
    with closing(sqlite3.connect(watch)) as connection:
        audit = connection.execute("SELECT * FROM alert_source_epoch").fetchone()
        positions = tuple(
            connection.execute("SELECT * FROM alert_legacy_positions ORDER BY position")
        )
        assert audit[-2:] == (1, 1)
    storage = Path(os.environ["THREADSENSE_STORAGE_ROOT"])
    frozen = {
        path.relative_to(storage): path.read_bytes()
        for path in storage.rglob("*")
        if path.is_file() and path.suffix in {".json", ".md"}
    }
    copied = tmp_path / "copied-storage"
    shutil.copytree(storage, copied)
    copied_watch = copied / watch.relative_to(storage)
    assert copied_watch.stat().st_ino != watch.stat().st_ino
    with closing(sqlite3.connect(copied_watch)) as connection:
        # Ongoing identity must not depend on the old physical report rowid.
        guard = connection.execute(
            "SELECT sql FROM sqlite_master WHERE name='watch_alert_reports_no_update'"
        ).fetchone()[0]
        connection.execute("DROP TRIGGER watch_alert_reports_no_update")
        connection.execute("UPDATE watch_alert_reports SET rowid=rowid+1000")
        connection.execute(guard)
        connection.commit()
    monkeypatch.setenv("THREADSENSE_STORAGE_ROOT", str(copied))
    code, result = fresh_alerts(config_file(tmp_path))
    assert code == 0 and result["report_id"] == "copied-legacy", result
    with closing(sqlite3.connect(copied_watch)) as connection:
        assert connection.execute("SELECT * FROM alert_source_epoch").fetchone() == audit
        assert (
            tuple(connection.execute("SELECT * FROM alert_legacy_positions ORDER BY position"))
            == positions
        )
    assert all((copied / relative).read_bytes() == data for relative, data in frozen.items())
    assert artifact_bytes(completed) == historical


def test_postseal_pending_schema1_is_atomically_positioned_and_recovers(
    watch: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from threadsense.monitoring.alerts import render_alert_json, render_alert_markdown

    completed = sweep(monkeypatch, "initial")
    historical = artifact_bytes(completed)
    subsequent = sweep(monkeypatch, "unchanged")
    first = insert_pending(watch, completed, "late-first")
    second = insert_pending(watch, subsequent, "late-second")
    with closing(sqlite3.connect(watch)) as connection:
        rows = connection.execute(
            "SELECT report_id,position,predecessor_report_id "
            "FROM alert_legacy_reports ORDER BY position"
        ).fetchall()
        assert [row[0] for row in rows] == ["late-first", "late-second"]
        assert rows[0][1] < rows[1][1] and rows[1][2] == "late-first"
        assert connection.execute(
            "SELECT report_id,position FROM alert_legacy_heads WHERE watch_id=?", (WATCH_ID,)
        ).fetchone() == ("late-second", rows[1][1])
    tiny = config_file(tmp_path, input_bytes=32)
    code, refused = run_cli("watch", "alerts", "--id", WATCH_ID, "--config", str(tiny))
    assert code == 0 and refused["status"] == "resource_refused", refused
    for payload in (first, second):
        code, result = run_cli("watch", "alerts", "--id", WATCH_ID)
        assert code == 0 and result["report_id"] == payload["report_id"], result
        assert Path(result["json_path"]).read_bytes() == render_alert_json(payload)
        assert Path(result["markdown_path"]).read_bytes() == render_alert_markdown(payload)
    assert artifact_bytes(completed) == historical


@pytest.mark.parametrize("boundary", ["publication", "completion"])
def test_epoch_mutation_after_seal_refuses_publication_or_completion(
    watch: Path, monkeypatch: pytest.MonkeyPatch, boundary: str
) -> None:
    completed = sweep(monkeypatch, "initial")
    insert_pending(watch, completed, "epoch-boundary")
    reached = False
    if boundary == "publication":
        original = alert_artifacts._publish

        def publish(session: AlertSession, *args: Any, **kwargs: Any) -> None:
            nonlocal reached
            reached = True
            mutate_cookie(session.conn)
            return original(session, *args, **kwargs)

        monkeypatch.setattr(alert_artifacts, "_publish", publish)
    else:
        original_completion = MonitoringStore.complete_alert_report

        def complete(
            self: MonitoringStore, *args: Any, session: AlertSession, **kwargs: Any
        ) -> None:
            nonlocal reached
            reached = True
            mutate_cookie(session.conn)
            return original_completion(self, *args, session=session, **kwargs)

        monkeypatch.setattr(MonitoringStore, "complete_alert_report", complete)
    code, refused = run_cli("watch", "alerts", "--id", WATCH_ID)
    assert reached and code != 0, refused
    assert refused["error"]["details"]["reason"] == "source_epoch_changed"
    with closing(sqlite3.connect(watch)) as connection:
        assert (
            connection.execute(
                "SELECT 1 FROM watch_alert_completions WHERE report_id='epoch-boundary'"
            ).fetchone()
            is None
        )
        assert connection.execute(
            "SELECT completed FROM alert_legacy_reports WHERE report_id='epoch-boundary'"
        ).fetchone() == (0,)


@pytest.mark.parametrize(
    "corruption",
    [
        "report",
        "position",
        "aliased_position",
        "predecessor",
        "predecessor_report",
        "immutable_position",
        "unpositioned",
    ],
)
def test_accessed_postseal_legacy_corruption_refuses(
    watch: Path, monkeypatch: pytest.MonkeyPatch, corruption: str
) -> None:
    completed = sweep(monkeypatch, "initial")
    subsequent = sweep(monkeypatch, "unchanged")
    insert_pending(watch, completed, "accessed-first")
    insert_pending(watch, subsequent, "accessed-second")
    code, first = run_cli("watch", "alerts", "--id", WATCH_ID)
    assert code == 0 and first["report_id"] == "accessed-first", first
    with closing(sqlite3.connect(watch)) as connection:
        guard = connection.execute(
            "SELECT sql FROM sqlite_master WHERE name='watch_alert_reports_no_update'"
        ).fetchone()[0]
        connection.execute("DROP TRIGGER watch_alert_reports_no_update")
        if corruption == "report":
            connection.execute(
                "UPDATE watch_alert_reports SET payload_json='{}' WHERE report_id='accessed-second'"
            )
        elif corruption == "position":
            connection.execute(
                "UPDATE alert_legacy_reports SET position=position+100 "
                "WHERE report_id='accessed-second'"
            )
        elif corruption == "aliased_position":
            connection.execute(
                "UPDATE alert_legacy_reports SET position=("
                "SELECT position FROM alert_legacy_reports WHERE report_id='accessed-first') "
                "WHERE report_id='accessed-second'"
            )
        elif corruption == "predecessor":
            connection.execute(
                "UPDATE alert_legacy_reports SET predecessor_report_id=NULL "
                "WHERE report_id='accessed-second'"
            )
        elif corruption == "predecessor_report":
            connection.execute(
                "UPDATE watch_alert_reports SET watch_id='wrong-watch' "
                "WHERE report_id='accessed-first'"
            )
        elif corruption == "immutable_position":
            connection.execute("DROP TRIGGER alert_legacy_positions_no_update")
            connection.execute(
                "UPDATE alert_legacy_positions SET predecessor_report_id=NULL "
                "WHERE report_id='accessed-second'"
            )
        else:
            connection.execute(
                "UPDATE alert_legacy_reports SET position=NULL WHERE report_id='accessed-second'"
            )
        connection.execute(guard)
        connection.commit()
    code, refused = run_cli("watch", "alerts", "--id", WATCH_ID)
    assert code != 0 and refused["error"]["code"] == "monitoring_state_error", refused
    with closing(sqlite3.connect(watch)) as connection:
        assert (
            connection.execute(
                "SELECT 1 FROM watch_alert_completions WHERE report_id='accessed-second'"
            ).fetchone()
            is None
        )


def test_postseal_audit_ordinal_exhaustion_rolls_back_source_insert(
    watch: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    completed = sweep(monkeypatch, "initial")
    subsequent = sweep(monkeypatch, "unchanged")
    insert_pending(watch, completed, "ordinal-minimum")
    with closing(sqlite3.connect(watch)) as connection:
        connection.execute(
            "UPDATE alert_legacy_reports SET observed_ordinal=-9223372036854775808 "
            "WHERE report_id='ordinal-minimum'"
        )
        connection.commit()
        before = chronology(connection)
    with pytest.raises(sqlite3.IntegrityError, match="audit ordinal exhausted"):
        insert_pending(watch, subsequent, "ordinal-overflow")
    with closing(sqlite3.connect(watch)) as connection:
        assert chronology(connection) == before
        assert (
            connection.execute(
                "SELECT 1 FROM watch_alert_reports WHERE report_id='ordinal-overflow'"
            ).fetchone()
            is None
        )


def test_completed_bootstrap_shortcuts_do_not_reopen_capture_or_positioning(
    watch: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with closing(sqlite3.connect(watch)) as connection:
        initial = session_for(connection, watch.parent)
        alert_sources.bootstrap(initial, AlertBudget(check=initial.check))
        assert initial.expected_epoch == (
            watch.stat().st_dev,
            watch.stat().st_ino,
            connection.execute("PRAGMA schema_version").fetchone()[0],
        )
        assert connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name='alert_legacy_watch_position'"
        ).fetchone() == (1,)

        def forbidden(*args: Any, **kwargs: Any) -> None:
            raise AssertionError("sealed bootstrap reopened historical work")

        monkeypatch.setattr(alert_sources, "_bootstrap_one", forbidden)
        monkeypatch.setattr(alert_sources, "_order_legacy", forbidden)
        resumed = session_for(connection, watch.parent)
        assert alert_sources.bootstrap(resumed, AlertBudget(check=resumed.check))
