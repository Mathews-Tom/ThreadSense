"""Bounded, versioned alert inputs and immutable-primary-key legacy capture.

The only historical-table ordering is an existing PRIMARY KEY index. An old
rowid is used solely as a native TEXT handle and a captured legacy ordinal;
all ongoing positions belong to the new, explicit source/position tables.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from threadsense.errors import MonitoringStateError
from threadsense.monitoring.alert_budget import AlertBudget, AlertSession, text_size
from threadsense.monitoring.alert_source_schema import (
    ORDER_COLUMN_BY_KIND,
    SOURCE_SCHEMA_STATEMENTS,
    SOURCE_SPECS,
    SPEC_BY_KIND,
    SourceSpec,
)

__all__ = [
    "SOURCE_SCHEMA_STATEMENTS",
    "SourceChange",
    "bootstrap",
    "bootstrap_legacy",
    "get_version",
    "latest_change",
    "legacy_report",
    "next_change",
    "source_highwater",
]


@dataclass(frozen=True)
class SourceChange:
    sequence: int
    kind: str
    key: str
    watch_id: str | None
    data: dict[str, Any]


def _json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def _refuse(reason: str, **details: Any) -> MonitoringStateError:
    return MonitoringStateError(
        "alert source capture cannot safely continue", details={"reason": reason, **details}
    )


def _epoch(session: AlertSession, budget: AlertBudget) -> tuple[int, int, int]:
    session.check()
    budget.admit("source epoch metadata", input_bytes=256)
    rows = session.conn.execute("PRAGMA database_list").fetchall()
    main = next((row for row in rows if row[1] == "main"), None)
    if main is None or not main[2]:
        raise _refuse("source_epoch_requires_file")
    try:
        stat = Path(main[2]).stat()
    except OSError as error:
        raise _refuse("source_epoch_unreadable") from error
    cookie = int(session.conn.execute("PRAGMA main.schema_version").fetchone()[0])
    return stat.st_dev, stat.st_ino, cookie


def _validate_epoch(
    session: AlertSession,
    budget: AlertBudget,
    expected: tuple[int, int, int],
) -> None:
    if _epoch(session, budget) != expected:
        raise _refuse("source_epoch_changed")


def _ensure_epoch(session: AlertSession, budget: AlertBudget) -> tuple[int, int, int]:
    observed = _epoch(session, budget)
    row = session.conn.execute(
        "SELECT device,inode,schema_cookie FROM alert_source_epoch WHERE singleton=1"
    ).fetchone()
    if row is None:
        with session.fence():
            _validate_epoch(session, budget, observed)
            session.conn.execute(
                "INSERT OR IGNORE INTO alert_source_epoch(singleton,device,inode,schema_cookie) "
                "VALUES(1,?,?,?)",
                observed,
            )
            row = session.conn.execute(
                "SELECT device,inode,schema_cookie FROM alert_source_epoch WHERE singleton=1"
            ).fetchone()
    if row is None or tuple(row) != observed:
        raise _refuse("source_epoch_changed")
    return observed


def _read_columns(
    session: AlertSession,
    budget: AlertBudget,
    table: str,
    rowid: int,
    columns: tuple[str, ...],
    unit: str,
    *,
    encode: bool = False,
    decode: bool = False,
) -> dict[str, Any]:
    """Preflight every TEXT using native blob length, before SELECT materializes it."""
    session.check()
    budget.admit(unit + " scalar metadata", input_bytes=32 * len(columns))
    types = session.conn.execute(
        "SELECT " + ",".join(f'typeof("{c}")' for c in columns) + f' FROM "{table}" WHERE rowid=?',
        (rowid,),
    ).fetchone()
    if types is None:
        raise _refuse("source_disappeared", kind=table)
    size = 0
    for column, storage in zip(columns, types, strict=True):
        if storage == "text":
            try:
                size += text_size(session.conn, table, column, rowid)
            except sqlite3.Error as error:
                raise _refuse("source_text_unreadable", kind=table, column=column) from error
        elif storage in ("integer", "real", "null"):
            size += 32
        else:
            raise _refuse("source_storage_type", kind=table, column=column)
    # Reserve all known application passes before selecting any TEXT.
    native_bound = size + sum(len(column) + 4 for column in columns) + 64
    passes = 15 if encode else 3 if decode else 1
    budget.admit(unit + " admitted passes", input_bytes=native_bound * passes)
    row = session.conn.execute(
        "SELECT " + ",".join(f'"{c}"' for c in columns) + f' FROM "{table}" WHERE rowid=?',
        (rowid,),
    ).fetchone()
    if row is None:
        raise _refuse("source_disappeared", kind=table)
    return dict(zip(columns, row, strict=True))


def _encode(value: Any) -> str:
    # The owning native column read reserved serialization/binding in advance.
    try:
        return _json(value)
    except (ValueError, TypeError) as error:
        raise _refuse("source_not_json") from error


def _anchor(
    session: AlertSession,
    budget: AlertBudget,
    report_id: str | None,
    ordinal: int | None,
) -> None:
    if report_id is None:
        return
    budget.admit("legacy predecessor identity", source_rows=1, input_bytes=64)
    row = session.conn.execute(
        "SELECT rowid FROM watch_alert_reports WHERE report_id=?",
        (report_id,),
    ).fetchone()
    if row is None or row[0] != ordinal:
        raise _refuse("legacy_order_changed", report_id=report_id)


def _state(
    session: AlertSession,
    budget: AlertBudget,
    spec: SourceSpec,
) -> dict[str, Any] | None:
    row = session.conn.execute(
        "SELECT rowid FROM alert_source_bootstrap WHERE kind=?",
        (spec.name,),
    ).fetchone()
    if row is None:
        return None
    budget.admit("bootstrap cursor " + spec.name, input_bytes=64)
    return _read_columns(
        session,
        budget,
        "alert_source_bootstrap",
        int(row[0]),
        ("upper_json", "cursor_json", "complete", "anchor_report", "anchor_ordinal"),
        "bootstrap cursor " + spec.name,
        decode=True,
    )


def _decode_key(text: str | None) -> tuple[Any, ...] | None:
    if text is None:
        return None
    try:
        value = json.loads(text)
    except (ValueError, TypeError) as error:
        raise _refuse("source_cursor_invalid") from error
    if not isinstance(value, list):
        raise _refuse("source_cursor_invalid")
    return tuple(value)


def _freeze_family(
    session: AlertSession,
    budget: AlertBudget,
    spec: SourceSpec,
    epoch: tuple[int, int, int],
) -> None:
    budget.admit("bootstrap upper metadata " + spec.name, input_bytes=64)
    ordering = ",".join(f'"{column}" DESC' for column in spec.primary_key)
    _validate_epoch(session, budget, epoch)
    with session.fence():
        _validate_epoch(session, budget, epoch)
        row = session.conn.execute(
            f'SELECT rowid FROM "{spec.name}" ORDER BY {ordering} LIMIT 1'
        ).fetchone()
        upper = None
        if row is not None:
            budget.admit("bootstrap upper candidate " + spec.name, source_rows=1)
            values = _read_columns(
                session,
                budget,
                spec.name,
                int(row[0]),
                spec.primary_key,
                "bootstrap upper " + spec.name,
                encode=True,
            )
            upper = _encode(list(values.values()))
        _validate_epoch(session, budget, epoch)
        session.conn.execute(
            "INSERT OR IGNORE INTO alert_source_bootstrap(kind,upper_json,cursor_json,complete) "
            "VALUES(?,?,NULL,?)",
            (spec.name, upper, int(upper is None)),
        )


def _bootstrap_one(
    session: AlertSession,
    budget: AlertBudget,
    spec: SourceSpec,
    epoch: tuple[int, int, int],
) -> bool:
    state = _state(session, budget, spec)
    if state is None:
        _freeze_family(session, budget, spec, epoch)
        return False
    if state["complete"]:
        return True
    upper = _decode_key(state["upper_json"])
    cursor = _decode_key(state["cursor_json"])
    if upper is None:
        raise _refuse("source_cursor_invalid", kind=spec.name)
    columns = ",".join(f'"{column}"' for column in spec.primary_key)
    expression = f"({columns})" if len(spec.primary_key) > 1 else columns
    placeholders = "(" + ",".join("?" for _ in spec.primary_key) + ")"
    if len(spec.primary_key) == 1:
        placeholders = "?"
    where = f"{expression}<={placeholders}"
    parameters = upper
    if cursor is not None:
        where += f" AND {expression}>{placeholders}"
        parameters += cursor
    budget.admit("bootstrap candidate metadata " + spec.name, input_bytes=64)
    _validate_epoch(session, budget, epoch)
    _anchor(session, budget, state["anchor_report"], state["anchor_ordinal"])
    # Short fence prevents a concurrent UPDATE from changing raw columns between
    # native TEXT preflight, admitted read and capture. No file/artifact I/O.
    with session.fence():
        _validate_epoch(session, budget, epoch)
        _anchor(session, budget, state["anchor_report"], state["anchor_ordinal"])
        row = session.conn.execute(
            f'SELECT rowid FROM "{spec.name}" WHERE {where} ORDER BY {columns} LIMIT 1',
            parameters,
        ).fetchone()
        if row is None:
            session.conn.execute(
                "UPDATE alert_source_bootstrap SET complete=1 WHERE kind=?",
                (spec.name,),
            )
            _validate_epoch(session, budget, epoch)
            return True
        rowid = int(row[0])
        budget.admit("bootstrap candidate " + spec.name, source_rows=1)
        key_data = _read_columns(
            session,
            budget,
            spec.name,
            rowid,
            spec.primary_key,
            "bootstrap primary key " + spec.name,
            encode=True,
        )
        key = _encode(list(key_data.values()))
        captured = session.conn.execute(
            "SELECT sequence FROM alert_source_heads WHERE kind=? AND source_key=?",
            (spec.name, key),
        ).fetchone()
        if captured is None:
            data = _read_columns(
                session,
                budget,
                spec.name,
                rowid,
                spec.columns,
                "bootstrap raw columns " + spec.name,
                encode=True,
            )
            if spec.name == "watch_alert_reports":
                data["_legacy_ordinal"] = rowid
            payload = _encode(data)
            watch = data.get("watch_id") if spec.watch_column else None
            source_order = data.get(ORDER_COLUMN_BY_KIND.get(spec.name, ""), 0)
            result = session.conn.execute(
                "INSERT INTO alert_source_versions(kind,source_key,semantic_version,watch_id,source_order,data_json) "
                "VALUES(?,?,1,?,?,?)",
                (spec.name, key, watch, source_order, payload),
            )
            session.conn.execute(
                "INSERT INTO alert_source_heads(kind,source_key,semantic_version,sequence) "
                "VALUES(?,?,1,?)",
                (spec.name, key, result.lastrowid),
            )
        if spec.name == "watch_alert_reports":
            # Obtain watch from the immutable key lookup even when a trigger won
            # the deduplication race; this is not an old-table history query.
            watch_data = _read_columns(
                session, budget, spec.name, rowid, ("watch_id",), "legacy report watch"
            )
            completed = (
                session.conn.execute(
                    "SELECT 1 FROM watch_alert_completions WHERE report_id=?",
                    (key_data["report_id"],),
                ).fetchone()
                is not None
            )
            session.conn.execute(
                "INSERT OR IGNORE INTO alert_legacy_reports(report_id,watch_id,observed_ordinal,completed) "
                "VALUES(?,?,?,?)",
                (key_data["report_id"], watch_data["watch_id"], rowid, int(completed)),
            )
            _anchor(session, budget, str(key_data["report_id"]), rowid)
        if spec.name == "watch_alert_completions":
            session.conn.execute(
                "UPDATE alert_legacy_reports SET completed=1 WHERE report_id=?",
                (key_data["report_id"],),
            )
        _validate_epoch(session, budget, epoch)
        session.conn.execute(
            "UPDATE alert_source_bootstrap SET cursor_json=?,anchor_report=?,anchor_ordinal=? "
            "WHERE kind=? AND cursor_json IS ?",
            (
                key,
                key_data["report_id"] if spec.name == "watch_alert_reports" else None,
                rowid if spec.name == "watch_alert_reports" else None,
                spec.name,
                state["cursor_json"],
            ),
        )
    return False


def _order_legacy_one(
    session: AlertSession,
    budget: AlertBudget,
    epoch: tuple[int, int, int],
) -> bool:
    budget.admit("legacy explicit position metadata", input_bytes=64)
    row = session.conn.execute(
        "SELECT rowid FROM alert_legacy_reports WHERE position IS NULL "
        "ORDER BY watch_id,observed_ordinal,report_id LIMIT 1"
    ).fetchone()
    if row is None:
        return True
    budget.admit("legacy explicit position candidate", source_rows=1)
    data = _read_columns(
        session,
        budget,
        "alert_legacy_reports",
        int(row[0]),
        ("report_id", "watch_id", "observed_ordinal"),
        "legacy order record",
    )
    _validate_epoch(session, budget, epoch)
    _anchor(session, budget, data["report_id"], data["observed_ordinal"])
    with session.fence():
        _validate_epoch(session, budget, epoch)
        _anchor(session, budget, data["report_id"], data["observed_ordinal"])
        current = session.conn.execute(
            "SELECT position FROM alert_legacy_reports WHERE report_id=?",
            (data["report_id"],),
        ).fetchone()
        if current is None:
            raise _refuse("legacy_source_missing", report_id=data["report_id"])
        if current[0] is not None:
            return False
        head_row = session.conn.execute(
            "SELECT rowid FROM alert_legacy_heads WHERE watch_id=?",
            (data["watch_id"],),
        ).fetchone()
        predecessor = None
        if head_row is not None:
            head = _read_columns(
                session,
                budget,
                "alert_legacy_heads",
                int(head_row[0]),
                ("report_id", "observed_ordinal"),
                "legacy order predecessor",
            )
            predecessor = head["report_id"]
            _anchor(session, budget, predecessor, head["observed_ordinal"])
            if head["observed_ordinal"] >= data["observed_ordinal"]:
                raise _refuse("legacy_order_ambiguous", report_id=data["report_id"])
        result = session.conn.execute(
            "INSERT INTO alert_legacy_positions(report_id,watch_id,predecessor_report_id) VALUES(?,?,?)",
            (data["report_id"], data["watch_id"], predecessor),
        )
        session.conn.execute(
            "UPDATE alert_legacy_reports SET position=?,predecessor_report_id=? WHERE report_id=?",
            (result.lastrowid, predecessor, data["report_id"]),
        )
        session.conn.execute(
            "INSERT INTO alert_legacy_heads(watch_id,report_id,observed_ordinal,position) "
            "VALUES(?,?,?,?) ON CONFLICT(watch_id) DO UPDATE SET "
            "report_id=excluded.report_id,observed_ordinal=excluded.observed_ordinal,position=excluded.position",
            (data["watch_id"], data["report_id"], data["observed_ordinal"], result.lastrowid),
        )
        _validate_epoch(session, budget, epoch)
    return False


def _order_legacy(
    session: AlertSession,
    budget: AlertBudget,
    epoch: tuple[int, int, int],
) -> None:
    while True:
        with budget.unit("legacy explicit position"):
            complete = _order_legacy_one(session, budget, epoch)
        if complete:
            return


def bootstrap_legacy(session: AlertSession, budget: AlertBudget) -> bool:
    """Admit reports/completions and their order before unrelated source payloads."""
    epoch = _ensure_epoch(session, budget)
    for spec in SOURCE_SPECS[:2]:
        while True:
            with budget.unit("legacy bootstrap " + spec.name):
                complete = _bootstrap_one(session, budget, spec, epoch)
            if complete:
                break
    _order_legacy(session, budget, epoch)
    with session.fence():
        _validate_epoch(session, budget, epoch)
        session.conn.execute("UPDATE alert_source_epoch SET legacy_complete=1 WHERE singleton=1")
    return True


def bootstrap(session: AlertSession, budget: AlertBudget) -> bool:
    """Finish all finite frozen PK keysets, or raise an honest admission boundary."""
    bootstrap_legacy(session, budget)
    epoch = _ensure_epoch(session, budget)
    for spec in SOURCE_SPECS[2:]:
        while True:
            with budget.unit("source bootstrap " + spec.name):
                complete = _bootstrap_one(session, budget, spec, epoch)
            if complete:
                break
    with session.fence():
        _validate_epoch(session, budget, epoch)
        session.conn.execute("UPDATE alert_source_epoch SET complete=1 WHERE singleton=1")
    return True


def source_highwater(session: AlertSession) -> int:
    session.check()
    complete = session.conn.execute(
        "SELECT complete FROM alert_source_epoch WHERE singleton=1"
    ).fetchone()
    if complete is None or not complete[0]:
        raise _refuse("source_bootstrap_incomplete")
    row = session.conn.execute(
        "SELECT sequence FROM alert_source_versions ORDER BY sequence DESC LIMIT 1"
    ).fetchone()
    return 0 if row is None else int(row[0])


def _change(session: AlertSession, budget: AlertBudget, sequence: int) -> SourceChange:
    raw = _read_columns(
        session,
        budget,
        "alert_source_versions",
        sequence,
        ("kind", "source_key", "watch_id", "data_json"),
        "source version",
        decode=True,
    )
    payload = raw["data_json"]
    if not isinstance(payload, str):
        raise _refuse("source_version_invalid", sequence=sequence)
    try:
        data = json.loads(payload)
    except (ValueError, TypeError) as error:
        raise _refuse("source_version_invalid", sequence=sequence) from error
    if not isinstance(data, dict) or raw["kind"] not in SPEC_BY_KIND:
        raise _refuse("source_version_invalid", sequence=sequence)
    return SourceChange(
        sequence,
        str(raw["kind"]),
        str(raw["source_key"]),
        None if raw["watch_id"] is None else str(raw["watch_id"]),
        data,
    )


def next_change(
    session: AlertSession,
    after: int,
    through: int,
    budget: AlertBudget,
) -> SourceChange | None:
    if after < 0 or through < 0:
        raise ValueError("negative alert source sequence")
    if after >= through:
        return None
    with budget.unit("next source candidate"):
        budget.admit("next source metadata", input_bytes=32)
        row = session.conn.execute(
            "SELECT sequence FROM alert_source_versions WHERE sequence>? AND sequence<=? "
            "ORDER BY sequence LIMIT 1",
            (after, through),
        ).fetchone()
        if row is None:
            return None
        budget.admit("next source candidate", source_rows=1)
        return _change(session, budget, int(row[0]))


def get_version(
    session: AlertSession,
    kind: str,
    key: str,
    through: int,
    budget: AlertBudget,
) -> dict[str, Any] | None:
    if kind not in SPEC_BY_KIND or through < 0:
        raise ValueError("invalid alert source version lookup")
    with budget.unit("source version point " + kind):
        budget.admit("source version point metadata " + kind, input_bytes=32)
        row = session.conn.execute(
            "SELECT sequence FROM alert_source_versions WHERE kind=? AND source_key=? "
            "AND sequence<=? ORDER BY sequence DESC LIMIT 1",
            (kind, key, through),
        ).fetchone()
        if row is None:
            return None
        budget.admit("source version point " + kind, source_rows=1)
        return _change(session, budget, int(row[0])).data


def latest_change(
    session: AlertSession,
    kind: str,
    budget: AlertBudget,
    through: int | None = None,
) -> SourceChange | None:
    if kind not in SPEC_BY_KIND or (through is not None and through < 0):
        raise ValueError("invalid alert source kind or highwater")
    with budget.unit("latest source candidate " + kind):
        budget.admit("latest source metadata " + kind, input_bytes=32)
        if through is None:
            row = session.conn.execute(
                "SELECT sequence FROM alert_source_latest WHERE watch_id=? AND kind=? "
                "ORDER BY sequence DESC LIMIT 1",
                (session.watch_id, kind),
            ).fetchone()
        else:
            row = session.conn.execute(
                "SELECT sequence FROM alert_source_latest WHERE watch_id=? AND kind=? "
                "AND sequence<=? ORDER BY sequence DESC LIMIT 1",
                (session.watch_id, kind, through),
            ).fetchone()
        if row is None:
            return None
        budget.admit("latest source candidate " + kind, source_rows=1)
        return _change(session, budget, int(row[0]))


def legacy_report(
    session: AlertSession,
    budget: AlertBudget,
    *,
    pending_only: bool = True,
    latest: bool = False,
) -> SourceChange | None:
    row = session.conn.execute(
        "SELECT legacy_complete FROM alert_source_epoch WHERE singleton=1"
    ).fetchone()
    if row is None or not row[0]:
        raise _refuse("legacy_bootstrap_incomplete")
    epoch = _ensure_epoch(session, budget)
    _order_legacy(session, budget, epoch)
    with budget.unit("legacy selected report"):
        budget.admit("legacy report metadata", input_bytes=64)
        direction = "DESC" if latest else "ASC"
        completed = " AND completed=0" if pending_only else ""
        row = session.conn.execute(
            "SELECT rowid FROM alert_legacy_reports WHERE watch_id=?"
            + completed
            + f" ORDER BY observed_ordinal {direction},report_id {direction} LIMIT 1",
            (session.watch_id,),
        ).fetchone()
        if row is None:
            return None
        budget.admit("legacy report candidate", source_rows=1)
        metadata = _read_columns(
            session,
            budget,
            "alert_legacy_reports",
            int(row[0]),
            ("report_id", "observed_ordinal", "completed", "predecessor_report_id"),
            "legacy selected report",
            encode=True,
        )
        _anchor(session, budget, metadata["report_id"], metadata["observed_ordinal"])
        key = _encode([metadata["report_id"]])
        source = session.conn.execute(
            "SELECT sequence FROM alert_source_heads WHERE kind='watch_alert_reports' AND source_key=?",
            (key,),
        ).fetchone()
        if source is None:
            raise _refuse("legacy_source_missing", report_id=metadata["report_id"])
        change = _change(session, budget, int(source[0]))
        change.data["_legacy_completed"] = bool(metadata["completed"])
        change.data["_legacy_predecessor_report_id"] = metadata["predecessor_report_id"]
        _validate_epoch(session, budget, epoch)
        return change
