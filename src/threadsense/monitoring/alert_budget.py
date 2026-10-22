"""Invocation admission for cumulative alert producers and readers."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from threadsense.config import AlertsConfig
from threadsense.errors import MonitoringStateError

_DEFAULT_ALERTS = AlertsConfig()


class AdmissionStop(Exception):
    """A recoverable boundary, never a successful empty comparison."""

    def __init__(
        self, status: str, unit: str, dimension: str, required: int, configured: int
    ) -> None:
        self.details: dict[str, str | int] = {
            "status": status,
            "unit": unit,
            "reason": dimension,
            "required": required,
            "configured": configured,
        }
        super().__init__(f"{status}: {unit}: {dimension}")


@dataclass
class AlertBudget:
    source_rows: int = _DEFAULT_ALERTS.source_rows
    derivations: int = _DEFAULT_ALERTS.derivations
    input_bytes: int = _DEFAULT_ALERTS.input_bytes
    artifact_bytes: int = _DEFAULT_ALERTS.artifact_bytes
    response_bytes: int = _DEFAULT_ALERTS.response_bytes
    used: dict[str, int] = field(
        default_factory=lambda: {
            "source_rows": 0,
            "derivations": 0,
            "input_bytes": 0,
            "artifact_bytes": 0,
            "response_bytes": 0,
        }
    )
    check: Callable[[], None] = field(default=lambda: None, repr=False)

    def __post_init__(self) -> None:
        for name in self.used:
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a strictly positive integer")

    def admit(self, unit: str, **costs: int) -> None:
        self.check()
        self._charge(unit, **costs)

    def _charge(self, unit: str, **costs: int) -> None:
        """Charge a raw fixed guard probe without recursively invoking the guard."""
        for dimension, required in costs.items():
            configured = int(getattr(self, dimension))
            if required < 0:
                raise ValueError("negative alert admission")
            if required > configured:
                raise AdmissionStop("resource_refused", unit, dimension, required, configured)
            if self.used[dimension] + required > configured:
                raise AdmissionStop("backlog", unit, dimension, required, configured)
        for dimension, required in costs.items():
            self.used[dimension] += required

    @contextmanager
    def unit(self, identity: str) -> Iterator[None]:
        """Distinguish a whole-unit excess from this invocation's remainder."""
        start = dict(self.used)
        try:
            yield
        except AdmissionStop as stop:
            dimension = str(stop.details["reason"])
            required = self.used[dimension] - start[dimension] + int(stop.details["required"])
            configured = int(getattr(self, dimension))
            if required > configured:
                raise AdmissionStop(
                    "resource_refused", identity, dimension, required, configured
                ) from stop
            raise

    def remaining(self, dimension: str) -> int:
        return int(getattr(self, dimension)) - self.used[dimension]

    def accounting(self) -> dict[str, Any]:
        return {
            "used": dict(self.used),
            "limits": {key: int(getattr(self, key)) for key in self.used},
        }


@dataclass
class AlertSession:
    conn: sqlite3.Connection
    root: Path
    watch_id: str
    transaction_fence: Callable[[], AbstractContextManager[None]]
    check_claim: Callable[[], None]
    expected_epoch: tuple[int, int, int] | None = None
    epoch_budget: AlertBudget | None = field(default=None, repr=False)
    exit_probe_allowance: int = 0
    exiting: bool = False

    def physical_epoch(self) -> tuple[int, int, int]:
        """Raw physical probe: never calls check/admission or opens another session."""
        if self.exiting and self.exit_probe_allowance:
            self.exit_probe_allowance -= 1
        elif self.epoch_budget is not None:
            self.epoch_budget._charge("source epoch guard", input_bytes=256)
        main = next(
            (row for row in self.conn.execute("PRAGMA database_list") if row[1] == "main"), None
        )
        if main is None or not main[2]:
            raise MonitoringStateError(
                "alert source requires a file", details={"reason": "source_epoch_requires_file"}
            )
        try:
            stat = Path(main[2]).stat()
        except OSError as error:
            raise MonitoringStateError(
                "alert source epoch unreadable", details={"reason": "source_epoch_unreadable"}
            ) from error
        cookie = int(self.conn.execute("PRAGMA main.schema_version").fetchone()[0])
        return stat.st_dev, stat.st_ino, cookie

    def bind_epoch(self, budget: AlertBudget) -> tuple[int, int, int]:
        if self.epoch_budget is None:
            budget._charge("source epoch exit probes", input_bytes=512)
            self.exit_probe_allowance = 2
        self.epoch_budget = budget
        observed = self.physical_epoch()
        if self.expected_epoch is None:
            self.expected_epoch = observed
        elif observed != self.expected_epoch:
            raise MonitoringStateError(
                "alert source epoch changed", details={"reason": "source_epoch_changed"}
            )
        return self.expected_epoch

    def check(self) -> None:
        self.check_claim()
        if self.expected_epoch is not None and self.physical_epoch() != self.expected_epoch:
            raise MonitoringStateError(
                "alert source epoch changed", details={"reason": "source_epoch_changed"}
            )

    @contextmanager
    def fence(self) -> Iterator[None]:
        self.check()
        with self.transaction_fence():
            self.check()
            yield
            self.check()


@dataclass(frozen=True)
class ArtifactReference:
    path: str
    sha256: str
    generation: int
    watch_id: str
    chain: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "sha256": self.sha256,
            "generation": self.generation,
            "watch_id": self.watch_id,
            "chain": self.chain,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ArtifactReference:
        return cls(
            str(value["path"]),
            str(value["sha256"]),
            int(value["generation"]),
            str(value["watch_id"]),
            str(value["chain"]),
        )


def json_bound(value: Any, depth: int = 0) -> int:
    """Conservative ASCII JSON/indent bound before escaping or encoding strings."""
    if isinstance(value, str):
        return 6 * len(value) + 2
    if isinstance(value, dict):
        return 2 + sum(
            json_bound(key, depth + 1) + json_bound(item, depth + 1) + 8 + 4 * depth
            for key, item in value.items()
        )
    if isinstance(value, (tuple, list)):
        return 2 + sum(json_bound(item, depth + 1) + 4 + 2 * depth for item in value)
    return 32


def encode_control(value: Any, budget: AlertBudget, unit: str, passes: int = 1) -> str:
    budget.admit(unit, input_bytes=json_bound(value) * passes)
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )


def text_size(conn: sqlite3.Connection, table: str, column: str, rowid: int) -> int:
    """Native readonly TEXT byte preflight; rowid is a point handle, never a cursor."""
    with conn.blobopen(table, column, rowid, readonly=True) as blob:
        return len(blob)


def read_json_text(
    conn: sqlite3.Connection, table: str, column: str, rowid: int, budget: AlertBudget, unit: str
) -> Any:
    size = text_size(conn, table, column, rowid)
    budget.admit(unit, input_bytes=size * 2)
    # Identifiers are private constant table/column names, never user arguments.
    row = conn.execute(f'SELECT "{column}" FROM "{table}" WHERE rowid=?', (rowid,)).fetchone()
    if row is None:
        raise MonitoringStateError("alert input disappeared", details={"reason": "input_missing"})
    try:
        return json.loads(row[0])
    except (ValueError, TypeError) as error:
        raise MonitoringStateError(
            "alert input is invalid", details={"reason": "input_unreadable"}
        ) from error


def verify_file(session: AlertSession, budget: AlertBudget, relative: str, expected: str) -> None:
    path = session.root / relative
    try:
        if not path.resolve().is_relative_to(session.root.resolve()):
            raise MonitoringStateError(
                "alert artifact escapes storage", details={"reason": "artifact_outside_state"}
            )
        size = path.stat().st_size
        budget.admit("verify:" + relative, input_bytes=2 * size)
        data = path.read_bytes()
        if len(data) != size or hashlib.sha256(data).hexdigest() != expected:
            raise MonitoringStateError(
                "alert artifact differs", details={"reason": "artifact_hash_mismatch"}
            )
    except OSError as error:
        raise MonitoringStateError(
            "alert artifact cannot be read", details={"reason": "artifact_unreadable"}
        ) from error
